package dev.dodeca.cloud.azure

import com.azure.core.exception.ClientAuthenticationException
import com.azure.core.util.BinaryData
import com.azure.core.util.Context
import com.azure.identity.DefaultAzureCredentialBuilder
import com.azure.storage.blob.BlobClient
import com.azure.storage.blob.BlobServiceClient
import com.azure.storage.blob.BlobServiceClientBuilder
import com.azure.storage.blob.models.BlobRange
import com.azure.storage.blob.models.BlobRequestConditions
import com.azure.storage.blob.models.BlobStorageException
import com.azure.storage.blob.models.DownloadRetryOptions
import com.azure.storage.blob.models.ListBlobsOptions
import com.microsoft.aad.msal4j.MsalException
import dev.dodeca.cloud.CloudStorageSettings
import dev.dodeca.cloud.internal.Listed
import dev.dodeca.cloud.internal.Location
import dev.dodeca.cloud.internal.ObjectInfo
import dev.dodeca.cloud.internal.ObjectStorageProvider
import dev.dodeca.cloud.internal.ObjectStore
import dev.dodeca.cloud.internal.Upload
import dev.dodeca.cloud.internal.growingPart
import dev.dodeca.cloud.internal.pagedListing
import java.io.IOException
import java.io.OutputStream
import java.io.UncheckedIOException
import java.net.URI
import java.nio.ByteBuffer
import java.nio.file.AccessDeniedException
import java.nio.file.NoSuchFileException
import java.time.Duration
import java.util.Base64
import java.util.concurrent.ConcurrentHashMap
import org.neo4j.cloud.storage.StorageSystemProviderFactory.ChunkChannelSupplier
import org.neo4j.configuration.Config
import org.neo4j.logging.InternalLogProvider
import org.neo4j.memory.MemoryTracker

/** `azb://` through Azure's SDK, created by name by [dev.dodeca.cloud.AzureStorageFactory]. */
internal class AzureStorageProvider(
    chunks: ChunkChannelSupplier,
    config: Config,
    logs: InternalLogProvider,
    memory: MemoryTracker,
) : ObjectStorageProvider(AzureStore.shared(config), chunks, config, logs, memory)

/**
 * Azure Blob Storage, as block blobs, with the default Azure credentials: the environment, workload
 * and managed identities, or the Azure CLI's sign-in. `AZURE_STORAGE_CONNECTION_STRING`, when set,
 * is used instead, for its storage account only; it also reaches the Azurite emulator. Containers
 * are named `<storage account>/<container>`.
 */
internal class AzureStore(
    private val endpointSuffix: String,
    private val authority: String,
    private val connectionString: String?,
    private val pageSize: Int = 1000,
) : ObjectStore {
    override val scheme = "azb"
    override val uploadConcurrency = 4

    private val accounts = ConcurrentHashMap<String, BlobServiceClient>()
    private val credential by lazy {
        DefaultAzureCredentialBuilder()
            .apply { if (authority.isNotEmpty()) authorityHost(authority) }
            .build()
    }

    override fun locate(uri: URI) = Location.accountContainer(uri)

    override fun head(container: String, key: String): ObjectInfo? =
        call(container, key) {
            try {
                val properties = blob(container, key).properties
                ObjectInfo(
                    properties.blobSize,
                    properties.lastModified.toInstant(),
                    properties.eTag,
                )
            } catch (e: BlobStorageException) {
                if (e.statusCode == 404) null else throw e
            }
        }

    override fun read(
        container: String,
        key: String,
        version: String?,
        from: Long,
        to: Long,
    ): ByteBuffer =
        call(container, key) {
            val data = ByteBuffer.allocate(Math.toIntExact(to - from))
            blob(container, key)
                .downloadStreamWithResponse(
                    BufferOutput(data),
                    BlobRange(from, to - from),
                    DownloadRetryOptions().setMaxRetryRequests(RETRIES),
                    BlobRequestConditions().setIfMatch(version),
                    false,
                    null,
                    Context.NONE,
                )
            if (data.hasRemaining()) {
                throw IOException(
                    "azb://$container/$key: asked for bytes $from to $to, got ${data.position()}"
                )
            }
            data.flip()
        }

    override fun list(container: String, prefix: String): Iterator<Listed> {
        val pages =
            call(container, prefix) {
                containerClient(container)
                    .listBlobsByHierarchy(
                        "/",
                        ListBlobsOptions().setPrefix(prefix).setMaxResultsPerPage(pageSize),
                        null,
                    )
                    .iterableByPage()
                    .iterator()
            }
        return pagedListing(
            generateSequence { if (guarded(container, prefix) { pages.hasNext() }) pages else null }
                .map { iterator ->
                    guarded(container, prefix) { iterator.next() }
                        .value
                        .map { item ->
                            if (item.isPrefix == true) Listed.Folder(item.name)
                            else
                                Listed.Object(
                                    item.name,
                                    ObjectInfo(
                                        item.properties.contentLength ?: 0,
                                        item.properties.lastModified.toInstant(),
                                        item.properties.eTag,
                                    ),
                                )
                        }
                }
        )
    }

    override fun delete(container: String, key: String) {
        call(container, key) { blob(container, key).deleteIfExists() }
    }

    override fun copy(fromContainer: String, fromKey: String, toContainer: String, toKey: String) {
        if (fromContainer.substringBefore('/') != toContainer.substringBefore('/')) {
            throw IOException(
                "azb://$toContainer/$toKey: Azure copies objects within a storage account only, " +
                    "not from azb://$fromContainer/$fromKey"
            )
        }
        call(toContainer, toKey) {
            blob(toContainer, toKey)
                .beginCopy(blob(fromContainer, fromKey).blobUrl, COPY_POLL)
                .waitForCompletion()
        }
    }

    override fun put(container: String, key: String, data: ByteBuffer) {
        call(container, key) {
            blob(container, key).blockBlobClient.upload(BinaryData.fromByteBuffer(data), true)
        }
    }

    override fun upload(container: String, key: String): Upload {
        val blocks = blob(container, key).blockBlobClient
        return object : Upload {
            override fun part(index: Int, data: ByteBuffer) {
                call(container, key) {
                    blocks.stageBlock(block(index), BinaryData.fromByteBuffer(data))
                }
            }

            override fun complete(parts: Int) {
                call(container, key) { blocks.commitBlockList((0 until parts).map(::block), true) }
            }

            /** Nothing: Azure drops blocks never committed within a week. */
            override fun abort() {}
        }
    }

    /**
     * Blocks of 8 MiB, twice as big every 5000 blocks, up to 1 GiB: a blob has up to 50,000 blocks,
     * which makes blobs of up to 19 TiB (Azure allows 190 TiB).
     */
    override fun partSize(index: Int) = growingPart(index, 5000)

    private fun block(index: Int) =
        Base64.getEncoder().encodeToString("%010d".format(index).toByteArray())

    private fun blob(container: String, key: String): BlobClient =
        containerClient(container).getBlobClient(key)

    private fun containerClient(container: String) =
        account(container.substringBefore('/'))
            .getBlobContainerClient(container.substringAfter('/'))

    private fun account(name: String): BlobServiceClient =
        accounts.computeIfAbsent(name) {
            val builder = BlobServiceClientBuilder()
            if (connectionString != null) {
                val named =
                    connectionString
                        .split(';')
                        .firstOrNull { it.startsWith("AccountName=") }
                        ?.substringAfter('=')
                if (named != name) {
                    throw IllegalArgumentException(
                        "AZURE_STORAGE_CONNECTION_STRING is for the storage account $named, not $name"
                    )
                }
                builder.connectionString(connectionString)
            } else {
                builder.endpoint("https://$name.$endpointSuffix").credential(credential)
            }
            builder.buildClient()
        }

    /** Runs a request, its failure as the IOException the provider expects. */
    private inline fun <T> call(container: String, key: String, request: () -> T): T =
        try {
            request()
        } catch (e: RuntimeException) {
            throw failure(container, key, e)
        }

    /** As [call], for iterators, whose failures must be unchecked. */
    private inline fun <T> guarded(container: String, key: String, request: () -> T): T =
        try {
            request()
        } catch (e: RuntimeException) {
            throw UncheckedIOException(failure(container, key, e))
        }

    private fun failure(container: String, key: String, e: RuntimeException): IOException {
        val uri = "azb://$container/$key"
        val failure =
            when (e) {
                is BlobStorageException -> {
                    val reason = reason(e)
                    when (e.statusCode) {
                        404 -> NoSuchFileException(uri, null, reason)
                        401,
                        403 -> AccessDeniedException(uri, null, reason)
                        412 -> IOException("$uri changed while it was read: $reason")
                        else -> IOException("$uri: $reason")
                    }
                }
                is UncheckedIOException -> e.cause ?: IOException("$uri: ${e.message}")
                else -> {
                    // Signing in fails with the identity library's exceptions, often wrapped:
                    // the innermost says why.
                    val causes = generateSequence<Throwable>(e) { it.cause }
                    val innermost = causes.last()
                    val message = (innermost.message ?: innermost.toString()).lines().first()
                    if (causes.any { it is ClientAuthenticationException || it is MsalException }) {
                        AccessDeniedException(uri, null, "signing in to Azure failed: $message")
                    } else {
                        IOException("$uri: $message")
                    }
                }
            }
        return failure.apply { if (cause == null) initCause(e) }
    }

    /** What Azure said: the message in its XML answer, if any, and its error code. */
    private fun reason(e: BlobStorageException): String {
        val message =
            MESSAGE.find(e.serviceMessage ?: e.message.orEmpty())?.groupValues?.get(1)?.trim()
        val code = e.errorCode?.toString() ?: "status ${e.statusCode}"
        return if (message.isNullOrEmpty()) code else "$message ($code)"
    }

    /** Writes a download into a buffer of the range's size. */
    private class BufferOutput(private val data: ByteBuffer) : OutputStream() {
        override fun write(b: Int) {
            data.put(b.toByte())
        }

        override fun write(bytes: ByteArray, offset: Int, length: Int) {
            data.put(bytes, offset, length)
        }
    }

    companion object {
        private const val RETRIES = 3
        private val COPY_POLL = Duration.ofSeconds(1)
        private val MESSAGE = Regex("<Message>([^<\\n]*)")

        private val stores = ConcurrentHashMap<List<String?>, AzureStore>()

        /** One store per settings for the process: each keeps its connections and credentials. */
        fun shared(config: Config): AzureStore {
            val suffix = config.get(CloudStorageSettings.azbBlobEndpointSuffix)
            val authority = config.get(CloudStorageSettings.azbAuthorityEndpoint)
            val connectionString =
                System.getenv("AZURE_STORAGE_CONNECTION_STRING")?.takeIf { it.isNotEmpty() }
            return stores.computeIfAbsent(listOf(suffix, authority, connectionString)) {
                AzureStore(suffix, authority, connectionString)
            }
        }
    }
}
