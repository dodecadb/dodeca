package org.dozendb.cloud.gcs

import com.google.cloud.BaseServiceException
import com.google.cloud.NoCredentials
import com.google.cloud.WriteChannel
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.Storage.BlobListOption
import com.google.cloud.storage.StorageOptions
import java.io.IOException
import java.io.UncheckedIOException
import java.net.URI
import java.nio.ByteBuffer
import java.nio.file.AccessDeniedException
import java.nio.file.NoSuchFileException
import java.time.Instant
import org.dozendb.cloud.internal.Listed
import org.dozendb.cloud.internal.Location
import org.dozendb.cloud.internal.ObjectInfo
import org.dozendb.cloud.internal.ObjectStorageProvider
import org.dozendb.cloud.internal.ObjectStore
import org.dozendb.cloud.internal.Upload
import org.neo4j.cloud.storage.StorageSystemProviderFactory.ChunkChannelSupplier
import org.neo4j.configuration.Config
import org.neo4j.logging.InternalLogProvider
import org.neo4j.memory.MemoryTracker

/** `gs://` through Google's SDK, created by name by [org.dozendb.cloud.GcsStorageFactory]. */
internal class GcsStorageProvider(
    chunks: ChunkChannelSupplier,
    config: Config,
    logs: InternalLogProvider,
    memory: MemoryTracker,
) : ObjectStorageProvider(GcsStore.shared, chunks, config, logs, memory)

/**
 * Google Cloud Storage over its JSON API, with Google's application default credentials:
 * `GOOGLE_APPLICATION_CREDENTIALS`, `gcloud auth application-default login`, or the instance's
 * service account. `STORAGE_EMULATOR_HOST` points it at an emulator, without credentials, as
 * Google's own tools do. Objects are written by resumable uploads, in order.
 */
internal class GcsStore(private val storage: Storage) : ObjectStore {
    override val scheme = "gs"
    override val uploadConcurrency = 1

    override fun locate(uri: URI) = Location.bucket(uri)

    override fun head(container: String, key: String): ObjectInfo? =
        call(container, key, null) { storage.get(BlobId.of(container, key))?.let(::info) }

    override fun read(
        container: String,
        key: String,
        version: String?,
        from: Long,
        to: Long,
    ): ByteBuffer =
        call(container, key, version) {
            storage.reader(BlobId.of(container, key, version?.toLong())).use { reader ->
                reader.seek(from)
                reader.limit(to)
                val data = ByteBuffer.allocate(Math.toIntExact(to - from))
                while (data.hasRemaining() && reader.read(data) >= 0) {
                    // Until the range is read.
                }
                if (data.hasRemaining()) {
                    throw IOException(
                        "gs://$container/$key: asked for bytes $from to $to, got ${data.position()}"
                    )
                }
                data.flip()
            }
        }

    override fun list(container: String, prefix: String): Iterator<Listed> {
        val blobs =
            call(container, prefix, null) {
                storage
                    .list(
                        container,
                        BlobListOption.prefix(prefix),
                        BlobListOption.currentDirectory(),
                    )
                    .iterateAll()
                    .iterator()
            }
        return object : Iterator<Listed> {
            override fun hasNext() = guarded(container, prefix) { blobs.hasNext() }

            override fun next(): Listed {
                val blob = guarded(container, prefix) { blobs.next() }
                return if (blob.isDirectory) Listed.Folder(blob.name)
                else Listed.Object(blob.name, info(blob))
            }
        }
    }

    override fun delete(container: String, key: String) {
        call(container, key, null) { storage.delete(BlobId.of(container, key)) }
    }

    override fun copy(fromContainer: String, fromKey: String, toContainer: String, toKey: String) {
        call(toContainer, toKey, null) {
            storage
                .copy(
                    Storage.CopyRequest.of(
                        BlobId.of(fromContainer, fromKey),
                        BlobId.of(toContainer, toKey),
                    )
                )
                .result
        }
    }

    override fun put(container: String, key: String, data: ByteBuffer) {
        val bytes = ByteArray(data.remaining()).also { data.get(it) }
        call(container, key, null) {
            storage.create(BlobInfo.newBuilder(container, key).build(), bytes)
        }
    }

    override fun upload(container: String, key: String): Upload {
        val writer: WriteChannel =
            call(container, key, null) {
                storage.writer(BlobInfo.newBuilder(container, key).build()).apply {
                    setChunkSize(PART)
                }
            }
        return object : Upload {
            override fun part(index: Int, data: ByteBuffer) {
                call(container, key, null) {
                    while (data.hasRemaining()) writer.write(data)
                }
            }

            override fun complete(parts: Int) {
                call(container, key, null) { writer.close() }
            }

            /**
             * Nothing: an upload never finished leaves no object, and Google drops it in a week.
             */
            override fun abort() {}
        }
    }

    /**
     * Parts of 16 MiB, a multiple of the 256 KiB resumable uploads take; their number has no limit.
     */
    override fun partSize(index: Int) = PART

    private fun info(blob: com.google.cloud.storage.Blob) =
        ObjectInfo(
            blob.size ?: 0,
            blob.updateTimeOffsetDateTime?.toInstant() ?: Instant.EPOCH,
            blob.generation?.toString(),
        )

    /** Runs a request, its failure as the IOException the provider expects. */
    private inline fun <T> call(
        container: String,
        key: String,
        version: String?,
        request: () -> T,
    ): T =
        try {
            request()
        } catch (e: BaseServiceException) {
            throw failure(container, key, version, e)
        } catch (e: UncheckedIOException) {
            throw e.cause ?: e
        }

    /** As [call], for iterators, whose failures must be unchecked. */
    private inline fun <T> guarded(container: String, key: String, request: () -> T): T =
        try {
            request()
        } catch (e: BaseServiceException) {
            throw UncheckedIOException(failure(container, key, null, e))
        }

    private fun failure(
        container: String,
        key: String,
        version: String?,
        e: BaseServiceException,
    ): IOException {
        val uri = "gs://$container/$key"
        val reason = listOfNotNull(e.message, e.reason?.let { "($it)" }).joinToString(" ")
        return when (e.code) {
            404 ->
                if (version != null) IOException("$uri changed while it was read: $reason")
                else NoSuchFileException(uri, null, reason)
            401,
            403 -> AccessDeniedException(uri, null, reason)
            412 -> IOException("$uri changed while it was read: $reason")
            else -> IOException("$uri: $reason")
        }.apply { initCause(e) }
    }

    companion object {
        private const val PART = 16 shl 20

        /** One client for the process: it keeps its connections and credentials. */
        val shared: GcsStore by lazy { create() }

        private fun create(): GcsStore {
            val options = StorageOptions.http()
            System.getenv("STORAGE_EMULATOR_HOST")
                ?.takeIf { it.isNotEmpty() }
                ?.let { host ->
                    options
                        .setHost(if ("://" in host) host else "http://$host")
                        .setCredentials(NoCredentials.getInstance())
                        .setProjectId(System.getenv("GOOGLE_CLOUD_PROJECT") ?: "dozendb")
                }
            return GcsStore(options.build().service)
        }
    }
}
