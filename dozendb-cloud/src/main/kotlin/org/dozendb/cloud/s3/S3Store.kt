package org.dozendb.cloud.s3

import java.io.IOException
import java.io.UncheckedIOException
import java.net.URI
import java.nio.ByteBuffer
import java.nio.file.AccessDeniedException
import java.nio.file.NoSuchFileException
import java.util.concurrent.ConcurrentHashMap
import org.dozendb.cloud.internal.ByteBufferInput
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
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation
import software.amazon.awssdk.core.exception.SdkException
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.http.apache5.Apache5HttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm
import software.amazon.awssdk.services.s3.model.CompletedPart
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.S3Exception

/** `s3://` through the AWS SDK, created by name by [org.dozendb.cloud.S3StorageFactory]. */
internal class S3StorageProvider(
    chunks: ChunkChannelSupplier,
    config: Config,
    logs: InternalLogProvider,
    memory: MemoryTracker,
) : ObjectStorageProvider(S3Store.shared, chunks, config, logs, memory)

/**
 * Amazon S3, with the AWS SDK's credentials, region and endpoint: from the environment, the system
 * properties, `~/.aws/config` and `credentials`, or the instance's role. Without a region the
 * client starts in us-east-1 and follows buckets to their regions. With an endpoint override, as
 * for an S3-compatible store, it addresses buckets in the path and sends checksums only where S3
 * requires them, as such stores expect.
 */
internal class S3Store(private val s3: S3Client, private val checksums: Boolean) : ObjectStore {
    override val scheme = "s3"
    override val uploadConcurrency = 4

    override fun locate(uri: URI) = Location.bucket(uri)

    override fun head(container: String, key: String): ObjectInfo? =
        call(container, key) {
            try {
                val head = s3.headObject { it.bucket(container).key(key) }
                ObjectInfo(head.contentLength(), head.lastModified(), head.eTag())
            } catch (_: NoSuchKeyException) {
                null
            } catch (e: S3Exception) {
                if (e.statusCode() == 404) null else throw e
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
            val bytes = s3.getObjectAsBytes {
                it.bucket(container).key(key).range("bytes=$from-${to - 1}")
                if (version != null) it.ifMatch(version)
            }
            val data = bytes.asByteArrayUnsafe()
            if (data.size.toLong() != to - from) {
                throw IOException(
                    "s3://$container/$key: asked for bytes $from to $to, got ${data.size}"
                )
            }
            ByteBuffer.wrap(data)
        }

    override fun list(container: String, prefix: String): Iterator<Listed> {
        val pages =
            call(container, prefix) {
                s3.listObjectsV2Paginator { it.bucket(container).prefix(prefix).delimiter("/") }
                    .iterator()
            }
        val entries = generateSequence {
            if (guarded(container, prefix) { pages.hasNext() }) pages else null
        }
            .flatMap { iterator ->
                val page = guarded(container, prefix) { iterator.next() }
                (page.contents().map {
                        Listed.Object(it.key(), ObjectInfo(it.size(), it.lastModified(), it.eTag()))
                    } + page.commonPrefixes().map { Listed.Folder(it.prefix()) })
                    .sortedBy { it.key }
            }
        return entries.iterator()
    }

    override fun delete(container: String, key: String) {
        call(container, key) { s3.deleteObject { it.bucket(container).key(key) } }
    }

    override fun copy(fromContainer: String, fromKey: String, toContainer: String, toKey: String) {
        val size =
            head(fromContainer, fromKey)?.size
                ?: throw NoSuchFileException("s3://$fromContainer/$fromKey")
        if (size <= MAX_COPY) {
            call(toContainer, toKey) {
                s3.copyObject {
                    it.sourceBucket(fromContainer)
                        .sourceKey(fromKey)
                        .destinationBucket(toContainer)
                        .destinationKey(toKey)
                }
            }
            return
        }
        // Single copies stop at 5 GiB: bigger objects copy in ranges.
        val id = start(toContainer, toKey)
        try {
            val parts =
                (0 until size step COPY_PART).mapIndexed { index, from ->
                    val to = minOf(from + COPY_PART, size) - 1
                    val copied =
                        call(toContainer, toKey) {
                            s3.uploadPartCopy {
                                it.sourceBucket(fromContainer)
                                    .sourceKey(fromKey)
                                    .destinationBucket(toContainer)
                                    .destinationKey(toKey)
                                    .uploadId(id)
                                    .partNumber(index + 1)
                                    .copySourceRange("bytes=$from-$to")
                            }
                        }
                    CompletedPart.builder()
                        .partNumber(index + 1)
                        .eTag(copied.copyPartResult().eTag())
                        .checksumCRC32(copied.copyPartResult().checksumCRC32())
                        .build()
                }
            finish(toContainer, toKey, id, parts)
        } catch (e: Exception) {
            abort(toContainer, toKey, id)
            throw e
        }
    }

    override fun put(container: String, key: String, data: ByteBuffer) {
        call(container, key) {
            s3.putObject(
                { it.bucket(container).key(key) },
                body(data),
            )
        }
    }

    override fun upload(container: String, key: String): Upload {
        val id = start(container, key)
        return object : Upload {
            private val sent = ConcurrentHashMap<Int, CompletedPart>()

            override fun part(index: Int, data: ByteBuffer) {
                val response =
                    call(container, key) {
                        s3.uploadPart(
                            {
                                it.bucket(container)
                                    .key(key)
                                    .uploadId(id)
                                    .partNumber(index + 1)
                                    .contentLength(data.remaining().toLong())
                                if (checksums) it.checksumAlgorithm(ChecksumAlgorithm.CRC32)
                            },
                            body(data),
                        )
                    }
                sent[index] =
                    CompletedPart.builder()
                        .partNumber(index + 1)
                        .eTag(response.eTag())
                        .checksumCRC32(response.checksumCRC32())
                        .build()
            }

            override fun complete(parts: Int) {
                finish(container, key, id, (0 until parts).map { sent.getValue(it) })
            }

            override fun abort() = abort(container, key, id)
        }
    }

    /**
     * Parts of 8 MiB, twice as big every 1000 parts: S3 takes 10,000 parts, which makes objects of
     * up to 8 TiB, more than the 5 TiB S3 allows.
     */
    override fun partSize(index: Int) = FIRST_PART shl (index / 1000)

    private fun start(container: String, key: String): String =
        call(container, key) {
            s3.createMultipartUpload {
                    it.bucket(container).key(key)
                    if (checksums) it.checksumAlgorithm(ChecksumAlgorithm.CRC32)
                }
                .uploadId()
        }

    private fun finish(container: String, key: String, id: String, parts: List<CompletedPart>) {
        call(container, key) {
            s3.completeMultipartUpload {
                it.bucket(container).key(key).uploadId(id).multipartUpload { upload ->
                    upload.parts(parts)
                }
            }
        }
    }

    private fun abort(container: String, key: String, id: String) {
        try {
            s3.abortMultipartUpload { it.bucket(container).key(key).uploadId(id) }
        } catch (_: Exception) {
            // The bucket's lifecycle rules, if any, remove parts left behind.
        }
    }

    /** The bytes left in [data], read anew for each attempt, without copying them. */
    private fun body(data: ByteBuffer): RequestBody =
        RequestBody.fromContentProvider(
            { ByteBufferInput(data.duplicate()) },
            data.remaining().toLong(),
            "application/octet-stream",
        )

    /** Runs a request, its failure as the IOException the provider expects. */
    private inline fun <T> call(container: String, key: String, request: () -> T): T =
        try {
            request()
        } catch (e: SdkException) {
            throw failure(container, key, e)
        }

    /** As [call], for iterators, whose failures must be unchecked. */
    private inline fun <T> guarded(container: String, key: String, request: () -> T): T =
        try {
            request()
        } catch (e: SdkException) {
            throw UncheckedIOException(failure(container, key, e))
        }

    private fun failure(container: String, key: String, e: SdkException): IOException {
        val uri = "s3://$container/$key"
        if (e !is S3Exception) return IOException("$uri: ${e.message}", e)
        val details = e.awsErrorDetails()
        val reason =
            listOfNotNull(
                    details?.errorMessage() ?: e.message,
                    details?.errorCode()?.let { "($it)" },
                )
                .joinToString(" ")
        return when (e.statusCode()) {
            404 -> NoSuchFileException(uri, null, reason)
            401,
            403 -> AccessDeniedException(uri, null, reason)
            412 -> IOException("$uri changed while it was read: $reason", e)
            else -> IOException("$uri: $reason", e)
        }.apply { if (cause == null) initCause(e) }
    }

    companion object {
        private const val FIRST_PART = 8 shl 20
        private const val MAX_COPY = 5L shl 30
        private const val COPY_PART = 512L shl 20

        /** One client for the process: it keeps its connections and credentials. */
        val shared: S3Store by lazy { create() }

        private fun create(): S3Store {
            val region =
                try {
                    DefaultAwsRegionProviderChain().region
                } catch (_: SdkException) {
                    null
                }
            val http = Apache5HttpClient.builder().build()
            fun build(overridden: Boolean): S3Client =
                S3Client.builder()
                    .httpClient(http)
                    .region(region ?: Region.US_EAST_1)
                    .crossRegionAccessEnabled(region == null && !overridden)
                    .forcePathStyle(overridden)
                    .apply {
                        if (overridden) {
                            requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
                            responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED)
                        }
                    }
                    .build()
            val probe = build(false)
            if (probe.serviceClientConfiguration().endpointOverride().isEmpty) {
                return S3Store(probe, checksums = true)
            }
            probe.close()
            return S3Store(build(true), checksums = false)
        }
    }
}
