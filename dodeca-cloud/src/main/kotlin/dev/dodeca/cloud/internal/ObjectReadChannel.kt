package dev.dodeca.cloud.internal

import java.io.IOException
import java.io.OutputStream
import java.io.UncheckedIOException
import java.nio.ByteBuffer
import java.util.OptionalLong
import java.util.concurrent.CompletableFuture
import org.neo4j.cloud.storage.StoragePath
import org.neo4j.cloud.storage.io.ReadableChannel
import org.neo4j.cloud.storage.queues.PullQueue
import org.neo4j.cloud.storage.queues.PushQueue
import org.neo4j.cloud.storage.queues.RequestQueueConfigs.QueueConfig
import org.neo4j.internal.helpers.progress.ProgressListener
import org.neo4j.logging.InternalLog

/**
 * Reads one version of an object, the one [info] describes, in ranges: reading ahead a few ranges
 * at once, and a new range when a seek leaves the ones fetched. Copied whole, as by transferTo, it
 * fetches more ranges at once; copied to an object of the same cloud, the cloud copies it.
 */
internal class ObjectReadChannel
private constructor(
    private val store: ObjectStore,
    private val container: String,
    private val key: String,
    private val info: ObjectInfo,
    private val queues: QueueConfigs,
    log: InternalLog,
    description: String,
) :
    ReadableChannel(
        Ranges(store, container, key, info, queues.pull),
        info.size,
        queues.pull.chunkSize(),
        "Downloading $description",
        log,
    ) {
    constructor(
        store: ObjectStore,
        container: String,
        key: String,
        info: ObjectInfo,
        path: StoragePath,
        description: String,
        log: InternalLog,
    ) : this(store, container, key, info, QueueConfigs.of(path), log, description)

    override fun replicateWithinSameProvider(out: OutputStream): OptionalLong {
        if (out !is ObjectWriteChannel || !out.copyFrom(store, container, key, position)) {
            return OptionalLong.empty()
        }
        val copied = channelSize - position
        position = channelSize
        return OptionalLong.of(copied)
    }

    override fun newPushQueue(handler: ByteBufferHandler, progress: ProgressListener): PushQueue =
        object : PushQueue(queues.push, info.size, position) {
            override fun get(from: Long, to: Long) = fetch(store, container, key, info, from, to)

            override fun onData(data: ByteBuffer) {
                progress.add(handler.apply(data))
            }

            override fun onError(ex: Exception) {
                throw if (ex is IOException) UncheckedIOException(ex)
                else UncheckedIOException(IOException(ex))
            }
        }

    private class Ranges(
        private val store: ObjectStore,
        private val container: String,
        private val key: String,
        private val info: ObjectInfo,
        config: QueueConfig,
    ) : PullQueue(config, info.size) {
        override fun get(from: Long, to: Long) = fetch(store, container, key, info, from, to)
    }

    companion object {
        private fun fetch(
            store: ObjectStore,
            container: String,
            key: String,
            info: ObjectInfo,
            from: Long,
            to: Long,
        ): CompletableFuture<ByteBuffer> =
            CompletableFuture.supplyAsync(
                {
                    try {
                        store.read(container, key, info.version, from, to)
                    } catch (e: IOException) {
                        throw UncheckedIOException(e)
                    }
                },
                Transfers.executor,
            )
    }
}
