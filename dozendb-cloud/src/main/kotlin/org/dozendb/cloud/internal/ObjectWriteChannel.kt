package org.dozendb.cloud.internal

import java.io.IOException
import java.io.InterruptedIOException
import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Semaphore
import org.neo4j.cloud.storage.io.WriteableChannel
import org.neo4j.memory.MemoryTracker

/**
 * Writes an object: an object no bigger than its first part in one request when closed, a bigger
 * one in parts sent as they fill, up to [ObjectStore.uploadConcurrency] at once. The object appears
 * when the channel closes. A part that fails gives the upload up, and the next write or the close
 * throws its error.
 */
internal class ObjectWriteChannel(
    private val store: ObjectStore,
    private val container: String,
    private val key: String,
    private val description: String,
    memory: MemoryTracker,
) : WriteableChannel(CHUNK_SIZE, memory) {
    private val permits = Semaphore(store.uploadConcurrency)
    private val sent = mutableListOf<CompletableFuture<Void>>()
    private var upload: Upload? = null
    private var part: ByteBuffer? = null
    private var parts = 0
    private var size = 0L
    private var replicated = false

    override fun internalGetSize() = size + buffer.position()

    override fun doBufferWrite(data: ByteBuffer) {
        while (data.hasRemaining()) {
            val current = part?.takeIf { it.hasRemaining() } ?: nextPart()
            val count = minOf(current.remaining(), data.remaining())
            current.put(current.position(), data, data.position(), count)
            current.position(current.position() + count)
            data.position(data.position() + count)
            size += count
        }
    }

    override fun reportChunksWritten(chunks: Long) {}

    override fun hasBeenReplicated() = replicated

    override fun completeWriteProcess() {
        if (replicated) return
        try {
            doBufferWrite(buffer.flip())
            val started = upload
            if (started == null) {
                store.put(container, key, (part ?: ByteBuffer.allocate(0)).flip())
            } else {
                part?.takeIf { it.position() > 0 }?.let { send(started, it) }
                awaitSent()
                started.complete(parts)
            }
        } catch (e: Throwable) {
            upload?.abort()
            throw if (e is InterruptedException) interrupted(e) else e
        } finally {
            part = null
        }
    }

    /**
     * Copies the object at [fromKey] of [fromStore] here within the cloud, if nothing was written
     * yet, [from] is 0 and the cloud is the same; whether it did.
     */
    fun copyFrom(
        fromStore: ObjectStore,
        fromContainer: String,
        fromKey: String,
        from: Long,
    ): Boolean {
        ensureOk()
        if (fromStore !== store || from != 0L || internalGetSize() != 0L) return false
        store.copy(fromContainer, fromKey, container, key)
        replicated = true
        return true
    }

    /** Sends the filled part, if any, and starts the next one. */
    private fun nextPart(): ByteBuffer {
        part?.let { send(upload ?: store.upload(container, key).also { upload = it }, it) }
        return ByteBuffer.allocate(store.partSize(parts)).also { part = it }
    }

    private fun send(to: Upload, data: ByteBuffer) {
        failure()?.let { throw it }
        try {
            permits.acquire()
        } catch (e: InterruptedException) {
            throw interrupted(e)
        }
        val index = parts++
        data.flip()
        sent +=
            CompletableFuture.runAsync(
                {
                    try {
                        to.part(index, data)
                    } finally {
                        permits.release()
                    }
                },
                Transfers.executor,
            )
        part = null
    }

    private fun awaitSent() {
        for (part in sent) {
            try {
                part.join()
            } catch (_: Exception) {
                // Reported below, as the first failure.
            }
        }
        failure()?.let { throw it }
    }

    /** The first failure of a part sent, as the IOException that the writer gets. */
    private fun failure(): IOException? {
        val failed = sent.firstOrNull { it.isCompletedExceptionally } ?: return null
        val cause =
            try {
                failed.join()
                null
            } catch (e: Exception) {
                generateSequence<Throwable>(e) { it.cause }.firstOrNull { it is IOException }
                    ?: e.cause
                    ?: e
            }
        return cause as? IOException ?: IOException("Writing $description failed: $cause", cause)
    }

    private fun interrupted(e: InterruptedException): InterruptedIOException {
        Thread.currentThread().interrupt()
        return InterruptedIOException("Interrupted while writing $description").apply {
            initCause(e)
        }
    }

    companion object {
        /** What the writer buffers before it moves the bytes to the part being filled. */
        private const val CHUNK_SIZE = 1 shl 20
    }
}
