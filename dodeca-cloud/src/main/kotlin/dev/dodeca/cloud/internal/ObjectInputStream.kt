package dev.dodeca.cloud.internal

import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.channels.ClosedChannelException
import org.neo4j.cloud.storage.io.PathBasedOutputStream

/**
 * An object's stream: its first [HEAD] bytes in one small request, the rest through an
 * [ObjectReadChannel] opened where the reader goes past them, which fetches ranges ahead. Reading a
 * header, such as a backup's, costs one small request; copying the whole object, untouched, is the
 * channel's copy.
 */
internal class ObjectInputStream(
    private val size: Long,
    private val head: () -> ByteBuffer,
    private val channel: () -> ObjectReadChannel,
) : InputStream() {
    private val headEnd = minOf(HEAD.toLong(), size)
    private var fetched: ByteBuffer? = null
    private var rest: ObjectReadChannel? = null
    private var position = 0L
    private var mark = -1L
    private var closed = false

    override fun read(): Int {
        val one = ByteArray(1)
        return if (read(one, 0, 1) < 0) -1 else one[0].toInt() and 0xFF
    }

    override fun read(bytes: ByteArray, offset: Int, length: Int): Int {
        ensureOpen()
        if (length == 0) return 0
        if (position >= size) return -1
        val open = rest
        if (open == null && position < headEnd) {
            val data = fetched ?: head().also { fetched = it }
            val count = minOf(length.toLong(), headEnd - position).toInt()
            data.get(position.toInt(), bytes, offset, count)
            position += count
            return count
        }
        val read = (open ?: rest()).read(bytes, offset, length)
        if (read > 0) position += read
        return read
    }

    override fun skip(n: Long): Long {
        ensureOpen()
        val skipped = n.coerceIn(0, size - position)
        position += skipped
        rest?.position(position)
        return skipped
    }

    override fun available(): Int =
        if (rest == null && fetched != null) (headEnd - position).coerceAtLeast(0).toInt() else 0

    override fun markSupported() = true

    @Synchronized
    override fun mark(readLimit: Int) {
        mark = position
    }

    @Synchronized
    override fun reset() {
        ensureOpen()
        if (mark < 0) throw IOException("No mark has been set on this stream")
        position = mark
        rest?.position(position)
    }

    override fun transferTo(out: OutputStream): Long {
        ensureOpen()
        val from = position
        if (from == 0L && fetched == null && rest == null) {
            // Untouched: the channel copies it whole, ranges at once, or within the cloud.
            return rest().transferTo(out).also { position = size }
        }
        if (out is PathBasedOutputStream) {
            // Such a stream takes either writes or the channel's copy to its file, not both.
            return super.transferTo(out)
        }
        fetched?.let { data ->
            if (rest == null && position < headEnd) {
                // Not closed: that would close the stream.
                Channels.newChannel(out)
                    .write(data.duplicate().position(position.toInt()).limit(headEnd.toInt()))
                position = headEnd
            }
        }
        if (position < size) (rest ?: rest()).transferTo(out)
        position = size
        return size - from
    }

    override fun close() {
        if (closed) return
        closed = true
        rest?.close()
    }

    private fun rest(): ObjectReadChannel =
        channel().also {
            it.position(position)
            rest = it
        }

    private fun ensureOpen() {
        if (closed) throw ClosedChannelException()
    }

    companion object {
        /** What the first request reads: a backup's header, a CSV's first lines. */
        const val HEAD = 256 shl 10
    }
}
