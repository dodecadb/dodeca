package org.dozendb.cloud.internal

import java.io.InputStream
import java.nio.ByteBuffer

/** The bytes left in a buffer, as a stream; reading moves the buffer's position. */
internal class ByteBufferInput(private val data: ByteBuffer) : InputStream() {
    override fun read(): Int = if (data.hasRemaining()) data.get().toInt() and 0xFF else -1

    override fun read(bytes: ByteArray, offset: Int, length: Int): Int {
        if (length == 0) return 0
        if (!data.hasRemaining()) return -1
        val count = minOf(length, data.remaining())
        data.get(bytes, offset, count)
        return count
    }

    override fun skip(n: Long): Long {
        val count = n.coerceIn(0, data.remaining().toLong()).toInt()
        data.position(data.position() + count)
        return count.toLong()
    }

    override fun available() = data.remaining()
}
