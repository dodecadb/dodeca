package org.dozendb.cluster.internal

import java.nio.ByteBuffer
import java.nio.file.OpenOption
import java.nio.file.Path
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import org.neo4j.io.fs.DelegatingStoreChannel
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.io.fs.StoreChannel

/**
 * Neo4j's file system, pausing before each write to a transaction log while [slow] is set: a member
 * on it applies what its Raft group commits late, while it still acknowledges Raft's appends.
 */
internal class SlowLogFileSystem(
    private val delegate: FileSystemAbstraction = DefaultFileSystemAbstraction()
) : FileSystemAbstraction by delegate {
    @Volatile var slow = false

    override fun write(fileName: Path): StoreChannel = wrap(fileName, delegate.write(fileName))

    override fun open(fileName: Path, options: Set<OpenOption>): StoreChannel =
        wrap(fileName, delegate.open(fileName, options))

    private fun wrap(path: Path, channel: StoreChannel): StoreChannel =
        if (path.fileName.toString().startsWith("neostore.transaction.db")) Slow(channel)
        else channel

    private inner class Slow(channel: StoreChannel) :
        DelegatingStoreChannel<StoreChannel>(channel) {
        private fun pause() {
            if (slow) Thread.sleep(PAUSE_MILLIS)
        }

        override fun write(src: ByteBuffer): Int {
            pause()
            return super.write(src)
        }

        override fun write(srcs: Array<out ByteBuffer>): Long {
            pause()
            return super.write(srcs)
        }

        override fun write(srcs: Array<out ByteBuffer>, offset: Int, length: Int): Long {
            pause()
            return super.write(srcs, offset, length)
        }

        override fun writeAll(src: ByteBuffer) {
            pause()
            super.writeAll(src)
        }

        override fun writeAll(src: ByteBuffer, position: Long) {
            pause()
            super.writeAll(src, position)
        }
    }

    private companion object {
        const val PAUSE_MILLIS = 20L
    }
}
