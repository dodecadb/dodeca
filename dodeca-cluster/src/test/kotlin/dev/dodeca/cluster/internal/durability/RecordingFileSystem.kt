package dev.dodeca.cluster.internal.durability

import java.nio.ByteBuffer
import java.nio.file.OpenOption
import java.nio.file.Path
import java.util.Collections
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import org.neo4j.io.fs.DelegatingStoreChannel
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.io.fs.StoreChannel

/** Neo4j's file system, recording every write and force on its channels in order. */
internal class RecordingFileSystem(
    private val delegate: FileSystemAbstraction = DefaultFileSystemAbstraction()
) : FileSystemAbstraction by delegate {
    /** "write <file name>" and "force <file name>", in the order they happened. */
    val events: MutableList<String> = Collections.synchronizedList(mutableListOf())

    /** How often channels on files matching [file] were forced. */
    fun forces(file: (Path) -> Boolean): Int = synchronized(forced) { forced.count { file(it) } }

    private val forced = mutableListOf<Path>()

    override fun write(fileName: Path): StoreChannel = Recording(fileName, delegate.write(fileName))

    override fun read(fileName: Path): StoreChannel = Recording(fileName, delegate.read(fileName))

    override fun open(fileName: Path, options: Set<OpenOption>): StoreChannel =
        Recording(fileName, delegate.open(fileName, options))

    private inner class Recording(private val path: Path, channel: StoreChannel) :
        DelegatingStoreChannel<StoreChannel>(channel) {
        private fun wrote() {
            events += "write ${path.fileName}"
        }

        override fun write(src: ByteBuffer): Int = super.write(src).also { wrote() }

        override fun write(srcs: Array<out ByteBuffer>): Long = super.write(srcs).also { wrote() }

        override fun write(srcs: Array<out ByteBuffer>, offset: Int, length: Int): Long =
            super.write(srcs, offset, length).also { wrote() }

        override fun writeAll(src: ByteBuffer) = super.writeAll(src).also { wrote() }

        override fun writeAll(src: ByteBuffer, position: Long) =
            super.writeAll(src, position).also { wrote() }

        override fun flush() = force(false)

        override fun force(metaData: Boolean) {
            super.force(metaData)
            events += "force ${path.fileName}"
            synchronized(forced) { forced.add(path) }
        }
    }
}
