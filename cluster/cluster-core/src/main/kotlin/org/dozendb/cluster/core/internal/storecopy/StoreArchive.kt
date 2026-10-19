package org.dozendb.cluster.core.internal.storecopy

import java.io.BufferedInputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.EOFException
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.nio.ByteBuffer
import java.nio.file.Path
import java.util.Arrays
import java.util.UUID
import org.dozendb.cluster.api.LogPosition
import org.dozendb.cluster.api.Snapshot
import org.neo4j.io.fs.FileSystemAbstraction

/**
 * The stream format of a database copy: a header with the database name and id, a description and
 * the Raft snapshot the copy holds, if the database is replicated; then each file with its root
 * directory, relative path and length; then a trailer with the totals so a truncated stream is
 * detected.
 *
 * A file's bytes follow in chunks of [CHUNK_SIZE], the last one shorter. A chunk of zeros is sent
 * as its tag alone: Neo4j preallocates transaction log files with zeros, 256 MiB each on Linux,
 * which would otherwise make up most of a small database's copy. The receiver writes the zeros, so
 * every file keeps its length and its allocated space.
 */
internal object StoreArchive {
    private const val MAGIC = 0x445A5343 // "DZSC"
    private const val VERSION: Byte = 4
    private const val TAG_END: Byte = 0
    private const val TAG_FILE: Byte = 1
    private const val CHUNK_DATA: Byte = 0
    private const val CHUNK_ZEROS: Byte = 1
    private const val CHUNK_SIZE = 1 shl 16
    private const val BUFFER_SIZE = 1 shl 20
    private val ZEROS = ByteArray(CHUNK_SIZE)

    enum class Root(val code: Byte) {
        DATABASE(0),
        TRANSACTION_LOGS(1);

        companion object {
            fun of(code: Byte): Root =
                entries.firstOrNull { it.code == code } ?: throw IOException("unknown root $code")
        }
    }

    /** @property snapshot the Raft snapshot the copy holds, null if it is not replicated. */
    data class Summary(
        val database: String,
        val databaseId: UUID,
        val description: String,
        val snapshot: Snapshot?,
        val files: Int,
        val bytes: Long,
    )

    class Writer(out: OutputStream, private val fs: FileSystemAbstraction) {
        private val data = DataOutputStream(out)
        private val buffer = ByteBuffer.allocate(CHUNK_SIZE)
        private var files = 0
        private var bytes = 0L

        fun header(database: String, databaseId: UUID, description: String, snapshot: Snapshot?) {
            data.writeInt(MAGIC)
            data.writeByte(VERSION.toInt())
            data.writeUTF(database)
            data.writeLong(databaseId.mostSignificantBits)
            data.writeLong(databaseId.leastSignificantBits)
            data.writeUTF(description)
            data.writeBoolean(snapshot != null)
            if (snapshot != null) {
                data.writeLong(snapshot.position.index)
                data.writeLong(snapshot.position.term)
                data.writeLong(snapshot.appliedIndex)
            }
        }

        /** Streams the bytes [path] has now; a live file may grow while it is copied. */
        fun file(root: Root, relativePath: Path, path: Path) {
            val length = fs.getFileSize(path)
            data.writeByte(TAG_FILE.toInt())
            data.writeByte(root.code.toInt())
            data.writeUTF(relativePath.joinToString("/"))
            data.writeLong(length)
            fs.read(path).use { channel ->
                var remaining = length
                while (remaining > 0) {
                    val count = minOf(remaining, CHUNK_SIZE.toLong()).toInt()
                    buffer.clear().limit(count)
                    while (buffer.hasRemaining()) {
                        if (channel.read(buffer) < 0) {
                            throw EOFException(
                                "$path ended ${remaining - buffer.position()} bytes before its " +
                                    "length $length"
                            )
                        }
                    }
                    if (Arrays.mismatch(buffer.array(), 0, count, ZEROS, 0, count) < 0) {
                        data.writeByte(CHUNK_ZEROS.toInt())
                    } else {
                        data.writeByte(CHUNK_DATA.toInt())
                        data.write(buffer.array(), 0, count)
                    }
                    remaining -= count
                }
            }
            files++
            bytes += length
        }

        fun end() {
            data.writeByte(TAG_END.toInt())
            data.writeInt(files)
            data.writeLong(bytes)
            data.flush()
        }
    }

    /** Writes the files of a copy below the two directories, which must exist, and syncs them. */
    fun read(
        input: InputStream,
        fs: FileSystemAbstraction,
        databaseDirectory: Path,
        transactionLogsDirectory: Path,
    ): Summary {
        val data = DataInputStream(BufferedInputStream(input, BUFFER_SIZE))
        if (data.readInt() != MAGIC) throw IOException("not a DozenDB database copy")
        val version = data.readByte()
        if (version != VERSION) throw IOException("unsupported database copy version $version")
        val database = data.readUTF()
        val databaseId = UUID(data.readLong(), data.readLong())
        val description = data.readUTF()
        val snapshot =
            if (data.readBoolean()) {
                val position = LogPosition(index = data.readLong(), term = data.readLong())
                val appliedIndex = data.readLong()
                if (appliedIndex > position.index) {
                    throw IOException("invalid snapshot $position with applied index $appliedIndex")
                }
                Snapshot(position, appliedIndex)
            } else {
                null
            }
        val buffer = ByteBuffer.allocate(CHUNK_SIZE)
        var files = 0
        var bytes = 0L
        while (true) {
            when (val tag = data.readByte()) {
                TAG_FILE -> {
                    val root =
                        when (Root.of(data.readByte())) {
                            Root.DATABASE -> databaseDirectory
                            Root.TRANSACTION_LOGS -> transactionLogsDirectory
                        }
                    val target = resolveSafely(root, data.readUTF())
                    val length = data.readLong()
                    fs.mkdirs(target.parent)
                    fs.write(target).use { channel ->
                        var remaining = length
                        while (remaining > 0) {
                            val count = minOf(remaining, CHUNK_SIZE.toLong()).toInt()
                            when (val chunk = data.readByte()) {
                                CHUNK_DATA -> {
                                    data.readFully(buffer.array(), 0, count)
                                    channel.writeAll(buffer.clear().limit(count))
                                }
                                CHUNK_ZEROS -> channel.writeAll(ByteBuffer.wrap(ZEROS, 0, count))
                                else -> throw IOException("unknown database copy chunk $chunk")
                            }
                            remaining -= count
                        }
                        channel.force(false)
                    }
                    files++
                    bytes += length
                }
                TAG_END -> {
                    val expectedFiles = data.readInt()
                    val expectedBytes = data.readLong()
                    if (expectedFiles != files || expectedBytes != bytes) {
                        throw IOException(
                            "database copy is incomplete: got $files files and $bytes bytes, " +
                                "expected $expectedFiles and $expectedBytes"
                        )
                    }
                    return Summary(database, databaseId, description, snapshot, files, bytes)
                }
                else -> throw IOException("unknown database copy entry $tag")
            }
        }
    }

    private fun resolveSafely(root: Path, relativePath: String): Path {
        val parts = relativePath.split('/')
        if (parts.any { it.isEmpty() || it == "." || it == ".." || '\\' in it || ':' in it }) {
            throw IOException("invalid path in database copy: $relativePath")
        }
        return parts.fold(root) { path, part -> path.resolve(part) }
    }
}
