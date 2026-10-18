package org.dozendb.cluster.ratis

import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.util.zip.CRC32
import org.dozendb.cluster.api.LogPosition
import org.dozendb.cluster.api.Snapshot
import org.slf4j.LoggerFactory

/**
 * The latest snapshot of one group on this member, kept in a small file in the group's state
 * machine directory. The state lives in the database, so a snapshot is only a pointer: every entry
 * up to it is held by the state machine.
 *
 * @property installed whether the snapshot came from a copy installed while the group was stopped,
 *   rather than from the local state machine. Every entry of the local log was at or below it then.
 */
internal data class SnapshotRecord(val snapshot: Snapshot, val installed: Boolean) {
    companion object {
        private val log = LoggerFactory.getLogger(SnapshotRecord::class.java)
        const val FILE_NAME = "dozendb-snapshot"
        private const val MAGIC = 0x445a534e // "DZSN"
        private const val VERSION = 1
        private val OPEN_SEGMENT = Regex("""log_inprogress_(\d+)""")
        private val CLOSED_SEGMENT = Regex("""log_(\d+)-(\d+)""")

        /** Reads the record in [directory], or null if there is none. */
        fun read(directory: Path): SnapshotRecord? {
            val bytes =
                try {
                    Files.readAllBytes(directory.resolve(FILE_NAME))
                } catch (_: NoSuchFileException) {
                    return null
                }
            if (bytes.size < Int.SIZE_BYTES) throw IOException("snapshot record is truncated")
            val body = bytes.copyOfRange(0, bytes.size - Int.SIZE_BYTES)
            val checksum = ByteBuffer.wrap(bytes, body.size, Int.SIZE_BYTES).int
            if (crc(body) != checksum) {
                throw IOException("snapshot record in $directory fails its checksum")
            }
            DataInputStream(body.inputStream()).use { input ->
                if (input.readInt() != MAGIC) throw IOException("not a snapshot record")
                val version = input.readInt()
                if (version != VERSION) {
                    throw IOException("unsupported snapshot record version $version")
                }
                val installed = input.readBoolean()
                val term = input.readLong()
                val index = input.readLong()
                val appliedIndex = input.readLong()
                return SnapshotRecord(Snapshot(LogPosition(index, term), appliedIndex), installed)
            }
        }

        /** Replaces the record in [directory] atomically and durably. */
        fun write(directory: Path, record: SnapshotRecord) {
            val body =
                ByteArrayOutputStream().use { buffer ->
                    DataOutputStream(buffer).use { out ->
                        out.writeInt(MAGIC)
                        out.writeInt(VERSION)
                        out.writeBoolean(record.installed)
                        out.writeLong(record.snapshot.position.term)
                        out.writeLong(record.snapshot.position.index)
                        out.writeLong(record.snapshot.appliedIndex)
                    }
                    buffer.toByteArray()
                }
            val bytes = ByteBuffer.allocate(body.size + Int.SIZE_BYTES).put(body).putInt(crc(body))
            Files.createDirectories(directory)
            val temporary = directory.resolve("$FILE_NAME.tmp")
            FileChannel.open(
                    temporary,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                )
                .use { channel ->
                    bytes.flip()
                    while (bytes.hasRemaining()) channel.write(bytes)
                    channel.force(true)
                }
            Files.move(
                temporary,
                directory.resolve(FILE_NAME),
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING,
            )
            syncDirectory(directory)
        }

        /**
         * Deletes the segments of the log in [logDirectory] whose entries are all at or below
         * [index]. Only valid for an installed snapshot, when no local entry was beyond it: Ratis
         * would otherwise keep appending to an open segment that ends before the snapshot.
         */
        fun discardCoveredSegments(logDirectory: Path, index: Long) {
            if (!Files.isDirectory(logDirectory)) return
            val covered =
                Files.list(logDirectory).use { files ->
                    files
                        .filter { file ->
                            val name = file.fileName.toString()
                            val open = OPEN_SEGMENT.matchEntire(name)
                            val closed = CLOSED_SEGMENT.matchEntire(name)
                            when {
                                open != null -> open.groupValues[1].toLong() <= index
                                closed != null -> closed.groupValues[2].toLong() <= index
                                else -> false
                            }
                        }
                        .toList()
                }
            for (segment in covered) {
                log.info("Deleting log segment {}, covered by the installed snapshot", segment)
                Files.delete(segment)
            }
            if (covered.isNotEmpty()) syncDirectory(logDirectory)
        }

        private fun crc(bytes: ByteArray): Int = CRC32().apply { update(bytes) }.value.toInt()

        private fun syncDirectory(directory: Path) {
            try {
                FileChannel.open(directory, StandardOpenOption.READ).use { it.force(true) }
            } catch (_: IOException) {
                // Some platforms cannot open directories; the rename is then as durable as it gets.
            }
        }
    }
}
