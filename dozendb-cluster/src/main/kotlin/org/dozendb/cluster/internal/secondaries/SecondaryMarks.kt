package org.dozendb.cluster.internal.secondaries

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.util.UUID

/**
 * The databases this member holds as secondaries, by id, in a file of their ids, one per line: a
 * secondary's store counts only with its id here, as a primary's counts only with its Raft group.
 * An id is added once a copy of the database is in place, and goes with the database. Written as
 * the membership file is, to a temporary file that replaces it once forced.
 */
internal class SecondaryMarks(private val path: Path) {
    /** The ids of the databases this member holds as secondaries. */
    @Synchronized
    fun ids(): Set<UUID> {
        if (!Files.exists(path)) return emptySet()
        return Files.readAllLines(path)
            .filter { it.isNotBlank() }
            .mapTo(HashSet()) { UUID.fromString(it.trim()) }
    }

    /** Records that this member holds the database [id] as a secondary. */
    @Synchronized
    fun add(id: UUID) {
        val ids = ids()
        if (id !in ids) write(ids + id)
    }

    /** Forgets the database [id]. */
    @Synchronized
    fun remove(id: UUID) {
        val ids = ids()
        if (id in ids) write(ids - id)
    }

    private fun write(ids: Set<UUID>) {
        val text = ids.sorted().joinToString("") { "$it\n" }
        val temporary = path.resolveSibling("${path.fileName}.tmp")
        Files.createDirectories(path.parent)
        FileChannel.open(
                temporary,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING,
            )
            .use { channel ->
                val bytes = ByteBuffer.wrap(text.encodeToByteArray())
                while (bytes.hasRemaining()) channel.write(bytes)
                channel.force(true)
            }
        Files.move(
            temporary,
            path,
            StandardCopyOption.ATOMIC_MOVE,
            StandardCopyOption.REPLACE_EXISTING,
        )
        FileChannel.open(path.parent, StandardOpenOption.READ).use { it.force(true) }
    }
}
