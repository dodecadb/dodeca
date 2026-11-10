package org.dozendb.core.edition

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.UUID
import org.neo4j.kernel.database.DatabaseId

/**
 * A copy of a database put in quarantine on this server: by [user] at [time], for [reason]. The
 * server's own quarantines, of copies that failed while running, name [SERVER] as the user.
 */
data class Quarantine(val user: String, val time: Instant, val reason: String) {
    /**
     * What `SHOW DATABASES` shows as the copy's status message and `dbms.quarantineDatabase`
     * returns, as in Neo4j Enterprise: "By <user> at <time>: <reason>", the time in UTC with
     * milliseconds.
     */
    val message: String
        get() = "By $user at ${TIME.format(time)}: ${reason.ifEmpty { NO_REASON }}"

    companion object {
        /** The user of a quarantine that this server put a copy in. */
        const val SERVER = "the server"

        /** The reason of a quarantine given none, as Neo4j Enterprise shows it. */
        const val NO_REASON = "No reason given"

        private val TIME: DateTimeFormatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC)
    }
}

/**
 * The copies of databases in quarantine on this server, by database id, kept in [file] across
 * restarts. `system`'s is kept only in memory, so that a restart lifts it, as in Neo4j Enterprise.
 *
 * The file has a line per database: its id, the user, the time in milliseconds since the epoch and
 * the reason, separated by tabs; tabs and line breaks in the user and the reason are written as
 * spaces. It is written as the membership file is, to a temporary file that replaces it once
 * forced. A file that cannot be read fails the server's start with its name: deleting a line by
 * hand lifts that quarantine.
 */
class Quarantines(private val file: Path) {
    private var stored: Map<UUID, Quarantine>? = null
    private var system: Quarantine? = null

    /** The quarantine of the copy of database [id] here, if it is in one. */
    @Synchronized fun of(id: UUID): Quarantine? = if (id == SYSTEM) system else records()[id]

    /** Every quarantine here, by database id. */
    @Synchronized
    fun all(): Map<UUID, Quarantine> =
        system?.let { records() + (SYSTEM to it) } ?: HashMap(records())

    /** Puts the copy of database [id] in [quarantine], replacing the one it is in, if any. */
    @Synchronized
    fun put(id: UUID, quarantine: Quarantine) {
        if (id == SYSTEM) system = quarantine else write(records() + (id to quarantine))
    }

    /** Lifts the quarantine of the copy of database [id]; returns it, or null if there was none. */
    @Synchronized
    fun remove(id: UUID): Quarantine? {
        if (id == SYSTEM) return system.also { system = null }
        val records = records()
        val removed = records[id] ?: return null
        write(records - id)
        return removed
    }

    /** Forgets the quarantines of the databases other than [ids] and `system`, which are gone. */
    @Synchronized
    fun retainOnly(ids: Set<UUID>) {
        val records = records()
        if (records.keys.all { it in ids }) return
        write(records.filterKeys { it in ids })
    }

    private fun records(): Map<UUID, Quarantine> = stored ?: read().also { stored = it }

    private fun read(): Map<UUID, Quarantine> {
        if (!Files.exists(file)) return emptyMap()
        return Files.readAllLines(file)
            .filter { it.isNotBlank() }
            .associate { line ->
                val fields = line.split('\t', limit = 4)
                try {
                    require(fields.size == 4)
                    UUID.fromString(fields[0]) to
                        Quarantine(
                            fields[1],
                            Instant.ofEpochMilli(fields[2].toLong()),
                            fields[3],
                        )
                } catch (e: IllegalArgumentException) {
                    throw IllegalStateException(
                        "$file holds a line that is not a quarantine: '$line'. Each line is a " +
                            "database id, a user, a time in milliseconds since the epoch and a " +
                            "reason, separated by tabs; delete the line to lift that quarantine.",
                        e,
                    )
                }
            }
    }

    private fun write(records: Map<UUID, Quarantine>) {
        val text =
            records.entries
                .sortedBy { it.key }
                .joinToString("") { (id, quarantine) ->
                    listOf(
                            id.toString(),
                            quarantine.user.flattened(),
                            quarantine.time.toEpochMilli().toString(),
                            quarantine.reason.flattened(),
                        )
                        .joinToString("\t", postfix = "\n")
                }
        val temporary = file.resolveSibling("${file.fileName}.tmp")
        Files.createDirectories(file.parent)
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
            file,
            StandardCopyOption.ATOMIC_MOVE,
            StandardCopyOption.REPLACE_EXISTING,
        )
        FileChannel.open(file.parent, StandardOpenOption.READ).use { it.force(true) }
        stored = records
    }

    private fun String.flattened(): String = replace(Regex("[\t\r\n]+"), " ")

    companion object {
        /** The name of the file in the data directory. */
        const val FILE_NAME = "quarantine"

        private val SYSTEM: UUID = DatabaseId.SYSTEM_DATABASE_ID.uuid()
    }
}
