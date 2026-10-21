package org.dozendb.backup

import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.UUID
import org.neo4j.dbms.archive.Dumper
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.io.layout.DatabaseLayout
import org.neo4j.storageengine.api.CommandReaderFactory

/**
 * Writes and reads backup artifacts, Neo4j's `.backup` files. Community's `neo4j-admin database
 * load` reads full ones, and refuses differential ones as it does Enterprise's.
 */
object Artifacts {
    const val EXTENSION = ".backup"
    private val TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss").withZone(ZoneOffset.UTC)

    /**
     * The name of a backup of [database] taken at [time]:
     * `<database>-<yyyy-MM-ddTHH-mm-ss>.backup`, the time in UTC, and for the [n]th backup of that
     * second `-<n>` before the extension.
     */
    fun fileName(database: String, time: Instant, n: Int = 1): String {
        val name = "$database-${TIME.format(time)}"
        return if (n == 1) "$name$EXTENSION" else "$name-$n$EXTENSION"
    }

    /** A file in [directory] for a new backup of [database] taken at [time]. */
    fun newFile(directory: Path, database: String, time: Instant, fs: FileSystemAbstraction): Path =
        generateSequence(1) { it + 1 }
            .map { directory.resolve(fileName(database, time, it)) }
            .first { !fs.fileExists(it) }

    /**
     * Writes a new backup of [database] taken at [time] into [directory] with [write], which must
     * close the stream it gets: under a hidden temporary name until it is complete and synced, then
     * under its own ([newFile]), which it returns. What was written is deleted if [write] fails.
     */
    fun writeNew(
        directory: Path,
        database: String,
        time: Instant,
        fs: FileSystemAbstraction,
        write: (OutputStream) -> Unit,
    ): Path {
        val partial = directory.resolve(".$database-${UUID.randomUUID()}$EXTENSION.partial")
        try {
            write(fs.openAsOutputStream(partial, false))
            fs.write(partial).use { it.force(true) }
            val file = newFile(directory, database, time, fs)
            fs.renameFile(partial, file, StandardCopyOption.ATOMIC_MOVE)
            return file
        } catch (e: Throwable) {
            fs.deleteFile(partial)
            throw e
        }
    }

    /**
     * Writes a full backup of the store in [layout], which must be stopped and recovered, to [out]
     * and closes it. As Neo4j's dump, it leaves out the store's lock file and quarantine marker.
     */
    fun writeFull(
        out: OutputStream,
        header: ArtifactHeader,
        layout: DatabaseLayout,
        fs: FileSystemAbstraction,
    ) {
        require(header.full) { "the header is not a full backup's" }
        val excluded =
            setOf(layout.databaseLockFile(), layout.quarantineFile()).map { it.fileName.toString() }
        Dumper(fs).dump(
            layout.databaseDirectory(),
            layout.transactionLogsDirectory,
            out,
            ArtifactContainer.format(header),
        ) {
            it.fileName.toString() in excluded
        }
    }

    /**
     * Writes a differential backup of [transactions] to [out] and closes it. The transactions go to
     * [staging] first, a file it creates and then deletes.
     */
    fun writeDifferential(
        out: OutputStream,
        header: ArtifactHeader,
        transactions: Sequence<ArtifactTransaction>,
        staging: Path,
        fs: FileSystemAbstraction,
    ) {
        require(!header.full) { "the header is not a differential backup's" }
        require(!fs.fileExists(staging)) { "$staging exists" }
        out.use {
            try {
                val totals =
                    fs.openAsOutputStream(staging, false).use { records ->
                        DifferentialBody.writeRecords(records, header, transactions)
                    }
                ArtifactContainer.open(out, header).use { body ->
                    fs.openAsInputStream(staging).use { records ->
                        DifferentialBody.write(body, header, totals, records)
                    }
                }
            } finally {
                fs.deleteFile(staging)
            }
        }
    }

    /**
     * Writes an uncompressed differential backup of [transactions] to [out], as they are read, and
     * flushes it.
     */
    fun writeUncompressedDifferential(
        out: OutputStream,
        header: ArtifactHeader,
        transactions: Sequence<ArtifactTransaction>,
    ) {
        require(!header.full && !header.compressed) {
            "the header is not an uncompressed differential backup's"
        }
        val body = ArtifactContainer.open(out, header)
        DifferentialBody.writeRecords(body, header, transactions)
        body.flush()
    }

    /** Reads the header of the artifact in [input] and closes it. */
    fun readHeader(input: InputStream): ArtifactHeader = input.use {
        ArtifactContainer.read(it).apply { body.close() }.header
    }

    /**
     * Reads the differential backup in [input], passing its header and transactions to [block], and
     * closes it. The transactions are read as [block] takes them, and checked: one that is corrupt,
     * missing or out of order fails with an [IOException].
     */
    fun <T> readDifferential(
        input: InputStream,
        readers: CommandReaderFactory,
        block: (ArtifactHeader, Sequence<ArtifactTransaction>) -> T,
    ): T {
        input.use {
            val opened = ArtifactContainer.read(input)
            return opened.body.use { body ->
                if (opened.header.full) throw IOException("a full backup, not a differential one")
                block(opened.header, DifferentialBody.read(body, opened.header, readers))
            }
        }
    }
}
