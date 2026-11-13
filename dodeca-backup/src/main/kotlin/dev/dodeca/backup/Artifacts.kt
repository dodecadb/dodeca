package dev.dodeca.backup

import dev.dodeca.core.store.CommittedTransaction
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.UUID
import org.neo4j.cloud.storage.StoragePath
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

    /**
     * A file in [directory] for a new backup of [database] taken at [time], which no file has, nor
     * the first part of a split archive.
     */
    fun newFile(directory: Path, database: String, time: Instant, fs: FileSystemAbstraction): Path =
        generateSequence(1) { it + 1 }
            .map { directory.resolve(fileName(database, time, it)) }
            .first { !fs.fileExists(it) && !fs.fileExists(SplitArchives.part(it, 1)) }

    /**
     * Writes a new backup of [database] taken at [time] into [directory], local or in cloud
     * storage, with [write], which must close the stream it gets: under a hidden temporary name
     * until it is complete and synced, then under its own ([newFile]), which it returns. What was
     * written is deleted if [write] fails. In cloud storage, the new name is a copy within the
     * cloud.
     *
     * With a [partSize], the backup is split into parts of at most that many bytes, which are
     * written under their own names (`<file>.1`, `<file>.2` and so on) and synced first; its own
     * file, which names them, comes last as above.
     */
    fun writeNew(
        directory: Path,
        database: String,
        time: Instant,
        fs: FileSystemAbstraction,
        partSize: Long = 0,
        write: (OutputStream) -> Unit,
    ): Path {
        val partial = directory.resolve(".$database-${UUID.randomUUID()}$EXTENSION.partial")
        val parts = mutableListOf<Path>()
        try {
            if (partSize == 0L) {
                write(fs.openAsOutputStream(partial, false))
                sync(partial, fs)
                val file = newFile(directory, database, time, fs)
                fs.renameFile(partial, file, StandardCopyOption.ATOMIC_MOVE)
                return file
            }
            val file = newFile(directory, database, time, fs)
            val (id, count) = SplitArchives.writeParts(fs, file, partSize, parts::add, write)
            parts.forEach { sync(it, fs) }
            SplitArchives.writeManifest(fs.openAsOutputStream(partial, false), id, count)
            sync(partial, fs)
            if (fs.fileExists(file)) throw IOException("$file exists")
            fs.renameFile(partial, file, StandardCopyOption.ATOMIC_MOVE)
            return file
        } catch (e: Throwable) {
            fs.deleteFile(partial)
            parts.forEach(fs::deleteFile)
            throw e
        }
    }

    private fun sync(file: Path, fs: FileSystemAbstraction) {
        // An object in cloud storage is stored once its stream closes; opening it again would
        // replace it.
        if (file !is StoragePath) fs.write(file).use { it.force(true) }
    }

    /** Deletes the backup in [file], with its parts if it is split. */
    fun delete(file: Path, fs: FileSystemAbstraction) = SplitArchives.delete(fs, file)

    /** The backup in [file], a split one's parts joined, as one stream. */
    fun open(file: Path, fs: FileSystemAbstraction): InputStream = SplitArchives.open(fs, file)

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
        val manifest =
            Dumper.collectManifest(layout.databaseDirectory(), layout.transactionLogsDirectory) {
                it.fileName.toString() in excluded
            }
        val output =
            object : Dumper.DumpOutput {
                override fun stream(): OutputStream = out

                override fun description() = "the backup of ${header.databaseName}"
            }
        Dumper(fs).dump(output, ArtifactContainer.format(header), manifest)
    }

    /**
     * Writes a differential backup of [transactions] to [out] and closes it. The transactions go to
     * [staging] first, a file it creates and then deletes.
     */
    fun writeDifferential(
        out: OutputStream,
        header: ArtifactHeader,
        transactions: Sequence<CommittedTransaction>,
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
        transactions: Sequence<CommittedTransaction>,
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
        block: (ArtifactHeader, Sequence<CommittedTransaction>) -> T,
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
