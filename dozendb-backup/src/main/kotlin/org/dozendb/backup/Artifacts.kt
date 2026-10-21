package org.dozendb.backup

import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.nio.file.Path
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
