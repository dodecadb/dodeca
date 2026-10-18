package org.dozendb.cluster.core.internal.storecopy

import java.io.IOException
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.util.UUID
import org.dozendb.cluster.api.BulkTransfer
import org.dozendb.cluster.api.DatabaseRef
import org.dozendb.cluster.api.HostPort
import org.dozendb.cluster.api.Snapshot
import org.dozendb.cluster.core.LocalStores
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.io.layout.Neo4jLayout
import org.neo4j.logging.InternalLog
import org.neo4j.storageengine.api.StorageEngineFactory

/**
 * The database files of this member, which it copies from another member when they are missing or
 * behind. [databaseNames] are the databases a member needs to form a cluster: `system` and the
 * default database.
 *
 * Databases are first copied to staging directories next to their final places and synced. Only
 * then are they moved into place, each with a marker file present while it is moved. A database
 * with a marker was left half-moved by an interrupted copy: it counts as missing and is copied
 * again, and a member with any marker counts as having no databases during formation.
 */
internal class Neo4jLocalStores(
    private val fs: FileSystemAbstraction,
    private val layout: Neo4jLayout,
    private val databaseNames: List<String>,
    private val transfer: BulkTransfer,
    private val log: InternalLog,
) : LocalStores {
    override val existed: Boolean =
        databaseNames.none(::incomplete) && databaseNames.any(::storageExists)

    override fun missing(): List<String> = databaseNames.filter {
        incomplete(it) || !storageExists(it)
    }

    override fun copyFrom(source: HostPort, databases: List<String>) {
        require(databases.all { it in databaseNames }) { "unknown databases in $databases" }
        val staged = databases.map { fetch(source, it, null, NO_MINIMUM) }
        staged.forEach(::moveIntoPlace)
    }

    /**
     * Replaces [database], which must be stopped, with a copy from [source] holding every Raft
     * entry up to [minimumIndex], and returns the Raft snapshot the copy holds.
     */
    fun copyReplicated(source: HostPort, database: DatabaseRef, minimumIndex: Long): Snapshot {
        val name = database.name
        val staged = fetch(source, name, database.id, minimumIndex)
        val snapshot =
            staged.summary.snapshot
                ?: throw IOException("$source sent a copy of $name without a Raft snapshot")
        if (snapshot.position.index < minimumIndex) {
            throw IOException(
                "$source sent a copy of $name holding $snapshot, short of index $minimumIndex"
            )
        }
        moveIntoPlace(staged)
        return snapshot
    }

    /** Copies the database [database], which this member lacks, from [source]. */
    fun copyNew(source: HostPort, database: DatabaseRef) {
        moveIntoPlace(fetch(source, database.name, database.id, NO_MINIMUM))
    }

    /** Names of the databases with files on this member, complete or not. */
    fun stored(): Set<String> {
        val names = mutableSetOf<String>()
        for (directory in
            listOf(layout.databasesDirectory(), layout.transactionLogsRootDirectory())) {
            if (!fs.isDirectory(directory)) continue
            for (path in fs.listFiles(directory)) {
                val name = path.fileName.toString()
                when {
                    name.startsWith(INCOMPLETE_MARKER_PREFIX) ->
                        names += name.removePrefix(INCOMPLETE_MARKER_PREFIX)
                    // Staging directories; database names start with a letter.
                    name.startsWith(".") -> {}
                    fs.isDirectory(path) -> names += name
                }
            }
        }
        return names
    }

    /** Whether the store of [name] is complete: present and not left half-moved. */
    fun complete(name: String): Boolean = !incomplete(name) && storageExists(name)

    /** Deletes the files of the database [name], which must not be running. */
    fun delete(name: String) {
        require(name !in databaseNames) { "database $name is needed to form a cluster" }
        val target = layout.databaseLayout(name)
        fs.deleteRecursively(target.databaseDirectory())
        fs.deleteRecursively(target.transactionLogsDirectory)
        fs.deleteFile(marker(name))
        log.info("Deleted the files of database %s", name)
    }

    private fun storageExists(name: String): Boolean =
        StorageEngineFactory.allAvailableStorageEngines().any { it.storageExists(fs, layout, name) }

    private fun marker(name: String): Path =
        layout.databasesDirectory().resolve(INCOMPLETE_MARKER_PREFIX + name)

    private fun incomplete(name: String): Boolean = fs.fileExists(marker(name))

    private class Staged(
        val name: String,
        val database: Path,
        val transactionLogs: Path,
        val summary: StoreArchive.Summary,
        val source: HostPort,
    )

    private fun fetch(source: HostPort, name: String, id: UUID?, minimumIndex: Long): Staged {
        val database = layout.databasesDirectory().resolve(STAGING_PREFIX + name)
        val transactionLogs = layout.transactionLogsRootDirectory().resolve(STAGING_PREFIX + name)
        for (staging in listOf(database, transactionLogs)) {
            fs.deleteRecursively(staging)
            fs.mkdirs(staging)
        }
        val summary =
            transfer.fetchBulk(
                source,
                StoreCopySource.NAME,
                StoreCopySource.encodeRequest(name, id, minimumIndex),
            ) { input ->
                StoreArchive.read(input, fs, database, transactionLogs)
            }
        check(summary.database == name) {
            "asked for database $name but received ${summary.database}"
        }
        return Staged(name, database, transactionLogs, summary, source)
    }

    private fun moveIntoPlace(copy: Staged) {
        val marker = marker(copy.name)
        fs.mkdirs(marker.parent)
        fs.write(marker).close()
        val target = layout.databaseLayout(copy.name)
        replace(copy.transactionLogs, target.transactionLogsDirectory)
        replace(copy.database, target.databaseDirectory())
        fs.deleteFile(marker)
        log.info(
            "Copied database %s from %s: %d files, %d bytes, %s, Raft snapshot %s",
            copy.name,
            copy.source,
            copy.summary.files,
            copy.summary.bytes,
            copy.summary.description,
            copy.summary.snapshot,
        )
    }

    private fun replace(staging: Path, target: Path) {
        fs.deleteRecursively(target)
        fs.mkdirs(target.parent)
        fs.renameFile(staging, target, StandardCopyOption.ATOMIC_MOVE)
    }

    private companion object {
        const val STAGING_PREFIX = ".dozendb-copy-"
        const val INCOMPLETE_MARKER_PREFIX = ".dozendb-incomplete-"
        const val NO_MINIMUM = -1L
    }
}
