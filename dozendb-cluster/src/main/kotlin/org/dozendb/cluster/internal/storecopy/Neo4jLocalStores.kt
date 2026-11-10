package org.dozendb.cluster.internal.storecopy

import java.io.IOException
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import org.dozendb.cluster.DatabaseGroups
import org.dozendb.cluster.LocalStores
import org.dozendb.cluster.internal.replication.StagedCopy
import org.dozendb.cluster.internal.secondaries.StagedStore
import org.dozendb.consensus.BulkTransfer
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.Snapshot
import org.dozendb.core.store.StoreArchive
import org.dozendb.core.store.StorePlacement
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.io.layout.DatabaseLayout
import org.neo4j.io.layout.Neo4jLayout
import org.neo4j.logging.InternalLog
import org.neo4j.storageengine.api.StorageEngineFactory

/**
 * The database files of this member, which it copies from another member when they are missing or
 * behind. [databaseNames] are the databases a member needs to form a cluster: `system` and the
 * default database.
 *
 * Databases are first copied to a staging place and synced, then moved into place
 * ([StorePlacement]). A database left half-moved by an interrupted copy counts as missing and is
 * copied again, and a member with one counts as having no databases during formation.
 *
 * A copy of a database whose group does not run here yet, as at startup or for a new database,
 * leaves its Raft position as the group's seed ([seed]), so that the group does not ask for
 * another. [received] is called with a database's name once a copy of it is in place.
 */
internal class Neo4jLocalStores(
    private val fs: FileSystemAbstraction,
    private val layout: Neo4jLayout,
    private val databaseNames: List<String>,
    private val transfer: BulkTransfer,
    private val log: InternalLog,
    private val received: (String) -> Unit = {},
    private val seed: (DatabaseRef, Snapshot) -> Unit,
    private val groups: DatabaseGroups = DatabaseGroups(),
) : LocalStores {
    private val placement = StorePlacement(fs, layout)
    private val copying = ConcurrentHashMap<String, UUID>()

    override val existed: Boolean =
        databaseNames.none(::incomplete) && databaseNames.any(::storageExists)

    override fun missing(): List<String> = databaseNames.filter {
        incomplete(it) || !storageExists(it)
    }

    override fun present(): List<String> =
        (databaseNames + storedDatabases()).distinct().filter(::complete)

    /** The databases with a directory of their own, staging places left out. */
    private fun storedDatabases(): List<String> {
        val directory = layout.databasesDirectory()
        if (!fs.isDirectory(directory)) return emptyList()
        return fs.listFiles(directory)
            .filter { fs.isDirectory(it) }
            .map { it.fileName.toString() }
            .filterNot { it.startsWith(".") }
    }

    override fun copyFrom(source: HostPort, databases: List<String>) {
        require(databases.all { it in databaseNames }) { "unknown databases in $databases" }
        val staged = databases.map { fetch(source, it, null, NO_MINIMUM) }
        staged.forEach {
            moveIntoPlace(it)
            leaveSeed(it)
        }
    }

    /**
     * Copies [database] from [source] next to it, holding every Raft entry up to [minimumIndex].
     * The database may keep running meanwhile; [StagedCopy.moveIntoPlace] replaces it once stopped.
     */
    fun stageReplicated(source: HostPort, database: DatabaseRef, minimumIndex: Long): StagedCopy {
        val name = database.name
        val staged = fetch(source, name, database.id, minimumIndex)
        val snapshot =
            staged.snapshot
                ?: throw IOException("$source sent a copy of $name without a Raft snapshot")
        if (snapshot.position.index < minimumIndex) {
            throw IOException(
                "$source sent a copy of $name holding $snapshot, short of index $minimumIndex"
            )
        }
        return object : StagedCopy {
            override val snapshot = snapshot

            override fun moveIntoPlace() = moveIntoPlace(staged)
        }
    }

    /**
     * Copies the database [database], which this member lacks, from [source]. A copy for a primary
     * leaves its Raft position as the group's seed; one for a secondary, which runs no group, does
     * not ([seeded]).
     */
    fun copyNew(source: HostPort, database: DatabaseRef, seeded: Boolean = true) {
        copying[database.name] = database.id
        try {
            val staged = fetch(source, database.name, database.id, NO_MINIMUM)
            moveIntoPlace(staged)
            if (seeded) leaveSeed(staged)
        } finally {
            copying.remove(database.name)
        }
    }

    /**
     * Copies [database] from [source] next to it, for a secondary that needs a newer copy, while it
     * keeps running: any copy will do, as its source's logs no longer hold what the secondary has.
     */
    fun stageSecondary(source: HostPort, database: DatabaseRef): StagedStore {
        val staged = fetch(source, database.name, database.id, NO_MINIMUM)
        return StagedStore { moveIntoPlace(staged) }
    }

    /** The ids of the groups of the databases that [copyNew] copies now, by name. */
    fun copying(): Map<String, UUID> = HashMap(copying)

    private fun leaveSeed(copy: Staged) {
        val snapshot = copy.snapshot ?: return
        // The source names the database by its Neo4j id.
        seed(groups.of(copy.name, copy.summary.databaseId), snapshot)
    }

    /** Names of the databases with files on this member, complete or not. */
    fun stored(): Set<String> {
        val names = mutableSetOf<String>()
        for (directory in
            listOf(layout.databasesDirectory(), layout.transactionLogsRootDirectory())) {
            if (!fs.isDirectory(directory)) continue
            for (path in fs.listFiles(directory)) {
                val name = path.fileName.toString()
                val marked = placement.markedDatabase(name)
                when {
                    marked != null -> names += marked
                    // Staging places; database names start with a letter.
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
        placement.delete(name)
        log.info("Deleted the files of database %s", name)
    }

    private fun storageExists(name: String): Boolean =
        StorageEngineFactory.allAvailableStorageEngines().any { it.storageExists(fs, layout, name) }

    private fun incomplete(name: String): Boolean = placement.incomplete(name)

    private class Staged(
        val name: String,
        val layout: DatabaseLayout,
        val summary: StoreArchive.Summary,
        val source: HostPort,
    ) {
        val snapshot = CopySnapshot.decode(summary.metadata)
    }

    private fun fetch(source: HostPort, name: String, id: UUID?, minimumIndex: Long): Staged {
        val staged = placement.stage(name, "copy")
        val summary =
            transfer.fetchBulk(
                source,
                StoreCopySource.NAME,
                StoreCopySource.encodeRequest(name, id, minimumIndex),
            ) { input ->
                StoreArchive.read(
                    input,
                    fs,
                    staged.databaseDirectory(),
                    staged.transactionLogsDirectory,
                )
            }
        check(summary.database == name) {
            "asked for database $name but received ${summary.database}"
        }
        return Staged(name, staged, summary, source)
    }

    private fun moveIntoPlace(copy: Staged) {
        placement.moveIntoPlace(copy.layout, copy.name)
        received(copy.name)
        log.info(
            "Copied database %s from %s: %d files, %d bytes, %s, Raft snapshot %s",
            copy.name,
            copy.source,
            copy.summary.files,
            copy.summary.bytes,
            copy.summary.description,
            copy.snapshot,
        )
    }

    private companion object {
        const val NO_MINIMUM = -1L
    }
}
