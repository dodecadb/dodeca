package dev.dodeca.cluster.internal.secondaries

import dev.dodeca.cluster.internal.importer.ImportedStores
import dev.dodeca.cluster.internal.replication.ReplicationService
import dev.dodeca.core.edition.DatabaseHosting
import dev.dodeca.core.edition.HostedDatabase
import dev.dodeca.core.servers.CopyMode
import java.util.concurrent.ConcurrentHashMap
import org.neo4j.kernel.database.NamedDatabaseId

/**
 * Hosts each database in the mode the system graph places it on this member in: as a primary,
 * through its Raft group ([primaries]), or as a secondary that pulls ([secondaries]). The
 * reconciler sets the mode before it creates the database ([host]); a database it set none for is a
 * primary. `system` is hosted in the mode this member hosts it in, and switches modes in place
 * ([switchSystem]). A store that an incremental import merged into is checked before each start
 * ([imported]). Neither reuses freed IDs from the start: a primary learns which are free first, and
 * a secondary allocates none.
 */
internal class ClusterHosting(
    private val primaries: ReplicationService,
    private val secondaries: Secondaries,
    private val imported: ImportedStores? = null,
) : DatabaseHosting {
    private val modes = ConcurrentHashMap<String, CopyMode>()

    /** `system`, once Neo4j created it. */
    @Volatile private var system: SwitchingHostedDatabase? = null

    override val reusesFreedIdsAtStart: Boolean = false

    /** Hosts [id] in [mode] from when it is next created, or copied in. */
    fun host(id: NamedDatabaseId, mode: CopyMode) {
        modes[id.name()] = mode
    }

    /** The mode [id] is hosted in, or is next created or copied in. */
    fun mode(id: NamedDatabaseId): CopyMode = mode(id.name())

    /** The mode the database [name] is hosted in, or is next created or copied in. */
    fun mode(name: String): CopyMode = modes[name] ?: CopyMode.PRIMARY

    override fun prepare(namedDatabaseId: NamedDatabaseId, storeExists: Boolean): HostedDatabase {
        val hosted = prepare(mode(namedDatabaseId), namedDatabaseId, storeExists)
        if (!namedDatabaseId.isSystemDatabase) return hosted
        return SwitchingHostedDatabase(hosted).also { system = it }
    }

    override fun beforeStart(namedDatabaseId: NamedDatabaseId) {
        if (!namedDatabaseId.isSystemDatabase)
            imported?.check(namedDatabaseId, mode(namedDatabaseId))
    }

    private fun prepare(
        mode: CopyMode,
        namedDatabaseId: NamedDatabaseId,
        storeExists: Boolean,
    ): HostedDatabase =
        when (mode) {
            CopyMode.PRIMARY -> primaries.prepare(namedDatabaseId, storeExists)
            CopyMode.SECONDARY -> secondaries.prepare(namedDatabaseId)
        }

    /**
     * Hosts `system`, which must be stopped and have its store, in [mode] from when it next starts,
     * as the same database.
     */
    fun switchSystem(mode: CopyMode) {
        val switching = checkNotNull(system) { "system was not created" }
        val id = NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID
        modes[id.name()] = mode
        switching.switchTo(prepare(mode, id, storeExists = true))
    }
}
