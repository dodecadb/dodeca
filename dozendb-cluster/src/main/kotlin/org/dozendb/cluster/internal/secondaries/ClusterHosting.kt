package org.dozendb.cluster.internal.secondaries

import java.util.concurrent.ConcurrentHashMap
import org.dozendb.cluster.internal.replication.ReplicationService
import org.dozendb.core.edition.DatabaseHosting
import org.dozendb.core.edition.HostedDatabase
import org.dozendb.core.servers.CopyMode
import org.neo4j.internal.id.IdGeneratorFactory
import org.neo4j.kernel.database.NamedDatabaseId

/**
 * Hosts each database in the mode the system graph places it on this member in: as a primary,
 * through its Raft group ([primaries]), or as a secondary that pulls ([secondaries]). The
 * reconciler sets the mode before it creates the database ([host]); `system`, and a database it set
 * none for, are primaries. Neither reuses freed IDs from the start: a primary learns which are free
 * first, and a secondary allocates none.
 */
internal class ClusterHosting(
    private val primaries: ReplicationService,
    private val secondaries: Secondaries,
) : DatabaseHosting {
    private val modes = ConcurrentHashMap<String, CopyMode>()

    override val reusesFreedIdsAtStart: Boolean = false

    /** Hosts [id] in [mode] from when it is next created, or copied in. */
    fun host(id: NamedDatabaseId, mode: CopyMode) {
        modes[id.name()] = mode
    }

    /** The mode [id] is hosted in, or is next created or copied in. */
    fun mode(id: NamedDatabaseId): CopyMode = mode(id.name())

    /** The mode the database [name] is hosted in, or is next created or copied in. */
    fun mode(name: String): CopyMode = modes[name] ?: CopyMode.PRIMARY

    override fun prepare(
        namedDatabaseId: NamedDatabaseId,
        idGeneratorFactory: IdGeneratorFactory,
        storeExists: Boolean,
    ): HostedDatabase =
        when (mode(namedDatabaseId)) {
            CopyMode.PRIMARY -> primaries.prepare(namedDatabaseId, idGeneratorFactory, storeExists)
            CopyMode.SECONDARY -> secondaries.prepare(namedDatabaseId)
        }
}
