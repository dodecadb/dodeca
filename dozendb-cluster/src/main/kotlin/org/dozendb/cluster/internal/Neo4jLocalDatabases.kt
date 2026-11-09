package org.dozendb.cluster.internal

import java.util.UUID
import org.dozendb.cluster.internal.replication.EntryTerm
import org.dozendb.cluster.internal.replication.LocalDatabase
import org.dozendb.cluster.internal.replication.LocalDatabases
import org.dozendb.cluster.internal.replication.ModeSwitch
import org.dozendb.cluster.internal.replication.ReplicationService
import org.dozendb.cluster.internal.replication.StoreSeed
import org.dozendb.cluster.internal.secondaries.ClusterHosting
import org.dozendb.cluster.internal.secondaries.Secondaries
import org.dozendb.cluster.internal.storecopy.Neo4jLocalStores
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.LogPosition
import org.dozendb.consensus.Snapshot
import org.dozendb.core.edition.DatabaseManager
import org.dozendb.core.edition.DatabaseSeeder
import org.dozendb.core.servers.CopyMode
import org.neo4j.dbms.database.DatabaseRepository
import org.neo4j.dbms.database.StandaloneDatabaseContext
import org.neo4j.kernel.database.NamedDatabaseId

/**
 * This member's databases, through the database manager, their files, and their replication as
 * primaries or secondaries.
 *
 * @property entryTerm the term of a database's log entry, as the leader of its group tells it.
 * @property seed tells a database's group that this member holds its state up to a snapshot.
 */
internal class Neo4jLocalDatabases(
    private val manager: DatabaseManager,
    private val repository: DatabaseRepository<StandaloneDatabaseContext>,
    private val stores: Neo4jLocalStores,
    private val hosting: ClusterHosting,
    private val replication: ReplicationService,
    private val secondaries: Secondaries,
    private val seeder: () -> DatabaseSeeder?,
    private val entryTerm: (DatabaseRef, Long) -> EntryTerm,
    private val seed: (DatabaseRef, Snapshot) -> Unit,
) : LocalDatabases {
    override fun registered(): Map<NamedDatabaseId, LocalDatabase> =
        repository.registeredDatabases().mapValues { (id, context) ->
            LocalDatabase(
                started = context.database().isStarted,
                failed = context.isFailed,
                mode = hosting.mode(id),
            )
        }

    override fun stored(): Set<String> = stores.stored()

    override fun complete(name: String): Boolean = stores.complete(name)

    override fun groups(): Set<UUID> = replication.localGroups()

    override fun secondaries(): Set<UUID> = secondaries.marked()

    override fun catchingUp(id: NamedDatabaseId): Boolean =
        replication.catchingUp(id) || secondaries.catchingUp(id)

    override fun votes(id: NamedDatabaseId): Boolean? = replication.votes(id)

    override fun left(id: NamedDatabaseId): Boolean = replication.left(id)

    override fun create(id: NamedDatabaseId, mode: CopyMode) {
        hosting.host(id, mode)
        manager.createDatabase(id)
    }

    override fun start(id: NamedDatabaseId, seed: StoreSeed?) {
        replication.replica(id)?.handOverOnStop = true
        manager.startDatabase(
            id,
            when (seed) {
                null -> null
                is StoreSeed.Uri -> DatabaseManager.Seed.Uri(seed.uri, seeder)
                StoreSeed.Restored -> DatabaseManager.Seed.Restored
            },
        )
    }

    override fun stop(id: NamedDatabaseId) {
        replication.replica(id)?.handOverOnStop = false
        manager.stopDatabase(id)
    }

    override fun leave(id: NamedDatabaseId) {
        replication.replica(id)?.handOverOnStop = true
        manager.stopDatabase(id)
    }

    override fun drop(id: NamedDatabaseId) {
        replication.replica(id)?.handOverOnStop = false
        manager.dropDatabase(id)
        replication.remove(id)
        secondaries.remove(id)
    }

    override fun deleteStore(name: String) = stores.delete(name)

    override fun removeGroup(id: UUID) = replication.removeGroup(id)

    override fun unmark(id: UUID) = secondaries.unmark(id)

    override fun switch(id: NamedDatabaseId, mode: CopyMode): ModeSwitch =
        when (mode) {
            CopyMode.SECONDARY -> {
                replication.replica(id)?.handOverOnStop = false
                manager.releaseDatabase(id)
                // Marked before its group goes: a store with neither counts as a leftover.
                secondaries.mark(id)
                replication.release(id)
                create(id, CopyMode.SECONDARY)
                ModeSwitch.SWITCHED
            }
            CopyMode.PRIMARY -> toPrimary(id)
        }

    /**
     * Hosts the secondary [id] as a primary from its own store, which seeds the group it joins with
     * the log entry of its last transaction, as far as the group's leader tells its term.
     */
    private fun toPrimary(id: NamedDatabaseId): ModeSwitch {
        val ref = DatabaseRef(id.name(), id.databaseId().uuid())
        // It pulls no more meanwhile, so that its last transaction stays its last.
        val last = secondaries.pause(id) ?: return ModeSwitch.NEEDS_COPY
        val index = last.consensusIndex()
        val told = if (index < 0) EntryTerm.Pruned else entryTerm(ref, index)
        if (told !is EntryTerm.Known) {
            secondaries.resume(id)
            return if (told == EntryTerm.Unknown) ModeSwitch.LATER else ModeSwitch.NEEDS_COPY
        }
        manager.releaseDatabase(id)
        seed(ref, Snapshot(LogPosition(index, told.term), index))
        secondaries.remove(id)
        create(id, CopyMode.PRIMARY)
        return ModeSwitch.SWITCHED
    }

    override fun copy(source: HostPort, id: NamedDatabaseId, mode: CopyMode) {
        // Reported in its mode while copied.
        hosting.host(id, mode)
        when (mode) {
            CopyMode.PRIMARY ->
                stores.copyNew(source, DatabaseRef(id.name(), id.databaseId().uuid()))
            CopyMode.SECONDARY -> secondaries.copy(source, id)
        }
    }
}
