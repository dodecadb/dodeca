package org.dozendb.cluster.internal

import java.io.IOException
import java.util.UUID
import org.dozendb.cluster.DatabaseGroups
import org.dozendb.cluster.internal.recreation.Seed
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
import org.dozendb.core.edition.Quarantines
import org.dozendb.core.servers.CopyMode
import org.neo4j.dbms.database.DatabaseRepository
import org.neo4j.dbms.database.StandaloneDatabaseContext
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.storageengine.api.TransactionId

/**
 * This member's databases, through the database manager, their files, and their replication as
 * primaries or secondaries.
 *
 * @property entryTerm the term of a database's log entry, as the leader of its group tells it.
 * @property seed tells a database's group that this member holds its state up to a snapshot.
 * @property quarantines the copies of databases in quarantine on this member.
 * @property detachOnStart has the store of a database detach from the Raft group it was replicated
 *   in as the database starts next.
 * @property lastTransaction the last transaction of a database's store that does not run here, read
 *   from its files; null if it has none.
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
    private val quarantines: Quarantines,
    private val groups: DatabaseGroups = DatabaseGroups(),
    private val detachOnStart: (String) -> Unit = {},
    private val lastTransaction: (NamedDatabaseId) -> TransactionId? = { null },
) : LocalDatabases {
    override fun registered(): Map<NamedDatabaseId, LocalDatabase> =
        repository.registeredDatabases().mapValues { (id, context) ->
            LocalDatabase(
                started = context.database().isStarted,
                failed = context.isFailed,
                mode = hosting.mode(id),
                quarantined = quarantines.of(id.databaseId().uuid()) != null,
            )
        }

    override fun stored(): Set<String> = stores.stored()

    override fun complete(name: String): Boolean = stores.complete(name)

    // By the ids of their databases: `system`'s group may have one of its own.
    override fun groups(): Set<UUID> =
        replication.localGroups().mapTo(HashSet(), groups::databaseOf)

    override fun secondaries(): Set<UUID> = secondaries.marked()

    override fun quarantined(): Set<UUID> = quarantines.all().keys

    override fun forgetQuarantine(id: UUID) {
        quarantines.remove(id)
    }

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
        // Recorded first: a store the old group's Raft indexes are in may start only detached.
        if (seed == StoreSeed.Kept) detachOnStart(id.name())
        manager.startDatabase(
            id,
            when (seed) {
                null,
                StoreSeed.Kept -> null
                is StoreSeed.Uri -> DatabaseManager.Seed.Uri(seed.uri, seeder)
                StoreSeed.Restored -> DatabaseManager.Seed.Restored
            },
        )
        if (
            seed == StoreSeed.Kept &&
                repository.getDatabaseContext(id).map { it.database().isStarted }.orElse(false)
        ) {
            stores.unmarkSeed(id.name())
        }
        if (repository.getDatabaseContext(id).map { it.isFailed }.orElse(false)) {
            replication.handOverWhileStopped(id)
        }
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

    override fun seeds(): Map<String, UUID> = stores.seeds()

    override fun keepAsSeed(id: NamedDatabaseId, recreated: UUID) {
        replication.replica(id)?.handOverOnStop = false
        // Marked before its group goes: a store with neither counts as a leftover.
        stores.markSeed(id.name(), recreated)
        manager.releaseDatabase(id)
        replication.remove(id)
        secondaries.remove(id)
    }

    override fun markSeed(name: String, recreated: UUID) = stores.markSeed(name, recreated)

    override fun unmarkSeed(name: String) = stores.unmarkSeed(name)

    override fun seedState(name: String): Seed? =
        try {
            stores.storeState(name)?.let {
                Seed(it.appendIndex, it.lastTransaction, it.checksum, it.storeId)
            }
        } catch (_: IOException) {
            null
        }

    override fun deleteStore(name: String) = stores.delete(name)

    override fun removeGroup(id: UUID) = replication.removeGroup(groups.groupOf(id))

    override fun unmark(id: UUID) = secondaries.unmark(id)

    override fun seedSecondary(id: NamedDatabaseId) {
        // Marked before its seed mark goes: a store with neither counts as a leftover.
        secondaries.mark(id)
        stores.unmarkSeed(id.name())
    }

    override fun switch(id: NamedDatabaseId, mode: CopyMode, pulledBeyond: Long?): ModeSwitch =
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
            CopyMode.PRIMARY -> toPrimary(id, pulledBeyond)
        }

    /**
     * Hosts the secondary [id] as a primary from its own store, which seeds the group it joins with
     * the log entry of its last transaction, as far as the group's leader tells its term. A store
     * without replicated transactions, only those that created it, needs no seed: the group starts
     * from its first entry, or asks for a copy if its leader no longer holds that. Not before its
     * last transaction is beyond [pulledBeyond].
     */
    private fun toPrimary(id: NamedDatabaseId, pulledBeyond: Long?): ModeSwitch {
        val ref = groups.of(id)
        // Its last transaction only moves on.
        val pulled = secondaries.secondary(id)?.takeIf { it.pulling }?.last()?.appendIndex()
        if (pulledBeyond != null && pulled != null && pulled <= pulledBeyond) {
            return ModeSwitch.LATER
        }
        // It pulls no more meanwhile, so that its last transaction stays its last.
        val last = secondaries.stopPulling(id) ?: return ModeSwitch.NEEDS_COPY
        val index = last.consensusIndex()
        val told = if (index < 0) null else entryTerm(ref, index)
        if (told != null && told !is EntryTerm.Known) {
            secondaries.startPulling(id)
            return if (told == EntryTerm.Unknown) ModeSwitch.LATER else ModeSwitch.NEEDS_COPY
        }
        manager.releaseDatabase(id)
        if (told is EntryTerm.Known) seed(ref, Snapshot(LogPosition(index, told.term), index))
        secondaries.remove(id)
        create(id, CopyMode.PRIMARY)
        return ModeSwitch.SWITCHED
    }

    override fun rejoinFromStore(id: NamedDatabaseId): ModeSwitch {
        val ref = groups.of(id)
        val last = lastTransaction(id) ?: return ModeSwitch.NEEDS_COPY
        val index = last.consensusIndex()
        val told = if (index < 0) null else entryTerm(ref, index)
        if (told != null && told !is EntryTerm.Known) {
            return if (told == EntryTerm.Unknown) ModeSwitch.LATER else ModeSwitch.NEEDS_COPY
        }
        replication.replica(id)?.handOverOnStop = false
        if (repository.getDatabaseContext(id).isPresent) manager.releaseDatabase(id)
        replication.remove(id)
        if (told is EntryTerm.Known) seed(ref, Snapshot(LogPosition(index, told.term), index))
        create(id, CopyMode.PRIMARY)
        return ModeSwitch.SWITCHED
    }

    override fun copy(source: HostPort, id: NamedDatabaseId, mode: CopyMode) {
        // Reported in its mode while copied.
        hosting.host(id, mode)
        when (mode) {
            CopyMode.PRIMARY -> stores.copyNew(source, groups.of(id))
            CopyMode.SECONDARY -> secondaries.copy(source, id)
        }
    }
}
