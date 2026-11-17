package dev.dodeca.cluster.internal.replication

import dev.dodeca.cluster.internal.recreation.Election
import dev.dodeca.cluster.internal.recreation.RecreationSeeds
import dev.dodeca.cluster.internal.recreation.Seed
import dev.dodeca.cluster.internal.recreation.SeedElection
import dev.dodeca.consensus.HostPort
import dev.dodeca.consensus.Member
import dev.dodeca.consensus.MemberId
import dev.dodeca.core.edition.ServerSeedUri
import dev.dodeca.core.edition.UriSeed
import dev.dodeca.core.servers.AllocationException
import dev.dodeca.core.servers.CopyMode
import java.time.Duration
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.logging.InternalLog

/** A database as the system graph describes it. */
internal data class DesiredDatabase(
    val id: NamedDatabaseId,
    val online: Boolean,
    /** The member that creates its store, once the system leader named one. */
    val creator: MemberId?,
    /** The seed its creator creates its store from, a backup or dump; null for none. */
    val seed: UriSeed? = null,
    /** The member to name its creator, which may hold its restored store; null for any. */
    val designatedSeeder: MemberId? = null,
    /**
     * The members whose stores a database recreated by `dbms.recreateDatabase` is recreated from,
     * `seeding_servers`; [designatedSeeder] is the first.
     */
    val seeders: Set<MemberId> = setOfNotNull(designatedSeeder),
    /** The servers that host its primaries; none while it is not placed yet. */
    val hosts: Set<MemberId> = emptySet(),
    /** The servers that host its secondaries. */
    val secondaries: Set<MemberId> = emptySet(),
    /** Whether its store was created: from then on every server copies it, its creator too. */
    val created: Boolean = false,
    /** Whether it is the default database. */
    val default: Boolean = false,
    /** The id it had before `dbms.recreateDatabase` recreated it, if it did. */
    val recreatedFrom: UUID? = null,
    /**
     * What naming the creator of a database recreated from [seeders] waits for, or why it failed,
     * as recorded; null once its creator is named.
     */
    val recreationStatus: String? = null,
    /** Whether naming its creator failed, which ends its recreation: it waits to be recreated. */
    val recreationFailed: Boolean = false,
    /**
     * For a database recreated from [seeders], the append index of the last transaction of the
     * creator's seed: the other seeders pull beyond it before they join its group.
     */
    val seedAppendIndex: Long? = null,
    /**
     * When START DATABASE last started it, as the system graph records it: a new value asks a copy
     * whose start failed to start again, as Neo4j Enterprise documents for retrying a start.
     */
    val startedAt: Any? = null,
)

/** What the creator of a database creates its store from, instead of an empty one. */
internal sealed interface StoreSeed {
    /** A backup or dump at a URI, [seed]. */
    data class Uri(val seed: UriSeed) : StoreSeed

    /** The store `neo4j-admin database restore` prepared on this member. */
    data object Restored : StoreSeed

    /**
     * The store of the database it was recreated from, kept in place on this member as its seed,
     * which detaches from that database's Raft group as it starts.
     */
    data object Kept : StoreSeed
}

/** The replicated system graph, as far as databases are concerned. */
internal interface SystemGraph {
    /** Every database except `system`, as this member's copy of the system graph has them. */
    fun databases(): List<DesiredDatabase>

    /**
     * Waits until this member's copy of the system graph holds every system transaction the cluster
     * had committed when it was called. Returns false if the system leader could not confirm that.
     */
    fun awaitCurrent(): Boolean

    /** Whether this member is the system leader and can write the system graph. */
    fun leading(): Boolean

    /**
     * Names [creator] the creator of [database] and [initialMembers] the initial members of its
     * group, unless a creator is named already; for a database recreated from its seeders, with the
     * [seedAppendIndex] of the creator's seed.
     */
    fun nameCreator(
        database: NamedDatabaseId,
        creator: MemberId,
        initialMembers: List<Member>,
        seedAppendIndex: Long? = null,
    )

    /**
     * Records how naming the creator of [database], recreated from its seeders, goes: [message],
     * and whether it [failed]; unless a creator is named. Naming one clears it.
     */
    fun recordRecreation(database: NamedDatabaseId, message: String, failed: Boolean)

    /**
     * Makes [server], which hosts a secondary of [database], host a primary of it instead, unless a
     * creator is named: a primary turns secondary.
     *
     * @throws dev.dodeca.core.servers.AllocationException if no placement allows it.
     */
    fun promote(database: NamedDatabaseId, server: MemberId)
}

/** What [LocalDatabases.switch] did. */
internal enum class ModeSwitch {
    /** The database is registered again in the other mode, from its own store, and stopped. */
    SWITCHED,

    /** Its store cannot serve in the other mode, as the group moved on: it needs a copy. */
    NEEDS_COPY,

    /** Not now, as while the group has no leader: to be tried again. */
    LATER,
}

/**
 * The state of a database registered on this member, hosted in [mode]; [quarantined] if its copy is
 * in quarantine here.
 */
internal data class LocalDatabase(
    val started: Boolean,
    val failed: Boolean,
    val mode: CopyMode = CopyMode.PRIMARY,
    val quarantined: Boolean = false,
)

/** This member's databases: the database manager, their stores and their Raft groups. */
internal interface LocalDatabases {
    /** The databases registered with the database manager, started or not. */
    fun registered(): Map<NamedDatabaseId, LocalDatabase>

    /** Names of the databases with files on this member, complete or not. */
    fun stored(): Set<String>

    /** Whether the store of [name] is complete: present and not left half-copied. */
    fun complete(name: String): Boolean

    /** Ids of the Raft groups with state on this member. */
    fun groups(): Set<UUID>

    /** Ids of the databases whose stores here are secondaries'. */
    fun secondaries(): Set<UUID>

    /** Ids of the databases whose copies here are in quarantine. */
    fun quarantined(): Set<UUID>

    /** Forgets the quarantine of the database [id], which is gone. */
    fun forgetQuarantine(id: UUID)

    /** Whether [id] is being replaced by a copy because it fell behind its log or its upstreams. */
    fun catchingUp(id: NamedDatabaseId): Boolean

    /**
     * Whether this member votes in the group of [id], as it knows the group; null while it cannot
     * tell, as before the group runs here.
     */
    fun votes(id: NamedDatabaseId): Boolean?

    /**
     * Whether the group of [id] stopped on this member because this member is not one of its
     * members: the group's leader told it so, or it learnt that it was removed.
     */
    fun left(id: NamedDatabaseId): Boolean

    /**
     * Registers [id] with the database manager, hosted in [mode]; starting it creates its store if
     * there is none.
     */
    fun create(id: NamedDatabaseId, mode: CopyMode)

    /** Starts [id]; first, if it has no store and a [seed], creates its store from the seed. */
    fun start(id: NamedDatabaseId, seed: StoreSeed?)

    /** Stops [id], which stops on every member at once: leadership is not handed over. */
    fun stop(id: NamedDatabaseId)

    /** Stops [id] on this member only, which leaves the cluster: leadership is handed over. */
    fun leave(id: NamedDatabaseId)

    /**
     * Stops [id] and deletes its store and its Raft group; a quarantine of it here goes with it,
     * unless [keepQuarantine].
     */
    fun drop(id: NamedDatabaseId, keepQuarantine: Boolean = false)

    /**
     * The databases whose stores here are kept as the seeds of databases that
     * `dbms.recreateDatabase` recreated from them, by name, with the ids they seed.
     */
    fun seeds(): Map<String, UUID>

    /**
     * Keeps the store of [id], whose database is gone, as the seed of [recreated], recreated from
     * it: stops and unregisters [id], deletes its Raft group, keeps its files, marked a seed.
     */
    fun keepAsSeed(id: NamedDatabaseId, recreated: UUID)

    /** Marks the store of [name], which no database registered here has, a seed of [recreated]. */
    fun markSeed(name: String, recreated: UUID)

    /** Takes the seed mark off the store of [name], which its database runs from now. */
    fun unmarkSeed(name: String)

    /** What the seed kept of the database [name] holds; null if it cannot be read. */
    fun seedState(name: String): Seed?

    /**
     * Keeps the seed of [id], which another seeder creates, as the store of a secondary of [id],
     * which pulls the transactions it lacks: marks it a secondary's, and takes its seed mark off.
     */
    fun seedSecondary(id: NamedDatabaseId)

    /** Deletes the files of the database [name], which is not registered. */
    fun deleteStore(name: String)

    /** Deletes the Raft group [id], whose database is gone. */
    fun removeGroup(id: UUID)

    /** Forgets that the store of the database [id] is a secondary's. */
    fun unmark(id: UUID)

    /**
     * Hosts [id], registered here in the other mode, in [mode] from its own store: registers it
     * again, stopped. A primary must have left its group: its group goes, and its store is marked a
     * secondary's. A secondary's store seeds the group it then joins, as far as the group's leader
     * still holds the entry of its last transaction; [LATER][ModeSwitch.LATER] until the append
     * index of its last transaction is beyond [pulledBeyond], if given.
     */
    fun switch(id: NamedDatabaseId, mode: CopyMode, pulledBeyond: Long? = null): ModeSwitch

    /**
     * Replaces the Raft state of the primary [id], stopped here, so that it joins its group as a
     * new member from its own store, as a secondary that turns primary does: the log entry of the
     * store's last transaction seeds the group, as far as the group's leader still holds it, and
     * [id] is registered again, stopped. [NEEDS_COPY][ModeSwitch.NEEDS_COPY], changing nothing, if
     * the leader no longer holds it; [LATER][ModeSwitch.LATER] if the leader cannot tell now.
     */
    fun rejoinFromStore(id: NamedDatabaseId): ModeSwitch

    /**
     * Copies the store of [id], which this member lacks, from the member at [source], for a copy in
     * [mode]: a secondary's is marked once it is in place.
     */
    fun copy(source: HostPort, id: NamedDatabaseId, mode: CopyMode)
}

/**
 * Makes this member's databases follow the replicated system graph.
 *
 * Without clustering, a transaction event listener makes the databases follow the system graph
 * after every local commit to it. With clustering every member instead compares the system graph
 * with its own databases once it has started and after every system transaction it applies,
 * whichever member ran it.
 *
 * A member hosts the databases placed on it (see `dev.dodeca.core.servers.Placements`), as
 * primaries or secondaries. The store of a new database is created by one of them, its creator, and
 * copied by the others. The system leader names the creator of a database without one, itself if
 * the database is placed on it, else one of the servers it is placed on, in a replicated system
 * transaction, before the store is created: so the creator survives restarts and leader changes,
 * and no two members create the same database. The same transaction records the initial members of
 * the database's group, the servers it is placed on: every member that creates the group without
 * local state uses them, so that no two members bootstrap it differently. A member creates a store
 * only as the creator, only while the system graph does not record the store created, and only if
 * it has no Raft group for the database yet; otherwise it copies the store, from the members that
 * hold it, and its group waits to be added (see `MembershipReconciler`): so a creator that the
 * database moved off and back to copies it too. Stores are created and copied while their database
 * is online.
 *
 * A member that holds a database no longer placed on it keeps it while it votes in the database's
 * group, which the system leader then removes it from once the servers it is placed on vote, and
 * deletes it afterwards: a move never lowers the number of copies. A database not placed yet, as
 * the default database of a new cluster before the system leader places it, is left as it is.
 *
 * A secondary copies the store once its creator created it, and pulls its transactions. It votes in
 * no group: one no longer placed here goes once every secondary placed now runs, so that a move
 * never lowers the number of secondaries either; at once while the database is offline. A primary
 * placed here as a secondary leaves its group first, as one placed elsewhere does, then turns
 * secondary from its own store; a secondary placed here as a primary turns primary from its own
 * store and joins the group, unless the group's log moved on, when it is copied again
 * ([LocalDatabases.switch]).
 *
 * A local store counts only together with the Raft group of its database, or a secondary's with its
 * mark. A store without either is a leftover: of a database dropped while this member was down that
 * has the same name, or of a copy or creation that did not finish. The member that forms a cluster
 * with databases of its own starts their groups as it forms it, so that their stores count. Such
 * stores, Raft groups and databases the system graph lacks are deleted, but only once the system
 * leader confirmed that this member's copy of the system graph is current: after a restart it may
 * lack transactions this member already acted on.
 *
 * The default database is created, copied, started and stopped like any other; like without
 * clustering the reconciler never drops it, unless `dbms.recreateDatabase` recreated it under
 * another id. It is the one the system graph names, or while it names none, as before a cluster
 * formed, [initialDefault], the one `initial.dbms.default_database` names.
 *
 * A member removed from the cluster stops its databases, `system` aside, and changes nothing else.
 *
 * A copy in quarantine here is left as it is, stopped, until its quarantine is lifted, which
 * reconciles again; one whose database is dropped goes with its quarantine, and the quarantines of
 * databases that are gone are forgotten, as leftovers are.
 *
 * A database whose store is being replaced by catch-up is left alone. Work that cannot be done yet,
 * such as a copy whose creator has not created the store, is tried again after [retryDelay], and
 * after twice as long each time it still cannot be done, up to [maximumRetryDelay].
 *
 * @property sources the addresses of the members to copy a new database from, the members that hold
 *   it, its creator first if it has one.
 * @property systemMembers the servers of the cluster, the members of the `system` group and its
 *   secondaries, or null while a change of the members is not settled.
 * @property answering whether a member answers this one.
 * @property removed whether this member was removed from the cluster.
 * @property runs whether a member reports a database running, as far as this member knows.
 * @property seeds what the seeders of recreated databases reported of their seeds, for the system
 *   leader.
 */
internal class DatabaseReconciler(
    private val systemGraph: SystemGraph,
    private val databases: LocalDatabases,
    private val self: MemberId,
    private val initialDefault: String,
    private val sources: (DesiredDatabase) -> List<HostPort>,
    private val systemMembers: () -> List<Member>?,
    private val answering: (MemberId) -> Boolean,
    private val removed: () -> Boolean,
    private val runs: (MemberId, NamedDatabaseId) -> Boolean,
    private val seeds: RecreationSeeds,
    private val log: InternalLog,
    private val retryDelay: Duration = Duration.ofSeconds(2),
    private val maximumRetryDelay: Duration = Duration.ofSeconds(30),
) : AutoCloseable {
    private val executor: ScheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor { runnable ->
            Thread(runnable, "dodeca-reconciler").apply { isDaemon = true }
        }
    private val requested = AtomicBoolean(false)

    @Volatile private var started = false

    /**
     * What this member reports of the seeds it keeps as a seeder of recreated databases, by their
     * ids: each one's state, or null where it holds no store; left out until it kept its store.
     */
    @Volatile
    var seedReports: Map<UUID, Seed?> = emptyMap()
        private set

    // Used on the reconciler thread only.
    /** The states of the seeds reported, read once each. */
    private val seedStates = HashMap<UUID, Seed?>()

    /** The start request, [DesiredDatabase.startedAt], each copy here last started for. */
    private val startRequests = HashMap<UUID, Any?>()
    private var nextRetryDelay = retryDelay
    private var defaultDatabase = initialDefault
    private var awaitingConfirmation = false

    /** Reconciles now and whenever [request] asks for it. */
    fun start() {
        started = true
        request()
    }

    /** Asks for a reconciliation; requests made while one waits are merged. */
    fun request() {
        if (!started || !requested.compareAndSet(false, true)) return
        try {
            executor.execute(::run)
        } catch (_: RejectedExecutionException) {
            // Closing.
        }
    }

    private fun run() {
        requested.set(false)
        val again =
            try {
                reconcile()
            } catch (_: InterruptedException) {
                // Closing.
                Thread.currentThread().interrupt()
                return
            } catch (e: Exception) {
                log.error("Reconciling the databases with the system graph failed", e)
                true
            }
        if (!again) {
            nextRetryDelay = retryDelay
            return
        }
        try {
            executor.schedule(::request, nextRetryDelay.toMillis(), TimeUnit.MILLISECONDS)
        } catch (_: RejectedExecutionException) {
            // Closing.
        }
        nextRetryDelay = minOf(nextRetryDelay.multipliedBy(2), maximumRetryDelay)
    }

    /** Reconciles once, on the calling thread; returns whether some work has to be tried again. */
    fun reconcile(): Boolean {
        if (removed()) {
            for ((id, state) in databases.registered()) {
                if (id.isSystemDatabase || !state.started) continue
                log.info("Stopping database %s: this member was removed from the cluster", id)
                databases.leave(id)
            }
            return false
        }
        var desired = following(systemGraph.databases())
        var again = false
        if (deletions(desired).isNotEmpty()) {
            if (systemGraph.awaitCurrent()) {
                awaitingConfirmation = false
                desired = following(systemGraph.databases())
                for (deletion in deletions(desired)) {
                    again = !delete(deletion) || again
                }
            } else {
                if (!awaitingConfirmation) {
                    log.info(
                        "Deleting databases waits until the system leader confirms that this " +
                            "member's system graph is current"
                    )
                }
                awaitingConfirmation = true
                again = true
            }
        }
        reportSeeds(desired)
        for (database in desired) {
            again = !converge(database) || again
        }
        startRequests.keys.retainAll(desired.mapTo(HashSet()) { it.id.uuid })
        return again
    }

    /**
     * Updates [seedReports] for the databases among [desired] recreated from this member's store:
     * as soon as it is kept, or once there is none to keep.
     */
    private fun reportSeeds(desired: List<DesiredDatabase>) {
        val registered = databases.registered().keys.map { it.name() }.toSet()
        val stored = databases.stored()
        val reports = HashMap<UUID, Seed?>()
        for (database in desired) {
            if (!seeds(database)) continue
            val id = database.id.uuid
            val name = database.id.name()
            when {
                seeded(database) ->
                    reports[id] =
                        if (id in seedStates) seedStates[id]
                        else
                            databases.seedState(name).also {
                                if (it == null) {
                                    log.warn(
                                        "The store kept as the seed of %s cannot be read: this " +
                                            "member reports none",
                                        database.id,
                                    )
                                }
                                seedStates[id] = it
                            }
                name !in registered && name !in stored -> reports[id] = null
            }
        }
        seedStates.keys.retainAll(reports.keys)
        if (reports != seedReports) seedReports = reports
    }

    /** [desired], whose default database the reconciliation follows from now on. */
    private fun following(desired: List<DesiredDatabase>): List<DesiredDatabase> {
        defaultDatabase = desired.firstOrNull { it.default }?.id?.name() ?: initialDefault
        return desired
    }

    /** What [desired] tells to delete here. */
    private sealed interface Deletion {
        data class Database(val id: NamedDatabaseId) : Deletion

        /** The database [id] goes, its store kept as the seed of [recreated]. */
        data class Seed(val id: NamedDatabaseId, val recreated: UUID) : Deletion

        /** The store of [name], which no database here has, becomes the seed of [recreated]. */
        data class MarkSeed(val name: String, val recreated: UUID) : Deletion

        /** The seed mark of [name] goes: its database runs from the store. */
        data class Unmark(val name: String) : Deletion

        data class Store(val name: String) : Deletion

        data class Group(val id: UUID) : Deletion

        data class Mark(val id: UUID) : Deletion

        data class Quarantine(val id: UUID) : Deletion
    }

    private fun deletions(desired: List<DesiredDatabase>): List<Deletion> {
        val wanted = desired.associateBy { it.id.name() }
        val registered = databases.registered().keys
        // The default database stays, unless it was recreated under another id.
        val dropped = registered.filter {
            val database = wanted[it.name()]
            !it.isSystemDatabase &&
                database?.id != it &&
                (it.name() != defaultDatabase || database?.recreatedFrom == it.uuid)
        }
        val seeds = databases.seeds()
        // A seed is kept while it seeds the database of its name, and a store that no database
        // here has becomes one if it may.
        val (seeding, leftovers) =
            databases
                .stored()
                .filter { name ->
                    val database = wanted[name]
                    name != SYSTEM.name() &&
                        registered.none { it.name() == name } &&
                        (database == null || (database.online && !trusted(database.id))) &&
                        (name != defaultDatabase || database?.recreatedFrom != null)
                }
                .filterNot { name ->
                    wanted[name]?.let { seeds[name] == it.id.uuid && seeds(it) } == true
                }
                .partition { name ->
                    val database = wanted[name]
                    // A seed of a recreation that is recreated again seeds the new one.
                    database != null &&
                        seeds(database) &&
                        databases.complete(name) &&
                        (name !in seeds || seeds[name] == database.recreatedFrom)
                }
        val unmarked =
            seeds.keys.filter { name ->
                registered.any { it.name() == name && trusted(it) } &&
                    wanted[name]?.id?.uuid == seeds[name]
            }
        val known = desired.map { it.id.uuid }.toSet() + registered.map { it.uuid }
        val groups = databases.groups() - SYSTEM.uuid - known
        val marks = databases.secondaries() - known
        val quarantines = databases.quarantined() - SYSTEM.uuid - known
        return dropped.map { id ->
            val recreated = wanted[id.name()]?.takeIf { it.recreatedFrom == id.uuid && seeds(it) }
            if (recreated != null) Deletion.Seed(id, recreated.id.uuid) else Deletion.Database(id)
        } +
            seeding.map { Deletion.MarkSeed(it, checkNotNull(wanted[it]).id.uuid) } +
            unmarked.map { Deletion.Unmark(it) } +
            // The default database's files are replaced by a copy instead (see create).
            leftovers.filter { it != defaultDatabase }.map { Deletion.Store(it) } +
            groups.map { Deletion.Group(it) } +
            marks.map { Deletion.Mark(it) } +
            quarantines.map { Deletion.Quarantine(it) }
    }

    /** Returns false if [deletion] has to wait. */
    private fun delete(deletion: Deletion): Boolean {
        when (deletion) {
            is Deletion.Database -> {
                // Catch-up would start it again.
                if (databases.catchingUp(deletion.id)) return false
                log.info("Dropping database %s, which the system graph lacks", deletion.id)
                databases.drop(deletion.id)
            }
            is Deletion.Seed -> {
                if (databases.catchingUp(deletion.id)) return false
                log.info(
                    "Keeping the store of database %s as the seed of %s, recreated from it",
                    deletion.id,
                    deletion.recreated,
                )
                databases.keepAsSeed(deletion.id, deletion.recreated)
            }
            is Deletion.MarkSeed -> {
                log.info(
                    "Keeping the files of database %s as the seed of %s, recreated from it",
                    deletion.name,
                    deletion.recreated,
                )
                databases.markSeed(deletion.name, deletion.recreated)
            }
            is Deletion.Unmark -> databases.unmarkSeed(deletion.name)
            is Deletion.Store -> {
                log.info("Deleting the leftover files of database %s", deletion.name)
                databases.deleteStore(deletion.name)
            }
            is Deletion.Group -> {
                log.info(
                    "Deleting Raft group %s, whose database the system graph lacks",
                    deletion.id,
                )
                databases.removeGroup(deletion.id)
            }
            is Deletion.Mark -> {
                log.info(
                    "Forgetting secondary %s, whose database the system graph lacks",
                    deletion.id,
                )
                databases.unmark(deletion.id)
            }
            is Deletion.Quarantine -> {
                log.info(
                    "Forgetting the quarantine of %s, whose database the system graph lacks",
                    deletion.id,
                )
                databases.forgetQuarantine(deletion.id)
            }
        }
        return true
    }

    /**
     * Whether the local store of [id] belongs to it as a copy in [mode]: complete, with the
     * database's Raft group for a primary, marked for a secondary.
     */
    private fun trusted(id: NamedDatabaseId, mode: CopyMode): Boolean =
        databases.complete(id.name()) &&
            when (mode) {
                CopyMode.PRIMARY -> id.uuid in databases.groups()
                CopyMode.SECONDARY -> id.uuid in databases.secondaries()
            }

    /** Whether the local store of [id] belongs to it, in either mode. */
    private fun trusted(id: NamedDatabaseId): Boolean =
        trusted(id, CopyMode.PRIMARY) || trusted(id, CopyMode.SECONDARY)

    /** Brings [database] in line with the system graph; returns false to be tried again. */
    private fun converge(database: DesiredDatabase): Boolean {
        val id = database.id
        if (databases.catchingUp(id)) return false
        val state = databases.registered()[id]
        // Left as it is until its quarantine is lifted, which reconciles again; also once its store
        // and group are deleted, as when it joins as a new member: it must not run the group
        // before its quarantine is lifted.
        if (state?.quarantined == true) return true
        if (state == null && id.uuid in databases.quarantined() && !trusted(id)) return true
        val placed = database.hosts.isNotEmpty()
        val wanted =
            when (self) {
                in database.hosts -> CopyMode.PRIMARY
                in database.secondaries -> CopyMode.SECONDARY
                else -> null
            }
        // A primary no longer placed here as one, which waits to leave the database's group.
        val leaving = placed && state?.mode == CopyMode.PRIMARY && wanted != CopyMode.PRIMARY
        when {
            state == null ->
                return when {
                    // A copy of another database of the name goes first; the default database's
                    // is kept instead, as without clustering.
                    databases.registered().keys.any { it.name() == id.name() } ->
                        id.name() == defaultDatabase
                    wanted != null -> create(database, wanted)
                    // A copy that moved off this member while it was down starts as it was: a
                    // primary's group counts it in until the system leader removes it, and a
                    // secondary serves until its replacement runs. It goes as below.
                    database.online && trusted(id) -> create(database, CopyMode.PRIMARY)
                    // Placed elsewhere: as the system leader, it names the creator, in a system
                    // transaction, which reconciles again; tried again if it failed.
                    placed ->
                        if (
                            database.online &&
                                database.creator == null &&
                                !database.created &&
                                systemGraph.leading()
                        ) {
                            nameCreator(database)
                        } else {
                            true
                        }
                    // Not placed yet: the system leader places it.
                    else -> true
                }
            // No group counts a secondary in: it goes at once, or turns primary from its store.
            state.mode == CopyMode.SECONDARY && wanted != CopyMode.SECONDARY -> {
                if (wanted == CopyMode.PRIMARY) {
                    // A seeder's store holds the old group's transactions up to the creator's
                    // seed, whose Raft indexes mean nothing in the new group.
                    when (databases.switch(id, CopyMode.PRIMARY, database.seedAppendIndex)) {
                        ModeSwitch.SWITCHED -> {
                            log.info(
                                "Secondary %s here turned primary from its own store, placed " +
                                    "here as one: it joins the database's group",
                                id,
                            )
                            if (database.online) databases.start(id, seed(database))
                            return true
                        }
                        ModeSwitch.LATER -> return false
                        ModeSwitch.NEEDS_COPY -> {}
                    }
                }
                // A replacement first.
                if (
                    wanted == null && database.online && database.secondaries.any { !runs(it, id) }
                ) {
                    return false
                }
                log.info(
                    "Deleting secondary %s here: %s",
                    id,
                    if (wanted == null) "it is placed on other servers"
                    else
                        "it is placed here as a primary, and the database's group no longer " +
                            "holds what its store needs: it is copied anew",
                )
                databases.drop(id)
                // The primary is created next time.
                return wanted == null
            }
            leaving && databases.votes(id) == false -> {
                if (wanted == CopyMode.SECONDARY) {
                    when (databases.switch(id, CopyMode.SECONDARY)) {
                        ModeSwitch.SWITCHED -> {
                            log.info(
                                "Database %s here left its group, placed here as a secondary: it " +
                                    "turned secondary from its own store",
                                id,
                            )
                            if (database.online) databases.start(id, null)
                            return true
                        }
                        ModeSwitch.LATER -> return false
                        ModeSwitch.NEEDS_COPY -> {}
                    }
                }
                log.info(
                    "Deleting database %s here: it is placed on other servers%s, and this member " +
                        "left its group",
                    id,
                    if (wanted == CopyMode.SECONDARY) " as a primary, here as a secondary" else "",
                )
                databases.drop(id)
                // The secondary is copied next time.
                return wanted == null
            }
            // As when it started its group as a member, which the group removed meanwhile.
            wanted == CopyMode.PRIMARY && databases.left(id) -> {
                // Only once the system leader confirmed it is placed here: a member that was
                // removed from the cluster stops its databases instead, and keeps them.
                if (!systemGraph.awaitCurrent()) return false
                if (systemGraph.databases().none { it.id == id && self in it.hosts }) return false
                log.info(
                    "Deleting database %s here, which is placed on this member: its group " +
                        "stopped here, as its members do not count this member in. It is copied " +
                        "again, and joins the group",
                    id,
                )
                databases.drop(id)
                return false
            }
            database.online && !state.started && (!state.failed || newStartRequest(database)) -> {
                if (state.failed) {
                    log.info("Starting database %s again, as asked: its last start failed", id)
                } else {
                    log.info("Starting database %s, online in the system graph", id)
                }
                startRequests[id.uuid] = database.startedAt
                databases.start(id, if (state.mode == CopyMode.PRIMARY) seed(database) else null)
            }
            // Stopping also clears a failure, so that starting it again tries anew.
            !database.online && (state.started || state.failed) -> {
                log.info("Stopping database %s, offline in the system graph", id)
                databases.stop(id)
            }
        }
        // A copy no longer placed here waits until this member leaves the database's group.
        return !leaving
    }

    /**
     * Whether [database], whose copy here failed, was asked to start since this member last started
     * it. A failed copy that this member did not start, seen for the first time, failed for the
     * request it sees: it waits for the next.
     */
    private fun newStartRequest(database: DesiredDatabase): Boolean =
        startRequests.getOrPut(database.id.uuid) { database.startedAt } != database.startedAt

    /** Creates [database] here, as [wanted] places it; returns false to be tried again. */
    private fun create(database: DesiredDatabase, wanted: CopyMode): Boolean {
        val id = database.id
        // A seeder that does not create the database runs its seed as a secondary, which pulls
        // what it lacks from the others, and turns primary if it is placed here as one.
        val creator = database.creator
        if (database.online && creator != null && creator != self && seeded(database)) {
            log.info(
                "Starting the seed of %s as a secondary, which pulls the transactions it lacks; " +
                    "server %s creates the database",
                id,
                creator,
            )
            databases.seedSecondary(id)
        }
        // A store of the other mode starts in its mode, then switches, as one that runs does.
        val mode =
            when {
                trusted(id, wanted) -> wanted
                trusted(id, wanted.other) -> wanted.other
                else -> wanted
            }
        if (!trusted(id, mode)) {
            // Created or copied once it is started.
            if (!database.online) return true
            // Recreating it failed: it waits to be recreated again, or dropped.
            if (database.recreationFailed && database.creator == null) return true
            // A leftover, deleted once the system graph is confirmed current. The default
            // database's files are kept instead, and replaced by a copy, as when this member
            // stopped while it started a copy, before its group ran here. A kept seed is no
            // leftover: the creator creates the database from it, and another seeder keeps it
            // until a copy from the creator replaces it.
            if (
                id.name() in databases.stored() && id.name() != defaultDatabase && !seeded(database)
            ) {
                return false
            }
            if (!provideStore(database, mode)) return false
        }
        log.info("Creating %s %s", if (mode == CopyMode.PRIMARY) "database" else "secondary", id)
        databases.create(id, mode)
        if (database.online) {
            databases.start(id, if (mode == CopyMode.PRIMARY) seed(database) else null)
        }
        return true
    }

    /**
     * Whether this member keeps its store of the database [database] was recreated from as its
     * seed: as one of its seeders, until its store was created, by its creator from its own seed.
     * Another seeder keeps it until a copy from the creator replaces it, should the recreation be
     * tried again meanwhile.
     */
    private fun seeds(database: DesiredDatabase): Boolean =
        database.recreatedFrom != null && self in database.seeders && !database.created

    /** Whether this member holds a kept seed of [database]. */
    private fun seeded(database: DesiredDatabase): Boolean =
        databases.seeds()[database.id.name()] == database.id.uuid

    /** What this member creates the store of [database] from: only as its creator. */
    private fun seed(database: DesiredDatabase): StoreSeed? =
        when {
            database.creator != self -> null
            seeded(database) -> StoreSeed.Kept
            database.seed != null -> StoreSeed.Uri(database.seed)
            database.designatedSeeder == self -> StoreSeed.Restored
            else -> null
        }

    /**
     * Makes sure that [database] can start here in [mode]: with a copy of its store, or as its
     * creator, which is a primary. Returns false to be tried again.
     */
    private fun provideStore(database: DesiredDatabase, mode: CopyMode): Boolean {
        val id = database.id
        val creator = database.creator
        return when {
            database.created -> copyFromCluster(database, mode)
            // Also for a secondary: nothing is copied before its creator created the store.
            creator == null -> {
                // The creator is named in a system transaction, which reconciles again.
                if (systemGraph.leading()) nameCreator(database)
                false
            }
            mode == CopyMode.PRIMARY && creator == self && id.uuid !in databases.groups() -> {
                log.info("Creating the store of new database %s, as its creator", id)
                true
            }
            else -> copyFromCluster(database, mode)
        }
    }

    /** The server that reads the `server:` seed of [database], by its id; null for none. */
    private fun reader(database: DesiredDatabase): MemberId? =
        database.seed
            ?.let { runCatching { ServerSeedUri.parse(it.uri) }.getOrNull() }
            ?.let { runCatching { MemberId.parse(it.server) }.getOrNull() }

    /**
     * Names the creator of [database] among the servers it is placed on: for one recreated from the
     * stores of its seeders, the seeder whose store is the latest ([SeedElection]); else its
     * designated seeder if it has one, which is placed on it first, else this member, else those
     * that answer first, then by id. They are the initial members of its group. Returns false to be
     * tried again; true once recreating the database failed, which it records.
     */
    private fun nameCreator(database: DesiredDatabase): Boolean {
        val id = database.id
        if (database.recreationFailed) return true
        val members = systemMembers()
        if (members == null) {
            log.info("Naming the creator of %s waits until the change of members settles", id)
            return false
        }
        val hosting = members.filter { it.id in database.hosts }
        var seedAppendIndex: Long? = null
        val creator =
            if (database.recreatedFrom != null && database.seeders.isNotEmpty()) {
                when (val elected = SeedElection.elect(id, database.seeders, seeds)) {
                    is Election.Creator ->
                        if (elected.member in database.secondaries) {
                            return promote(database, elected.member)
                        } else {
                            seedAppendIndex = elected.appendIndex
                            elected.member
                        }
                    is Election.Waiting -> {
                        seeds.refresh()
                        recordRecreation(database, elected.message, failed = false)
                        return false
                    }
                    is Election.Failed -> {
                        recordRecreation(database, elected.message, failed = true)
                        return true
                    }
                    is Election.NoStore -> {
                        if (database.seed == null) {
                            recordRecreation(database, elected.message, failed = true)
                            return true
                        }
                        log.info(
                            "%s Database %s is recreated from its seed URI instead",
                            elected.message,
                            id,
                        )
                        reader(database) ?: anyCreator(database, hosting)
                    }
                }
            } else {
                database.designatedSeeder ?: reader(database) ?: anyCreator(database, hosting)
            }
        // It starts the database's group, so it must be one of its initial members.
        if (creator == null || hosting.none { it.id == creator }) {
            log.warn(
                "Naming the creator of %s waits until the servers it is placed on are members of " +
                    "the cluster",
                id,
            )
            return false
        }
        try {
            systemGraph.nameCreator(id, creator, hosting, seedAppendIndex)
        } catch (e: InterruptedException) {
            throw e
        } catch (e: Exception) {
            log.info("Naming %s the creator of %s failed: %s", creator, id, e.message)
        }
        return false
    }

    /** This member if [database] is placed on it, else of [hosting] the first that answers. */
    private fun anyCreator(database: DesiredDatabase, hosting: List<Member>): MemberId? =
        self.takeIf { it in database.hosts }
            ?: hosting
                .sortedWith(compareBy<Member> { !answering(it.id) }.thenBy { it.id })
                .firstOrNull()
                ?.id

    /**
     * Makes [seeder], which holds the latest store of the recreated [database] and is placed on it
     * as a secondary, host a primary, so that it creates the database's group; records the failure
     * if no placement allows it. Returns as [nameCreator] does.
     */
    private fun promote(database: DesiredDatabase, seeder: MemberId): Boolean {
        log.info(
            "Seeder %s holds the latest store of recreated database %s, where it hosts a " +
                "secondary: it hosts a primary instead, to create it",
            seeder,
            database.id,
        )
        return try {
            systemGraph.promote(database.id, seeder)
            false
        } catch (e: InterruptedException) {
            throw e
        } catch (e: AllocationException) {
            recordRecreation(
                database,
                "Seeder $seeder holds the latest store, but cannot host a primary: ${e.message}.",
                failed = true,
            )
            true
        } catch (e: Exception) {
            log.info("Placing a primary of %s on %s failed: %s", database.id, seeder, e.message)
            false
        }
    }

    /** Records [message] as the status of recreating [database], unless it is recorded already. */
    private fun recordRecreation(database: DesiredDatabase, message: String, failed: Boolean) {
        if (database.recreationStatus == message && database.recreationFailed == failed) return
        if (failed) {
            log.warn("Recreating database %s failed: %s", database.id, message)
        } else {
            log.info("Recreating database %s: %s", database.id, message)
        }
        try {
            systemGraph.recordRecreation(database.id, message, failed)
        } catch (e: InterruptedException) {
            throw e
        } catch (e: Exception) {
            log.info("Recording how recreating %s goes failed: %s", database.id, e.message)
        }
    }

    private fun copyFromCluster(database: DesiredDatabase, mode: CopyMode): Boolean {
        val id = database.id
        for (source in sources(database)) {
            try {
                log.info(
                    "Copying new %s %s from %s",
                    if (mode == CopyMode.PRIMARY) "database" else "secondary",
                    id,
                    source,
                )
                databases.copy(source, id, mode)
                return true
            } catch (e: InterruptedException) {
                throw e
            } catch (e: Exception) {
                log.info("Copying database %s from %s failed: %s", id, source, e.message)
            }
        }
        return false
    }

    override fun close() {
        started = false
        executor.shutdownNow()
        executor.awaitTermination(30, TimeUnit.SECONDS)
    }

    private companion object {
        val SYSTEM: NamedDatabaseId = NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID

        val NamedDatabaseId.uuid: UUID
            get() = databaseId().uuid()
    }
}
