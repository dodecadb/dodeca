package org.dozendb.cluster.internal.replication

import java.time.Duration
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.Member
import org.dozendb.consensus.MemberId
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.logging.InternalLog

/** A database as the system graph describes it. */
internal data class DesiredDatabase(
    val id: NamedDatabaseId,
    val online: Boolean,
    /** The member that creates its store, once the system leader named one. */
    val creator: MemberId?,
    /** The seed its creator creates its store from, a dump or full backup; null for none. */
    val seedUri: String? = null,
    /** The member to name its creator, which may hold its restored store; null for any. */
    val designatedSeeder: MemberId? = null,
    /** The servers it is placed on; none while it is not placed yet. */
    val hosts: Set<MemberId> = emptySet(),
    /** Whether its store was created: from then on every server copies it, its creator too. */
    val created: Boolean = false,
)

/** What the creator of a database creates its store from, instead of an empty one. */
internal sealed interface StoreSeed {
    /** A dump or full backup at [uri]. */
    data class Uri(val uri: String) : StoreSeed

    /** The store `neo4j-admin database restore` prepared on this member. */
    data object Restored : StoreSeed
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
     * group, unless a creator is named already.
     */
    fun nameCreator(database: NamedDatabaseId, creator: MemberId, initialMembers: List<Member>)
}

/** The state of a database registered on this member. */
internal data class LocalDatabase(val started: Boolean, val failed: Boolean)

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

    /** Whether [id] is being replaced by a copy because it fell behind its group's log. */
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

    /** Registers [id] with the database manager; starting it creates its store if there is none. */
    fun create(id: NamedDatabaseId)

    /** Starts [id]; first, if it has no store and a [seed], creates its store from the seed. */
    fun start(id: NamedDatabaseId, seed: StoreSeed?)

    /** Stops [id], which stops on every member at once: leadership is not handed over. */
    fun stop(id: NamedDatabaseId)

    /** Stops [id] on this member only, which leaves the cluster: leadership is handed over. */
    fun leave(id: NamedDatabaseId)

    /** Stops [id] and deletes its store and its Raft group. */
    fun drop(id: NamedDatabaseId)

    /** Deletes the files of the database [name], which is not registered. */
    fun deleteStore(name: String)

    /** Deletes the Raft group [id], whose database is gone. */
    fun removeGroup(id: UUID)

    /** Copies the store of [id], which this member lacks, from the member at [source]. */
    fun copy(source: HostPort, id: NamedDatabaseId)
}

/**
 * Makes this member's databases follow the replicated system graph.
 *
 * Without clustering, a transaction event listener makes the databases follow the system graph
 * after every local commit to it. With clustering every member instead compares the system graph
 * with its own databases once it has started and after every system transaction it applies,
 * whichever member ran it.
 *
 * A member hosts the databases placed on it (see `org.dozendb.core.servers.Placements`). The store
 * of a new database is created by one of them, its creator, and copied by the others. The system
 * leader names the creator of a database without one, itself if the database is placed on it, else
 * one of the servers it is placed on, in a replicated system transaction, before the store is
 * created: so the creator survives restarts and leader changes, and no two members create the same
 * database. The same transaction records the initial members of the database's group, the servers
 * it is placed on: every member that creates the group without local state uses them, so that no
 * two members bootstrap it differently. A member creates a store only as the creator, only while
 * the system graph does not record the store created, and only if it has no Raft group for the
 * database yet; otherwise it copies the store, from the members that hold it, and its group waits
 * to be added (see `MembershipReconciler`): so a creator that the database moved off and back to
 * copies it too. Stores are created and copied while their database is online.
 *
 * A member that holds a database no longer placed on it keeps it while it votes in the database's
 * group, which the system leader then removes it from once the servers it is placed on vote, and
 * deletes it afterwards: a move never lowers the number of copies. A database not placed yet, as
 * the default database of a new cluster before the system leader places it, is left as it is.
 *
 * A local store counts only together with the Raft group of its database. A store without one is a
 * leftover: of a database dropped while this member was down that has the same name, or of a copy
 * or creation that did not finish. The member that forms a cluster with databases of its own starts
 * their groups as it forms it, so that their stores count. Such stores, Raft groups and databases
 * the system graph lacks are deleted, but only once the system leader confirmed that this member's
 * copy of the system graph is current: after a restart it may lack transactions this member already
 * acted on.
 *
 * The default database is created, copied, started and stopped like any other; like without
 * clustering the reconciler never drops it while the system graph has it.
 *
 * A member removed from the cluster stops its databases, `system` aside, and changes nothing else.
 *
 * A database whose store is being replaced by catch-up is left alone. Work that cannot be done yet,
 * such as a copy whose creator has not created the store, is tried again after [retryDelay], and
 * after twice as long each time it still cannot be done, up to [maximumRetryDelay].
 *
 * @property sources the addresses of the members to copy a new database from, the members that hold
 *   it, its creator first if it has one.
 * @property systemMembers the members of the `system` group, or null while a change of them is not
 *   settled.
 * @property answering whether a member answers this one.
 * @property removed whether this member was removed from the cluster.
 */
internal class DatabaseReconciler(
    private val systemGraph: SystemGraph,
    private val databases: LocalDatabases,
    private val self: MemberId,
    private val defaultDatabase: String,
    private val sources: (DesiredDatabase) -> List<HostPort>,
    private val systemMembers: () -> List<Member>?,
    private val answering: (MemberId) -> Boolean,
    private val removed: () -> Boolean,
    private val log: InternalLog,
    private val retryDelay: Duration = Duration.ofSeconds(2),
    private val maximumRetryDelay: Duration = Duration.ofSeconds(30),
) : AutoCloseable {
    private val executor: ScheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor { runnable ->
            Thread(runnable, "dozendb-reconciler").apply { isDaemon = true }
        }
    private val requested = AtomicBoolean(false)

    @Volatile private var started = false

    // Used on the reconciler thread only.
    private var nextRetryDelay = retryDelay
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
        var desired = systemGraph.databases()
        var again = false
        if (deletions(desired).isNotEmpty()) {
            if (systemGraph.awaitCurrent()) {
                awaitingConfirmation = false
                desired = systemGraph.databases()
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
        for (database in desired) {
            again = !converge(database) || again
        }
        return again
    }

    /** What [desired] tells to delete here. */
    private sealed interface Deletion {
        data class Database(val id: NamedDatabaseId) : Deletion

        data class Store(val name: String) : Deletion

        data class Group(val id: UUID) : Deletion
    }

    private fun deletions(desired: List<DesiredDatabase>): List<Deletion> {
        val wanted = desired.associateBy { it.id.name() }
        val registered = databases.registered().keys
        val dropped = registered.filter {
            !it.isSystemDatabase && it.name() != defaultDatabase && wanted[it.name()]?.id != it
        }
        // The files of system and the default database are kept, to form a cluster from; see
        // create for those of the default database that need replacing.
        val stores =
            databases.stored().filter { name ->
                val database = wanted[name]
                name != SYSTEM.name() &&
                    name != defaultDatabase &&
                    registered.none { it.name() == name } &&
                    (database == null || (database.online && !trusted(database.id)))
            }
        val groups =
            databases.groups() -
                SYSTEM.uuid -
                desired.map { it.id.uuid }.toSet() -
                registered.map { it.uuid }.toSet()
        return dropped.map { Deletion.Database(it) } +
            stores.map { Deletion.Store(it) } +
            groups.map { Deletion.Group(it) }
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
        }
        return true
    }

    /** Whether the local store of [id] belongs to it: complete, with the database's Raft group. */
    private fun trusted(id: NamedDatabaseId): Boolean =
        databases.complete(id.name()) && id.uuid in databases.groups()

    /** Brings [database] in line with the system graph; returns false to be tried again. */
    private fun converge(database: DesiredDatabase): Boolean {
        val id = database.id
        if (databases.catchingUp(id)) return false
        val state = databases.registered()[id]
        val placed = database.hosts.isNotEmpty()
        val here = self in database.hosts
        val leaving = placed && !here
        when {
            state == null ->
                return when {
                    // A copy of another database of the name goes first; the default database's
                    // is kept instead, as without clustering.
                    databases.registered().keys.any { it.name() == id.name() } ->
                        id.name() == defaultDatabase
                    here -> create(database)
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
                            false
                        } else {
                            true
                        }
                    // Not placed yet: the system leader places it.
                    else -> true
                }
            leaving && databases.votes(id) == false -> {
                log.info(
                    "Deleting database %s here: it is placed on other servers, and this member " +
                        "left its group",
                    id,
                )
                databases.drop(id)
                return true
            }
            // As when it started its group as a member, which the group removed meanwhile.
            here && databases.left(id) -> {
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
            database.online && !state.started && !state.failed -> {
                log.info("Starting database %s, online in the system graph", id)
                databases.start(id, seed(database))
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

    /** Creates [database] here; returns false to be tried again. */
    private fun create(database: DesiredDatabase): Boolean {
        val id = database.id
        if (!trusted(id)) {
            // Created or copied once it is started.
            if (!database.online) return true
            // A leftover, deleted once the system graph is confirmed current. The default
            // database's files are kept instead, and replaced by a copy, as when this member
            // stopped while it started a copy, before its group ran here.
            if (id.name() in databases.stored() && id.name() != defaultDatabase) return false
            if (!provideStore(database)) return false
        }
        log.info("Creating database %s", id)
        databases.create(id)
        if (database.online) databases.start(id, seed(database))
        return true
    }

    /** What this member creates the store of [database] from: only as its creator. */
    private fun seed(database: DesiredDatabase): StoreSeed? =
        when {
            database.creator != self -> null
            database.seedUri != null -> StoreSeed.Uri(database.seedUri)
            database.designatedSeeder == self -> StoreSeed.Restored
            else -> null
        }

    /**
     * Makes sure that [database] can start here: with a copy of its store, or as its creator.
     * Returns false to be tried again.
     */
    private fun provideStore(database: DesiredDatabase): Boolean {
        val id = database.id
        val creator = database.creator
        return when {
            database.created -> copyFromCluster(database)
            creator == null -> {
                // The creator is named in a system transaction, which reconciles again.
                if (systemGraph.leading()) nameCreator(database)
                false
            }
            creator == self && id.uuid !in databases.groups() -> {
                log.info("Creating the store of new database %s, as its creator", id)
                true
            }
            else -> copyFromCluster(database)
        }
    }

    /**
     * Names the creator of [database] among the servers it is placed on: its designated seeder if
     * it has one, which is placed on it first, else this member, else those that answer first, then
     * by id. They are the initial members of its group.
     */
    private fun nameCreator(database: DesiredDatabase) {
        val id = database.id
        val members = systemMembers()
        if (members == null) {
            log.info("Naming the creator of %s waits until the change of members settles", id)
            return
        }
        val hosting = members.filter { it.id in database.hosts }
        val creator =
            database.designatedSeeder
                ?: self.takeIf { it in database.hosts }
                ?: hosting
                    .sortedWith(compareBy<Member> { !answering(it.id) }.thenBy { it.id })
                    .firstOrNull()
                    ?.id
        // It starts the database's group, so it must be one of its initial members.
        if (creator == null || hosting.none { it.id == creator }) {
            log.warn(
                "Naming the creator of %s waits until the servers it is placed on are members of " +
                    "the cluster",
                id,
            )
            return
        }
        try {
            systemGraph.nameCreator(id, creator, hosting)
        } catch (e: InterruptedException) {
            throw e
        } catch (e: Exception) {
            log.info("Naming %s the creator of %s failed: %s", creator, id, e.message)
        }
    }

    private fun copyFromCluster(database: DesiredDatabase): Boolean {
        val id = database.id
        for (source in sources(database)) {
            try {
                log.info("Copying new database %s from %s", id, source)
                databases.copy(source, id)
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
