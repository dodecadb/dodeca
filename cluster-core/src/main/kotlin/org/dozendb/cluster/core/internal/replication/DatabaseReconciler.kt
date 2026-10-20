package org.dozendb.cluster.core.internal.replication

import java.time.Duration
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import org.dozendb.cluster.api.HostPort
import org.dozendb.cluster.api.Member
import org.dozendb.cluster.api.MemberId
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.logging.InternalLog

/** A database as the system graph describes it. */
internal data class DesiredDatabase(
    val id: NamedDatabaseId,
    val online: Boolean,
    /** The member that creates its store, once the system leader named one. */
    val creator: MemberId?,
)

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

    /** Registers [id] with the database manager; starting it creates its store if there is none. */
    fun create(id: NamedDatabaseId)

    fun start(id: NamedDatabaseId)

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
 * The store of a new database is created by one member, its creator, and copied by every other
 * member. The system leader names itself the creator of a database without one, in a replicated
 * system transaction, before it creates the store: so the creator survives restarts and leader
 * changes, and no two members create the same database. The same transaction records the initial
 * members of the database's group, the settled members of the `system` group: every member that
 * creates the group without local state uses them, so that no two members bootstrap it differently
 * while the membership changes. A member creates a store only as the creator, and only if it has no
 * Raft group for the database yet; otherwise it copies the store. Stores are created and copied
 * while their database is online.
 *
 * A local store counts only together with the Raft group of its database. A store without one is a
 * leftover: of a database dropped while this member was down that has the same name, or of a copy
 * or creation that did not finish. Such stores, Raft groups and databases the system graph lacks
 * are deleted, but only once the system leader confirmed that this member's copy of the system
 * graph is current: after a restart it may lack transactions this member already acted on.
 *
 * The default database is created at startup, and like without clustering the reconciler never
 * creates or drops it; it starts and stops it like any other.
 *
 * A member removed from the cluster stops its databases, `system` aside, and changes nothing else.
 *
 * A database whose store is being replaced by catch-up is left alone. Work that cannot be done yet,
 * such as a copy whose creator has not created the store, is tried again after [retryDelay], and
 * after twice as long each time it still cannot be done, up to [maximumRetryDelay].
 *
 * @property sources the addresses of the members to copy a new database from, its creator first.
 * @property systemMembers the members of the `system` group, or null while a change of them is not
 *   settled.
 * @property removed whether this member was removed from the cluster.
 */
internal class DatabaseReconciler(
    private val systemGraph: SystemGraph,
    private val databases: LocalDatabases,
    private val self: MemberId,
    private val defaultDatabase: String,
    private val sources: (creator: MemberId) -> List<HostPort>,
    private val systemMembers: () -> List<Member>?,
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
        when {
            state == null -> return id.name() == defaultDatabase || create(database)
            database.online && !state.started && !state.failed -> {
                log.info("Starting database %s, online in the system graph", id)
                databases.start(id)
            }
            // Stopping also clears a failure, so that starting it again tries anew.
            !database.online && (state.started || state.failed) -> {
                log.info("Stopping database %s, offline in the system graph", id)
                databases.stop(id)
            }
        }
        return true
    }

    /** Creates [database] here; returns false to be tried again. */
    private fun create(database: DesiredDatabase): Boolean {
        val id = database.id
        if (!trusted(id)) {
            // Created or copied once it is started.
            if (!database.online) return true
            // A leftover, deleted once the system graph is confirmed current.
            if (id.name() in databases.stored()) return false
            if (!provideStore(database)) return false
        }
        log.info("Creating database %s", id)
        databases.create(id)
        if (database.online) databases.start(id)
        return true
    }

    /**
     * Makes sure that [database] can start here: with a copy of its store, or as its creator.
     * Returns false to be tried again.
     */
    private fun provideStore(database: DesiredDatabase): Boolean {
        val id = database.id
        val creator = database.creator
        return when {
            creator == null -> {
                // The creator is named in a system transaction, which reconciles again.
                if (systemGraph.leading()) nameCreator(id)
                false
            }
            creator == self && id.uuid !in databases.groups() -> {
                log.info("Creating the store of new database %s, as its creator", id)
                true
            }
            else -> copyFromCluster(id, creator)
        }
    }

    private fun nameCreator(id: NamedDatabaseId) {
        val members = systemMembers()
        if (members == null) {
            log.info("Naming the creator of %s waits until the change of members settles", id)
            return
        }
        try {
            systemGraph.nameCreator(id, self, members)
        } catch (e: InterruptedException) {
            throw e
        } catch (e: Exception) {
            log.info("Naming this member the creator of %s failed: %s", id, e.message)
        }
    }

    private fun copyFromCluster(id: NamedDatabaseId, creator: MemberId): Boolean {
        for (source in sources(creator)) {
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
