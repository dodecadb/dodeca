package org.dozendb.cluster.internal.replication

import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.write
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.dozendb.cluster.internal.ClusterMetrics
import org.dozendb.cluster.internal.reads.ReadConsistencyException
import org.dozendb.consensus.ConsensusLog
import org.dozendb.consensus.ConsensusUnavailableException
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.Member
import org.dozendb.consensus.MemberId
import org.dozendb.core.edition.DatabaseHosting
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.HostedOnMode
import org.neo4j.graphdb.event.DatabaseEventContext
import org.neo4j.graphdb.event.DatabaseEventListenerAdapter
import org.neo4j.internal.id.IdGeneratorFactory
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.logging.InternalLogProvider

/**
 * Runs the Raft group of every database alongside it. [initialMembers] tells the initial members of
 * a database's group, which a member that has no local state for it creates it with. [mustDetach]
 * tells whether this member brought a database's store to the cluster it formed and has yet to
 * detach it from the Raft group it may come from; [detached] records that it has. [adoptDatabases]
 * runs once this member detached the system database it brought, in local transactions on it: it
 * makes the databases that `system` lists the new cluster's. [metrics], when given, counts each
 * database's replication.
 */
internal class ReplicationService(
    private val consensus: ConsensusLog,
    private val systemDatabase: DatabaseRef,
    private val defaultDatabaseName: String,
    private val catchUp: CatchUp,
    private val logProvider: InternalLogProvider,
    private val initialMembers: (NamedDatabaseId) -> List<Member>,
    private val mustDetach: (String) -> Boolean,
    private val detached: (String) -> Unit,
    private val adoptDatabases: () -> Unit,
    private val notLeader: (String, MemberId?) -> String,
    private val metrics: ClusterMetrics? = null,
) : DatabaseEventListenerAdapter(), DatabaseHosting {
    override val hostedOnMode: HostedOnMode = HostedOnMode.RAFT

    /** See [ReplicatedDatabase]. */
    override val reusesFreedIdsAtStart: Boolean = false

    private val log = logProvider.getLog(ReplicationService::class.java)
    private val databases = ConcurrentHashMap<String, ReplicatedDatabase>()
    private val scheduler: ScheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor { runnable ->
            Thread(runnable, "dozendb-replication").apply { isDaemon = true }
        }
    private val catchUpExecutor: ExecutorService = Executors.newCachedThreadPool { runnable ->
        Thread(runnable, "dozendb-catch-up").apply { isDaemon = true }
    }
    private val catchingUp: MutableSet<String> = ConcurrentHashMap.newKeySet()

    /**
     * Held shared by readers of the system database that run in the background, and exclusively
     * while `system` is stopped to be replaced by a copy, so that those readers wait instead of
     * finding it stopped. Fair, so that readers do not starve a waiting replacement.
     */
    private val systemLock = ReentrantReadWriteLock(true)

    /**
     * Runs [block], which reads the system database, while `system` is not being replaced by a
     * copy. [block] must not wait for anything that needs `system` replaced first.
     */
    fun <T> readingSystem(block: () -> T): T {
        // Interruptible, so that closing a waiting reader does not wait for the replacement.
        systemLock.readLock().lockInterruptibly()
        try {
            return block()
        } finally {
            systemLock.readLock().unlock()
        }
    }

    /** The id of the default database, once it was created here. */
    val defaultDatabaseId: UUID?
        get() = databases[defaultDatabaseName]?.ref?.id

    /** Whether this member founds the cluster and has not initialised its system graph yet. */
    val systemBootstrapping: Boolean
        get() = databases[systemDatabase.name]?.bootstrapping == true

    /** Runs after every transaction applied to the system database, on its apply thread. */
    @Volatile var afterSystemApply: () -> Unit = {}

    /**
     * Runs after the status of a database's group changed here: its role, leader or members, or
     * Ratis stopped it, as for a member removed from it.
     */
    @Volatile var afterGroupChange: () -> Unit = {}

    /** Whether this member is the system leader and can commit system transactions. */
    val systemWritable: Boolean
        get() = databases[systemDatabase.name]?.writable == true

    /**
     * Waits until the system database here holds every transaction the cluster had committed when
     * it was called. Returns false if the system leader could not confirm that in time.
     */
    fun awaitSystemCurrent(): Boolean =
        try {
            runBlocking {
                withTimeoutOrNull(SYSTEM_READ_TIMEOUT.toMillis()) {
                    consensus.readIndex(systemDatabase)
                }
            } != null
        } catch (_: ConsensusUnavailableException) {
            false
        }

    /**
     * Waits until the database [id] here holds everything its Raft group had committed when this
     * was called, as confirmed by its leader: for a linearizable read.
     */
    fun awaitLinearizable(id: NamedDatabaseId) {
        val replica =
            replica(id)?.takeIf { it.isRunning() }
                ?: throw ReadConsistencyException(
                    Status.Cluster.ReplicationFailure,
                    "Database ${id.name()} is not replicating on this member",
                )
        replica.awaitLinearizable()
    }

    /** The replication of [namedDatabaseId] on this member, if it was created. */
    fun replica(namedDatabaseId: NamedDatabaseId): ReplicatedDatabase? =
        databases[namedDatabaseId.name()]?.takeIf { it.namedDatabaseId == namedDatabaseId }

    /** The replication of [database] on this member, if it was created. */
    fun replica(database: DatabaseRef): ReplicatedDatabase? =
        databases[database.name]?.takeIf { it.ref == database }

    /** Forgets the dropped database [namedDatabaseId] and deletes its Raft group here. */
    fun remove(namedDatabaseId: NamedDatabaseId) {
        replica(namedDatabaseId)?.let { databases.remove(namedDatabaseId.name(), it) }
        metrics?.forget(namedDatabaseId.name())
        consensus.removeGroup(
            DatabaseRef(namedDatabaseId.name(), namedDatabaseId.databaseId().uuid())
        )
    }

    /** Deletes the Raft group [id] here, whose database was dropped. */
    fun removeGroup(id: UUID) {
        consensus.removeGroup(DatabaseRef(DROPPED, id))
    }

    /** Ids of the Raft groups with state on this member. */
    fun localGroups(): Set<UUID> = consensus.localGroups()

    /** Whether [namedDatabaseId] is being replaced by a copy because it fell behind its log. */
    fun catchingUp(namedDatabaseId: NamedDatabaseId): Boolean = namedDatabaseId.name() in catchingUp

    /** The names of the databases being replaced by a copy because they fell behind their log. */
    fun catchingUp(): Set<String> = HashSet(catchingUp)

    /**
     * Whether this member votes in the group of [namedDatabaseId] as it knows the group: false once
     * it was removed or knows voters without itself, null while the group does not run here.
     */
    fun votes(namedDatabaseId: NamedDatabaseId): Boolean? {
        val ref = DatabaseRef(namedDatabaseId.name(), namedDatabaseId.databaseId().uuid())
        val membership =
            try {
                consensus.membership(ref)
            } catch (_: ConsensusUnavailableException) {
                return null
            }
        return !membership.removed && membership.members.any { it.id == consensus.self.id }
    }

    /** Whether commits to the database [name] are durable through the Raft log alone. */
    fun singleFsync(name: String): Boolean = databases[name]?.replicating == true

    /**
     * Creates the replication of a database that is being created. [storeExists] tells whether the
     * database's files exist or Neo4j is about to create them.
     */
    override fun prepare(
        namedDatabaseId: NamedDatabaseId,
        idGeneratorFactory: IdGeneratorFactory,
        storeExists: Boolean,
    ): ReplicatedDatabase {
        val replica =
            ReplicatedDatabase(
                namedDatabaseId,
                consensus,
                { initialMembers(namedDatabaseId) },
                { mustDetach(namedDatabaseId.name()) },
                { detached(namedDatabaseId.name()) },
                if (namedDatabaseId.isSystemDatabase) adoptDatabases else ({}),
                idGeneratorFactory,
                storeExists,
                scheduler,
                logProvider.getLog(ReplicatedDatabase::class.java),
                afterApply =
                    if (namedDatabaseId.isSystemDatabase) {
                        { afterSystemApply() }
                    } else {
                        {}
                    },
                onSnapshotNeeded = ::catchUp,
                notLeaderMessage = { leader -> notLeader(namedDatabaseId.name(), leader) },
                counts = metrics?.of(namedDatabaseId.name()) ?: ClusterMetrics.Counters(),
                afterGroupChange = { afterGroupChange() },
            )
        databases[namedDatabaseId.name()] = replica
        return replica
    }

    /** The replicated databases created here, running or not. */
    fun created(): List<DatabaseRef> = databases.values.map { it.ref }

    /** The replicated databases that are running. */
    fun running(): List<ReplicatedDatabase> = databases.values.filter { it.isRunning() }

    override fun databaseStart(eventContext: DatabaseEventContext) {
        val replica = databases[eventContext.databaseName] ?: return
        if (replica.bootstrapping && replica.ref.name == systemDatabase.name) {
            // Neo4j initialises the system graph later, in its own transactions; see
            // [systemGraphInitialized].
            return
        }
        if (replica.bootstrapping) {
            // Neo4j initialises the new store after this event returns; replicate after that.
            inBackground("Starting the replication of ${replica.ref.name}") {
                joining(replica, replica::startAfterBootstrap)
            }
        } else {
            joining(replica, replica::start)
        }
    }

    /**
     * Runs [join], which joins the group of [replica]. Only stopping this member interrupts it: the
     * database then stops too, which is no reason for an error.
     */
    private fun joining(replica: ReplicatedDatabase, join: () -> Unit) {
        try {
            join()
        } catch (e: Exception) {
            if (generateSequence<Throwable>(e) { it.cause }.none { it is InterruptedException }) {
                throw e
            }
            Thread.currentThread().interrupt()
            log.info(
                "Stopped joining the Raft group of %s: this member is stopping",
                replica.ref.name,
            )
        }
    }

    override fun databaseShutdown(eventContext: DatabaseEventContext) {
        databases[eventContext.databaseName]?.stop()
    }

    override fun databasePanic(eventContext: DatabaseEventContext) {
        val replica = databases[eventContext.databaseName] ?: return
        // A panic can be raised on the Raft apply thread itself, which stopping the group waits
        // for.
        inBackground("Stopping the replication of panicked database ${replica.ref.name}") {
            replica.stop()
        }
    }

    /**
     * Starts replicating the system database of the member that founds the cluster, once Neo4j
     * initialised its system graph with local transactions. Every other member copies the result.
     */
    fun systemGraphInitialized() {
        val replica = databases[systemDatabase.name] ?: return
        if (replica.bootstrapping) replica.start()
    }

    /** Replaces [replica] with a copy, unless that is already under way. */
    private fun catchUp(replica: ReplicatedDatabase, leader: Member?, minimumIndex: Long) {
        val name = replica.ref.name
        if (!catchingUp.add(name)) return
        try {
            catchUpExecutor.execute {
                try {
                    if (name == systemDatabase.name) {
                        catchUp.run(replica, leader, minimumIndex) { replace ->
                            systemLock.write(replace)
                        }
                        // The copy brings changes that no applied transaction announced.
                        afterSystemApply()
                    } else {
                        catchUp.run(replica, leader, minimumIndex)
                    }
                } catch (e: Exception) {
                    log.error("Catching up database $name failed", e)
                } finally {
                    catchingUp.remove(name)
                }
            }
        } catch (_: RejectedExecutionException) {
            // Shutting down.
            catchingUp.remove(name)
        }
    }

    private fun inBackground(description: String, task: () -> Unit) {
        scheduler.execute {
            try {
                task()
            } catch (e: Exception) {
                log.error("$description failed", e)
            }
        }
    }

    fun close() {
        catchUpExecutor.shutdownNow()
        scheduler.shutdownNow()
    }

    private companion object {
        val SYSTEM_READ_TIMEOUT: Duration = Duration.ofSeconds(10)

        /** The name logged for a Raft group whose database was dropped and is unknown here. */
        const val DROPPED = "dropped"
    }
}
