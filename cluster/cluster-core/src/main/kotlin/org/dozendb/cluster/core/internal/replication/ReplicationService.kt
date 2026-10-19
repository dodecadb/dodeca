package org.dozendb.cluster.core.internal.replication

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
import org.dozendb.cluster.api.ConsensusLog
import org.dozendb.cluster.api.ConsensusUnavailableException
import org.dozendb.cluster.api.DatabaseRef
import org.dozendb.cluster.api.Member
import org.neo4j.graphdb.event.DatabaseEventContext
import org.neo4j.graphdb.event.DatabaseEventListenerAdapter
import org.neo4j.internal.id.IdGeneratorFactory
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.logging.InternalLogProvider

/**
 * Runs the Raft group of every database alongside it. [initialMembers] tells the initial members of
 * a database's group, which a member that has no local state for it creates it with.
 */
internal class ReplicationService(
    private val consensus: ConsensusLog,
    private val systemDatabase: DatabaseRef,
    private val defaultDatabaseName: String,
    private val catchUp: CatchUp,
    private val logProvider: InternalLogProvider,
    private val initialMembers: (NamedDatabaseId) -> List<Member>,
) : DatabaseEventListenerAdapter() {
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

    /** Whether the default database is replicating here, so other members can copy it. */
    val defaultDatabaseStarted: Boolean
        get() = databases[defaultDatabaseName]?.isRunning() == true

    /** Whether this member founds the cluster and has not initialised its system graph yet. */
    val systemBootstrapping: Boolean
        get() = databases[systemDatabase.name]?.bootstrapping == true

    /** Runs after every transaction applied to the system database, on its apply thread. */
    @Volatile var afterSystemApply: () -> Unit = {}

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

    /** The replication of [namedDatabaseId] on this member, if it was created. */
    fun replica(namedDatabaseId: NamedDatabaseId): ReplicatedDatabase? =
        databases[namedDatabaseId.name()]?.takeIf { it.namedDatabaseId == namedDatabaseId }

    /** Forgets the dropped database [namedDatabaseId] and deletes its Raft group here. */
    fun remove(namedDatabaseId: NamedDatabaseId) {
        replica(namedDatabaseId)?.let { databases.remove(namedDatabaseId.name(), it) }
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

    /** Whether commits to the database [name] are durable through the Raft log alone. */
    fun singleFsync(name: String): Boolean = databases[name]?.replicating == true

    /**
     * Creates the replication of a database that is being created. [storeExists] tells whether the
     * database's files exist or Neo4j is about to create them.
     */
    fun prepare(
        namedDatabaseId: NamedDatabaseId,
        idGeneratorFactory: IdGeneratorFactory,
        storeExists: Boolean,
    ): ReplicatedDatabase {
        val replica =
            ReplicatedDatabase(
                namedDatabaseId,
                consensus,
                { initialMembers(namedDatabaseId) },
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
                replica.startAfterBootstrap()
            }
        } else {
            replica.start()
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
