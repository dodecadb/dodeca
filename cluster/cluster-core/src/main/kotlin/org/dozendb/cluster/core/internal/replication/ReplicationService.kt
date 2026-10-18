package org.dozendb.cluster.core.internal.replication

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ScheduledExecutorService
import org.dozendb.cluster.api.ConsensusLog
import org.dozendb.cluster.api.DatabaseRef
import org.dozendb.cluster.api.Member
import org.neo4j.graphdb.event.DatabaseEventContext
import org.neo4j.graphdb.event.DatabaseEventListenerAdapter
import org.neo4j.internal.id.IdGeneratorFactory
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.logging.InternalLogProvider

/**
 * Decides which databases are replicated and runs their Raft groups alongside them.
 *
 * Only the default database is replicated; `system` and other databases stay local.
 */
internal class ReplicationService(
    private val consensus: ConsensusLog,
    private val systemDatabase: DatabaseRef,
    private val defaultDatabaseName: String,
    private val catchUp: CatchUp,
    private val logProvider: InternalLogProvider,
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

    /** Whether the default database is replicating here, so other members can copy it. */
    val defaultDatabaseStarted: Boolean
        get() = databases[defaultDatabaseName]?.isRunning() == true

    fun replicates(namedDatabaseId: NamedDatabaseId): Boolean =
        !namedDatabaseId.isSystemDatabase && replicates(namedDatabaseId.name())

    fun replicates(name: String): Boolean =
        name != systemDatabase.name && name == defaultDatabaseName

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
                systemDatabase,
                idGeneratorFactory,
                storeExists,
                scheduler,
                logProvider.getLog(ReplicatedDatabase::class.java),
                ::catchUp,
            )
        databases[namedDatabaseId.name()] = replica
        return replica
    }

    /** The replicated databases that are running. */
    fun running(): List<ReplicatedDatabase> = databases.values.filter { it.isRunning() }

    override fun databaseStart(eventContext: DatabaseEventContext) {
        val replica = databases[eventContext.databaseName] ?: return
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

    /** Replaces [replica] with a copy, unless that is already under way. */
    private fun catchUp(replica: ReplicatedDatabase, leader: Member?, minimumIndex: Long) {
        val name = replica.ref.name
        if (!catchingUp.add(name)) return
        try {
            catchUpExecutor.execute {
                try {
                    catchUp.run(replica, leader, minimumIndex)
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
}
