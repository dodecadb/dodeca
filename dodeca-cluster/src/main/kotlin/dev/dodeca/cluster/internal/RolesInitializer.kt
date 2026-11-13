package dev.dodeca.cluster.internal

import dev.dodeca.cluster.DatabaseGroups
import dev.dodeca.consensus.ConsensusLog
import dev.dodeca.consensus.DatabaseRef
import dev.dodeca.consensus.Role
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import org.neo4j.dbms.database.SystemGraphComponent
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.logging.InternalLog

/**
 * Creates the roles of a cluster whose system graph has none, as one formed before Dodeca had them:
 * the `system` leader does it, as a first start does, in replicated transactions. Until a member
 * applied them, it refuses logins.
 *
 * It looks whenever this member becomes the leader, and while creating them fails, again after
 * [retryDelay], twice as long each time up to [maximumRetryDelay]. Two leaders in quick succession
 * cannot both create them: the second fails on the roles' unique names and finds them next time.
 *
 * @property reading runs a read of [system] while it is not being replaced by a copy.
 */
internal class RolesInitializer(
    private val consensus: ConsensusLog,
    private val groups: DatabaseGroups,
    private val system: () -> GraphDatabaseService,
    private val reading: (() -> SystemGraphComponent.Status) -> SystemGraphComponent.Status,
    private val component: SystemGraphComponent,
    private val log: InternalLog,
    private val retryDelay: Duration = Duration.ofSeconds(2),
    private val maximumRetryDelay: Duration = Duration.ofSeconds(30),
) : AutoCloseable {
    /** The group of `system` (see [DatabaseGroups]). */
    private val systemDatabase: DatabaseRef
        get() = groups.system

    private val scheduler: ScheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor { runnable ->
            Thread(runnable, "dodeca-roles").apply { isDaemon = true }
        }
    private var statusListener: AutoCloseable? = null

    // Used on the scheduler thread only.
    private var nextRetryDelay = retryDelay
    private var retry: ScheduledFuture<*>? = null

    fun start() {
        statusListener = consensus.addStatusListener(systemDatabase) { request() }
        request()
    }

    private fun request() {
        try {
            scheduler.execute(::run)
        } catch (_: RejectedExecutionException) {
            // Closing.
        }
    }

    private fun run() {
        retry?.cancel(false)
        retry = null
        if (consensus.role(systemDatabase) != Role.Leader) return
        try {
            if (
                reading { component.detect(system()) } == SystemGraphComponent.Status.UNINITIALIZED
            ) {
                log.info("The system graph has no roles yet: creating them, as on a first start.")
                component.upgradeToCurrent(system())
            }
            nextRetryDelay = retryDelay
        } catch (e: Exception) {
            log.error(
                "Creating the roles of the cluster failed; members refuse logins until it " +
                    "succeeds. Trying again in ${nextRetryDelay.toMillis()} ms.",
                e,
            )
            try {
                retry = scheduler.schedule(::run, nextRetryDelay.toMillis(), TimeUnit.MILLISECONDS)
            } catch (_: RejectedExecutionException) {
                // Closing.
            }
            nextRetryDelay = minOf(nextRetryDelay.multipliedBy(2), maximumRetryDelay)
        }
    }

    override fun close() {
        statusListener?.close()
        scheduler.shutdownNow()
    }
}
