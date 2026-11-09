package org.dozendb.cluster.internal.secondaries

import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit
import org.dozendb.cluster.internal.replication.DatabaseControl
import org.dozendb.cluster.internal.replication.ForeignStoreException
import org.dozendb.cluster.internal.secondaries.PullProtocol.Status
import org.dozendb.consensus.BulkTransfer
import org.dozendb.consensus.ConsensusUnavailableException
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.HostPort
import org.neo4j.graphdb.event.DatabaseEventContext
import org.neo4j.graphdb.event.DatabaseEventListenerAdapter
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.logging.InternalLogProvider

/** Copies of a database for a secondary, from another member that runs it. */
internal interface SecondaryCopies {
    /** Copies [database], which this member lacks, from [source]. */
    fun copy(source: HostPort, database: DatabaseRef)

    /**
     * Copies [database] from [source] next to it while it keeps running; the result replaces it
     * once it is stopped.
     */
    fun stage(source: HostPort, database: DatabaseRef): StagedStore
}

/** A copy of a database next to it, ready to replace it. */
internal fun interface StagedStore {
    /** Replaces the database, which must be stopped, with this copy. */
    fun moveIntoPlace()
}

/**
 * Hosts the databases that the system graph places on this member as secondaries
 * ([SecondaryDatabase]). Each pulls the transactions it lacks from the first of its [upstreams]
 * every [pullInterval], and at once again while an answer brought as much as a pull carries. An
 * upstream that refuses or cannot be reached is warned about once, until it answers again; the next
 * pull may pick another. When its upstream's logs no longer hold the transactions it needs, it is
 * replaced with a copy, fetched next to it from its upstreams while it keeps serving reads, as
 * primaries catch up; meanwhile it pulls nothing.
 *
 * [marks] records which stores are secondaries': one counts only once it is marked.
 */
internal class Secondaries(
    private val transfer: BulkTransfer,
    private val upstreams: (DatabaseRef) -> List<HostPort>,
    private val copies: SecondaryCopies,
    private val marks: SecondaryMarks,
    private val control: () -> DatabaseControl,
    private val pullInterval: Duration,
    private val refusal: (DatabaseRef) -> String,
    private val logProvider: InternalLogProvider,
    private val retryDelay: Duration = Duration.ofSeconds(2),
) : DatabaseEventListenerAdapter() {
    private val log = logProvider.getLog(Secondaries::class.java)
    private val databases = ConcurrentHashMap<String, Puller>()
    private val scheduler =
        ScheduledThreadPoolExecutor(PULL_THREADS) { runnable ->
            Thread(runnable, "dozendb-secondaries").apply { isDaemon = true }
        }
    private val copier: ExecutorService = Executors.newCachedThreadPool { runnable ->
        Thread(runnable, "dozendb-secondary-copy").apply { isDaemon = true }
    }
    private val catchingUp: MutableSet<String> = ConcurrentHashMap.newKeySet()

    /**
     * The upstreams each database could not pull from, by address, with why, until they answer
     * again; the empty address while it found none.
     */
    private val failing = ConcurrentHashMap<String, MutableMap<String, String>>()

    /** Hosts [id] as a secondary, before Neo4j creates it. */
    fun prepare(id: NamedDatabaseId): SecondaryDatabase {
        val secondary =
            SecondaryDatabase(
                id,
                transfer,
                { refusal(ref(id)) },
                logProvider.getLog(SecondaryDatabase::class.java),
            )
        databases[id.name()] = Puller(secondary)
        return secondary
    }

    /** The secondary [id] on this member, if it was created. */
    fun secondary(id: NamedDatabaseId): SecondaryDatabase? =
        databases[id.name()]?.secondary?.takeIf { it.namedDatabaseId == id }

    /** Whether [id] runs here as a secondary, and pulls. */
    fun running(id: NamedDatabaseId): Boolean = secondary(id)?.pulling == true

    /**
     * Whether [id] is being replaced by a copy because its upstreams no longer hold its history.
     */
    fun catchingUp(id: NamedDatabaseId): Boolean = id.name() in catchingUp

    /** The names of the secondaries being replaced by a copy. */
    fun catchingUp(): Set<String> = HashSet(catchingUp)

    /** The ids of the databases whose stores here are secondaries'. */
    fun marked(): Set<UUID> = marks.ids()

    /** Copies [id], which this member lacks, from [source], and marks it a secondary's. */
    fun copy(source: HostPort, id: NamedDatabaseId) {
        copies.copy(source, ref(id))
        marks.add(id.databaseId().uuid())
    }

    /** Forgets [id], whose database was dropped here, and its mark. */
    fun remove(id: NamedDatabaseId) {
        databases[id.name()]
            ?.takeIf { it.secondary.namedDatabaseId == id }
            ?.let {
                databases.remove(id.name(), it)
            }
        marks.remove(id.databaseId().uuid())
    }

    /** Forgets the mark of the database [id], which is gone. */
    fun unmark(id: UUID) = marks.remove(id)

    override fun databaseStart(eventContext: DatabaseEventContext) {
        val puller = databases[eventContext.databaseName] ?: return
        puller.secondary.start()
        puller.schedule(Duration.ZERO)
    }

    override fun databaseShutdown(eventContext: DatabaseEventContext) {
        databases[eventContext.databaseName]?.secondary?.stop()
    }

    override fun databasePanic(eventContext: DatabaseEventContext) {
        val secondary = databases[eventContext.databaseName]?.secondary ?: return
        // A panic can be raised while a chain is applied, which stopping waits for.
        inBackground("Stopping secondary ${secondary.ref.name}") { secondary.stop() }
    }

    /**
     * Runs the pulls of [secondary] one at a time, on the scheduler: one asked for while a pull
     * runs follows it, and one asked for sooner than the one scheduled replaces it.
     */
    private inner class Puller(val secondary: SecondaryDatabase) {
        private var scheduled: ScheduledFuture<*>? = null
        private var running = false

        /** The soonest delay asked for while a pull ran. */
        private var asked: Duration? = null

        /** Pulls after [delay], unless a pull is due sooner. */
        @Synchronized
        fun schedule(delay: Duration) {
            if (running) {
                asked = minOf(asked ?: delay, delay)
                return
            }
            val pending = scheduled
            if (
                pending != null &&
                    !pending.isDone &&
                    pending.getDelay(TimeUnit.MILLISECONDS) <= delay.toMillis()
            ) {
                return
            }
            pending?.cancel(false)
            scheduled =
                try {
                    scheduler.schedule(::run, delay.toMillis(), TimeUnit.MILLISECONDS)
                } catch (_: RejectedExecutionException) {
                    // Closing.
                    null
                }
        }

        private fun run() {
            synchronized(this) {
                if (running) {
                    // A replaced pull that had begun: the running one pulls again.
                    asked = Duration.ZERO
                    return
                }
                running = true
                scheduled = null
            }
            var next: Duration? = null
            try {
                next = pull(secondary)
            } finally {
                synchronized(this) {
                    running = false
                    val delay = listOfNotNull(next, asked).minOrNull()
                    asked = null
                    if (delay != null) schedule(delay)
                }
            }
        }
    }

    /** Pulls once for [secondary]; returns when to pull next, or null to stop. */
    private fun pull(secondary: SecondaryDatabase): Duration? {
        val name = secondary.ref.name
        if (!secondary.pulling || databases[name]?.secondary !== secondary) return null
        val upstream = upstreams(secondary.ref).firstOrNull()
        if (upstream == null) {
            failed(name, null, "no other server is known to run it")
            return pullInterval
        }
        val outcome =
            try {
                secondary.pull(upstream, PULL_BYTES)
            } catch (e: ConsensusUnavailableException) {
                failed(name, upstream, "it cannot be reached: ${e.message}")
                return pullInterval
            } catch (_: ForeignStoreException) {
                // The database panicked.
                return null
            } catch (e: Exception) {
                if (!secondary.pulling) return null
                log.error("Pulling the transactions of secondary $name from $upstream failed", e)
                return pullInterval
            }
        return when (outcome) {
            PullOutcome.CaughtUp -> {
                answered(name, upstream)
                pullInterval
            }
            PullOutcome.Behind -> {
                answered(name, upstream)
                Duration.ZERO
            }
            PullOutcome.Stopped -> null
            is PullOutcome.Refused ->
                if (outcome.status == Status.PRUNED) {
                    answered(name, upstream)
                    catchUp(secondary, upstream, outcome.message)
                    null
                } else {
                    failed(name, upstream, outcome.message)
                    pullInterval
                }
        }
    }

    /**
     * Warns that [name] could not pull from [upstream], or found none if null, unless it did so
     * already.
     */
    private fun failed(name: String, upstream: HostPort?, why: String) {
        val upstreams = failing.computeIfAbsent(name) { ConcurrentHashMap() }
        if (upstreams.put(upstream?.toString().orEmpty(), why) == null) {
            if (upstream == null) {
                log.warn("Secondary %s has no upstream to pull from: %s", name, why)
            } else {
                log.warn("Secondary %s cannot pull from %s: %s", name, upstream, why)
            }
        }
    }

    /** Tells, once, that [upstream] answers [name] again. */
    private fun answered(name: String, upstream: HostPort) {
        val upstreams = failing[name] ?: return
        upstreams.remove("")
        if (upstreams.remove(upstream.toString()) != null) {
            log.info("Secondary %s pulls from %s again", name, upstream)
        }
    }

    /** Replaces [secondary] with a copy, unless that is already under way. */
    private fun catchUp(secondary: SecondaryDatabase, upstream: HostPort, why: String) {
        val name = secondary.ref.name
        if (!catchingUp.add(name)) return
        try {
            copier.execute {
                try {
                    log.warn(
                        "Secondary %s needs transactions its upstream no longer has (%s); " +
                            "replacing it with a copy",
                        name,
                        why,
                    )
                    replace(secondary, upstream)
                } catch (_: InterruptedException) {
                    Thread.currentThread().interrupt()
                    log.warn("Replacing secondary %s with a copy was interrupted", name)
                } catch (e: Exception) {
                    log.error("Replacing secondary $name with a copy failed", e)
                    databases[name]?.takeIf { it.secondary === secondary }?.schedule(retryDelay)
                } finally {
                    catchingUp.remove(name)
                }
            }
        } catch (_: RejectedExecutionException) {
            // Closing.
            catchingUp.remove(name)
        }
    }

    /**
     * Copies [secondary] from [upstream], else from its other upstreams, next to it while it keeps
     * serving reads, until one copy succeeds or it stops; then stops it, moves the copy into place
     * and starts it again.
     */
    private fun replace(secondary: SecondaryDatabase, upstream: HostPort) {
        val ref = secondary.ref
        var staged: StagedStore? = null
        while (staged == null) {
            for (source in (listOf(upstream) + upstreams(ref)).distinct()) {
                if (!secondary.pulling || databases[ref.name]?.secondary !== secondary) return
                try {
                    staged = copies.stage(source, ref)
                    break
                } catch (e: InterruptedException) {
                    throw e
                } catch (e: Exception) {
                    log.warn("Copying secondary %s from %s failed: %s", ref.name, source, e.message)
                }
            }
            if (staged == null) Thread.sleep(retryDelay.toMillis())
        }
        control().stop(secondary.namedDatabaseId)
        staged.moveIntoPlace()
        if (control().start(secondary.namedDatabaseId)) {
            log.info("Secondary %s runs again, from a copy", ref.name)
        } else {
            log.error("Secondary %s did not start after it was replaced with a copy", ref.name)
        }
    }

    private fun inBackground(description: String, task: () -> Unit) {
        try {
            copier.execute {
                try {
                    task()
                } catch (e: Exception) {
                    log.error("$description failed", e)
                }
            }
        } catch (_: RejectedExecutionException) {
            // Closing.
        }
    }

    fun close() {
        scheduler.shutdownNow()
        copier.shutdownNow()
    }

    private companion object {
        /** How much a pull carries: the transactions until the one that reaches it. */
        const val PULL_BYTES = 4 shl 20

        /** Pulls are short: a few threads serve every secondary of a member. */
        const val PULL_THREADS = 4

        fun ref(id: NamedDatabaseId) = DatabaseRef(id.name(), id.databaseId().uuid())
    }
}
