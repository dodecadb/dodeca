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
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import org.dozendb.cluster.DatabaseGroups
import org.dozendb.cluster.internal.ClusterMetrics
import org.dozendb.cluster.internal.of
import org.dozendb.cluster.internal.reads.ReadConsistencyException
import org.dozendb.cluster.internal.replication.DatabaseControl
import org.dozendb.cluster.internal.replication.ForeignStoreException
import org.dozendb.cluster.internal.secondaries.PullProtocol.Status
import org.dozendb.consensus.BulkTransfer
import org.dozendb.consensus.ConsensusUnavailableException
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.HostPort
import org.neo4j.graphdb.event.DatabaseEventContext
import org.neo4j.graphdb.event.DatabaseEventListenerAdapter
import org.neo4j.kernel.api.exceptions.Status.Cluster
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.logging.InternalLogProvider
import org.neo4j.storageengine.api.TransactionId

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
 * Reads that must see earlier writes do not wait for the interval: a secondary pulls at once, and
 * on until it has what they wait for, from one of its [primaries] that has it ([awaitLatest],
 * [pullUpTo]).
 *
 * [marks] records which stores are secondaries': one counts only once it is marked.
 *
 * @property primaries the consensus addresses of the primaries of a database, its leader first.
 * @property afterApply runs after a secondary applied pulled transactions, or was replaced by a
 *   copy, on the thread that did.
 * @property replacing runs the stop, move and start that replace a secondary with a copy, as
 *   `system`'s must run while no reader of it does.
 */
internal class Secondaries(
    private val transfer: BulkTransfer,
    private val upstreams: (DatabaseRef) -> List<HostPort>,
    private val primaries: (DatabaseRef) -> List<HostPort>,
    private val copies: SecondaryCopies,
    private val marks: SecondaryMarks,
    private val control: () -> DatabaseControl,
    private val pullInterval: Duration,
    private val refusal: (DatabaseRef) -> String,
    private val metrics: ClusterMetrics,
    private val logProvider: InternalLogProvider,
    private val retryDelay: Duration = Duration.ofSeconds(2),
    private val afterApply: (NamedDatabaseId) -> Unit = {},
    private val replacing: (NamedDatabaseId, () -> Unit) -> Unit = { _, replace -> replace() },
    private val groups: DatabaseGroups = DatabaseGroups(),
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
                metrics.of(id.name()),
                logProvider.getLog(SecondaryDatabase::class.java),
                afterApply = { afterApply(id) },
                groups = groups,
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
     * Pauses the secondary of the database [name] here if [pause], else resumes it, for
     * `dbms.cluster.secondaryReplicationDisable`: a paused secondary pulls nothing, also not for
     * reads that wait, until it is resumed or starts again. Returns its state, `paused` or
     * `running`.
     *
     * @throws IllegalArgumentException if [name] does not run here as a secondary.
     */
    fun toggle(name: String, pause: Boolean): String {
        val puller =
            databases[name]?.takeIf { it.secondary.pulling }
                ?: throw IllegalArgumentException(
                    "Database '$name' does not run as a secondary on this server"
                )
        puller.paused = pause
        if (pause) {
            log.info("Paused the pulls of secondary %s", name)
            puller.wake()
        } else {
            log.info("Resumed the pulls of secondary %s", name)
            puller.schedule(Duration.ZERO)
        }
        return if (pause) PAUSED else RUNNING
    }

    /** Whether the secondary of the database [name] here is paused. */
    fun paused(name: String): Boolean = databases[name]?.paused == true

    /**
     * Waits up to [timeout] until the secondary [id] holds every write acknowledged before the
     * call, for a linearizable read or a backup: a read barrier ([ReadBarrierProtocol]) on one of
     * its primaries, its leader first, tells which transaction that takes, and the secondary pulls
     * from that primary until it applied it.
     *
     * @throws ReadConsistencyException if no primary tells, or it did not pull that far in time.
     */
    fun awaitLatest(id: NamedDatabaseId, timeout: Duration) {
        val until = System.nanoTime() + timeout.toNanos()
        val puller =
            pulling(id)
                ?: throw ReadConsistencyException(
                    Cluster.ReplicationFailure,
                    "Database ${id.name()} is not running as a secondary on this member",
                )
        if (puller.paused) {
            throw ReadConsistencyException(
                Cluster.ReplicationFailure,
                "The replication of secondary ${id.name()} is paused on this member: it holds no " +
                    "writes after the pause",
            )
        }
        val ref = ref(id)
        val refusals = mutableListOf<String>()
        for (primary in primaries(ref)) {
            val answer =
                try {
                    transfer.fetchBulk(
                        primary,
                        ReadBarrierProtocol.NAME,
                        ReadBarrierProtocol.encode(ref),
                        ReadBarrierProtocol::read,
                    )
                } catch (e: ConsensusUnavailableException) {
                    refusals += "$primary cannot be reached: ${e.message}"
                    continue
                }
            when (answer) {
                is ReadBarrierProtocol.Answer.Refused -> refusals += "$primary: ${answer.message}"
                is ReadBarrierProtocol.Answer.Reached -> {
                    if (puller.await(answer.transactionId, primary, until)) return
                    throw ReadConsistencyException(
                        Cluster.ReplicationFailure,
                        "Database ${id.name()} on this member did not pull transaction " +
                            "${answer.transactionId} from $primary within $timeout; retry later.",
                    )
                }
            }
        }
        throw ReadConsistencyException(
            Cluster.ReplicationFailure,
            "The latest writes to ${id.name()} could not be confirmed with a primary; retry " +
                "later. " +
                refusals.ifEmpty { listOf("No primary of it is known.") }.joinToString("; "),
        )
    }

    /**
     * Has the secondary [id], if it runs here and lacks the transaction [transactionId], pull at
     * once and on from its leader until it has it, for up to [timeout]: for a read whose bookmark
     * names that transaction, which Neo4j waits for.
     */
    fun pullUpTo(id: NamedDatabaseId, transactionId: Long, timeout: Duration) {
        pulling(id)
            ?.takeIf { it.secondary.last().id() < transactionId }
            ?.want(
                transactionId,
                primaries(ref(id)).firstOrNull(),
                System.nanoTime() + timeout.toNanos(),
            )
    }

    private fun pulling(id: NamedDatabaseId): Puller? =
        databases[id.name()]?.takeIf {
            it.secondary.namedDatabaseId == id && it.secondary.pulling
        }

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

    /** Marks the store of [id], a primary's until now, a secondary's. */
    fun mark(id: NamedDatabaseId) = marks.add(id.databaseId().uuid())

    /**
     * Stops the secondary [id] pulling, once no pull applies transactions, and returns its last
     * transaction; null if it does not run here and pull.
     */
    fun stopPulling(id: NamedDatabaseId): TransactionId? {
        val puller = pulling(id) ?: return null
        puller.secondary.stop()
        puller.wake()
        return puller.secondary.last()
    }

    /** Has the secondary [id], which [stopPulling] stopped, pull again. */
    fun startPulling(id: NamedDatabaseId) {
        val puller = databases[id.name()]?.takeIf { it.secondary.namedDatabaseId == id } ?: return
        puller.secondary.start()
        puller.schedule(Duration.ZERO)
    }

    override fun databaseStart(eventContext: DatabaseEventContext) {
        val puller = databases[eventContext.databaseName] ?: return
        // A pause is not kept across a start.
        puller.paused = false
        puller.secondary.start()
        puller.schedule(Duration.ZERO)
    }

    override fun databaseShutdown(eventContext: DatabaseEventContext) {
        val puller = databases[eventContext.databaseName] ?: return
        puller.secondary.stop()
        puller.wake()
    }

    override fun databasePanic(eventContext: DatabaseEventContext) {
        val puller = databases[eventContext.databaseName] ?: return
        // A panic can be raised while a chain is applied, which stopping waits for.
        inBackground("Stopping secondary ${puller.secondary.ref.name}") {
            puller.secondary.stop()
            puller.wake()
        }
    }

    /** A transaction reads wait for, a server that has it, and the nano time they wait until. */
    private data class Wanted(val transactionId: Long, val upstream: HostPort?, val until: Long)

    /**
     * Runs the pulls of [secondary] one at a time, on the scheduler: one asked for while a pull
     * runs follows it, and one asked for sooner than the one scheduled replaces it. Reads that wait
     * for a transaction are woken after each pull.
     */
    private inner class Puller(val secondary: SecondaryDatabase) {
        private var scheduled: ScheduledFuture<*>? = null
        private var running = false

        /** The soonest delay asked for while a pull ran. */
        private var asked: Duration? = null

        /** The transaction reads wait for, while the secondary lacks it. */
        private var wanted: Wanted? = null

        /** Paused by `dbms.cluster.secondaryReplicationDisable`: it pulls nothing. */
        @Volatile var paused = false

        private val progress = ReentrantLock()
        private val pulled = progress.newCondition()

        /**
         * Pulls at once, and on until the secondary applied [transactionId], from [upstream] if
         * known, until the nano time [until].
         */
        @Synchronized
        fun want(transactionId: Long, upstream: HostPort?, until: Long) {
            val current = wanted?.takeIf { System.nanoTime() - it.until < 0 }
            val latest = if (current == null || until - current.until > 0) until else current.until
            wanted =
                if (current == null || transactionId >= current.transactionId) {
                    Wanted(transactionId, upstream ?: current?.upstream, latest)
                } else {
                    current.copy(until = latest)
                }
            schedule(Duration.ZERO)
        }

        /** The transaction reads wait for, if the secondary still lacks it and they still wait. */
        @Synchronized
        fun wanted(): Wanted? {
            val current = wanted ?: return null
            if (
                System.nanoTime() - current.until >= 0 ||
                    secondary.last().id() >= current.transactionId
            ) {
                wanted = null
                return null
            }
            return current
        }

        /**
         * Waits until the secondary applied [transactionId], pulling from [upstream], until the
         * nano time [until]; returns whether it did.
         */
        fun await(transactionId: Long, upstream: HostPort, until: Long): Boolean {
            want(transactionId, upstream, until)
            progress.withLock {
                while (true) {
                    if (!secondary.pulling || paused) return false
                    if (secondary.last().id() >= transactionId) return true
                    val left = until - System.nanoTime()
                    if (left <= 0) return false
                    pulled.awaitNanos(left)
                }
            }
        }

        /** Wakes the reads that wait, to look again. */
        fun wake() {
            progress.withLock { pulled.signalAll() }
        }

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
                next = pull(this)
            } finally {
                synchronized(this) {
                    running = false
                    val delay = listOfNotNull(next, asked).minOrNull()
                    asked = null
                    if (delay != null) schedule(delay)
                }
                wake()
            }
        }
    }

    /**
     * Pulls once for the secondary of [puller], from where reads that wait need it to, else from
     * its first upstream; returns when to pull next, or null to stop.
     */
    private fun pull(puller: Puller): Duration? {
        val secondary = puller.secondary
        val name = secondary.ref.name
        if (!secondary.pulling || puller.paused || databases[name] !== puller) return null
        // While reads wait, it pulls again soon.
        val idle = { if (puller.wanted() == null) pullInterval else WANTED_RETRY }
        val upstream = puller.wanted()?.upstream ?: upstreams(secondary.ref).firstOrNull()
        if (upstream == null) {
            failed(name, null, "no other server is known to run it")
            return idle()
        }
        val outcome =
            try {
                secondary.pull(upstream, PULL_BYTES)
            } catch (e: ConsensusUnavailableException) {
                failed(name, upstream, "it cannot be reached: ${e.message}")
                return idle()
            } catch (_: ForeignStoreException) {
                // The database panicked.
                return null
            } catch (e: Exception) {
                if (!secondary.pulling) return null
                log.error("Pulling the transactions of secondary $name from $upstream failed", e)
                return idle()
            }
        return when (outcome) {
            PullOutcome.CaughtUp -> {
                answered(name, upstream)
                idle()
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
                    idle()
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
        var started = false
        replacing(secondary.namedDatabaseId) {
            control().stop(secondary.namedDatabaseId)
            staged.moveIntoPlace()
            started = control().start(secondary.namedDatabaseId)
        }
        if (started) {
            log.info("Secondary %s runs again, from a copy", ref.name)
        } else {
            log.error("Secondary %s did not start after it was replaced with a copy", ref.name)
        }
        // The copy brings changes that no applied transaction announced.
        afterApply(secondary.namedDatabaseId)
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

    /** The group of the database [id]. */
    private fun ref(id: NamedDatabaseId): DatabaseRef = groups.of(id)

    private companion object {
        /** How much a pull carries: the transactions until the one that reaches it. */
        const val PULL_BYTES = 4 shl 20

        /** Pulls are short: a few threads serve every secondary of a member. */
        const val PULL_THREADS = 4

        const val PAUSED = "paused"
        const val RUNNING = "running"

        /** How soon a secondary pulls again while reads wait for what it lacks. */
        val WANTED_RETRY: Duration = Duration.ofMillis(50)
    }
}
