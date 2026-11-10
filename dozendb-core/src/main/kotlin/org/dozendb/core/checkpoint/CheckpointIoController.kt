package org.dozendb.core.checkpoint

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.LockSupport
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.io.pagecache.IOController
import org.neo4j.io.pagecache.tracing.FileFlushEvent
import org.neo4j.kernel.impl.pagecache.IOControllerService
import org.neo4j.scheduler.Group
import org.neo4j.scheduler.JobScheduler
import org.neo4j.scheduler.SchedulerThreadFactory
import org.neo4j.time.SystemNanoClock

/**
 * Neo4j Enterprise's `db.checkpoint.iops.limit` for one database: the background checkpoints, those
 * on the threads [background] tells, flush at most that many I/Os a second, an I/O being a write
 * Neo4j's page cache counts as one. Up to a tenth of a second's worth goes at once; beyond that the
 * flushing thread waits until the I/Os are within the limit. -1 or 0 means no limit. Checkpoints
 * that someone waits for run on that someone's thread, such as recovery's, a new database's and a
 * stopping database's, and are not limited; nor are reads and evictions.
 *
 * The limit follows the dynamic setting of [config], the database's. [lift] lifts it while someone
 * waits for a background checkpoint under way, as Enterprise's `db.checkpoint()` does. Neo4j's
 * checkpoint log line and the `check_point.io_limit` metric say whether a checkpoint was limited.
 */
class CheckpointIoController
internal constructor(
    config: Config,
    private val clock: SystemNanoClock,
    /** Whether the calling thread is one of the background checkpoints'. */
    private val background: () -> Boolean = { true },
    /** Parks the calling thread for up to the nanoseconds given, as [LockSupport.parkNanos]. */
    private val park: (Long) -> Unit = LockSupport::parkNanos,
) : IOController {
    @Volatile private var limit: Int = config.get(GraphDatabaseSettings.check_point_iops_limit)
    @Volatile private var lifts = 0

    /** Changes when the limit does, or a lift starts or ends: waiting threads then go on. */
    @Volatile private var generation = 0L

    /** The time up to which the I/Os so far are within the limit. */
    private var paidUntil = Long.MIN_VALUE

    private val waiting: MutableSet<Thread> = ConcurrentHashMap.newKeySet()

    init {
        config.addListener(GraphDatabaseSettings.check_point_iops_limit) { _, value ->
            changed { limit = value }
        }
    }

    override fun maybeLimitIO(recentlyCompletedIOs: Int, flushEvent: FileFlushEvent) {
        // Read once: the setting may change to none meanwhile.
        val limit = limit
        if (!limited(limit)) return
        val start = clock.nanos()
        val (until, generation) =
            synchronized(this) {
                val cost = recentlyCompletedIOs * (NANOS_PER_SECOND / limit)
                paidUntil = maxOf(paidUntil, start - BURST_NANOS) + cost
                paidUntil to generation
            }
        if (until <= start) return
        val thread = Thread.currentThread()
        waiting += thread
        try {
            var now = start
            while (now < until && generation == this.generation && !thread.isInterrupted) {
                park(until - now)
                now = clock.nanos()
            }
        } finally {
            waiting -= thread
        }
        val millis = TimeUnit.NANOSECONDS.toMillis(clock.nanos() - start)
        flushEvent.throttle(recentlyCompletedIOs.toLong(), millis)
    }

    override fun reportIO(completedIOs: Int) {}

    override fun configuredLimit(): Long = limit.let { if (limited(it)) it.toLong() else -1 }

    /** Whether the calling thread's flushes are limited now. */
    override fun isEnabled() = limited(limit)

    private fun limited(limit: Int) = limit > 0 && lifts == 0 && background()

    /**
     * Lifts the limit until the returned lift is closed: while someone waits for a checkpoint. A
     * flush waiting for the limit goes on at once.
     */
    fun lift(): AutoCloseable {
        changed { lifts++ }
        val closed = AtomicBoolean()
        return AutoCloseable { if (closed.compareAndSet(false, true)) changed { lifts-- } }
    }

    /** Applies [change], starts counting I/Os anew and lets waiting threads go on. */
    private fun changed(change: () -> Unit) {
        synchronized(this) {
            change()
            paidUntil = Long.MIN_VALUE
            generation++
        }
        waiting.forEach(LockSupport::unpark)
    }

    companion object {
        private val NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1)

        /** The I/Os that go at once are those of this long at the limit. */
        val BURST_NANOS = TimeUnit.MILLISECONDS.toNanos(100)

        /**
         * Runs [block], which waits for a checkpoint, with [controller] lifted if it is DozenDB's:
         * a background checkpoint under way then completes unlimited.
         */
        fun <T> unlimited(controller: IOController?, block: () -> T): T =
            (controller as? CheckpointIoController)?.lift().use { block() }
    }
}

/**
 * Gives each database a [CheckpointIoController], in place of Neo4j Community's none, that limits
 * the threads of [scheduler]'s [Group.CHECKPOINT], where Neo4j runs the background checkpoints.
 */
class CheckpointIoControllerService(private val scheduler: JobScheduler) : IOControllerService {
    /** The threads' group; null if the scheduler does not tell, and every thread is limited. */
    private val checkpointThreads: ThreadGroup? by lazy {
        (scheduler.threadFactory(Group.CHECKPOINT) as? SchedulerThreadFactory)?.threadGroup
    }

    override fun createIOController(config: Config, clock: SystemNanoClock): IOController =
        CheckpointIoController(config, clock, ::background)

    private fun background(): Boolean = checkpointThreads.let {
        it == null || Thread.currentThread().threadGroup === it
    }
}
