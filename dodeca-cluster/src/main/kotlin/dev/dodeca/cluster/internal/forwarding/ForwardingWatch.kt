package dev.dodeca.cluster.internal.forwarding

import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import org.neo4j.kernel.lifecycle.LifecycleAdapter

/**
 * Gives up on forwarded transactions whose member stopped leading their database, or whose database
 * has had no leader for [leaderlessFor], several times a second; when this member stops, lets the
 * transactions of [remote] end, as long as they may, then closes its connections.
 */
internal class ForwardingWatch(
    private val locations: ForwardingLocations,
    private val remote: RemoteTransactions,
    private val leaderlessFor: Duration,
) : LifecycleAdapter() {
    private var scheduler: ScheduledExecutorService? = null

    override fun start() {
        val executor = Executors.newSingleThreadScheduledExecutor { runnable ->
            Thread(runnable, "dodeca-forwarding-watch").apply { isDaemon = true }
        }
        scheduler = executor
        executor.scheduleWithFixedDelay(
            { runCatching { locations.watch(remote.open(), leaderlessFor) } },
            INTERVAL_MILLIS,
            INTERVAL_MILLIS,
            TimeUnit.MILLISECONDS,
        )
    }

    override fun stop() {
        // While transactions end, those whose leader goes away are still given up on.
        remote.drain()
        scheduler?.shutdownNow()
        scheduler = null
        remote.close()
    }

    private companion object {
        const val INTERVAL_MILLIS = 200L
    }
}
