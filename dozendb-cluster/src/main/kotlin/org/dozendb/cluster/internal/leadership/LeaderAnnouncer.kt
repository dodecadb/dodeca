package org.dozendb.cluster.internal.leadership

import java.time.Duration
import java.util.UUID
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.dozendb.consensus.ConsensusLog
import org.dozendb.consensus.ConsensusUnavailableException
import org.dozendb.consensus.DatabaseRef
import org.neo4j.logging.Log

/**
 * Tells the other members at once when the groups this member leads change ([leading], with their
 * terms), so that a member that does not run a group learns its new leader before it next probes
 * this one: its routing tables then send writes there within moments of an election, also while the
 * previous leader is gone and counts as leading as it last reported, with a lower term. Each
 * announcement goes to every member of the `system` group at once, waiting for each for at most
 * [deadline]; changes meanwhile are announced together after it. A member that misses one, or runs
 * a version without announcements, learns of the change when it next probes this member.
 */
internal class LeaderAnnouncer(
    private val consensus: ConsensusLog,
    private val systemDatabase: DatabaseRef,
    private val leading: () -> Map<UUID, Long>,
    private val deadline: Duration,
    private val log: Log,
) {
    private val pending = AtomicBoolean()
    @Volatile private var executor: ExecutorService? = null

    /** What this member announced last that it leads; read and written by [executor] only. */
    private var announced: Map<UUID, Long> = emptyMap()

    fun start() {
        executor = Executors.newSingleThreadExecutor { runnable ->
            Thread(runnable, "dozendb-leader-announcer").apply { isDaemon = true }
        }
    }

    fun stop() {
        executor?.shutdownNow()
        executor = null
    }

    /** Announces soon which groups this member leads, if that changed since it last did. */
    fun request() {
        val executor = executor ?: return
        if (pending.compareAndSet(false, true)) {
            try {
                executor.execute {
                    pending.set(false)
                    announceChange()
                }
            } catch (_: RejectedExecutionException) {
                // Stopped.
                pending.set(false)
            }
        }
    }

    private fun announceChange() {
        try {
            val now = leading()
            if (now == announced) return
            announced = now
            val others = consensus.members(systemDatabase).filter { it.id != consensus.self.id }
            runBlocking {
                others
                    .map { member ->
                        async {
                            try {
                                withTimeoutOrNull(deadline.toMillis()) {
                                    consensus.announce(member.raftAddress)
                                } ?: log.debug("Member %s took no announcement in time", member.id)
                            } catch (e: ConsensusUnavailableException) {
                                log.debug(
                                    "Member %s took no announcement: %s",
                                    member.id,
                                    e.message,
                                )
                            }
                        }
                    }
                    .awaitAll()
            }
        } catch (_: InterruptedException) {
            // Stopped.
        } catch (e: Exception) {
            log.warn("Announcing the groups this member leads failed: %s", e.toString())
        }
    }
}
