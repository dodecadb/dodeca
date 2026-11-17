package dev.dodeca.cluster.internal

import dev.dodeca.consensus.ConsensusLog
import dev.dodeca.consensus.ConsensusUnavailableException
import dev.dodeca.consensus.Member
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
import org.neo4j.logging.Log

/**
 * Tells the other members at once when the [news] of this member's groups change: what it answers
 * when probed, the groups it runs with their voting members.
 *
 * When the groups it leads change, a member that does not run a group learns its new leader before
 * it next probes this one: its routing tables then send writes there within moments of an election,
 * also while the previous leader is gone and counts as leading as it last reported, with a lower
 * term. When this member starts running a group it does not vote in yet, the `system` leader adds
 * it to the group at once (see `MembershipReconciler`).
 *
 * Each announcement goes to every one of the cluster's [servers] at once, waiting for each for at
 * most [deadline]; changes meanwhile are announced together after it. A member that misses one, or
 * runs a version without announcements, learns of the change when it next probes this member.
 */
internal class GroupAnnouncer(
    private val consensus: ConsensusLog,
    private val servers: () -> List<Member>,
    private val news: () -> GroupNews,
    private val deadline: Duration,
    private val log: Log,
) {
    private val pending = AtomicBoolean()
    @Volatile private var executor: ExecutorService? = null

    /** What this member announced last; read and written by [executor] only. */
    private var announced = GroupNews()

    fun start() {
        executor = Executors.newSingleThreadExecutor { runnable ->
            Thread(runnable, "dodeca-group-announcer").apply { isDaemon = true }
        }
    }

    fun stop() {
        executor?.shutdownNow()
        executor = null
    }

    /** Announces soon what this member answers when probed, if its [news] changed since. */
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
            val now = news()
            if (now == announced) return
            announced = now
            val others = servers().filter { it.id != consensus.self.id }
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
            log.warn("Announcing the groups of this member failed: %s", e.toString())
        }
    }
}

/**
 * What [GroupAnnouncer] tells the other members of as it changes.
 *
 * @property leading the groups this member leads, with their terms.
 * @property joining the groups this member runs without voting in them yet.
 */
internal data class GroupNews(
    val leading: Map<UUID, Long> = emptyMap(),
    val joining: Set<UUID> = emptySet(),
)
