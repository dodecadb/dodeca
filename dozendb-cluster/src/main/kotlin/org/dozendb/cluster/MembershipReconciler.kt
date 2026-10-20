package org.dozendb.cluster

import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executor
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.runBlocking
import org.dozendb.consensus.ConsensusLog
import org.dozendb.consensus.ConsensusUnavailableException
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.GroupMembership
import org.dozendb.consensus.Member
import org.dozendb.consensus.MemberId
import org.dozendb.consensus.Role
import org.neo4j.logging.Log

/**
 * Makes the members of every database's group follow the members of the cluster, the members of the
 * `system` group.
 *
 * Every member runs one, but only the `system` leader acts, so that one member changes each group:
 * a configuration change reaches a group's leader from any member, whatever its database does, also
 * when it is offline or its leader was removed from the cluster. Nothing changes while the
 * cluster's members change.
 *
 * For each group the reconciler first removes members the cluster no longer has, then adds the
 * members it lacks, one change at a time in each group and groups in parallel. A member is added
 * only once it runs the group, which it does once it has copied the database; the group's leader
 * then brings it up to date before it votes. A group keeps at least one of the cluster's members:
 * if it has none, it gains one before it loses any.
 *
 * Work that remains is looked at again after [retryDelay], and after twice as long each time work
 * still remains, up to [maximumRetryDelay]; a finished change looks at once.
 *
 * @property groups the database groups that run on this member, `system` among them or not.
 * @property changes runs the changes, which wait for the group's leader.
 */
class MembershipReconciler(
    private val consensus: ConsensusLog,
    private val systemDatabase: DatabaseRef,
    private val groups: () -> List<DatabaseRef>,
    private val log: Log,
    private val changeTimeout: Duration = Duration.ofMinutes(10),
    private val retryDelay: Duration = Duration.ofSeconds(2),
    private val maximumRetryDelay: Duration = Duration.ofSeconds(30),
    private val changes: Executor = newChangeExecutor(),
) : AutoCloseable {
    private val scheduler: ScheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor { runnable ->
            Thread(runnable, "dozendb-membership").apply { isDaemon = true }
        }
    private val requested = AtomicBoolean(false)
    private val inFlight: MutableSet<UUID> = ConcurrentHashMap.newKeySet()
    /** Members announced as waiting to run a group, so that the wait is logged once. */
    private val announced: MutableSet<Pair<UUID, MemberId>> = ConcurrentHashMap.newKeySet()
    private var statusListener: AutoCloseable? = null

    @Volatile private var started = false

    // Used on the scheduler thread only.
    private var nextRetryDelay = retryDelay
    private var retry: ScheduledFuture<*>? = null

    /** Reconciles now, whenever `system`'s leader or members change, and when [request]ed. */
    fun start() {
        started = true
        statusListener = consensus.addStatusListener(systemDatabase) { request() }
        request()
    }

    /** Asks for a reconciliation; requests made while one waits are merged. */
    fun request() {
        if (!started || !requested.compareAndSet(false, true)) return
        try {
            scheduler.execute(::run)
        } catch (_: RejectedExecutionException) {
            // Closing.
        }
    }

    private fun run() {
        requested.set(false)
        val again =
            try {
                reconcile()
            } catch (e: Exception) {
                log.error("Making the database groups follow the cluster's members failed", e)
                true
            }
        retry?.cancel(false)
        retry = null
        if (!again) {
            nextRetryDelay = retryDelay
            return
        }
        try {
            retry = scheduler.schedule(::request, nextRetryDelay.toMillis(), TimeUnit.MILLISECONDS)
        } catch (_: RejectedExecutionException) {
            // Closing.
        }
        nextRetryDelay = minOf(nextRetryDelay.multipliedBy(2), maximumRetryDelay)
    }

    /**
     * Starts the changes that are due, if this member leads `system`. Returns whether some group
     * still differs from the cluster's members.
     */
    fun reconcile(): Boolean {
        if (consensus.status(systemDatabase).role != Role.Leader) return false
        val cluster = consensus.membership(systemDatabase)
        if (!cluster.settled) return true
        var again = false
        for (group in groups()) {
            if (group.id == systemDatabase.id) continue
            val membership =
                try {
                    consensus.membership(group)
                } catch (_: ConsensusUnavailableException) {
                    // Not running here.
                    continue
                }
            val change = nextChange(cluster.members, membership) ?: continue
            again = true
            if (group.id !in inFlight) startIfPossible(group, change)
        }
        return again
    }

    private sealed interface Change {
        data class Remove(val member: Member) : Change

        data class Add(val member: Member) : Change
    }

    private fun nextChange(cluster: List<Member>, group: GroupMembership): Change? {
        val clusterIds = cluster.map { it.id }.toSet()
        val groupIds = group.members.map { it.id }.toSet()
        val extra = group.members.filter { it.id !in clusterIds }
        val missing = cluster.filter { it.id !in groupIds }
        return when {
            extra.isNotEmpty() && extra.size < group.members.size -> Change.Remove(extra.first())
            missing.isNotEmpty() -> Change.Add(missing.first())
            else -> null
        }
    }

    private fun startIfPossible(group: DatabaseRef, change: Change) {
        if (change is Change.Add && !runs(change.member, group)) {
            if (announced.add(group.id to change.member.id)) {
                log.info(
                    "Member %s joins the group of database %s once it has copied the database",
                    change.member,
                    group.name,
                )
            }
            return
        }
        inFlight += group.id
        try {
            changes.execute { apply(group, change) }
        } catch (e: RejectedExecutionException) {
            inFlight -= group.id
        }
    }

    private fun apply(group: DatabaseRef, change: Change) {
        var done = false
        try {
            when (change) {
                is Change.Remove -> {
                    log.info(
                        "Removing member %s from the group of database %s",
                        change.member,
                        group.name,
                    )
                    runBlocking { consensus.removeMember(group, change.member.id, changeTimeout) }
                    log.info(
                        "Removed member %s from the group of database %s",
                        change.member,
                        group.name,
                    )
                }
                is Change.Add -> {
                    log.info(
                        "Adding member %s to the group of database %s",
                        change.member,
                        group.name,
                    )
                    runBlocking { consensus.addMember(group, change.member, changeTimeout) }
                    log.info(
                        "Added member %s to the group of database %s",
                        change.member,
                        group.name,
                    )
                }
            }
            done = true
        } catch (e: ConsensusUnavailableException) {
            log.warn(
                "Changing the members of the group of database %s failed, trying again later: %s",
                group.name,
                e.message,
            )
        } catch (e: Exception) {
            log.error("Changing the members of the group of database ${group.name} failed", e)
        } finally {
            inFlight -= group.id
        }
        // A failed change waits for the next retry.
        if (done) request()
    }

    /** Whether [member] runs the group of [database], as it reports itself. */
    private fun runs(member: Member, database: DatabaseRef): Boolean =
        try {
            database.id in runBlocking { consensus.probe(member.raftAddress) }.groups
        } catch (_: ConsensusUnavailableException) {
            false
        }

    override fun close() {
        started = false
        statusListener?.close()
        statusListener = null
        scheduler.shutdownNow()
        (changes as? ExecutorService)?.shutdownNow()
    }

    private companion object {
        fun newChangeExecutor(): ExecutorService = Executors.newCachedThreadPool { runnable ->
            Thread(runnable, "dozendb-membership-change").apply { isDaemon = true }
        }
    }
}
