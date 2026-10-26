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
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.dozendb.consensus.ConsensusLog
import org.dozendb.consensus.ConsensusUnavailableException
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.Member
import org.dozendb.consensus.MemberId
import org.dozendb.consensus.Role
import org.neo4j.logging.Log

/**
 * Makes the members of every database's group follow the members of the cluster that host
 * databases: the members of the `system` group that [hosts], the enabled servers.
 *
 * Every member runs one, but only the `system` leader acts, so that one member changes each group:
 * a configuration change reaches a group's leader from any member, whatever its database does, also
 * when it is offline or its leader was removed from the cluster. Nothing changes while the
 * cluster's members change.
 *
 * The `system` leader need not run a group to change it, as a Free server runs none: it asks every
 * member which groups it runs and whom they have as voting members, and changes a group that does
 * not run here through the members that vote in it. It waits while those disagree, as while a
 * change is under way.
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
 * @property databases the databases of the system graph, `system` among them or not.
 * @property changes runs the changes, which wait for the group's leader.
 * @property hosts whether a member of the cluster hosts the databases other than `system`.
 * @property systemRunning whether the `system` database runs here, so that [databases] and [hosts]
 *   can read it: this member may lead `system` before.
 */
class MembershipReconciler(
    private val consensus: ConsensusLog,
    private val systemDatabase: DatabaseRef,
    private val databases: () -> List<DatabaseRef>,
    private val log: Log,
    private val changeTimeout: Duration = Duration.ofMinutes(10),
    private val retryDelay: Duration = Duration.ofSeconds(2),
    private val maximumRetryDelay: Duration = Duration.ofSeconds(30),
    private val changes: Executor = newChangeExecutor(),
    private val hosts: (MemberId) -> Boolean = { true },
    private val systemRunning: () -> Boolean = { true },
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
                // Closing interrupts the look in progress.
                if (!started) return
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
        if (!systemRunning()) return true
        val cluster = consensus.membership(systemDatabase)
        if (!cluster.settled) return true
        val hosting = cluster.members.filter { hosts(it.id) }
        val reports = groupsReported(cluster.members).toMutableMap()
        var again = false
        for (group in databases()) {
            if (group.id == systemDatabase.id) continue
            val voters = voters(group, reports)
            if (voters == null) {
                // No member that answers votes in it, or they disagree: look again later.
                again = true
                continue
            }
            val change = nextChange(hosting, voters) { runs(it, group, reports) } ?: continue
            again = true
            if (group.id !in inFlight) startIfPossible(group, change, voters, reports)
        }
        return again
    }

    /**
     * The groups that each of [members] runs, with the voting members it knows; a member that does
     * not answer is left out.
     */
    private fun groupsReported(members: List<Member>): Map<MemberId, Map<UUID, List<Member>>> =
        runBlocking {
            members
                .map { member ->
                    async {
                        try {
                            val peer = consensus.probe(member.raftAddress)
                            if (peer.member.id == member.id) member.id to peer.groups else null
                        } catch (_: ConsensusUnavailableException) {
                            null
                        }
                    }
                }
                .awaitAll()
        }
        .filterNotNull()
        .toMap()

    /**
     * The voting members of [group]: this member's view where it votes in the group, else the view
     * of the members that report voting in it, if they agree; null if none does or they disagree.
     * When no member of the cluster reports voting in it, the members that the others name as its
     * voters are asked too, and added to [reports]: they may have left the cluster.
     */
    private fun voters(
        group: DatabaseRef,
        reports: MutableMap<MemberId, Map<UUID, List<Member>>>,
    ): List<Member>? {
        val local =
            try {
                consensus.membership(group)
            } catch (_: ConsensusUnavailableException) {
                null
            }
        if (local != null && !local.removed && local.members.any { it.id == consensus.self.id }) {
            return local.members
        }
        var views = votersReported(group, reports)
        if (views.isEmpty()) {
            val named =
                (reports.values.flatMap { it[group.id].orEmpty() } + local?.members.orEmpty())
                    .distinctBy { it.id }
                    .filter { it.id !in reports }
            reports += groupsReported(named)
            views = votersReported(group, reports)
        }
        return views.firstOrNull()?.takeIf { first ->
            views.all { view -> view.map { it.id }.toSet() == first.map { it.id }.toSet() }
        }
    }

    /**
     * The views of [group]'s voting members that its voters report. A member waiting to be added,
     * or removed, knows other members than the voters.
     */
    private fun votersReported(
        group: DatabaseRef,
        reports: Map<MemberId, Map<UUID, List<Member>>>,
    ): List<List<Member>> = reports.mapNotNull { (member, groups) ->
        groups[group.id]?.takeIf { voters -> voters.any { it.id == member } }
    }

    private sealed interface Change {
        data class Remove(val member: Member) : Change

        data class Add(val member: Member) : Change
    }

    /** The next change of a group with [voters]: a member that [runs] it is added first. */
    private fun nextChange(
        cluster: List<Member>,
        voters: List<Member>,
        runs: (Member) -> Boolean,
    ): Change? {
        val clusterIds = cluster.map { it.id }.toSet()
        val groupIds = voters.map { it.id }.toSet()
        val extra = voters.filter { it.id !in clusterIds }
        val missing = cluster.filter { it.id !in groupIds }
        return when {
            extra.isNotEmpty() && extra.size < voters.size -> Change.Remove(extra.first())
            missing.isNotEmpty() -> Change.Add(missing.firstOrNull(runs) ?: missing.first())
            else -> null
        }
    }

    private fun startIfPossible(
        group: DatabaseRef,
        change: Change,
        voters: List<Member>,
        reports: Map<MemberId, Map<UUID, List<Member>>>,
    ) {
        if (change is Change.Add && !runs(change.member, group, reports)) {
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
            changes.execute { apply(group, change, voters) }
        } catch (e: RejectedExecutionException) {
            inFlight -= group.id
        }
    }

    /** Makes [change] to [group], through its [voters] if it does not run here. */
    private fun apply(group: DatabaseRef, change: Change, voters: List<Member>) {
        var done = false
        try {
            when (change) {
                is Change.Remove -> {
                    log.info(
                        "Removing member %s from the group of database %s",
                        change.member,
                        group.name,
                    )
                    runBlocking {
                        consensus.removeMember(group, change.member.id, changeTimeout, voters)
                    }
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
                    runBlocking {
                        consensus.addMember(group, change.member, changeTimeout, voters)
                    }
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
            // Closing interrupts the change in progress.
            if (started) {
                log.error("Changing the members of the group of database ${group.name} failed", e)
            }
        } finally {
            inFlight -= group.id
        }
        // A failed change waits for the next retry.
        if (done) request()
    }

    /** Whether [member] runs the group of [database], as it reported at this look. */
    private fun runs(
        member: Member,
        database: DatabaseRef,
        reports: Map<MemberId, Map<UUID, List<Member>>>,
    ): Boolean = reports[member.id]?.containsKey(database.id) == true

    /**
     * Stops reconciling. Waits for a look in progress, which reads the system graph, so that the
     * system database can stop once this returns.
     */
    override fun close() {
        started = false
        statusListener?.close()
        statusListener = null
        scheduler.shutdownNow()
        (changes as? ExecutorService)?.shutdownNow()
        scheduler.awaitTermination(30, TimeUnit.SECONDS)
    }

    private companion object {
        fun newChangeExecutor(): ExecutorService = Executors.newCachedThreadPool { runnable ->
            Thread(runnable, "dozendb-membership-change").apply { isDaemon = true }
        }
    }
}
