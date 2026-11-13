package dev.dodeca.cluster

import dev.dodeca.consensus.ConsensusLog
import dev.dodeca.consensus.ConsensusUnavailableException
import dev.dodeca.consensus.DatabaseRef
import dev.dodeca.consensus.Member
import dev.dodeca.consensus.MemberId
import dev.dodeca.consensus.PeerInfo
import dev.dodeca.consensus.Role
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
import org.neo4j.logging.Log

/**
 * Makes the voting members of every database's group follow the servers the database is placed on
 * (see `dev.dodeca.core.servers.Placements`), among the members of the cluster.
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
 * For each group the reconciler first adds the members it is placed on that do not vote in it, and
 * only once they all vote removes the others, one change at a time in each group and groups in
 * parallel: a group never has fewer voters than before while its copies move. A member is added
 * only once it runs the group, which it does once it has copied the database; the group's leader
 * then brings it up to date before it votes. A group always keeps one voter. A database not placed
 * yet keeps its voters.
 *
 * A lost server, which `system` was re-formed without, keeps its place in the groups of the
 * databases placed on it, as the members of the cluster do, until DEALLOCATE moves them off it:
 * removing it would leave groups that lost their majority smaller still once they had it back.
 *
 * The group's leader is removed last, once it has handed the leadership over to a member the
 * database is placed on, as its leader balancer does (see `LeaderMoves.leaving`), so that the group
 * need not elect a leader; if it still leads after [handOverGrace], it is removed anyway.
 *
 * Work that remains is looked at again after [retryDelay], and after twice as long each time work
 * still remains, up to [maximumRetryDelay], except while a hand-over is awaited; a finished change
 * looks at once.
 *
 * @property databases the databases of the system graph, `system` among them or not, each with the
 *   servers it is placed on; none while it is not placed yet.
 * @property changes runs the changes, which wait for the group's leader.
 * @property handOverGrace how long a leader that is to leave a group may take to hand the
 *   leadership over.
 * @property systemRunning whether the `system` database runs here, so that [databases] can read it:
 *   this member may lead `system` before.
 * @property systemSecondaries the system secondaries, servers of the cluster that pull `system`
 *   instead of voting in it, which databases can be placed on as on any server.
 * @property lostServers the servers that `system` was re-formed without, as the system graph marks
 *   them.
 */
class MembershipReconciler(
    private val consensus: ConsensusLog,
    private val groups: DatabaseGroups,
    private val databases: () -> Map<DatabaseRef, Set<MemberId>>,
    private val log: Log,
    private val changeTimeout: Duration = Duration.ofMinutes(10),
    private val retryDelay: Duration = Duration.ofSeconds(2),
    private val maximumRetryDelay: Duration = Duration.ofSeconds(30),
    private val changes: Executor = newChangeExecutor(),
    private val systemRunning: () -> Boolean = { true },
    private val handOverGrace: Duration = Duration.ofSeconds(30),
    private val nanoTime: () -> Long = System::nanoTime,
    private val systemSecondaries: () -> List<Member> = { emptyList() },
    private val lostServers: () -> Set<MemberId> = { emptySet() },
) : AutoCloseable {
    /** The group of `system` (see [DatabaseGroups]). */
    private val systemDatabase: DatabaseRef
        get() = groups.system

    private val scheduler: ScheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor { runnable ->
            Thread(runnable, "dodeca-membership").apply { isDaemon = true }
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
    /** When each group began to wait for its leader to hand the leadership over. */
    private val handOvers = mutableMapOf<UUID, Long>()
    /** Groups whose leader is removed although it did not hand the leadership over. */
    private val overdue = mutableSetOf<UUID>()

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
        // A hand-over takes about as long as the first delay.
        if (handOvers.isNotEmpty()) nextRetryDelay = retryDelay
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
        // A system secondary can host primaries of the other databases.
        val servers = (cluster.members + systemSecondaries()).distinctBy { it.id }
        val reports = groupsReported(servers).toMutableMap()
        var again = false
        val databases = databases()
        val lost = lostServers()
        handOvers.keys.retainAll(databases.keys.map { it.id }.toSet())
        overdue.retainAll(handOvers.keys)
        for ((group, placed) in databases) {
            if (group.id == systemDatabase.id || placed.isEmpty()) continue
            val hosting = servers.filter { it.id in placed }
            val voters = voters(group, reports)
            if (voters == null) {
                // No member that answers votes in it, or they disagree: look again later.
                again = true
                continue
            }
            val leader = leader(group, voters, reports)
            val kept = placed.intersect(lost)
            val change = nextChange(hosting, kept, voters, leader) { runs(it, group, reports) }
            if (change !is Change.Remove || change.member.id != leader) {
                handOvers -= group.id
                overdue -= group.id
            }
            if (change == null) continue
            again = true
            if (change is Change.Remove && change.member.id == leader) {
                if (awaitsHandOver(group, change.member)) continue
            }
            if (group.id !in inFlight) startIfPossible(group, change, voters, reports)
        }
        return again
    }

    /**
     * Whether removing [leader], which leads [group], waits for it to hand the leadership over: for
     * at most [handOverGrace] from the first time it was due.
     */
    private fun awaitsHandOver(group: DatabaseRef, leader: Member): Boolean {
        val now = nanoTime()
        val since =
            handOvers.getOrPut(group.id) {
                log.info(
                    "Member %s leads database %s, which moves off it: it leaves the database's " +
                        "group once it has handed the leadership over, within %s",
                    leader,
                    group.name,
                    handOverGrace,
                )
                now
            }
        if (now - since < handOverGrace.toNanos()) return true
        if (overdue.add(group.id)) {
            log.warn(
                "Member %s still leads database %s, which moved off it, after %s: removing it " +
                    "from the database's group anyway, and the others elect a leader",
                leader,
                group.name,
                handOverGrace,
            )
        }
        return false
    }

    /**
     * The leader of [group], which has [voters]: as this member knows it where it votes in the
     * group, else the voter that reported leading it at this look with the highest term, as one
     * that lost the leadership may not have said so yet.
     */
    private fun leader(
        group: DatabaseRef,
        voters: List<Member>,
        reports: Map<MemberId, PeerInfo>,
    ): MemberId? {
        if (voters.any { it.id == consensus.self.id }) {
            return try {
                consensus.status(group).leader
            } catch (_: ConsensusUnavailableException) {
                null
            }
        }
        return voters
            .mapNotNull { voter ->
                reports[voter.id]
                    ?.metadata
                    ?.get(MemberMetadata.LEADING)
                    ?.let(MemberMetadata::numbersById)
                    ?.get(group.id)
                    ?.let { voter.id to it }
            }
            .maxByOrNull { it.second }
            ?.first
    }

    /**
     * What each of [members] reports: the groups it runs, with the voting members it knows, and
     * those it leads; a member that does not answer is left out.
     */
    private fun groupsReported(members: List<Member>): Map<MemberId, PeerInfo> = runBlocking {
        members
            .map { member ->
                async {
                    try {
                        val peer = consensus.probe(member.raftAddress)
                        if (peer.member.id == member.id) member.id to peer else null
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
    private fun voters(group: DatabaseRef, reports: MutableMap<MemberId, PeerInfo>): List<Member>? {
        val local =
            try {
                consensus.membership(group)
            } catch (_: ConsensusUnavailableException) {
                null
            }
        if (local != null && !local.removed && local.members.any { it.id == consensus.self.id }) {
            return local.members
        }
        var views = GroupViews.reportedByVoters(group.id, groups(reports))
        if (views.isEmpty()) {
            val named =
                (reports.values.flatMap { it.groups[group.id].orEmpty() } +
                        local?.members.orEmpty())
                    .distinctBy { it.id }
                    .filter { it.id !in reports }
            reports += groupsReported(named)
            views = GroupViews.reportedByVoters(group.id, groups(reports))
        }
        return GroupViews.agreed(views.values)
    }

    private sealed interface Change {
        data class Remove(val member: Member) : Change

        data class Add(val member: Member) : Change
    }

    /** The groups each member reports running, with the voting members it knows. */
    private fun groups(reports: Map<MemberId, PeerInfo>): Map<MemberId, Map<UUID, List<Member>>> =
        reports.mapValues {
            it.value.groups
        }

    /**
     * The next change of a group with [voters] towards [hosting], the members it is placed on: one
     * of them that does not vote is added, those that [runs] it first; once they all vote, one that
     * is not among them nor [kept], the lost servers it is placed on, is removed, its [leader]
     * last.
     */
    private fun nextChange(
        hosting: List<Member>,
        kept: Set<MemberId>,
        voters: List<Member>,
        leader: MemberId?,
        runs: (Member) -> Boolean,
    ): Change? {
        val hostingIds = hosting.map { it.id }.toSet()
        val groupIds = voters.map { it.id }.toSet()
        val extra =
            voters.filter { it.id !in hostingIds && it.id !in kept }.sortedBy { it.id == leader }
        val missing = hosting.filter { it.id !in groupIds }
        return when {
            missing.isNotEmpty() -> Change.Add(missing.firstOrNull(runs) ?: missing.first())
            extra.isNotEmpty() && extra.size < voters.size -> Change.Remove(extra.first())
            else -> null
        }
    }

    private fun startIfPossible(
        group: DatabaseRef,
        change: Change,
        voters: List<Member>,
        reports: Map<MemberId, PeerInfo>,
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
        reports: Map<MemberId, PeerInfo>,
    ): Boolean = reports[member.id]?.groups?.containsKey(database.id) == true

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
            Thread(runnable, "dodeca-membership-change").apply { isDaemon = true }
        }
    }
}
