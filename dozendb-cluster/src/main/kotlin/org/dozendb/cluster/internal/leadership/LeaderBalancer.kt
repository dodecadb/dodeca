package org.dozendb.cluster.internal.leadership

import java.time.Duration
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import org.dozendb.cluster.MemberDirectory
import org.dozendb.cluster.internal.topology.DatabaseLocations
import org.dozendb.consensus.ConsensusLog
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.LeaderPlacement
import org.dozendb.consensus.MemberId
import org.dozendb.core.edition.DatabaseState
import org.neo4j.logging.InternalLog

/**
 * Moves the leadership of databases this member leads to other members, where Neo4j Enterprise
 * documents that leadership moves: away from a member where the database is read-only, to a member
 * with the database's priority tag (see [LeaderMoves.priority]), and, with
 * [BalancingStrategy.EQUAL_BALANCING], so that every member leads as many databases as the others,
 * give or take one (see [LeaderMoves.balancing]).
 *
 * Looks every [interval], and soon after [request]. Balancing waits until leaders have stayed put
 * everywhere for [stabilityDelay], every database has a leader and no group changes its members;
 * moving off a read-only member or to one with the priority tag does not wait. A database goes to
 * the member, among those that could lead it (see [Leadership.eligible]), that carries its priority
 * tag and leads the fewest databases; this member checks first that the member answers it and keeps
 * up with its log (see [LeaderMoves.keepsUp]), and hands the leadership over without failing
 * commits under way (see `ReplicatedDatabase.transferLeadership`). It also ranks the members a
 * leadership is handed over to when this member stops applying a database, by the server tags its
 * last look read.
 */
internal class LeaderBalancer(
    private val consensus: ConsensusLog,
    private val directory: MemberDirectory,
    private val locations: DatabaseLocations,
    /** The databases replicated on this member, running or not. */
    private val created: () -> List<DatabaseRef>,
    /**
     * Every database of the cluster, also those not hosted here, so that every member sees the
     * same; those replicated here while the system graph cannot be read yet.
     */
    private val databases: () -> List<DatabaseRef>,
    /**
     * Hands the leadership of a database over to a member, for a reason, within a timeout; returns
     * whether the member leads it afterwards.
     */
    private val handOver: (DatabaseRef, MemberId, String, Duration) -> Boolean,
    private val strategy: () -> BalancingStrategy,
    /** The priority tag of the database of a name, if any. */
    private val priorityTag: (String) -> String?,
    /** The server tags of each member, from the server records. */
    private val tags: () -> Map<MemberId, Set<String>>,
    private val stabilityDelay: Duration,
    /** How long a transfer, and the commits under way before it, may take. */
    private val transferTimeout: Duration,
    /** How long ago a member may have last answered this member to take a leadership over. */
    private val contactLimit: Duration,
    private val log: InternalLog,
    private val interval: Duration = Duration.ofSeconds(1),
    private val nanoTime: () -> Long = System::nanoTime,
) : LeaderPlacement {
    private val executor = Executors.newSingleThreadScheduledExecutor { runnable ->
        Thread(runnable, "dozendb-leader-balancer").apply { isDaemon = true }
    }
    private val requested = AtomicBoolean()

    @Volatile private var stopped = false

    /** The server tags as the last look read them, for hand-overs. */
    @Volatile private var lastTags: Map<MemberId, Set<String>> = emptyMap()

    /** Databases read-only here that no member could take over, logged once; executor only. */
    private val stuck = mutableSetOf<UUID>()

    /**
     * Databases led here without their priority tag that no member with it could take over, logged
     * once; executor only.
     */
    private val unprioritized = mutableSetOf<UUID>()

    /** The commit index of each database this member led at the last look; executor only. */
    private val committed = mutableMapOf<UUID, Long>()

    /** The leaders at the last look, and when they last changed; executor only. */
    private var leaders: Map<UUID, MemberId?> = emptyMap()
    private var leadersChanged = nanoTime()

    fun start() {
        executor.scheduleWithFixedDelay(
            ::lookQuietly,
            interval.toMillis(),
            interval.toMillis(),
            TimeUnit.MILLISECONDS,
        )
    }

    /** Asks for a look soon, as when the databases read-only here changed. */
    fun request() {
        if (!requested.compareAndSet(false, true)) return
        try {
            executor.execute {
                requested.set(false)
                lookQuietly()
            }
        } catch (_: RejectedExecutionException) {
            // Stopping.
        }
    }

    /**
     * Stops looking. Waits for a look in progress, which reads the system graph and may move a
     * leadership, so that the databases can stop once this returns.
     */
    fun stop() {
        stopped = true
        executor.shutdownNow()
        executor.awaitTermination(30, TimeUnit.SECONDS)
    }

    /**
     * The members to hand the leadership of [database] over to, those that could lead it first,
     * those with its priority tag first among them, fewest led first. The tags are those the last
     * look read: a hand-over runs while databases stop, `system` among them, and does not read the
     * system graph. A member with the tag that did not get the leadership gets it at the next look
     * of the new leader.
     */
    override fun rank(database: DatabaseRef, candidates: List<MemberId>): List<MemberId> {
        val view = view(created()) { lastTags }
        val leadership = view.databases.firstOrNull { it.database == database }
        return LeaderMoves.rank(
            view,
            candidates.filter { it in leadership?.eligible.orEmpty() },
            leadership?.priorityTag,
        )
    }

    /**
     * Who leads the cluster's databases and who could, as this member sees it now. Keeps the server
     * tags it reads for hand-overs.
     */
    fun view(): LeadershipView = view(databases()) { tags().also { lastTags = it } }

    private fun view(
        of: List<DatabaseRef>,
        tags: () -> Map<MemberId, Set<String>>,
    ): LeadershipView {
        val databases = of.mapNotNull(::leadership)
        // The server records are read only when a database has a priority tag.
        return LeadershipView(
            databases,
            if (databases.any { it.priorityTag != null }) tags() else emptyMap(),
        )
    }

    /** Who leads [database] and who could: from its group here, or from what others report. */
    private fun leadership(database: DatabaseRef): Leadership? {
        val location = locations.of(database) ?: return null
        val leader = location.leader
        return Leadership(
            database,
            leader,
            location.voters
                .map { it.id }
                .filter { it != leader && canLead(it, database.id) }
                .toSet(),
            writableOnLeader =
                leader == null || database.id !in directory.readOnlyDatabases(leader),
            settled = location.settled,
            priorityTag = priorityTag(database.name),
        )
    }

    /** Whether [member] answers, runs the database [id] and can write to it. */
    private fun canLead(member: MemberId, id: UUID): Boolean =
        directory.answering(member) &&
            directory.databaseStates(member).orEmpty().any {
                it.id == id && it.status == DatabaseState.Status.ONLINE
            } &&
            id !in directory.readOnlyDatabases(member)

    private fun lookQuietly() {
        try {
            look()
            noteCommits()
        } catch (e: Exception) {
            // Stopping interrupts the look in progress.
            if (stopped) return
            log.warn("Deciding where databases are led failed", e)
        }
    }

    private fun look() {
        // Until every member was probed once, none counts as answering.
        if (!directory.refreshed) return
        val self = consensus.self.id
        var view = view()
        if (LeaderMoves.readOnly(view, self).keys.any { it.id !in stuck }) {
            // Other members may have reported before the database turned read-only there too.
            directory.refresh()
            view = view()
        }
        var moved = false
        for ((database, candidates) in LeaderMoves.readOnly(view, self)) {
            if (candidates.isEmpty()) {
                if (stuck.add(database.id)) {
                    log.warn(
                        "Database %s is read-only on this member, which leads it, and no other " +
                            "member can take it over: its writes fail until one can",
                        database.name,
                    )
                }
                continue
            }
            stuck -= database.id
            if (candidates.any { transfer(database, it, MoveReason.READ_ONLY) }) moved = true
        }
        stuck.retainAll(
            view.databases
                .filter { it.leader == self && !it.writableOnLeader }
                .map { it.database.id }
        )
        var prioritized = LeaderMoves.priority(view, self)
        if (
            prioritized.any { (database, candidates) -> candidates.any { keepsUp(database, it) } }
        ) {
            // A member that stops hands its leaderships over before it stops answering.
            directory.refresh()
            prioritized = LeaderMoves.priority(view(), self)
        }
        for ((database, candidates) in prioritized) {
            if (candidates.isEmpty()) {
                if (unprioritized.add(database.id)) {
                    log.info(
                        "Database %s should be led by a member with the server tag %s, but none " +
                            "can take it over: this member leads it until one can",
                        database.name,
                        priorityTag(database.name),
                    )
                }
                continue
            }
            unprioritized -= database.id
            if (candidates.any { transfer(database, it, MoveReason.PRIORITY) }) moved = true
        }
        unprioritized.retainAll(prioritized.filterValues { it.isEmpty() }.keys.map { it.id })
        // Balancing sees the moves first, at the next look, and waits for leaders to stay put.
        if (!moved) balance(view)
    }

    /** Makes the cluster's next balancing move if this member is the one to make it. */
    private fun balance(seen: LeadershipView) {
        val now = nanoTime()
        val current = seen.databases.associate { it.database.id to it.leader }
        if (current != leaders) {
            leaders = current
            leadersChanged = now
            return
        }
        if (strategy() != BalancingStrategy.EQUAL_BALANCING) return
        if (now - leadersChanged < stabilityDelay.toNanos()) return
        if (seen.databases.any { it.leader == null || !it.settled }) return
        if (LeaderMoves.balancing(seen)?.from != consensus.self.id) return
        // What the others report may have changed since they last answered.
        directory.refresh()
        val move = LeaderMoves.balancing(view()) ?: return
        if (move.from == consensus.self.id) transfer(move.database, move.to, move.reason)
    }

    /** Notes the commit index of each database this member leads, for the next look. */
    private fun noteCommits() {
        committed.clear()
        for (database in created()) {
            val progress = consensus.progress(database) ?: continue
            if (progress.leader) committed[database.id] = progress.commitIndex
        }
    }

    /**
     * Transfers the leadership of [database] to [to] if [to] keeps up with this member (see
     * [LeaderMoves.keepsUp]); returns whether [to] leads it afterwards.
     */
    private fun transfer(database: DatabaseRef, to: MemberId, reason: MoveReason): Boolean {
        if (!keepsUp(database, to)) return false
        return handOver(database, to, reason.text, transferTimeout)
    }

    /** Whether [to] keeps up with this member's log of [database] (see [LeaderMoves.keepsUp]). */
    private fun keepsUp(database: DatabaseRef, to: MemberId): Boolean {
        val progress = consensus.progress(database) ?: return false
        return LeaderMoves.keepsUp(progress, to, committed[database.id], contactLimit)
    }
}
