package dev.dodeca.cluster.internal.leadership

import dev.dodeca.cluster.MemberDirectory
import dev.dodeca.cluster.internal.topology.DatabaseLocations
import dev.dodeca.consensus.ConsensusLog
import dev.dodeca.consensus.DatabaseRef
import dev.dodeca.consensus.LeaderPlacement
import dev.dodeca.consensus.MemberId
import dev.dodeca.core.edition.DatabaseState
import java.time.Duration
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import org.neo4j.logging.InternalLog

/**
 * Moves the leadership of databases this member leads to other members, where Neo4j Enterprise
 * documents that leadership moves: away from a member where the database is read-only, to a member
 * with the database's priority tag (see [LeaderMoves.priority]), and, with
 * [BalancingStrategy.EQUAL_BALANCING], so that every member leads as many databases as the others,
 * give or take one (see [LeaderMoves.balancing]). It also moves the leadership of a database off a
 * member it is no longer placed on, so that the member leaves the database's group without an
 * election (see [LeaderMoves.leaving]).
 *
 * Looks every [interval], and soon after [request]. Balancing waits until leaders have stayed put
 * everywhere for [stabilityDelay], every database has a leader and no group changes its members;
 * the other moves do not wait. A database goes to the member, among those that could lead it (see
 * [Leadership.eligible]) and that it is placed on, that carries its priority tag and leads the
 * fewest databases; this member checks first that the member answers it and keeps up with its log
 * (see [LeaderMoves.keepsUp]), and hands the leadership over without failing commits under way (see
 * `ReplicatedDatabase.transferLeadership`). It also ranks the members a leadership is handed over
 * to when this member stops applying a database, by the server tags its last look read.
 */
internal class LeaderBalancer(
    private val consensus: ConsensusLog,
    private val directory: MemberDirectory,
    private val locations: DatabaseLocations,
    /** The databases replicated on this member, running or not. */
    private val created: () -> List<DatabaseRef>,
    /**
     * Every database of the cluster, also those not hosted here, so that every member sees the
     * same, with the members it is placed on (none for `system` and a database not placed yet);
     * those replicated here, without placements, while the system graph cannot be read yet.
     */
    private val databases: () -> Map<DatabaseRef, Set<MemberId>>,
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
        Thread(runnable, "dodeca-leader-balancer").apply { isDaemon = true }
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

    /**
     * Databases led here that move off this member, which no member they move to could take over
     * yet, logged once; executor only.
     */
    private val unplaced = mutableSetOf<UUID>()

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
        val view = view(created().associateWith { emptySet() }) { lastTags }
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
        of: Map<DatabaseRef, Set<MemberId>>,
        tags: () -> Map<MemberId, Set<String>>,
    ): LeadershipView {
        val databases = of.mapNotNull { (database, placed) -> leadership(database, placed) }
        // The server records are read only when a database has a priority tag.
        return LeadershipView(
            databases,
            if (databases.any { it.priorityTag != null }) tags() else emptyMap(),
        )
    }

    /**
     * Who leads [database] and who could, among the members it is [placed] on if it is placed: from
     * its group here, or from what others report.
     */
    private fun leadership(database: DatabaseRef, placed: Set<MemberId>): Leadership? {
        val location = locations.of(database) ?: return null
        val leader = location.leader
        return Leadership(
            database,
            leader,
            location.voters
                .map { it.id }
                .filter {
                    it != leader && (placed.isEmpty() || it in placed) && canLead(it, database.id)
                }
                .toSet(),
            writableOnLeader =
                leader == null || database.id !in directory.readOnlyDatabases(leader),
            settled = location.settled,
            priorityTag = priorityTag(database.name),
            placed = placed,
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
        var moved =
            handOver(LeaderMoves.readOnly(view, self), MoveReason.READ_ONLY, stuck) { database ->
                log.warn(
                    "Database %s is read-only on this member, which leads it, and no other " +
                        "member can take it over: its writes fail until one can",
                    database.name,
                )
            }
        val leaving = current(view) { LeaderMoves.leaving(it, self) }
        moved =
            handOver(leaving, MoveReason.LEAVING, unplaced) { database ->
                log.info(
                    "Database %s moves off this member, which leads it, but no member it moves " +
                        "to can take it over yet: this member leads it until one can",
                    database.name,
                )
            } || moved
        val prioritized = current(view) { LeaderMoves.priority(it, self) }
        moved =
            handOver(prioritized, MoveReason.PRIORITY, unprioritized) { database ->
                log.info(
                    "Database %s should be led by a member with the server tag %s, but none " +
                        "can take it over: this member leads it until one can",
                    database.name,
                    priorityTag(database.name),
                )
            } || moved
        // Balancing sees the moves first, at the next look, and waits for leaders to stay put.
        if (!moved) balance(view)
    }

    /**
     * The moves [rule] makes in [view], decided again on what the members report now if one of them
     * can be made: a member that stops hands its leaderships over before it stops answering.
     */
    private fun current(
        view: LeadershipView,
        rule: (LeadershipView) -> Map<DatabaseRef, List<MemberId>>,
    ): Map<DatabaseRef, List<MemberId>> {
        val moves = rule(view)
        if (moves.none { (database, candidates) -> candidates.any { keepsUp(database, it) } }) {
            return moves
        }
        directory.refresh()
        return rule(view())
    }

    /**
     * Hands the leadership of each database of [moves] over to the first of its candidates that
     * takes it, for [reason]; returns whether one moved. One that no member can take over is kept
     * in [waiting] and [announce]d once.
     */
    private fun handOver(
        moves: Map<DatabaseRef, List<MemberId>>,
        reason: MoveReason,
        waiting: MutableSet<UUID>,
        announce: (DatabaseRef) -> Unit,
    ): Boolean {
        var moved = false
        for ((database, candidates) in moves) {
            if (candidates.isEmpty()) {
                if (waiting.add(database.id)) announce(database)
                continue
            }
            waiting -= database.id
            if (candidates.any { transfer(database, it, reason) }) moved = true
        }
        waiting.retainAll(moves.filterValues { it.isEmpty() }.keys.map { it.id })
        return moved
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
