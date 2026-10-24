package org.dozendb.cluster.internal.leadership

import java.time.Duration
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import org.dozendb.cluster.MemberDirectory
import org.dozendb.cluster.internal.replication.ReplicationService
import org.dozendb.consensus.ConsensusLog
import org.dozendb.consensus.ConsensusUnavailableException
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.LeaderPlacement
import org.dozendb.consensus.MemberId
import org.dozendb.consensus.Role
import org.dozendb.core.edition.DatabaseState
import org.neo4j.logging.InternalLog

/**
 * Moves the leadership of databases this member leads to other members, where Neo4j Enterprise
 * documents that leadership moves: away from a member where the database is read-only, and, with
 * [BalancingStrategy.EQUAL_BALANCING], so that every member leads as many databases as the others,
 * give or take one (see [LeaderMoves.balancing]).
 *
 * Looks every [interval], and soon after [request]. Balancing waits until leaders have stayed put
 * everywhere for [stabilityDelay], every database has a leader and no group changes its members;
 * moving off a read-only member does not wait. A database goes to the member, among those that
 * could lead it (see [Leadership.eligible]), that leads the fewest databases; this member checks
 * first that the member answers it and keeps up with its log (see [LeaderMoves.keepsUp]), and hands
 * the leadership over without failing commits under way (see
 * `ReplicatedDatabase.transferLeadership`). It also ranks the members a leadership is handed over
 * to when this member stops applying a database.
 */
internal class LeaderBalancer(
    private val consensus: ConsensusLog,
    private val replication: ReplicationService,
    private val directory: MemberDirectory,
    private val strategy: () -> BalancingStrategy,
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

    /** Databases read-only here that no member could take over, logged once; executor only. */
    private val stuck = mutableSetOf<UUID>()

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

    fun stop() {
        executor.shutdownNow()
    }

    /**
     * The members to hand the leadership of [database] over to, those that could lead it first,
     * fewest led first.
     */
    override fun rank(database: DatabaseRef, candidates: List<MemberId>): List<MemberId> {
        val view = view()
        val eligible = view.databases.firstOrNull { it.database == database }?.eligible.orEmpty()
        return LeaderMoves.rank(view, candidates.filter { it in eligible })
    }

    /** Who leads the databases replicated here and who could, as this member sees it now. */
    fun view(): LeadershipView = LeadershipView(replication.created().mapNotNull(::leadership))

    private fun leadership(database: DatabaseRef): Leadership? {
        val status = consensus.status(database)
        if (status.role == Role.Inactive) return null
        val membership =
            try {
                consensus.membership(database)
            } catch (_: ConsensusUnavailableException) {
                return null
            }
        val leader = status.leader
        return Leadership(
            database,
            leader,
            membership.members
                .map { it.id }
                .filter { it != leader && canLead(it, database.id) }
                .toSet(),
            writableOnLeader =
                leader == null || database.id !in directory.readOnlyDatabases(leader),
            settled = membership.settled,
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
        for (database in replication.created()) {
            val progress = consensus.progress(database) ?: continue
            if (progress.leader) committed[database.id] = progress.commitIndex
        }
    }

    /**
     * Transfers the leadership of [database] to [to] if [to] keeps up with this member (see
     * [LeaderMoves.keepsUp]); returns whether [to] leads it afterwards.
     */
    private fun transfer(database: DatabaseRef, to: MemberId, reason: MoveReason): Boolean {
        val replica = replication.replica(database) ?: return false
        val progress = consensus.progress(database) ?: return false
        if (!LeaderMoves.keepsUp(progress, to, committed[database.id], contactLimit)) return false
        return replica.transferLeadership(to, reason.text, transferTimeout)
    }
}
