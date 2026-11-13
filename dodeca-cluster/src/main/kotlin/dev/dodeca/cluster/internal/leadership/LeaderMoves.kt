package dev.dodeca.cluster.internal.leadership

import dev.dodeca.consensus.DatabaseRef
import dev.dodeca.consensus.GroupProgress
import dev.dodeca.consensus.MemberId
import java.time.Duration

/**
 * Who leads the replicated databases and who could, as one member sees it when it decides where
 * leadership goes. Members that see the same decide the same.
 */
internal data class LeadershipView(
    val databases: List<Leadership>,
    /** The server tags of each member, as in its server record. */
    val tags: Map<MemberId, Set<String>> = emptyMap(),
) {
    /** How many databases [member] leads. */
    fun led(member: MemberId): Int = databases.count { it.leader == member }

    /** Whether [member] carries the server tag [tag]; no member carries none. */
    fun carries(member: MemberId, tag: String?): Boolean =
        tag != null && tag in tags[member].orEmpty()
}

/**
 * @property leader the member that leads [database], if one is known.
 * @property eligible the members other than [leader] that could take the leadership over: voting
 *   members of its group that run it, where it is writable, and that answer.
 * @property writableOnLeader false when [database] is read-only on [leader].
 * @property settled whether no change of the group's members is in progress.
 * @property priorityTag the server tag of the members that should lead [database], if any.
 * @property placed the members [database] is placed on; none if it is not placed, as `system`,
 *   which every member hosts.
 */
internal data class Leadership(
    val database: DatabaseRef,
    val leader: MemberId?,
    val eligible: Set<MemberId>,
    val writableOnLeader: Boolean = true,
    val settled: Boolean = true,
    val priorityTag: String? = null,
    val placed: Set<MemberId> = emptySet(),
) {
    /** Whether [leader] leads [database] although it is placed on other members. */
    val movesOffLeader: Boolean
        get() = leader != null && placed.isNotEmpty() && leader !in placed
}

/** A transfer of the leadership of [database] from [from] to [to]. */
internal data class Move(
    val database: DatabaseRef,
    val from: MemberId,
    val to: MemberId,
    val reason: MoveReason,
)

internal enum class MoveReason(val text: String) {
    READ_ONLY("the database is read-only on this member"),
    LEAVING("the database moves off this member"),
    PRIORITY("to a member with the database's priority tag"),
    BALANCING("to even out how many databases each member leads"),
}

/**
 * Where leadership goes: the rules of `dbms.cluster.raft.leader_transfer.balancing_strategy` and
 * `db.cluster.raft.leader_transfer.priority_tag`, and off a member a database moves off.
 */
internal object LeaderMoves {
    /**
     * [candidates] in the order they take a leadership over: those that carry [tag] first, then
     * fewest databases led, then by id.
     */
    fun rank(
        view: LeadershipView,
        candidates: Collection<MemberId>,
        tag: String? = null,
    ): List<MemberId> =
        candidates.sortedWith(
            compareBy<MemberId> { !view.carries(it, tag) }.thenBy { view.led(it) }.thenBy { it }
        )

    /**
     * The databases that [self] leads although they are read-only on it, each with the eligible
     * members in the order to try them; empty when none could take it over.
     */
    fun readOnly(view: LeadershipView, self: MemberId): Map<DatabaseRef, List<MemberId>> =
        view.databases
            .filter { it.leader == self && !it.writableOnLeader }
            .associate { it.database to rank(view, it.eligible, it.priorityTag) }

    /**
     * The databases that [self] leads although they are placed on other members, as while their
     * copies move off it, each with the eligible members in the order to try them; empty when none
     * could take it over yet. Those read-only on [self] are left to [readOnly].
     */
    fun leaving(view: LeadershipView, self: MemberId): Map<DatabaseRef, List<MemberId>> =
        view.databases
            .filter { it.leader == self && it.writableOnLeader && it.movesOffLeader }
            .associate { it.database to rank(view, it.eligible, it.priorityTag) }

    /**
     * The databases with a priority tag that [self] leads without carrying the tag, each with the
     * eligible members that carry it in the order to try them; empty when none could take it over,
     * and [self] keeps leading it. Those read-only on [self] are left to [readOnly], those that
     * move off it to [leaving].
     */
    fun priority(view: LeadershipView, self: MemberId): Map<DatabaseRef, List<MemberId>> =
        view.databases
            .filter {
                it.leader == self &&
                    it.writableOnLeader &&
                    !it.movesOffLeader &&
                    it.priorityTag != null &&
                    !view.carries(self, it.priorityTag)
            }
            .associate { leadership ->
                leadership.database to
                    rank(
                        view,
                        leadership.eligible.filter { view.carries(it, leadership.priorityTag) },
                    )
            }

    /**
     * The cluster's next move to even out how many databases each member leads, or null if none
     * helps. The giver is the member that leads the most databases (ties: the lowest id) and leads
     * one that an eligible member leading at least two fewer can take over; it moves the first such
     * database (by name, `system` last) to the eligible member that leads the fewest. Each move
     * lowers the sum of the squared counts, so moves stop. Databases with a priority tag count, but
     * are never moved: [priority] decides where they go; nor are those that move off their leader,
     * which [leaving] moves.
     */
    fun balancing(view: LeadershipView): Move? {
        val leaders = view.databases.mapNotNull { it.leader }.toSet()
        val givers =
            leaders.sortedWith(compareByDescending<MemberId> { view.led(it) }.thenBy { it })
        for (giver in givers) {
            val count = view.led(giver)
            val databases =
                view.databases
                    .filter {
                        it.leader == giver &&
                            it.writableOnLeader &&
                            !it.movesOffLeader &&
                            it.priorityTag == null
                    }
                    .sortedWith(
                        compareBy<Leadership> { it.database.name == SYSTEM }
                            .thenBy { it.database.name }
                    )
            for (leadership in databases) {
                val to =
                    rank(view, leadership.eligible).firstOrNull { view.led(it) <= count - 2 }
                        ?: continue
                return Move(leadership.database, giver, to, MoveReason.BALANCING)
            }
        }
        return null
    }

    /**
     * Whether [follower] can take the leadership of a group over from the leader that reports
     * [progress] without holding its writes back for long: it answered within [contactLimit], and
     * its log holds every entry the group had committed at the leader's previous look,
     * [committedBefore] (none before the first). Under steady writes the faster follower
     * acknowledges each batch first and moves the commit index on, so one that keeps up still
     * trails it by the entries in flight, which the transfer waits for. One catching up after a
     * restart trails the earlier commit index too.
     */
    fun keepsUp(
        progress: GroupProgress,
        follower: MemberId,
        committedBefore: Long?,
        contactLimit: Duration,
    ): Boolean {
        val contact = progress.followerContactMillis?.get(follower) ?: return false
        val match = progress.followerMatchIndexes?.get(follower) ?: return false
        return contact <= contactLimit.toMillis() &&
            committedBefore != null &&
            match >= committedBefore
    }

    private const val SYSTEM = "system"
}
