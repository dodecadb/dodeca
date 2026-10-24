package org.dozendb.cluster.internal.leadership

import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.MemberId

/**
 * Who leads the replicated databases and who could, as one member sees it when it decides where
 * leadership goes. Members that see the same decide the same.
 */
internal data class LeadershipView(val databases: List<Leadership>) {
    /** How many databases [member] leads. */
    fun led(member: MemberId): Int = databases.count { it.leader == member }
}

/**
 * @property leader the member that leads [database], if one is known.
 * @property eligible the members other than [leader] that could take the leadership over: voting
 *   members of its group that run it, where it is writable, and that answer.
 * @property writableOnLeader false when [database] is read-only on [leader].
 * @property settled whether no change of the group's members is in progress.
 */
internal data class Leadership(
    val database: DatabaseRef,
    val leader: MemberId?,
    val eligible: Set<MemberId>,
    val writableOnLeader: Boolean = true,
    val settled: Boolean = true,
)

/** A transfer of the leadership of [database] from [from] to [to]. */
internal data class Move(
    val database: DatabaseRef,
    val from: MemberId,
    val to: MemberId,
    val reason: MoveReason,
)

internal enum class MoveReason(val text: String) {
    READ_ONLY("the database is read-only on this member"),
    BALANCING("to even out how many databases each member leads"),
}

/** Where leadership goes: the rules of `dbms.cluster.raft.leader_transfer.balancing_strategy`. */
internal object LeaderMoves {
    /** [candidates] in the order they take a leadership over: fewest databases led, then by id. */
    fun rank(view: LeadershipView, candidates: Collection<MemberId>): List<MemberId> =
        candidates.sortedWith(compareBy<MemberId> { view.led(it) }.thenBy { it })

    /**
     * The databases that [self] leads although they are read-only on it, each with the eligible
     * members in the order to try them; empty when none could take it over.
     */
    fun readOnly(view: LeadershipView, self: MemberId): Map<DatabaseRef, List<MemberId>> =
        view.databases
            .filter { it.leader == self && !it.writableOnLeader }
            .associate { it.database to rank(view, it.eligible) }

    /**
     * The cluster's next move to even out how many databases each member leads, or null if none
     * helps. The giver is the member that leads the most databases (ties: the lowest id) and leads
     * one that an eligible member leading at least two fewer can take over; it moves the first such
     * database (by name, `system` last) to the eligible member that leads the fewest. Each move
     * lowers the sum of the squared counts, so moves stop.
     */
    fun balancing(view: LeadershipView): Move? {
        val leaders = view.databases.mapNotNull { it.leader }.toSet()
        val givers =
            leaders.sortedWith(compareByDescending<MemberId> { view.led(it) }.thenBy { it })
        for (giver in givers) {
            val count = view.led(giver)
            val databases =
                view.databases
                    .filter { it.leader == giver && it.writableOnLeader }
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

    private const val SYSTEM = "system"
}
