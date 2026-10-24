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

internal enum class MoveReason(val text: String) {
    READ_ONLY("the database is read-only on this member")
}

/** Where leadership goes. */
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
}
