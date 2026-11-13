package dev.dodeca.consensus

/**
 * The role of the local member in one consensus group.
 *
 * Sealed so that adding a role, such as a non-voting read replica, forces every `when` over roles
 * to handle it.
 */
public sealed interface Role {
    /**
     * Accepts writes for the group: it leads the current term and has applied every entry committed
     * before that term.
     */
    public data object Leader : Role

    /** Votes and replicates, rejects writes. */
    public data object Follower : Role

    /** Is running an election, or has won one and cannot accept writes yet. */
    public data object Candidate : Role

    /**
     * Does not take part in the group: not started, stopped, shutting down, or not one of its
     * voting members.
     */
    public data object Inactive : Role
}

/** A snapshot of the local member's view of one consensus group. */
public data class GroupStatus(
    val database: DatabaseRef,
    val role: Role,
    val leader: MemberId?,
    val term: Long,
    val commitIndex: Long,
    val appliedIndex: Long,
)
