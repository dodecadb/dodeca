package dev.dodeca.consensus

/**
 * How far the local member is in one consensus group, for metrics: read together, so that the
 * values are consistent with each other.
 */
public data class GroupProgress(
    /** The index of the last entry in the local log, committed or not. */
    val appendIndex: Long,
    /** The index of the last entry known committed. */
    val commitIndex: Long,
    /** The index of the last entry applied to the state machine. */
    val appliedIndex: Long,
    /** The index of the first entry still in the local log: earlier ones were purged. */
    val pruneIndex: Long,
    val term: Long,
    /** Whether the local member leads the group and accepts writes, as [Role.Leader]. */
    val leader: Boolean,
    /**
     * Milliseconds since the local member last heard from the leader: 0 on the leader, null when
     * there is none.
     */
    val sinceLeaderContactMillis: Long?,
    /**
     * On the leader, the index up to which each follower's log matches the leader's. Null on a
     * follower, or when the followers changed while they were read.
     */
    val followerMatchIndexes: Map<MemberId, Long>?,
    /**
     * On the leader, milliseconds since each follower last answered it. Null when
     * [followerMatchIndexes] is.
     */
    val followerContactMillis: Map<MemberId, Long>?,
    /** Snapshots taken since the group started on this member. */
    val snapshotsTaken: Long,
    /** Changes of the known leader since the group started on this member. */
    val leaderChanges: Long,
    /** Leaderships this member transferred to another member since the group started on it. */
    val leadershipTransfers: Long,
)
