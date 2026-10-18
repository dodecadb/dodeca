package org.dozendb.cluster.api

/** Outcome of [ConsensusLog.submit]. */
public sealed interface CommitResult {
    /** The entry is committed by a majority at [index], written in [term]. */
    public data class Committed(val index: Long, val term: Long) : CommitResult

    /**
     * Rejected before it was appended, because this member cannot accept writes for the group. The
     * entry will never commit and the caller may retry, preferably on [leaderHint].
     */
    public data class NotLeader(val leaderHint: MemberId?) : CommitResult

    /**
     * The entry may or may not commit, for example because leadership was lost after it was
     * appended. Callers must not assume either outcome.
     */
    public data class OutcomeUnknown(val cause: Throwable) : CommitResult
}
