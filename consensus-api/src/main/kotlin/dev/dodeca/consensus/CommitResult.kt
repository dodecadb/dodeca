package dev.dodeca.consensus

/** Outcome of [ConsensusLog.submit]. */
public sealed interface CommitResult {
    /**
     * The entry is committed by a majority at [index], written in [term], and applied by the local
     * state machine, which answered [response].
     */
    public class Committed(
        public val index: Long,
        public val term: Long,
        public val response: ByteArray = ByteArray(0),
    ) : CommitResult {
        override fun equals(other: Any?): Boolean =
            other is Committed &&
                index == other.index &&
                term == other.term &&
                response.contentEquals(other.response)

        override fun hashCode(): Int =
            (index.hashCode() * 31 + term.hashCode()) * 31 + response.contentHashCode()

        override fun toString(): String =
            "Committed(index=$index, term=$term, response=${response.size} bytes)"
    }

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
