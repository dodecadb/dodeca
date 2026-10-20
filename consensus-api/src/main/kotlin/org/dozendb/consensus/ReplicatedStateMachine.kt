package org.dozendb.consensus

/** A committed log entry, delivered to [ReplicatedStateMachine.apply] in log order. */
public class LogEntry(
    public val index: Long,
    public val term: Long,
    public val payload: ByteArray,
) {
    override fun toString(): String = "LogEntry(index=$index, term=$term, ${payload.size} bytes)"
}

/** The position of an entry in a group's log: its index and the term it was appended in. */
public data class LogPosition(val index: Long, val term: Long) {
    override fun toString(): String = "$index@$term"
}

/**
 * A point up to which a state machine holds every entry of its group's log, so that the log is no
 * longer needed up to there.
 *
 * @property position the last entry covered.
 * @property appliedIndex the [ReplicatedStateMachine.lastAppliedIndex] that a state machine holding
 *   this state reports at least. Lower than the index of [position] when the last entries covered
 *   did not change the state.
 */
public data class Snapshot(val position: LogPosition, val appliedIndex: Long) {
    init {
        require(appliedIndex <= position.index) {
            "applied index $appliedIndex is beyond the snapshot position $position"
        }
    }
}

/**
 * The state replicated by one consensus group, implemented by the database integration.
 *
 * Entries arrive one at a time, in log order, on a thread owned by the consensus implementation.
 * After a restart the consensus implementation may deliver entries the state machine has already
 * applied, so [apply] must ignore entries at or below [lastAppliedIndex].
 *
 * The consensus implementation discards log entries the state machine reports as [durableIndex]. A
 * member that later needs discarded entries replaces its state with a copy from another member: see
 * [onSnapshotNeeded].
 */
public interface ReplicatedStateMachine {
    /**
     * Applies [entry] and returns a response, which reaches the member that submitted the entry if
     * that member applies it while it waits. May block. An exception is treated as a fatal error
     * for the group.
     */
    public fun apply(entry: LogEntry): ByteArray

    /** The index of the last entry this state machine has applied, or -1 if none. */
    public fun lastAppliedIndex(): Long

    /**
     * The index of an applied entry up to which every effect survives a crash of this member
     * without the log, so that [lastAppliedIndex] reports at least this index after a restart; -1
     * if none. The consensus implementation may discard log entries up to an earlier such index.
     * Called from any thread; must not block.
     */
    public fun durableIndex(): Long

    /**
     * This member needs entries that the group's log no longer holds: it fell behind the start of
     * the leader's log, or its state is older than the group's snapshot. The state machine must
     * replace its state with a copy from another member that holds every entry up to at least
     * [minimumIndex]: detach it with [ConsensusLog.detachGroup], replace the state, record the copy
     * with [ConsensusLog.installSnapshot], and attach it again with [ConsensusLog.startGroup].
     *
     * Called at most once per attachment, on an internal thread; must not block.
     *
     * @param leader the leader that reported it, if known.
     */
    public fun onSnapshotNeeded(leader: Member?, minimumIndex: Long)
}
