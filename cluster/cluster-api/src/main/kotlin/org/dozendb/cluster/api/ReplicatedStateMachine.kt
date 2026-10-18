package org.dozendb.cluster.api

/** A committed log entry, delivered to [ReplicatedStateMachine.apply] in log order. */
public class LogEntry(
    public val index: Long,
    public val term: Long,
    public val payload: ByteArray,
) {
    override fun toString(): String = "LogEntry(index=$index, term=$term, ${payload.size} bytes)"
}

/**
 * The state replicated by one consensus group, implemented by the database integration.
 *
 * Entries arrive one at a time, in log order, on a thread owned by the consensus implementation.
 * After a restart the consensus implementation may deliver entries the state machine has already
 * applied, so [apply] must ignore entries at or below [lastAppliedIndex].
 */
public interface ReplicatedStateMachine {
    /**
     * Applies [entry] and returns a response, which reaches the member that submitted the entry if
     * that member applies it while it waits. May block. An exception is treated as a fatal error
     * for the group.
     */
    public fun apply(entry: LogEntry): ByteArray

    /** The index of the last entry this state machine has applied durably, or -1 if none. */
    public fun lastAppliedIndex(): Long
}
