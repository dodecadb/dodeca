package org.dozendb.cluster.api.fake

import org.dozendb.cluster.api.LogEntry
import org.dozendb.cluster.api.ReplicatedStateMachine

/**
 * A state machine that records applied entries in memory. Thread-safe, because real consensus
 * implementations apply on their own threads while tests inspect from the test thread.
 */
class RecordingStateMachine : ReplicatedStateMachine {
    private val applied = mutableListOf<LogEntry>()

    @Synchronized
    override fun apply(entry: LogEntry) {
        check(entry.index > lastAppliedIndex()) {
            "entry ${entry.index} applied twice or out of order, last applied ${lastAppliedIndex()}"
        }
        applied += entry
    }

    @Synchronized override fun lastAppliedIndex(): Long = applied.lastOrNull()?.index ?: -1

    /** The payloads applied so far, decoded as UTF-8. */
    @Synchronized fun payloads(): List<String> = applied.map { it.payload.decodeToString() }

    @Synchronized fun entries(): List<LogEntry> = applied.toList()
}
