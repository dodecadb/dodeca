package org.dozendb.cluster.core

import org.dozendb.cluster.api.LogEntry
import org.dozendb.cluster.api.Member
import org.dozendb.cluster.api.ReplicatedStateMachine

/**
 * Placeholder for groups whose database is not replicated yet: it accepts and discards entries. The
 * system database uses it until commit interception replicates its transactions. Its log is never
 * discarded, so no member ever needs a copy of it.
 */
class NoOpStateMachine : ReplicatedStateMachine {
    @Volatile private var applied = -1L

    override fun apply(entry: LogEntry): ByteArray {
        applied = entry.index
        return ByteArray(0)
    }

    override fun lastAppliedIndex(): Long = applied

    override fun durableIndex(): Long = -1

    /** Never called: no member discards the log of a group without state. */
    override fun onSnapshotNeeded(leader: Member?, minimumIndex: Long) {}
}
