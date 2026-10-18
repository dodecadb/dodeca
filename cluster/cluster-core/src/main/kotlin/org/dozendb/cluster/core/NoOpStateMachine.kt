package org.dozendb.cluster.core

import org.dozendb.cluster.api.LogEntry
import org.dozendb.cluster.api.ReplicatedStateMachine

/**
 * Placeholder for groups whose database is not replicated yet: it accepts and discards entries. The
 * system database uses it until commit interception replicates its transactions.
 */
class NoOpStateMachine : ReplicatedStateMachine {
    @Volatile private var applied = -1L

    override fun apply(entry: LogEntry) {
        applied = entry.index
    }

    override fun lastAppliedIndex(): Long = applied
}
