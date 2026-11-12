package org.dozendb.core.checkpoint

import org.neo4j.configuration.Config
import org.neo4j.logging.InternalLogProvider
import org.neo4j.time.SystemNanoClock
import org.neo4j.wal.LogPosition
import org.neo4j.wal.checkpoint.AbstractCheckPointThreshold
import org.neo4j.wal.checkpoint.CheckPointThreshold
import org.neo4j.wal.checkpoint.CheckPointThresholdPolicy
import org.neo4j.wal.pruning.LogPruning

/**
 * Neo4j Enterprise's `db.checkpoint=CONTINUOUS`: a checkpoint whenever transactions were appended
 * since the last one, checked every [CHECK_MILLIS] ms, whatever `db.checkpoint.interval.*` say.
 * `db.checkpoint.iops.limit` spreads its flushing out. Neo4j loads it by name as a service.
 */
class ContinuousThresholdPolicy : CheckPointThresholdPolicy {
    override fun getName() = "continuous"

    override fun createThreshold(
        config: Config,
        clock: SystemNanoClock,
        logPruning: LogPruning,
        logProvider: InternalLogProvider,
    ): CheckPointThreshold = ContinuousThreshold()

    companion object {
        const val CHECK_MILLIS = 100L
    }
}

/**
 * Neo4j Enterprise's `db.checkpoint=VOLUMETRIC`: a checkpoint when transactions were appended since
 * the last one and moving the checkpoint on would let `db.tx_log.rotation.retention_policy` delete
 * transaction logs, checked every 10 seconds. Neo4j loads it by name as a service.
 */
class VolumetricThresholdPolicy : CheckPointThresholdPolicy {
    override fun getName() = "volumetric"

    override fun createThreshold(
        config: Config,
        clock: SystemNanoClock,
        logPruning: LogPruning,
        logProvider: InternalLogProvider,
    ): CheckPointThreshold = VolumetricThreshold(logPruning)
}

/** A threshold reached only once transactions were appended since the last checkpoint. */
internal abstract class AppendedThreshold(description: String) :
    AbstractCheckPointThreshold(description) {
    @Volatile private var checkpointed = 0L

    override fun initialize(appendIndex: Long, logPosition: LogPosition) {
        checkpointed = appendIndex
    }

    override fun checkPointHappened(appendIndex: Long, logPosition: LogPosition) {
        checkpointed = appendIndex
    }

    final override fun thresholdReached(lastAppendIndex: Long, logPosition: LogPosition) =
        lastAppendIndex > checkpointed && reached(logPosition)

    /** Whether to checkpoint now that transactions were appended, the last ending at [position]. */
    protected abstract fun reached(position: LogPosition): Boolean
}

internal class ContinuousThreshold : AppendedThreshold("continuous checkpointing") {
    override fun reached(position: LogPosition) = true

    override fun checkFrequencyMillis() = ContinuousThresholdPolicy.CHECK_MILLIS
}

/**
 * Asks [pruning] whether a checkpoint now would delete logs: a checkpoint prunes the logs before
 * its own, at most the current one. Every 10 seconds, not every second as Neo4j's volume threshold,
 * because a time-based retention policy reads the logs to answer.
 */
internal class VolumetricThreshold(private val pruning: LogPruning) :
    AppendedThreshold("transaction logs to prune") {
    override fun reached(position: LogPosition) = pruning.mightHaveLogsToPrune(position.logVersion)

    override fun checkFrequencyMillis() = CheckPointThreshold.DEFAULT_CHECKING_FREQUENCY_MILLIS
}
