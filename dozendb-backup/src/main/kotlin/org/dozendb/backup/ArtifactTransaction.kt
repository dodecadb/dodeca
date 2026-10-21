package org.dozendb.backup

import org.neo4j.kernel.impl.transaction.CommittedCommandBatchRepresentation
import org.neo4j.kernel.impl.transaction.CompleteBatchRepresentation
import org.neo4j.storageengine.api.CommandBatch

/**
 * A committed transaction in a differential backup: what Neo4j's transaction log holds of it in its
 * start, command and commit entries. Appending [batch] as [transactionId] at [appendIndex] after
 * [previousChecksum] writes the same entries, whose checksum is [checksum].
 */
class ArtifactTransaction(
    val transactionId: Long,
    val appendIndex: Long,
    val previousChecksum: Int,
    val checksum: Int,
    val batch: CommandBatch,
) {
    override fun toString() = "transaction $transactionId (append index $appendIndex)"

    companion object {
        /** The transaction Neo4j read from its log as [committed]. */
        fun of(committed: CommittedCommandBatchRepresentation): ArtifactTransaction {
            // Community's record storage never writes chunked transactions or rollbacks.
            require(committed is CompleteBatchRepresentation) {
                "transaction ${committed.txId()} is not a complete batch: $committed"
            }
            return ArtifactTransaction(
                committed.txId(),
                committed.appendIndex(),
                committed.startEntry().previousChecksum,
                committed.checksum(),
                committed.commandBatch(),
            )
        }
    }
}
