package dev.dodeca.core.store

import java.io.IOException
import org.neo4j.kernel.impl.transaction.CommittedCommandBatchRepresentation
import org.neo4j.kernel.impl.transaction.CompleteBatchRepresentation
import org.neo4j.storageengine.api.CommandBatch
import org.neo4j.wal.entry.v42.LogEntryStartV4_2
import org.neo4j.wal.entry.v520.LogEntryStartV5_20

/**
 * A committed transaction, as a differential backup or a secondary copy of a database takes it:
 * what Neo4j's transaction log holds of it in its start, command and commit entries, and the
 * checksum of the transaction before it, [previousChecksum]. Appending [batch] as [transactionId]
 * at [appendIndex] writes the same entries, whose checksum is [checksum].
 */
class CommittedTransaction(
    val transactionId: Long,
    val appendIndex: Long,
    val previousChecksum: Int,
    val checksum: Int,
    val batch: CommandBatch,
) {
    override fun toString() = "transaction $transactionId (append index $appendIndex)"

    companion object {
        /**
         * The transaction Neo4j read from its log as [committed], which follows the transaction
         * whose checksum is [previousChecksum]. Neo4j's logs since 2025.05 do not tell that
         * checksum; older ones do, in the start entry, which must then agree.
         */
        fun of(
            committed: CommittedCommandBatchRepresentation,
            previousChecksum: Int,
        ): CommittedTransaction {
            // Community's record storage never writes chunked transactions or rollbacks.
            require(committed is CompleteBatchRepresentation) {
                "transaction ${committed.txId()} is not a complete batch: $committed"
            }
            val recorded =
                when (val start = committed.startEntry()) {
                    is LogEntryStartV5_20 -> start.previousChecksum
                    is LogEntryStartV4_2 -> start.previousChecksum
                    else -> previousChecksum
                }
            val transaction =
                CommittedTransaction(
                    committed.txId(),
                    committed.appendIndex(),
                    previousChecksum,
                    committed.checksum(),
                    committed.commandBatch(),
                )
            if (recorded != previousChecksum) {
                throw IOException(
                    "the log holds $transaction after checksum $recorded, not $previousChecksum"
                )
            }
            return transaction
        }
    }
}
