package org.dozendb.cluster.core.internal.replication

import org.dozendb.cluster.api.LogEntry
import org.dozendb.cluster.api.Member
import org.dozendb.cluster.api.ReplicatedStateMachine
import org.neo4j.kernel.database.Database
import org.neo4j.kernel.impl.api.CompleteTransaction
import org.neo4j.kernel.impl.api.TransactionCommitProcess
import org.neo4j.kernel.impl.api.txid.IdStoreTransactionIdGenerator
import org.neo4j.kernel.impl.transaction.log.TransactionCommitmentFactory
import org.neo4j.kernel.impl.transaction.tracing.TransactionWriteEvent
import org.neo4j.storageengine.api.StorageEngine
import org.neo4j.storageengine.api.TransactionApplicationMode
import org.neo4j.storageengine.api.TransactionIdStore

/**
 * Applies committed entries of a replicated database, on every member, in log order.
 *
 * Transactions are applied in external mode, as commands that may come from another member, so ID
 * generators track the IDs they use. Each gets a local transaction ID from the database's own
 * sequence, which advances identically everywhere because every member applies the same entries
 * from the same store. The consensus index of the last applied entry is stored with the
 * transaction, which makes it the state machine's applied index across restarts.
 *
 * Any exception propagates: Neo4j has then already panicked the database, and Ratis stops the group
 * on this member instead of skipping the entry.
 */
internal class DatabaseStateMachine(
    database: Database,
    private val applyProcess: TransactionCommitProcess,
) : ReplicatedStateMachine {
    private val resolver = database.dependencyResolver
    private val transactionIdStore = resolver.resolveDependency(TransactionIdStore::class.java)
    private val storageEngine = resolver.resolveDependency(StorageEngine::class.java)
    private val commandReaders = database.storageEngineFactory.commandReaderFactory()
    private val contexts = database.cursorContextFactory
    private val commitments = TransactionCommitmentFactory(transactionIdStore)
    private val transactionIds = IdStoreTransactionIdGenerator(transactionIdStore)

    override fun lastAppliedIndex(): Long =
        transactionIdStore.lastCommittedTransaction.consensusIndex()

    /** None yet: the log is kept whole, so no member ever needs a copy. */
    override fun durableIndex(): Long = -1

    override fun onSnapshotNeeded(leader: Member?, minimumIndex: Long) {}

    override fun apply(entry: LogEntry): ByteArray {
        val batch = TransactionEnvelope.decode(entry.payload, entry.index, commandReaders)
        if (batch.leaseId.toLong() != entry.term) {
            // It ran under a lease of an earlier term; another leader may have changed what it
            // read.
            return ApplyOutcome.encode(ApplyOutcome.LeaseExpired(batch.leaseId, entry.term))
        }
        contexts.create(APPLY_TAG).use { cursorContext ->
            storageEngine.createStorageCursors(cursorContext).use { cursors ->
                val transaction =
                    CompleteTransaction(
                        batch,
                        cursorContext,
                        cursors,
                        commitments.newCommitment(),
                        transactionIds,
                    )
                val appendIndex =
                    applyProcess.commit(
                        transaction,
                        TransactionWriteEvent.NULL,
                        TransactionApplicationMode.EXTERNAL,
                    )
                return ApplyOutcome.encode(
                    ApplyOutcome.Applied(appendIndex, transaction.transactionId())
                )
            }
        }
    }

    private companion object {
        const val APPLY_TAG = "dozendbRaftApply"
    }
}
