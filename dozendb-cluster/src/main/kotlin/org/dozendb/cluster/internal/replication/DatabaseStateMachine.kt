package org.dozendb.cluster.internal.replication

import org.dozendb.consensus.LogEntry
import org.dozendb.consensus.Member
import org.dozendb.consensus.ReplicatedStateMachine
import org.neo4j.common.Subject
import org.neo4j.kernel.database.Database
import org.neo4j.kernel.impl.api.CompleteTransaction
import org.neo4j.kernel.impl.api.LeaseService
import org.neo4j.kernel.impl.api.TransactionCommitProcess
import org.neo4j.kernel.impl.api.txid.IdStoreTransactionIdGenerator
import org.neo4j.kernel.impl.transaction.log.CompleteCommandBatch
import org.neo4j.kernel.impl.transaction.log.TransactionCommitmentFactory
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo
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
 * Neo4j's latest checkpoint makes the entries up to its transaction durable: recovery restores them
 * without the Raft log, which may then discard them.
 *
 * Any exception propagates: Neo4j has then already panicked the database, and Ratis stops the group
 * on this member instead of skipping the entry.
 *
 * @property afterApply runs on the apply thread after each applied transaction.
 * @property onSnapshotNeeded replaces the database with a copy; see
 *   [ReplicatedStateMachine.onSnapshotNeeded].
 */
internal class DatabaseStateMachine(
    database: Database,
    private val applyProcess: TransactionCommitProcess,
    private val afterApply: () -> Unit,
    private val onSnapshotNeeded: (leader: Member?, minimumIndex: Long) -> Unit,
) : ReplicatedStateMachine {
    private val resolver = database.dependencyResolver
    private val transactionIdStore = resolver.resolveDependency(TransactionIdStore::class.java)
    private val storageEngine = resolver.resolveDependency(StorageEngine::class.java)
    private val checkPointer = resolver.resolveDependency(CheckPointer::class.java)
    private val commandReaders = database.storageEngineFactory.commandReaderFactory()
    private val contexts = database.cursorContextFactory
    private val commitments = TransactionCommitmentFactory(transactionIdStore)
    private val transactionIds = IdStoreTransactionIdGenerator(transactionIdStore)

    override fun lastAppliedIndex(): Long =
        transactionIdStore.lastCommittedTransaction.consensusIndex()

    override fun durableIndex(): Long =
        minOf(
            checkPointer
                .latestCheckPointInfo()
                .highestObservedClosedTransactionId()
                .consensusIndex(),
            lastAppliedIndex(),
        )

    /**
     * Makes this store the start of a new Raft group's history if its last transaction was
     * replicated in another group, as in a dump or copy of a clustered database: the new group's
     * entries up to that transaction's index would otherwise count as applied. Commits an empty
     * transaction without an index, and forces a checkpoint so that [durableIndex] does not report
     * the old index either. Only before the group starts and before the store is copied, while
     * nothing else commits. Returns the old group's index, or null if the store had none.
     */
    fun detachFromPreviousGroup(): Long? {
        val last = transactionIdStore.lastCommittedTransaction
        if (last.consensusIndex() < 0) return null
        val now = System.currentTimeMillis()
        val marker =
            CompleteCommandBatch(
                emptyList(),
                TransactionIdStore.UNKNOWN_CONSENSUS_INDEX,
                now,
                last.id(),
                now,
                LeaseService.NO_LEASE,
                last.kernelVersion(),
                Subject.SYSTEM,
            )
        contexts.create(DETACH_TAG).use { cursorContext ->
            storageEngine.createStorageCursors(cursorContext).use { cursors ->
                applyProcess.commit(
                    CompleteTransaction(
                        marker,
                        cursorContext,
                        cursors,
                        commitments.newCommitment(),
                        transactionIds,
                    ),
                    TransactionWriteEvent.NULL,
                    TransactionApplicationMode.EXTERNAL,
                )
            }
        }
        checkPointer.forceCheckPoint(SimpleTriggerInfo("Start of a new Raft group"))
        return last.consensusIndex()
    }

    override fun onSnapshotNeeded(leader: Member?, minimumIndex: Long) {
        onSnapshotNeeded.invoke(leader, minimumIndex)
    }

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
                afterApply()
                return ApplyOutcome.encode(
                    ApplyOutcome.Applied(appendIndex, transaction.transactionId())
                )
            }
        }
    }

    private companion object {
        const val APPLY_TAG = "dozendbRaftApply"
        const val DETACH_TAG = "dozendbDetachFromPreviousGroup"
    }
}
