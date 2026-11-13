package dev.dodeca.cluster.internal.replication

import dev.dodeca.consensus.LogEntry
import dev.dodeca.consensus.Member
import dev.dodeca.consensus.ReplicatedStateMachine
import dev.dodeca.core.checkpoint.CheckpointIoController
import java.io.ByteArrayOutputStream
import org.neo4j.common.Subject
import org.neo4j.io.fs.OutputStreamWritableChannel
import org.neo4j.kernel.database.Database
import org.neo4j.kernel.impl.api.CompleteTransaction
import org.neo4j.kernel.impl.api.LeaseService
import org.neo4j.kernel.impl.api.TransactionCommitProcess
import org.neo4j.kernel.impl.api.txid.IdStoreTransactionIdGenerator
import org.neo4j.kernel.impl.transaction.tracing.TransactionWriteEvent
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.monitoring.DatabaseHealth
import org.neo4j.storageengine.api.CommandBatch
import org.neo4j.storageengine.api.Leases
import org.neo4j.storageengine.api.StorageEngine
import org.neo4j.storageengine.api.TransactionApplicationMode
import org.neo4j.storageengine.api.TransactionIdStore
import org.neo4j.wal.CompleteCommandBatch
import org.neo4j.wal.LogicalTransactionStore
import org.neo4j.wal.NoSuchLogEntryException
import org.neo4j.wal.TransactionCommitmentFactory
import org.neo4j.wal.checkpoint.CheckPointer
import org.neo4j.wal.checkpoint.SimpleTriggerInfo

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
 * on this member instead of skipping the entry. A store that holds another group's transactions
 * panics the database too, with a message saying how to replace it.
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
    private val ioController = database.ioController
    private val commandReaders = database.storageEngineFactory.commandReaderFactory()
    private val contexts = database.cursorContextFactory
    private val commitments = TransactionCommitmentFactory(transactionIdStore)
    private val transactionIds = IdStoreTransactionIdGenerator(transactionIdStore)
    private val transactionStore = resolver.resolveDependency(LogicalTransactionStore::class.java)
    private val health = resolver.resolveDependency(DatabaseHealth::class.java)
    private val layout = database.databaseLayout
    private val log = database.internalLogProvider.getLog(javaClass)

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
                Leases.NO_LEASES,
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
                    EmptyMemoryTracker.INSTANCE,
                )
            }
        }
        CheckpointIoController.unlimited(ioController) {
            checkPointer.forceCheckPoint(SimpleTriggerInfo("Start of a new Raft group"))
        }
        return last.consensusIndex()
    }

    override fun onSnapshotNeeded(leader: Member?, minimumIndex: Long) {
        onSnapshotNeeded.invoke(leader, minimumIndex)
    }

    /**
     * Whether the store's last transaction is the one [entry] carries: the same kernel version,
     * times and commands. True if the transaction log no longer holds it.
     */
    override fun isLastApplied(entry: LogEntry): Boolean {
        val last = transactionIdStore.lastCommittedTransaction
        check(last.consensusIndex() == entry.index) {
            "entry ${entry.index} is not the last applied, ${last.consensusIndex()}"
        }
        val stored =
            try {
                transactionStore.getCommandBatches(last.appendIndex()).use { cursor ->
                    if (cursor.next()) cursor.get().commandBatch() else null
                }
            } catch (_: NoSuchLogEntryException) {
                null
            }
        if (stored == null) {
            log.warn(
                "Cannot check that the last transaction of %s, %d, is the Raft group's entry %d: " +
                    "the transaction log no longer holds it.",
                layout.databaseName,
                last.id(),
                entry.index,
            )
            return true
        }
        val replicated =
            try {
                TransactionEnvelope.decode(entry.payload, entry.index, commandReaders)
            } catch (_: Exception) {
                return false
            }
        return fingerprint(stored).contentEquals(fingerprint(replicated))
    }

    /** What [isLastApplied] compares: what the transaction log keeps of a transaction. */
    private fun fingerprint(batch: CommandBatch): ByteArray {
        val bytes = ByteArrayOutputStream()
        val channel = OutputStreamWritableChannel(bytes)
        channel.put(batch.kernelVersion().version())
        channel.putLong(batch.timeStarted)
        channel.putLong(batch.latestCommittedTxWhenStarted)
        channel.putLong(batch.timeCommitted)
        channel.putInt(batch.commandCount())
        batch.accept { command ->
            command.serialize(channel)
            false
        }
        channel.close()
        return bytes.toByteArray()
    }

    /** Panics the database, whose store holds another Raft group's transactions. */
    override fun onForeignState(reason: String) {
        val message =
            "The store of database `${layout.databaseName}` holds transactions that its Raft " +
                "group does not have: $reason. Its files were probably put in place by hand, " +
                "from another cluster or database. To copy the database from the other members " +
                "instead, stop this server, delete ${layout.databaseDirectory()} and " +
                "${layout.transactionLogsDirectory}, and start it again."
        log.error(message)
        health.panic(ForeignStoreException(message))
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
                        EmptyMemoryTracker.INSTANCE,
                    )
                afterApply()
                return ApplyOutcome.encode(
                    ApplyOutcome.Applied(appendIndex, transaction.transactionId())
                )
            }
        }
    }

    private companion object {
        const val APPLY_TAG = "dodecaRaftApply"
        const val DETACH_TAG = "dodecaDetachFromPreviousGroup"
    }
}

/** A store holds transactions of another Raft group than its database's. */
internal class ForeignStoreException(message: String) : RuntimeException(message)
