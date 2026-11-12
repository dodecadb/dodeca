package org.dozendb.cluster.internal.replication

import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker
import org.neo4j.kernel.impl.api.CommandCommitListeners
import org.neo4j.kernel.impl.api.InternalTransactionCommitProcess
import org.neo4j.kernel.impl.api.LeaseService
import org.neo4j.kernel.impl.api.TransactionCommitProcess
import org.neo4j.kernel.impl.api.TransactionalProcessFactory
import org.neo4j.kernel.impl.transaction.tracing.TransactionWriteEvent
import org.neo4j.logging.LogProvider
import org.neo4j.memory.MemoryTracker
import org.neo4j.storageengine.api.StorageEngine
import org.neo4j.storageengine.api.StorageEngineTransaction
import org.neo4j.storageengine.api.TransactionApplicationMode
import org.neo4j.wal.LogicalTransactionStore
import org.neo4j.wal.TransactionAppender

/**
 * Commit processes of a replicated database. Kernel transactions commit through Raft; committed
 * entries are applied, on every member, by a plain local commit process that the state machine
 * uses.
 */
internal class RaftCommitProcessFactory(
    private val replica: ReplicatedDatabase,
    private val local: TransactionalProcessFactory,
) : TransactionalProcessFactory {
    override fun create(
        appender: TransactionAppender,
        storageEngine: StorageEngine,
        readOnlyDatabaseChecker: DatabaseReadOnlyChecker,
        preAllocateSpaceInStoreFiles: Boolean,
        commandCommitListeners: CommandCommitListeners,
        prefetchPages: Boolean,
        logProvider: LogProvider,
    ): TransactionCommitProcess {
        replica.applyProcess =
            InternalTransactionCommitProcess(
                appender,
                storageEngine,
                preAllocateSpaceInStoreFiles,
                commandCommitListeners,
                { prefetchPages },
                logProvider,
            )
        return RaftTransactionCommitProcess(replica, readOnlyDatabaseChecker)
    }

    override fun createRollbackProcess(
        storageEngine: StorageEngine,
        transactionStore: LogicalTransactionStore,
    ) = local.createRollbackProcess(storageEngine, transactionStore)
}

/** Commits a kernel transaction by replicating it and waiting for the local apply. */
internal class RaftTransactionCommitProcess(
    private val replica: ReplicatedDatabase,
    private val readOnlyChecker: DatabaseReadOnlyChecker,
) : TransactionCommitProcess {
    override fun commit(
        batch: StorageEngineTransaction,
        transactionWriteEvent: TransactionWriteEvent,
        mode: TransactionApplicationMode,
        memoryTracker: MemoryTracker,
    ): Long {
        if (batch.next() != null) {
            // Only multi-versioned stores commit in chunks; Community uses the record format.
            throw CommitFailure.COMMIT_FAILED.exception("Chunked transactions cannot be replicated")
        }
        readOnlyChecker.check()
        if (replica.commitsLocally && batch.commandBatch().leaseId == LeaseService.NO_LEASE) {
            // Before replication starts: Neo4j initialising a store it just created, or adapting
            // one this member brought to the cluster it formed.
            return checkNotNull(replica.applyProcess)
                .commit(batch, transactionWriteEvent, mode, memoryTracker)
        }
        return replica.replicate(batch.commandBatch())
    }
}
