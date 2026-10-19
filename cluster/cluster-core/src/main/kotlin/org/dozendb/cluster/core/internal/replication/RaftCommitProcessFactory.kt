package org.dozendb.cluster.core.internal.replication

import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker
import org.neo4j.kernel.impl.api.CommandCommitListeners
import org.neo4j.kernel.impl.api.InternalTransactionCommitProcess
import org.neo4j.kernel.impl.api.LeaseService
import org.neo4j.kernel.impl.api.TransactionCommitProcess
import org.neo4j.kernel.impl.api.TransactionalProcessFactory
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore
import org.neo4j.kernel.impl.transaction.log.TransactionAppender
import org.neo4j.kernel.impl.transaction.tracing.TransactionWriteEvent
import org.neo4j.storageengine.api.StorageEngine
import org.neo4j.storageengine.api.StorageEngineTransaction
import org.neo4j.storageengine.api.TransactionApplicationMode

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
    ): TransactionCommitProcess {
        replica.applyProcess =
            InternalTransactionCommitProcess(
                appender,
                storageEngine,
                preAllocateSpaceInStoreFiles,
                commandCommitListeners,
            ) {
                prefetchPages
            }
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
    ): Long {
        if (batch.next() != null) {
            // Only multi-versioned stores commit in chunks; Community uses the record format.
            throw CommitFailure.COMMIT_FAILED.exception("Chunked transactions cannot be replicated")
        }
        readOnlyChecker.check()
        if (replica.bootstrapping && batch.commandBatch().leaseId == LeaseService.NO_LEASE) {
            // Neo4j initialising a store it just created, before replication starts.
            return checkNotNull(replica.applyProcess).commit(batch, transactionWriteEvent, mode)
        }
        return replica.replicate(batch.commandBatch())
    }
}
