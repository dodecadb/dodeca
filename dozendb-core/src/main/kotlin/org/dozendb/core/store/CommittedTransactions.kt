package org.dozendb.core.store

import java.io.IOException
import org.neo4j.kernel.database.Database
import org.neo4j.kernel.impl.transaction.log.CommandBatchCursor
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore
import org.neo4j.kernel.impl.transaction.log.NoSuchLogEntryException
import org.neo4j.storageengine.api.TransactionId
import org.neo4j.storageengine.api.TransactionIdStore

/**
 * A running database's committed transactions, read from its logs for a copy of it that continues
 * from its last transaction: a differential backup, or a secondary.
 */
object CommittedTransactions {
    /** What [read] finds after a copy's last transaction. */
    sealed interface Range

    /**
     * The database continues the copy: [transactions] are the ones after the copy's last, up to
     * [last], the database's last closed transaction when reading began.
     */
    class Continued(val last: TransactionId, val transactions: Sequence<CommittedTransaction>) :
        Range

    /** The database's logs no longer hold the transaction after the copy's last. */
    data object Pruned : Range

    /** The database does not continue the copy's history, for [reason]. */
    class Diverged(val reason: String) : Range

    /**
     * Passes [block] what [database] holds after the transaction at append index [after], with
     * checksum [checksum], the last of a copy. The transactions are read from the logs as [block]
     * takes them, and only then: checkpoints, and with them the pruning of logs, wait until it
     * returns.
     */
    fun <T> read(database: Database, after: Long, checksum: Int, block: (Range) -> T): T {
        require(after > 0) { "invalid append index $after" }
        val dependencies = database.dependencyResolver
        database.storeCopyCheckPointMutex
            .storeCopy {}
            .use {
                val last =
                    dependencies
                        .resolveDependency(TransactionIdStore::class.java)
                        .lastClosedTransaction
                        .transactionId()
                if (after > last.appendIndex()) {
                    return block(
                        Diverged(
                            "the database's last transaction has append index ${last.appendIndex()}, " +
                                "before the copy's last"
                        )
                    )
                }
                if (after == last.appendIndex()) {
                    return block(
                        if (last.checksum() == checksum) Continued(last, emptySequence())
                        else diverged(after, last.checksum(), checksum)
                    )
                }
                val cursor =
                    try {
                        dependencies
                            .resolveDependency(LogicalTransactionStore::class.java)
                            .getCommandBatches(after + 1)
                    } catch (_: NoSuchLogEntryException) {
                        return block(Pruned)
                    }
                cursor.use {
                    val first = next(cursor, after + 1)
                    if (first.previousChecksum != checksum) {
                        return block(diverged(after, first.previousChecksum, checksum))
                    }
                    val transactions = sequence {
                        var transaction = first
                        yield(transaction)
                        while (transaction.appendIndex < last.appendIndex()) {
                            transaction = next(cursor, transaction.appendIndex + 1)
                            yield(transaction)
                        }
                    }
                    return block(Continued(last, transactions))
                }
            }
    }

    private fun next(
        cursor: CommandBatchCursor,
        appendIndex: Long,
    ): CommittedTransaction {
        if (!cursor.next()) throw IOException("the logs end before append index $appendIndex")
        val transaction = CommittedTransaction.of(cursor.get())
        if (transaction.appendIndex != appendIndex) {
            throw IOException("the logs hold $transaction where append index $appendIndex was")
        }
        return transaction
    }

    private fun diverged(after: Long, actual: Int, expected: Int) =
        Diverged(
            "the database's transaction at append index $after has checksum $actual, not the " +
                "copy's $expected"
        )
}
