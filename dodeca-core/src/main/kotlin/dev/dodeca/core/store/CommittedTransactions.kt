package dev.dodeca.core.store

import java.io.IOException
import org.neo4j.kernel.database.Database
import org.neo4j.storageengine.AppendIndexProvider
import org.neo4j.storageengine.api.TransactionId
import org.neo4j.storageengine.api.TransactionIdStore
import org.neo4j.wal.CommandBatchCursor
import org.neo4j.wal.LogicalTransactionStore
import org.neo4j.wal.NoSuchLogEntryException

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

    /**
     * The database's logs no longer hold the copy's last transaction, which tells whether the
     * database continues the copy, or the one after it.
     */
    data object Pruned : Range

    /**
     * The database's last closed transaction, [last], comes before the copy's last: it holds
     * nothing after it, and cannot tell whether it continues the copy's history.
     */
    class Behind(val last: TransactionId) : Range

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
                        .highestGapFreeClosedTransaction
                        .transactionId()
                if (after > last.appendIndex()) return block(Behind(last))
                if (after == last.appendIndex()) {
                    return block(
                        if (last.checksum() == checksum) Continued(last, emptySequence())
                        else diverged(after, last.checksum(), checksum)
                    )
                }
                // The copy's last transaction too, but for the base one, which no log holds: its
                // checksum tells whether the database continues the copy, which Neo4j's logs
                // since 2025.05 no longer tell after it.
                val base = after == AppendIndexProvider.BASE_APPEND_INDEX
                val cursor =
                    try {
                        dependencies
                            .resolveDependency(LogicalTransactionStore::class.java)
                            .getCommandBatches(if (base) after + 1 else after)
                    } catch (_: NoSuchLogEntryException) {
                        return block(Pruned)
                    }
                cursor.use {
                    val copied =
                        if (base) {
                            TransactionIdStore.BASE_TX_CHECKSUM
                        } else {
                            if (!cursor.next()) {
                                throw IOException("the logs end before append index $after")
                            }
                            val copied = cursor.get()
                            if (copied.appendIndex() != after) {
                                throw IOException(
                                    "the logs hold ${copied.appendIndex()} where $after was"
                                )
                            }
                            copied.checksum()
                        }
                    if (copied != checksum) return block(diverged(after, copied, checksum))
                    val transactions = sequence {
                        var previous = checksum
                        var appendIndex = after
                        while (appendIndex < last.appendIndex()) {
                            val transaction = next(cursor, appendIndex + 1, previous)
                            yield(transaction)
                            previous = transaction.checksum
                            appendIndex = transaction.appendIndex
                        }
                    }
                    return block(Continued(last, transactions))
                }
            }
    }

    private fun next(
        cursor: CommandBatchCursor,
        appendIndex: Long,
        previousChecksum: Int,
    ): CommittedTransaction {
        if (!cursor.next()) throw IOException("the logs end before append index $appendIndex")
        val transaction = CommittedTransaction.of(cursor.get(), previousChecksum)
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
