package org.dozendb.cluster.internal.reads

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Consumer
import org.dozendb.cluster.internal.ReadConsistency
import org.neo4j.dbms.database.DatabaseContext
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.fabric.bookmark.LocalGraphTransactionIdTracker
import org.neo4j.fabric.bookmark.TransactionBookmarkManager
import org.neo4j.fabric.executor.Location
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.impl.query.ConstituentTransactionFactory
import org.neo4j.router.impl.transaction.database.LocalDatabaseTransactionFactory
import org.neo4j.router.transaction.DatabaseTransaction
import org.neo4j.router.transaction.TransactionInfo

/**
 * Neo4j's transactions on the databases of this member, which begin after their [waits]. The wait
 * happens where a transaction runs: one that Neo4j's query router runs on the leader of its
 * database waits there, not here too. A transaction whose bookmarks name a transaction of its
 * database tells [bookmarked] before Neo4j waits until this member holds it, so that a secondary
 * pulls it at once.
 */
internal class LinearizableLocalTransactions(
    databaseProvider: DatabaseContextProvider<out DatabaseContext>,
    transactionIdTracker: LocalGraphTransactionIdTracker,
    private val waits: LinearizableWaits,
    private val bookmarked: (NamedDatabaseId, Long) -> Unit = { _, _ -> },
) : LocalDatabaseTransactionFactory(databaseProvider, transactionIdTracker) {
    override fun beginTransaction(
        location: Location.Local,
        transactionInfo: TransactionInfo,
        bookmarkManager: TransactionBookmarkManager,
        terminationCallback: Consumer<Status>,
        // Null when Neo4j's query router begins the transaction with BEGIN, before any query.
        constituentTransactionFactory: ConstituentTransactionFactory?,
    ): DatabaseTransaction {
        val database = location.databaseReference().databaseId()
        waits.await(transactionInfo, database)
        bookmarkManager.getBookmarkForLocal(location).ifPresent {
            bookmarked(database, it.transactionId())
        }
        return super.beginTransaction(
            location,
            transactionInfo,
            bookmarkManager,
            terminationCallback,
            constituentTransactionFactory,
        )
    }
}

/**
 * The waits of transactions that ask for linearizable reads: they begin on a database only once
 * this member has applied everything the cluster had committed when they asked
 * ([awaitLinearizable]).
 */
internal class LinearizableWaits(
    private val defaultConsistency: ReadConsistency,
    private val awaitLinearizable: (NamedDatabaseId) -> Unit,
) {
    /**
     * The waits that failed, by transaction and database. Neo4j's query router begins a transaction
     * on the session's database as the transaction begins and, if that fails, again with its first
     * query, which then fails the same way without waiting again. Transactions are told apart by
     * identity: two of one connection may be equal.
     */
    private val failures: Cache<TransactionInfo, MutableMap<UUID, RuntimeException>> =
        Caffeine.newBuilder().weakKeys().build()

    /** Waits, if [transaction] asks for linearizable reads, before it begins on [database]. */
    fun await(transaction: TransactionInfo, database: NamedDatabaseId) {
        val consistency = ReadConsistency.of(transaction.txMetadata(), defaultConsistency)
        if (consistency != ReadConsistency.LINEARIZABLE) return
        val id = database.databaseId().uuid()
        failures.getIfPresent(transaction)?.get(id)?.let { throw it }
        try {
            awaitLinearizable(database)
        } catch (e: RuntimeException) {
            failures.get(transaction) { ConcurrentHashMap() }[id] = e
            throw e
        }
    }
}
