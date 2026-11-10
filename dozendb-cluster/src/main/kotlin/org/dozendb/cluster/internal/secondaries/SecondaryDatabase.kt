package org.dozendb.cluster.internal.secondaries

import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.math.max
import org.dozendb.cluster.DatabaseGroups
import org.dozendb.cluster.internal.ClusterMetrics
import org.dozendb.cluster.internal.of
import org.dozendb.cluster.internal.replication.ClusterLeaseService
import org.dozendb.cluster.internal.replication.CommitFailure
import org.dozendb.cluster.internal.replication.ForeignStoreException
import org.dozendb.cluster.internal.replication.NotLeaderException
import org.dozendb.cluster.internal.secondaries.PullProtocol.Answer
import org.dozendb.cluster.internal.secondaries.PullProtocol.Status
import org.dozendb.consensus.BulkTransfer
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.HostPort
import org.dozendb.core.edition.HostedDatabase
import org.dozendb.core.store.CommittedTransaction
import org.dozendb.core.store.CommittedTransactions
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.HostedOnMode
import org.neo4j.io.pagecache.context.CursorContext
import org.neo4j.kernel.database.Database
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.impl.api.CommandCommitListeners
import org.neo4j.kernel.impl.api.CompleteTransaction
import org.neo4j.kernel.impl.api.InternalTransactionCommitProcess
import org.neo4j.kernel.impl.api.TransactionCommitProcess
import org.neo4j.kernel.impl.api.TransactionalProcessFactory
import org.neo4j.kernel.impl.api.txid.IdStoreTransactionIdGenerator
import org.neo4j.kernel.impl.transaction.log.TransactionAppender
import org.neo4j.kernel.impl.transaction.log.TransactionCommitmentFactory
import org.neo4j.kernel.impl.transaction.tracing.TransactionWriteEvent
import org.neo4j.logging.InternalLog
import org.neo4j.monitoring.DatabaseHealth
import org.neo4j.storageengine.api.StorageEngine
import org.neo4j.storageengine.api.TransactionApplicationMode
import org.neo4j.storageengine.api.TransactionIdStore
import org.neo4j.storageengine.api.cursor.StoreCursors

/**
 * One database that this member hosts as a secondary. It takes no writes: a transaction that writes
 * fails as on a follower, with [refusal]. It applies the transactions it pulls from an upstream
 * ([pull]) as a follower applies Raft entries: in external mode, so that ID generators track the
 * IDs they use, with the consensus index each carries. A pull's transactions are applied as one
 * chain, which forces the transaction log once: a secondary has no Raft log, so its transaction log
 * is forced as in Neo4j Community, and recovery after a crash restores a prefix of what it applied,
 * from where it pulls on.
 *
 * Each transaction must follow the last one here, by append index and checksum, and its checksum
 * here must be the upstream's once applied: every copy's transaction log holds the same
 * transactions. Otherwise the store holds another history, and the database panics.
 *
 * @property counts the database's metrics, of the pulls among them.
 * @property afterApply runs after each chain of pulled transactions is applied.
 * @property groups the groups of the databases: the database's [ref] names its group, whose members
 *   it pulls from.
 */
internal class SecondaryDatabase(
    val namedDatabaseId: NamedDatabaseId,
    private val transfer: BulkTransfer,
    private val refusal: () -> String,
    private val counts: ClusterMetrics.Counters,
    private val log: InternalLog,
    private val afterApply: () -> Unit = {},
    private val groups: DatabaseGroups = DatabaseGroups(),
) : HostedDatabase {
    val ref: DatabaseRef
        get() = groups.of(namedDatabaseId)

    override val hostedOnMode: HostedOnMode = HostedOnMode.REPLICA

    override val leaseService = ClusterLeaseService({ false }) { NotLeaderException(refusal()) }

    /** The local commit process that applies pulled transactions, set when the database starts. */
    @Volatile private var applyProcess: TransactionCommitProcess? = null

    private lateinit var database: Database

    /** Held while a chain of pulled transactions is applied. */
    private val applying = ReentrantLock()

    /** Whether the database runs and pulls, from [start] to [stop]. */
    @Volatile
    var pulling: Boolean = false
        private set

    override fun commitProcessFactory(
        local: TransactionalProcessFactory
    ): TransactionalProcessFactory =
        object : TransactionalProcessFactory by local {
            override fun create(
                appender: TransactionAppender,
                storageEngine: StorageEngine,
                readOnlyDatabaseChecker: DatabaseReadOnlyChecker,
                preAllocateSpaceInStoreFiles: Boolean,
                commandCommitListeners: CommandCommitListeners,
                prefetchPages: Boolean,
            ): TransactionCommitProcess {
                applyProcess =
                    InternalTransactionCommitProcess(
                        appender,
                        storageEngine,
                        preAllocateSpaceInStoreFiles,
                        commandCommitListeners,
                    ) {
                        prefetchPages
                    }
                return TransactionCommitProcess { _, _, _ ->
                    throw CommitFailure.NOT_LEADER.exception(refusal())
                }
            }
        }

    override fun attach(database: Database) {
        this.database = database
    }

    /** Starts pulling, once the database runs. */
    fun start() {
        pulling = true
    }

    /** Stops pulling, before the database stops; returns once no transaction is being applied. */
    fun stop() {
        pulling = false
        applying.withLock {}
    }

    /** The last transaction applied here. */
    fun last() =
        database.dependencyResolver
            .resolveDependency(TransactionIdStore::class.java)
            .lastCommittedTransaction

    /**
     * Pulls the transactions after the last one here from [upstream], up to about [maximumBytes] of
     * them, and applies them.
     *
     * @throws org.dozendb.consensus.ConsensusUnavailableException if [upstream] cannot be reached.
     * @throws ForeignStoreException if the store holds another history than the upstream's, after
     *   panicking the database.
     */
    fun pull(upstream: HostPort, maximumBytes: Int): PullOutcome {
        if (!pulling) return PullOutcome.Stopped
        val last = last()
        val request = PullProtocol.Request(ref, last.appendIndex(), last.checksum(), maximumBytes)
        counts.pullsMade.increment()
        counts.highestRequested.accumulateAndGet(last.id()) { a, b -> max(a, b) }
        val readers = database.storageEngineFactory.commandReaderFactory()
        return transfer.fetchBulk(upstream, PullProtocol.NAME, PullProtocol.encode(request)) { input
            ->
            PullProtocol.read(input, readers) { answer ->
                when (answer) {
                    is Answer.Refused ->
                        if (answer.status == Status.DIVERGED) fail(answer.message)
                        else PullOutcome.Refused(answer.status, answer.message)
                    is Answer.Behind -> behind(answer)
                    is Answer.Pulled -> {
                        val chain = answer.transactions.toList()
                        when {
                            chain.isEmpty() -> PullOutcome.CaughtUp
                            !apply(chain) -> PullOutcome.Stopped
                            chain.last().appendIndex >= answer.last -> PullOutcome.CaughtUp
                            else -> PullOutcome.Behind
                        }
                    }
                }
            }
        }
    }

    /**
     * The upstream is behind this secondary, as when it pulled from another further ahead: the
     * upstream's last transaction must be one of this store's, else the store holds another
     * history. Where this store's logs no longer hold it, it cannot tell.
     */
    private fun behind(answer: Answer.Behind): PullOutcome =
        CommittedTransactions.read(database, answer.last, answer.checksum) { range ->
            if (range is CommittedTransactions.Diverged) {
                fail(
                    "its upstream's last transaction, at append index ${answer.last} with " +
                        "checksum ${answer.checksum}, is not this store's"
                )
            }
            PullOutcome.CaughtUp
        }

    /** Applies [chain] unless pulling stopped; returns whether it did. */
    private fun apply(chain: List<CommittedTransaction>): Boolean {
        if (!applyChain(chain)) return false
        afterApply()
        return true
    }

    private fun applyChain(chain: List<CommittedTransaction>): Boolean = applying.withLock {
        if (!pulling) return false
        val resolver = database.dependencyResolver
        val idStore = resolver.resolveDependency(TransactionIdStore::class.java)
        val storageEngine = resolver.resolveDependency(StorageEngine::class.java)
        var previous = idStore.lastCommittedTransaction.let { it.appendIndex() to it.checksum() }
        for (transaction in chain) {
            if (
                transaction.appendIndex != previous.first + 1 ||
                    transaction.previousChecksum != previous.second
            ) {
                fail(
                    "$transaction after checksum ${transaction.previousChecksum} does not " +
                        "follow append index ${previous.first} with checksum ${previous.second}"
                )
            }
            previous = transaction.appendIndex to transaction.checksum
        }
        val commitments = TransactionCommitmentFactory(idStore)
        val ids = IdStoreTransactionIdGenerator(idStore)
        val opened = mutableListOf<AutoCloseable>()
        try {
            var first: CompleteTransaction? = null
            var last: CompleteTransaction? = null
            for (transaction in chain) {
                val context: CursorContext = database.cursorContextFactory.create(APPLY_TAG)
                opened += context
                val cursors: StoreCursors = storageEngine.createStorageCursors(context)
                opened += cursors
                val next =
                    CompleteTransaction(
                        transaction.batch,
                        context,
                        cursors,
                        commitments.newCommitment(),
                        ids,
                    )
                if (last == null) first = next else last.next(next)
                last = next
            }
            checkNotNull(applyProcess) { "no commit process" }
                .commit(first, TransactionWriteEvent.NULL, TransactionApplicationMode.EXTERNAL)
        } finally {
            opened.asReversed().forEach { it.close() }
        }
        val applied = idStore.lastCommittedTransaction
        val expected = chain.last()
        if (applied.id() != expected.transactionId || applied.checksum() != expected.checksum) {
            fail(
                "$expected was applied as transaction ${applied.id()} with checksum " +
                    "${applied.checksum()}, not ${expected.checksum}"
            )
        }
        counts.highestReceived.accumulateAndGet(applied.id()) { a, b -> max(a, b) }
        true
    }

    /** Panics the database, whose store does not continue its upstream's history: [reason]. */
    private fun fail(reason: String): Nothing {
        pulling = false
        val layout = database.databaseLayout
        val message =
            "The store of database `${layout.databaseName}` holds transactions that its upstream " +
                "does not have: $reason. Its files were probably put in place by hand, from " +
                "another cluster or database. To copy the database from the other servers " +
                "instead, stop this server, delete ${layout.databaseDirectory()} and " +
                "${layout.transactionLogsDirectory}, and start it again."
        log.error(message)
        val failure = ForeignStoreException(message)
        database.dependencyResolver.resolveDependency(DatabaseHealth::class.java).panic(failure)
        throw failure
    }

    private companion object {
        const val APPLY_TAG = "dozendbSecondaryApply"
    }
}

/** What one pull of a secondary came to. */
internal sealed interface PullOutcome {
    /** It applied everything its upstream had when the pull began. */
    data object CaughtUp : PullOutcome

    /** It applied as much as one pull carries, and its upstream has more. */
    data object Behind : PullOutcome

    /** It stopped pulling meanwhile. */
    data object Stopped : PullOutcome

    /** The upstream refused, with [status] and why. */
    data class Refused(val status: Status, val message: String) : PullOutcome
}
