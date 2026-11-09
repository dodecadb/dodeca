package org.dozendb.cluster.internal.secondaries

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.nio.file.Path
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.dozendb.cluster.internal.secondaries.PullProtocol.Answer
import org.dozendb.cluster.internal.secondaries.PullProtocol.Status
import org.dozendb.consensus.DatabaseRef
import org.dozendb.core.store.CommittedTransaction
import org.dozendb.core.store.CommittedTransactions
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.configuration.connectors.HttpConnector
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.io.ByteUnit
import org.neo4j.kernel.database.Database
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.NullLog
import org.neo4j.storageengine.api.TransactionId
import org.neo4j.storageengine.api.TransactionIdStore

/** Pulls the transactions of a running database through [PullSource], as a secondary does. */
class PullSourceTest {
    @TempDir lateinit var root: Path
    private var dbms: DatabaseManagementService? = null

    @AfterEach
    fun stop() {
        dbms?.shutdown()
    }

    private fun start(pruning: Boolean = false): GraphDatabaseAPI {
        val builder =
            DatabaseManagementServiceBuilder(root)
                .setConfig(BoltConnector.enabled, false)
                .setConfig(HttpConnector.enabled, false)
                .setConfig(GraphDatabaseSettings.pagecache_memory, ByteUnit.mebiBytes(8))
        if (pruning) {
            builder
                .setConfig(
                    GraphDatabaseSettings.logical_log_rotation_threshold,
                    ByteUnit.kibiBytes(128),
                )
                .setConfig(GraphDatabaseSettings.keep_logical_logs, "false")
        }
        return builder.build().also { dbms = it }.database("neo4j") as GraphDatabaseAPI
    }

    private val GraphDatabaseAPI.internal: Database
        get() = dependencyResolver.resolveDependency(Database::class.java)

    private fun GraphDatabaseAPI.last(): TransactionId =
        dependencyResolver
            .resolveDependency(TransactionIdStore::class.java)
            .lastCommittedTransaction

    private fun GraphDatabaseAPI.ref(): DatabaseRef =
        DatabaseRef(databaseName(), databaseId().databaseId().uuid())

    private fun GraphDatabaseAPI.write(count: Int) =
        repeat(count) {
            executeTransactionally("CREATE (:Order {id: $it, filler: '${"x".repeat(2000)}'})")
        }

    /** Pulls through the source in this process, as a member would over the bulk transfer. */
    private fun <T> GraphDatabaseAPI.pull(request: PullProtocol.Request, block: (Answer) -> T): T {
        val source =
            PullSource(
                { dependencyResolver.resolveDependency(DatabaseContextProvider::class.java) },
                NullLog.getInstance(),
            )
        val answer = ByteArrayOutputStream()
        source.serve(PullProtocol.encode(request), answer)
        return PullProtocol.read(
            ByteArrayInputStream(answer.toByteArray()),
            internal.storageEngineFactory.commandReaderFactory(),
            block,
        )
    }

    private fun GraphDatabaseAPI.after(
        transaction: TransactionId,
        maximumBytes: Int = Int.MAX_VALUE,
    ) = PullProtocol.Request(ref(), transaction.appendIndex(), transaction.checksum(), maximumBytes)

    private fun fields(transactions: List<CommittedTransaction>) = transactions.map {
        listOf(
            it.transactionId,
            it.appendIndex,
            it.previousChecksum,
            it.checksum,
            it.batch.consensusIndex(),
        )
    }

    /** The transactions the database's own logs hold after [transaction]. */
    private fun GraphDatabaseAPI.logged(transaction: TransactionId): List<CommittedTransaction> =
        CommittedTransactions.read(internal, transaction.appendIndex(), transaction.checksum()) {
            (it as CommittedTransactions.Continued).transactions.toList()
        }

    @Test
    fun `pulls the transactions after a given one, up to the upstream's last`() {
        val database = start()
        // Creates the tokens, each in a transaction of its own.
        database.write(1)
        val after = database.last()
        database.write(20)

        val pulled =
            database.pull(database.after(after)) {
                it as Answer.Pulled
                assertThat(it.last).isEqualTo(database.last().appendIndex())
                it.transactions.toList()
            }

        assertThat(fields(pulled)).isEqualTo(fields(database.logged(after))).hasSize(20)
        assertThat(pulled.map { it.batch.commandCount() }).allMatch { it > 0 }
        database.pull(database.after(database.last())) {
            it as Answer.Pulled
            assertThat(it.transactions.toList()).describedAs("none after the last").isEmpty()
        }
    }

    @Test
    fun `a pull stops at its byte limit, and the next one takes the rest`() {
        val database = start()
        database.write(1)
        val after = database.last()
        database.write(5)

        val pulled = mutableListOf<CommittedTransaction>()
        var last = after
        while (last.appendIndex() < database.last().appendIndex()) {
            val batch =
                database.pull(database.after(last, maximumBytes = 1)) {
                    (it as Answer.Pulled).transactions.toList()
                }
            assertThat(batch).hasSize(1)
            pulled += batch
            val transaction = batch.single()
            last =
                TransactionId(
                    transaction.transactionId,
                    transaction.appendIndex,
                    transaction.batch.kernelVersion(),
                    transaction.checksum,
                    transaction.batch.timeCommitted,
                    transaction.batch.consensusIndex(),
                )
        }

        assertThat(fields(pulled)).isEqualTo(fields(database.logged(after))).hasSize(5)
    }

    @Test
    fun `refuses a history it does not continue, and a database it does not run`() {
        val database = start()
        database.write(2)
        val last = database.last()

        fun refusal(request: PullProtocol.Request) = database.pull(request) { it as Answer.Refused }

        assertThat(
                refusal(
                    PullProtocol.Request(database.ref(), last.appendIndex(), last.checksum() + 1, 1)
                )
            )
            .extracting { it.status }
            .isEqualTo(Status.DIVERGED)
        assertThat(
                refusal(
                    PullProtocol.Request(database.ref(), last.appendIndex() + 1, last.checksum(), 1)
                )
            )
            .extracting { it.status }
            .isEqualTo(Status.DIVERGED)
        assertThat(
                refusal(
                    database.after(last).copy(database = DatabaseRef("films", UUID.randomUUID()))
                )
            )
            .isEqualTo(Answer.Refused(Status.UNAVAILABLE, "there is no database films here"))
        val other = UUID.randomUUID()
        assertThat(
                refusal(database.after(last).copy(database = DatabaseRef("neo4j", other))).status
            )
            .isEqualTo(Status.UNAVAILABLE)
    }

    @Test
    fun `refuses transactions its logs no longer hold`() {
        val database = start(pruning = true)
        val after = database.last()
        database.write(200)
        database.dependencyResolver
            .resolveDependency(CheckPointer::class.java)
            .forceCheckPoint(SimpleTriggerInfo("test"))

        assertThat(database.pull(database.after(after)) { it as Answer.Refused }.status)
            .isEqualTo(Status.PRUNED)
    }
}
