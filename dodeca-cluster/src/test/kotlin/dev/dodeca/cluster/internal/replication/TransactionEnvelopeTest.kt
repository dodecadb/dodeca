package dev.dodeca.cluster.internal.replication

import java.io.ByteArrayOutputStream
import java.io.IOException
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.configuration.connectors.HttpConnector
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder
import org.neo4j.io.fs.OutputStreamWritableChannel
import org.neo4j.kernel.KernelVersionProvider
import org.neo4j.kernel.impl.api.LeaseService
import org.neo4j.kernel.impl.transaction.CommittedCommandBatchRepresentation
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.storageengine.api.StorageCommand
import org.neo4j.storageengine.api.TransactionIdStore
import org.neo4j.wal.LogicalTransactionStore

/** Round trips real transactions, read back from a standalone database's transaction log. */
class TransactionEnvelopeTest {
    @TempDir lateinit var home: Path
    private lateinit var dbms: DatabaseManagementService
    private lateinit var database: GraphDatabaseAPI

    @BeforeEach
    fun start() {
        dbms =
            DatabaseManagementServiceBuilder(home)
                // Neo4j preallocates every transaction log, 256 MB, on Linux: the tests' RAM disk
                // would fill.
                .setConfig(GraphDatabaseSettings.preallocate_logical_logs, false)
                .setConfig(BoltConnector.enabled, false)
                .setConfig(HttpConnector.enabled, false)
                .setConfig(GraphDatabaseSettings.pagecache_memory, 8L shl 20)
                .build()
        database = dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME) as GraphDatabaseAPI
    }

    @AfterEach
    fun stop() {
        dbms.shutdown()
    }

    private fun committedBatches(): List<CommittedCommandBatchRepresentation> {
        val store =
            database.dependencyResolver.resolveDependency(LogicalTransactionStore::class.java)
        val batches = mutableListOf<CommittedCommandBatchRepresentation>()
        store.getCommandBatches(TransactionIdStore.BASE_TX_ID + 1).use { cursor ->
            while (cursor.next()) batches += cursor.get()
        }
        return batches
    }

    private fun bytes(command: StorageCommand): ByteArray {
        val buffer = ByteArrayOutputStream()
        OutputStreamWritableChannel(buffer).use { command.serialize(it) }
        return buffer.toByteArray()
    }

    @Test
    fun `transactions survive encoding with every command and its metadata`() {
        database.executeTransactionally("CREATE CONSTRAINT FOR (p:Person) REQUIRE p.id IS UNIQUE")
        database.executeTransactionally(
            "UNWIND range(1, 50) AS i CREATE (:Person {id: i, name: 'p' + i, tags: ['a', 'b']})-[:KNOWS {since: i}]->(:Pet {big: i % 2 = 0})"
        )
        database.executeTransactionally("MATCH (p:Pet) WHERE p.big DETACH DELETE p")
        database.executeTransactionally(
            "MATCH (p:Person) SET p.text = reduce(s = '', x IN range(1, 200) | s + 'long')"
        )
        val readers =
            database.dependencyResolver
                .resolveDependency(org.neo4j.storageengine.api.StorageEngineFactory::class.java)
                .commandReaderFactory()
        val batches = committedBatches()
        assertThat(batches).hasSizeGreaterThan(3)

        batches.forEach { committed ->
            val original = committed.commandBatch()
            val decoded =
                TransactionEnvelope.decode(TransactionEnvelope.encode(original), 42, readers)

            assertThat(decoded.consensusIndex()).isEqualTo(42)
            assertThat(decoded.kernelVersion()).isEqualTo(original.kernelVersion())
            assertThat(decoded.timeStarted).isEqualTo(original.timeStarted)
            assertThat(decoded.latestCommittedTxWhenStarted)
                .isEqualTo(original.latestCommittedTxWhenStarted)
            assertThat(decoded.timeCommitted).isEqualTo(original.timeCommitted)
            assertThat(decoded.leaseId).isEqualTo(original.leaseId)
            assertThat(decoded.subject().toString()).isEqualTo(original.subject().toString())
            val originalCommands =
                mutableListOf<StorageCommand>().also { list ->
                    original.accept {
                        list += it
                        false
                    }
                }
            val decodedCommands =
                mutableListOf<StorageCommand>().also { list ->
                    decoded.accept {
                        list += it
                        false
                    }
                }
            assertThat(decodedCommands.map(::bytes))
                .containsExactlyElementsOf(originalCommands.map(::bytes))
        }
    }

    @Test
    fun `a status check is a transaction without commands under no lease`() {
        val resolver = database.dependencyResolver
        val readers =
            resolver
                .resolveDependency(org.neo4j.storageengine.api.StorageEngineFactory::class.java)
                .commandReaderFactory()
        val version = resolver.resolveDependency(KernelVersionProvider::class.java).kernelVersion()

        val decoded =
            TransactionEnvelope.decode(TransactionEnvelope.statusCheck(version), 7, readers)

        // As every version decodes it: members skip an entry whose lease is not its term.
        assertThat(decoded.commandCount()).isZero()
        assertThat(decoded.leaseId).isEqualTo(LeaseService.NO_LEASE)
        assertThat(decoded.kernelVersion()).isEqualTo(version)
    }

    @Test
    fun `trailing garbage and unknown formats are rejected`() {
        database.executeTransactionally("CREATE (:Thing)")
        val readers =
            database.dependencyResolver
                .resolveDependency(org.neo4j.storageengine.api.StorageEngineFactory::class.java)
                .commandReaderFactory()
        val payload = TransactionEnvelope.encode(committedBatches().last().commandBatch())

        assertThatThrownBy { TransactionEnvelope.decode(payload + byteArrayOf(0), 1, readers) }
            .isInstanceOf(IOException::class.java)
            .hasMessageContaining("unexpected bytes")
        assertThatThrownBy {
                TransactionEnvelope.decode(
                    byteArrayOf(9) + payload.copyOfRange(1, payload.size),
                    1,
                    readers,
                )
            }
            .isInstanceOf(IOException::class.java)
            .hasMessageContaining("format")
    }

    @Test
    fun `apply outcomes round trip`() {
        val outcomes = listOf(ApplyOutcome.Applied(17, 12), ApplyOutcome.LeaseExpired(3, 5))

        outcomes.forEach { assertThat(ApplyOutcome.decode(ApplyOutcome.encode(it))).isEqualTo(it) }
        assertThat(ApplyOutcome.decode(ByteArray(0))).isNull()
    }
}
