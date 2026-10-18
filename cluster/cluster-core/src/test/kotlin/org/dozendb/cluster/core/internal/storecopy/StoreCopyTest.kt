package org.dozendb.cluster.core.internal.storecopy

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread
import org.assertj.core.api.Assertions.assertThat
import org.dozendb.cluster.api.BulkSource
import org.dozendb.cluster.api.BulkTransfer
import org.dozendb.cluster.api.HostPort
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.configuration.connectors.HttpConnector
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import org.neo4j.io.layout.Neo4jLayout
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.NullLog

/** Copies the databases of a running standalone DBMS while it writes, and starts the copies. */
class StoreCopyTest {
    @TempDir lateinit var root: Path
    private val fs = DefaultFileSystemAbstraction()
    private val names =
        listOf(
            GraphDatabaseSettings.SYSTEM_DATABASE_NAME,
            GraphDatabaseSettings.DEFAULT_DATABASE_NAME,
        )
    private val services = mutableListOf<DatabaseManagementService>()

    @AfterEach
    fun stop() {
        services.forEach { it.shutdown() }
    }

    private fun dbms(home: Path): DatabaseManagementService =
        DatabaseManagementServiceBuilder(home)
            .setConfig(BoltConnector.enabled, false)
            .setConfig(HttpConnector.enabled, false)
            .setConfig(GraphDatabaseSettings.pagecache_memory, 8L shl 20)
            .build()
            .also { services += it }

    private fun DatabaseManagementService.neo4j() =
        database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME) as GraphDatabaseAPI

    /** Serves copies in process, as the bulk transfer does between members. */
    private class InProcessTransfer(private val source: BulkSource) : BulkTransfer {
        override fun registerBulkSource(name: String, source: BulkSource) = error("not used")

        override fun <T> fetchBulk(
            address: HostPort,
            name: String,
            request: ByteArray,
            consume: (InputStream) -> T,
        ): T {
            val bytes = ByteArrayOutputStream()
            source.serve(request, bytes)
            return consume(ByteArrayInputStream(bytes.toByteArray()))
        }
    }

    private fun stores(
        home: Path,
        transfer: BulkTransfer = InProcessTransfer { _, _ -> error("not used") },
    ) = Neo4jLocalStores(fs, Neo4jLayout.of(home), names, transfer, NullLog.getInstance())

    @Test
    fun `a copy taken during writes starts with whole transactions only`() {
        val source = dbms(root.resolve("source"))
        val database = source.neo4j()
        database.executeTransactionally("CREATE INDEX FOR (n:Batch) ON (n.id)")
        val writing = AtomicBoolean(true)
        val writer = thread {
            var batch = 0
            while (writing.get()) {
                database.executeTransactionally(
                    "UNWIND range(1, 10) AS i CREATE (:Batch {id: ${batch++}, i: i})"
                )
            }
        }
        val copySource =
            StoreCopySource(
                {
                    database.dependencyResolver.resolveDependency(
                        DatabaseContextProvider::class.java
                    )
                },
                fs,
                NullLog.getInstance(),
            )
        val target = root.resolve("target")
        Thread.sleep(300)

        stores(target, InProcessTransfer(copySource)).copyFrom(HostPort("source", 7700))
        writing.set(false)
        writer.join()

        assertThat(stores(target).existed).isTrue()
        val copy = dbms(target).neo4j()
        assertThat(copy.databaseId()).isEqualTo(database.databaseId())
        val batches =
            copy.executeTransactionally(
                "MATCH (n:Batch) RETURN n.id AS id, count(n) AS c",
                emptyMap(),
            ) { result ->
                result.stream().map { (it["id"] as Long) to (it["c"] as Long) }.toList()
            }
        assertThat(batches).isNotEmpty.allMatch { it.second == 10L }
        assertThat(batches.map { it.first }.sorted()).isEqualTo((0L until batches.size).toList())
    }

    @Test
    fun `an interrupted copy counts as no databases`() {
        val home = root.resolve("member")
        dbms(home).also { it.shutdown() }
        services.clear()
        assertThat(stores(home).existed).isTrue()

        Files.createFile(
            Neo4jLayout.of(home).databasesDirectory().resolve(".dozendb-copy-incomplete")
        )

        assertThat(stores(home).existed).isFalse()
    }
}
