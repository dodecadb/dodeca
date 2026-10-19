package org.dozendb.cluster.core.internal.storecopy

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.dozendb.cluster.api.BulkSource
import org.dozendb.cluster.api.BulkTransfer
import org.dozendb.cluster.api.DatabaseRef
import org.dozendb.cluster.api.HostPort
import org.dozendb.cluster.api.LogPosition
import org.dozendb.cluster.api.Snapshot
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
    ) =
        Neo4jLocalStores(fs, Neo4jLayout.of(home), names, transfer, NullLog.getInstance()) { _, _ ->
        }

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
        val copySource = copySource(database)
        val target = root.resolve("target")
        Thread.sleep(300)

        stores(target, InProcessTransfer(copySource)).copyFrom(SOURCE, names)
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

    private fun copySource(
        database: GraphDatabaseAPI,
        snapshot: Snapshot? = null,
        replicating: () -> Boolean = { true },
        maximumWait: Duration = Duration.ofMillis(100),
    ): StoreCopySource =
        StoreCopySource(
            { database.dependencyResolver.resolveDependency(DatabaseContextProvider::class.java) },
            fs,
            replicating = { replicating() },
            snapshots = { snapshot },
            log = NullLog.getInstance(),
            maximumWait = maximumWait,
        )

    private fun GraphDatabaseAPI.ref(): DatabaseRef =
        DatabaseRef(databaseName(), databaseId().databaseId().uuid())

    @Test
    fun `a half-moved database is missing and the member counts as having none`() {
        val home = root.resolve("member")
        dbms(home).also { it.shutdown() }
        services.clear()
        assertThat(stores(home).existed).isTrue()
        assertThat(stores(home).missing()).isEmpty()

        Files.createFile(
            Neo4jLayout.of(home).databasesDirectory().resolve(".dozendb-incomplete-neo4j")
        )

        assertThat(stores(home).existed).isFalse()
        assertThat(stores(home).missing())
            .containsExactly(GraphDatabaseSettings.DEFAULT_DATABASE_NAME)
    }

    @Test
    fun `database files are listed, complete or not, and deleted by name`() {
        val home = root.resolve("member")
        dbms(home).also { it.shutdown() }
        services.clear()
        val layout = Neo4jLayout.of(home)
        Files.createDirectories(layout.databasesDirectory().resolve("films"))
        Files.createDirectories(layout.transactionLogsRootDirectory().resolve("series"))
        Files.createFile(layout.databasesDirectory().resolve(".dozendb-incomplete-drama"))
        val stores = stores(home)

        assertThat(stores.stored())
            .containsExactlyInAnyOrder(*names.toTypedArray(), "films", "series", "drama")
        assertThat(stores.complete(GraphDatabaseSettings.DEFAULT_DATABASE_NAME)).isTrue()
        assertThat(stores.complete("films")).isFalse()

        listOf("films", "series", "drama").forEach(stores::delete)

        assertThat(stores.stored()).containsExactlyInAnyOrderElementsOf(names)
        assertThatThrownBy { stores.delete(GraphDatabaseSettings.DEFAULT_DATABASE_NAME) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `a member without databases misses them all`() {
        assertThat(stores(root.resolve("empty")).missing()).isEqualTo(names)
    }

    @Test
    fun `a copy of a replicated database holds the snapshot read first, at least the minimum`() {
        val database = dbms(root.resolve("source")).neo4j()
        database.executeTransactionally("CREATE (:Copied)")
        val snapshot = Snapshot(LogPosition(7, 2), 5)
        val transfer = InProcessTransfer(copySource(database, snapshot))
        val target = stores(root.resolve("target"), transfer)

        val staged = target.stageReplicated(SOURCE, database.ref(), 7)
        assertThat(staged.snapshot).isEqualTo(snapshot)
        assertThat(target.missing()).describedAs("only staged").isEqualTo(names)
        staged.moveIntoPlace()
        assertThatThrownBy { target.stageReplicated(SOURCE, database.ref(), 8) }
            .hasMessageContaining("not yet up to the requested index 8")
        assertThat(target.missing()).containsExactly(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    }

    @Test
    fun `a staged copy leaves the database in place until it is moved there`() {
        val home = root.resolve("target")
        dbms(home).also { it.shutdown() }
        services.clear()
        val kept =
            Neo4jLayout.of(home)
                .databaseLayout(GraphDatabaseSettings.DEFAULT_DATABASE_NAME)
                .databaseDirectory()
                .resolve("kept")
        Files.createFile(kept)
        val database = dbms(root.resolve("source")).neo4j()
        val target =
            stores(home, InProcessTransfer(copySource(database, Snapshot(LogPosition(7, 2), 5))))

        val staged = target.stageReplicated(SOURCE, database.ref(), 7)

        assertThat(kept).exists()
        staged.moveIntoPlace()
        assertThat(kept).doesNotExist()
        assertThat(target.missing()).isEmpty()
    }

    @Test
    fun `only the requested database is copied, and only while it replicates`() {
        val database = dbms(root.resolve("source")).neo4j()
        val snapshot = Snapshot(LogPosition(7, 2), 5)
        val namesake = DatabaseRef(database.databaseName(), UUID.randomUUID())

        assertThatThrownBy {
                stores(root.resolve("target"), InProcessTransfer(copySource(database, snapshot)))
                    .stageReplicated(SOURCE, namesake, 7)
            }
            .hasMessageContaining("not the requested ${namesake.id}")
        assertThatThrownBy {
                stores(
                        root.resolve("target"),
                        InProcessTransfer(copySource(database, snapshot, replicating = { false })),
                    )
                    .stageReplicated(SOURCE, database.ref(), 7)
            }
            .hasMessageContaining("is not replicating here")
        assertThat(stores(root.resolve("target")).missing()).isEqualTo(names)
    }

    @Test
    fun `a source waits for a new database to replicate before it sends a copy`() {
        val database = dbms(root.resolve("source")).neo4j()
        val replicatingFrom = System.nanoTime() + Duration.ofMillis(300).toNanos()
        val source =
            copySource(
                database,
                Snapshot(LogPosition(7, 2), 5),
                replicating = { System.nanoTime() > replicatingFrom },
                maximumWait = Duration.ofSeconds(10),
            )
        val target = stores(root.resolve("target"), InProcessTransfer(source))

        target.stageReplicated(SOURCE, database.ref(), 7).moveIntoPlace()

        assertThat(target.missing()).containsExactly(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    }

    @Test
    fun `copy requests round-trip with and without a database id`() {
        val id = UUID.randomUUID()

        assertThat(StoreCopySource.decodeRequest(StoreCopySource.encodeRequest("films", id, 9)))
            .isEqualTo(StoreCopySource.Request("films", id, 9))
        assertThat(StoreCopySource.decodeRequest(StoreCopySource.encodeRequest("films", null, -1)))
            .isEqualTo(StoreCopySource.Request("films", null, -1))
    }

    private companion object {
        val SOURCE = HostPort("source", 7700)
    }
}
