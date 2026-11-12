package org.dozendb.backup

import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import java.time.temporal.ChronoUnit
import org.dozendb.core.store.CommittedTransaction
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.consistency.ConsistencyCheckService
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.config.Setting
import org.neo4j.io.ByteUnit
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import org.neo4j.io.layout.DatabaseLayout
import org.neo4j.io.layout.Neo4jLayout
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.storageengine.AppendIndexProvider
import org.neo4j.storageengine.api.CommandReaderFactory
import org.neo4j.storageengine.api.StorageEngineFactory
import org.neo4j.storageengine.api.StoreIdProvider
import org.neo4j.storageengine.api.TransactionId
import org.neo4j.storageengine.api.TransactionIdStore
import org.neo4j.wal.LogicalTransactionStore

/**
 * A Neo4j Community DBMS without DozenDB, in this JVM, for tests, serving [databaseName] as its
 * default database.
 */
internal class CommunityServer(
    val home: Path,
    val databaseName: String = DATABASE,
    private val settings: Map<Setting<*>, Any> = emptyMap(),
) : AutoCloseable {
    private var dbms: DatabaseManagementService? = null

    val layout: DatabaseLayout
        get() = Neo4jLayout.of(config()).databaseLayout(databaseName)

    fun start(): CommunityServer {
        check(dbms == null) { "the server is running" }
        dbms =
            DatabaseManagementServiceBuilder(home)
                // Neo4j preallocates every transaction log, 256 MB, on Linux: the tests' RAM disk
                // would fill.
                .setConfig(GraphDatabaseSettings.preallocate_logical_logs, false)
                .setConfig(GraphDatabaseSettings.pagecache_memory, ByteUnit.mebiBytes(16))
                .setConfig(BoltConnector.enabled, false)
                .setConfig(GraphDatabaseSettings.initial_default_database, databaseName)
                .setConfig(settings)
                .build()
        return this
    }

    fun stop() {
        try {
            dbms?.shutdown()
        } finally {
            dbms = null
        }
    }

    fun database(): GraphDatabaseAPI =
        checkNotNull(dbms) { "the server is stopped" }.database(databaseName) as GraphDatabaseAPI

    fun rows(cypher: String): List<Map<String, Any?>> =
        database().executeTransactionally(cypher, emptyMap()) { result ->
            result.asSequence().map { it.toMap() }.toList()
        }

    private inline fun <reified T> dependency(): T =
        database().dependencyResolver.resolveDependency(T::class.java)

    fun lastCommitted(): TransactionId = dependency<TransactionIdStore>().lastCommittedTransaction

    /**
     * The transactions after append index [after], as Neo4j reads them from its log, which must
     * still hold the one at [after], unless it is the base one: its checksum is the one the first
     * follows.
     */
    fun transactionsAfter(after: Long): List<CommittedTransaction> {
        if (after == lastCommitted().appendIndex()) return emptyList()
        val base = after == AppendIndexProvider.BASE_APPEND_INDEX
        val logs = dependency<LogicalTransactionStore>()
        return logs.getCommandBatches(if (base) after + 1 else after).use { cursor ->
            var previous =
                if (base) {
                    TransactionIdStore.BASE_TX_CHECKSUM
                } else {
                    check(cursor.next()) { "the log ends before append index $after" }
                    cursor.get().checksum()
                }
            buildList {
                while (cursor.next()) {
                    add(
                        CommittedTransaction.of(cursor.get(), previous).also {
                            previous = it.checksum
                        }
                    )
                }
            }
        }
    }

    fun commandReaders(): CommandReaderFactory =
        dependency<StorageEngineFactory>().commandReaderFactory()

    /** The header of a full backup of the database as it is now. */
    fun fullHeader(compressed: Boolean): ArtifactHeader {
        val last = lastCommitted()
        return header(
            compressed,
            full = true,
            1,
            last.appendIndex(),
            null,
            last.checksum(),
            Instant.ofEpochMilli(last.commitTimestamp()),
        )
    }

    /** The header of a differential backup of [transactions]. */
    fun differentialHeader(
        transactions: List<CommittedTransaction>,
        compressed: Boolean,
    ): ArtifactHeader =
        header(
            compressed,
            full = false,
            transactions.firstOrNull()?.appendIndex ?: 0,
            transactions.lastOrNull()?.appendIndex ?: 0,
            transactions.firstOrNull()?.previousChecksum,
            transactions.lastOrNull()?.checksum,
            transactions.lastOrNull()?.let { Instant.ofEpochMilli(it.batch.timeCommitted) },
        )

    private fun header(
        compressed: Boolean,
        full: Boolean,
        lowest: Long,
        highest: Long,
        previousChecksum: Int?,
        lastChecksum: Int?,
        lastCommitTime: Instant?,
    ) =
        ArtifactHeader(
            databaseName = databaseName,
            databaseId = database().databaseId().databaseId(),
            storeId = dependency<StoreIdProvider>().storeId,
            time = Instant.now().truncatedTo(ChronoUnit.SECONDS),
            full = full,
            compressed = compressed,
            lowestAppendIndex = lowest,
            highestAppendIndex = highest,
            previousChecksum = previousChecksum,
            lastChecksum = lastChecksum,
            lastCommitTime = lastCommitTime,
            source = "test",
        )

    /** Writes a full backup of the database to [file]; stops the server meanwhile. */
    fun backupFull(file: Path, compressed: Boolean = true): ArtifactHeader {
        val header = fullHeader(compressed)
        stop()
        DefaultFileSystemAbstraction().use { fs ->
            Artifacts.writeFull(Files.newOutputStream(file), header, layout, fs)
        }
        start()
        return header
    }

    /** Writes a differential backup of the transactions after [after] to [file]. */
    fun backupDifferential(after: Long, file: Path, compressed: Boolean = true): ArtifactHeader {
        val transactions = transactionsAfter(after)
        val header = differentialHeader(transactions, compressed)
        DefaultFileSystemAbstraction().use { fs ->
            Artifacts.writeDifferential(
                Files.newOutputStream(file),
                header,
                transactions.asSequence(),
                file.resolveSibling("${file.fileName}.staging"),
                fs,
            )
        }
        return header
    }

    fun consistent(): Boolean {
        check(dbms == null) { "the server is running" }
        return ConsistencyCheckService(layout).with(config()).runFullConsistencyCheck().isSuccessful
    }

    override fun close() = stop()

    private fun config(): Config =
        Config.newBuilder().set(GraphDatabaseSettings.neo4j_home, home.toAbsolutePath()).build()

    companion object {
        const val DATABASE = "neo4j"

        /** Writes nodes, relationships and every kind of index, in several transactions. */
        fun populate(server: CommunityServer) = populate(server.database())

        fun populate(database: GraphDatabaseService) {
            database.executeTransactionally(
                "CREATE CONSTRAINT person_id FOR (p:Person) REQUIRE p.id IS UNIQUE"
            )
            database.executeTransactionally(
                "CREATE TEXT INDEX person_name FOR (p:Person) ON (p.name)"
            )
            database.executeTransactionally(
                "CREATE FULLTEXT INDEX person_bio FOR (p:Person) ON EACH [p.bio]"
            )
            database.executeTransactionally(
                "CREATE POINT INDEX person_home FOR (p:Person) ON (p.home)"
            )
            database.executeTransactionally(
                "CREATE INDEX knows_since FOR ()-[k:KNOWS]-() ON (k.since)"
            )
            database.executeTransactionally("CALL db.awaitIndexes(60)")
            for (batch in 0 until 10) {
                database.executeTransactionally(
                    """
                    UNWIND range(${'$'}from, ${'$'}from + 99) AS id
                    CREATE (p:Person {id: id, name: 'person ' + id, bio: 'likes graphs ' + id,
                        home: point({x: id, y: id}), tags: ['a', 'b', toString(id)],
                        notes: reduce(s = '', n IN range(1, 40) | s + 'note ')})
                    """
                        .trimIndent(),
                    mapOf("from" to batch * 100),
                )
            }
            database.executeTransactionally(
                "MATCH (a:Person), (b:Person) WHERE b.id = a.id + 1 CREATE (a)-[:KNOWS {since: a.id}]->(b)"
            )
            database.executeTransactionally("MATCH (p:Person) WHERE p.id % 7 = 0 DETACH DELETE p")
            database.executeTransactionally(
                "MATCH (p:Person) WHERE p.id % 5 = 0 SET p.name = 'renamed'"
            )
        }

        /** What [populate] wrote, as the queries that use its indexes find it. */
        fun summary(server: CommunityServer): Map<String, Any?> = summary(server.database())

        fun summary(database: GraphDatabaseService): Map<String, Any?> =
            database
                .executeTransactionally(
                    """
                    MATCH (p:Person) WITH count(p) AS people
                    MATCH ()-[k:KNOWS]->() WITH people, count(k) AS knows
                    CALL { MATCH (p:Person {id: 1}) RETURN p.name AS one }
                    CALL { MATCH (p:Person) WHERE p.name STARTS WITH 'renamed' RETURN count(p) AS renamed }
                    CALL { CALL db.index.fulltext.queryNodes('person_bio', 'graphs') YIELD node
                           RETURN count(node) AS bios }
                    CALL { MATCH (p:Person) WHERE point.distance(p.home, point({x: 0, y: 0})) < 10
                           RETURN count(p) AS near }
                    CALL { MATCH ()-[k:KNOWS]->() WHERE k.since > 500 RETURN count(k) AS later }
                    RETURN people, knows, one, renamed, bios, near, later
                    """
                        .trimIndent(),
                    emptyMap(),
                ) { result ->
                    result.asSequence().map { it.toMap() }.toList()
                }
                .single()
    }
}
