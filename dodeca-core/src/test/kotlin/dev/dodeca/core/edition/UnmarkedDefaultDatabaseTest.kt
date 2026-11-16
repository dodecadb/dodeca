package dev.dodeca.core.edition

import dev.dodeca.core.StandaloneServer
import dev.dodeca.core.StandaloneServer.Companion.SYSTEM
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel
import org.neo4j.graphdb.config.Setting
import org.neo4j.io.ByteUnit

/**
 * Dodeca on a system graph that marks no database as the default, as Neo4j Community 2026 writes
 * it, and DozerDB on it: the configured default database becomes the default, and every other
 * database stays as it was.
 */
class UnmarkedDefaultDatabaseTest {
    // Kept when a test fails, for the server's logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path

    /**
     * Runs Neo4j Community, without Dodeca, on the home [StandaloneServer] takes, then stops it.
     */
    private fun community(defaultDatabase: String, cypher: String) {
        val dbms =
            DatabaseManagementServiceBuilder(root.resolve("server"))
                .setConfig(GraphDatabaseSettings.preallocate_logical_logs, false)
                .setConfig(GraphDatabaseSettings.pagecache_memory, ByteUnit.mebiBytes(16))
                .setConfig(BoltConnector.enabled, false)
                .setConfig(GraphDatabaseSettings.initial_default_database, defaultDatabase)
                .build()
        try {
            dbms.database(defaultDatabase).executeTransactionally(cypher)
        } finally {
            dbms.shutdown()
        }
    }

    private fun dodeca(defaultDatabase: String? = null): StandaloneServer =
        StandaloneServer(
                root,
                defaultDatabase?.let {
                    mapOf<Setting<*>, Any>(GraphDatabaseSettings.initial_default_database to it)
                } ?: emptyMap(),
            )
            .also { it.start() }

    private fun StandaloneServer.defaultDatabase(): Any? =
        rows("SHOW DEFAULT DATABASE YIELD name", SYSTEM).single()["name"]

    private fun StandaloneServer.statuses(): Map<String, String> =
        rows("SHOW DATABASES YIELD name, currentStatus", SYSTEM).associate {
            it["name"] as String to it["currentStatus"] as String
        }

    private fun StandaloneServer.titles(database: String): List<Any?> =
        rows("MATCH (f:Film) RETURN f.title AS title", database).map { it["title"] }

    @Test
    fun `starts on Neo4j Community's data directory, whose default database stays the default`() {
        community("movies", "CREATE (:Film {title: 'Alien'})")

        dodeca(defaultDatabase = "movies").use { server ->
            assertThat(server.defaultDatabase()).isEqualTo("movies")
            assertThat(server.titles("movies")).containsExactly("Alien")

            // The system graph names the default now, not the setting, which still names movies.
            server.administer("CREATE DATABASE films WAIT")
            server.administer("STOP DATABASE movies")
            server.administer("CALL dbms.setDefaultDatabase('films')")
            server.restart()

            assertThat(server.defaultDatabase()).isEqualTo("films")
            assertThat(server.statuses())
                .containsEntry("films", "online")
                .containsEntry("movies", "offline")
        }
    }

    @Test
    fun `creates the configured default database if the system graph has none of that name`() {
        community("neo4j", "CREATE (:Film {title: 'Alien'})")

        dodeca(defaultDatabase = "movies").use { server ->
            assertThat(server.defaultDatabase()).isEqualTo("movies")
            assertThat(server.statuses())
                .containsEntry("movies", "online")
                .containsEntry("neo4j", "online")
            assertThat(server.titles("neo4j")).containsExactly("Alien")
        }
    }

    @Test
    fun `leaves every other database as it was`() {
        dodeca().use { server ->
            server.administer("CREATE DATABASE films WAIT")
            server.administer("CREATE DATABASE books WAIT")
            server.administer("STOP DATABASE books")
            server.database(SYSTEM).beginTx().use { tx ->
                tx.findNodes(TopologyGraphDbmsModel.DATABASE_LABEL).forEach {
                    it.removeProperty(DefaultDatabase.PROPERTY)
                }
                tx.commit()
            }

            server.restart()

            assertThat(server.defaultDatabase()).isEqualTo("neo4j")
            assertThat(server.statuses())
                .containsEntry("neo4j", "online")
                .containsEntry("films", "online")
                .containsEntry("books", "offline")
        }
    }
}
