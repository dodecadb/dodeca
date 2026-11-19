package dev.dodeca.cluster.internal

import dev.dodeca.cluster.internal.replication.SystemReading
import dev.dodeca.core.StandaloneServer
import dev.dodeca.core.StandaloneServer.Companion.SYSTEM
import dev.dodeca.core.VanishingGraph
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel
import org.neo4j.graphdb.GraphDatabaseService

/**
 * Reading this member's copy of the system graph while other transactions change it, as those
 * applied from the Raft log do: a DROP DATABASE can delete a database's node between the scan that
 * finds it and the read of its properties.
 */
class Neo4jSystemGraphTest {
    // Kept when a test fails, for the logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private lateinit var server: StandaloneServer

    @BeforeEach
    fun start() {
        server = StandaloneServer(root).also { it.start() }
        listOf("films", "series").forEach { server.administer("CREATE DATABASE $it WAIT") }
    }

    @AfterEach
    fun stop() {
        server.close()
    }

    private fun systemGraph(system: GraphDatabaseService) =
        Neo4jSystemGraph(
            { system },
            object : SystemReading {
                override fun <T> readingSystem(block: () -> T): T = block()

                override val systemWritable = true
            },
            current = { true },
            placement = { error("these tests place no database") },
        )

    /** The system database, where the first scan of the databases finds [name] dropped. */
    private fun dropping(name: String) =
        VanishingGraph(server.database(SYSTEM), TopologyGraphDbmsModel.DATABASE_LABEL) {
            it.getProperty(TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY, null) == name
        }

    @Test
    fun `the databases read while one is dropped are those after the drop`() {
        val system = dropping("films")

        val databases = systemGraph(system).databases()

        assertThat(system.deleted.get()).isEqualTo(1)
        assertThat(databases.map { it.id.name() }).containsExactlyInAnyOrder("neo4j", "series")
    }
}
