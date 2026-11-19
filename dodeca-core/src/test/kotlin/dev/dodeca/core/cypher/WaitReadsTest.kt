package dev.dodeca.core.cypher

import dev.dodeca.core.StandaloneServer
import dev.dodeca.core.StandaloneServer.Companion.SYSTEM
import dev.dodeca.core.VanishingGraph
import dev.dodeca.core.edition.WantedDatabase
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel

/**
 * What WAIT reads of the system graph, in the transaction of its command, while other transactions
 * change it: after its command committed, a DROP DATABASE can delete the database's node between
 * the lookup that finds it and the read of its properties.
 */
class WaitReadsTest {
    // Kept when a test fails, for the logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private lateinit var server: StandaloneServer

    @BeforeEach
    fun start() {
        server = StandaloneServer(root).also { it.start() }
        server.administer("CREATE DATABASE films")
    }

    @AfterEach
    fun stop() {
        server.close()
    }

    @Test
    fun `a database dropped as WAIT reads it is waited for as dropped`() {
        val system =
            VanishingGraph(server.database(SYSTEM), TopologyGraphDbmsModel.DATABASE_LABEL) {
                it.getProperty(TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY, null) == "films"
            }

        val wanted = system.beginTx().use { WaitExecutionPlan.wanted(it, "films") }

        assertThat(system.deleted.get()).isEqualTo(1)
        assertThat(wanted).isEqualTo(WantedDatabase("films", null, false))
    }
}
