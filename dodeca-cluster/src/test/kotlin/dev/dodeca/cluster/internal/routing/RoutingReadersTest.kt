package dev.dodeca.cluster.internal.routing

import dev.dodeca.cluster.internal.ClusterHarness
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.connectors.BoltConnector

/** Which members routing tables list as readers, as Neo4j Enterprise's settings say. */
class RoutingReadersTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var cluster: ClusterHarness? = null

    @AfterEach
    fun stop() {
        cluster?.close()
    }

    /** The addresses of each role of the routing table [this] has for `neo4j`. */
    private fun ClusterHarness.Member.routes(): Map<String, Set<Any?>> =
        database("system").executeTransactionally(
            "CALL dbms.routing.getRoutingTable({}, 'neo4j') YIELD servers",
            emptyMap(),
        ) { result ->
            (result.next()["servers"] as List<*>)
                .map { it as Map<*, *> }
                .groupBy({ it["role"] as String }, { it["addresses"] as List<*> })
                .mapValues { (_, addresses) -> addresses.flatten().toSet() }
        }

    @Test
    fun `the writer reads too only once dbms routing reads_on_writers_enabled says so, at once`() {
        val cluster =
            ClusterHarness(root, 3, settings = mapOf(BoltConnector.enabled to true)).also {
                cluster = it
                it.start()
            }
        val leader = cluster.awaitLeader()
        val asked = cluster.members.first { it != leader }
        val writer = asked.routes().getValue("WRITE").single()

        assertThat(asked.routes().getValue("READ")).hasSize(2).doesNotContain(writer)
        asked
            .database("system")
            .executeTransactionally(
                "CALL dbms.setConfigValue('dbms.routing.reads_on_writers_enabled', 'true')"
            )
        assertThat(asked.routes().getValue("READ")).hasSize(3).contains(writer)
    }
}
