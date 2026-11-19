package dev.dodeca.core.security

import dev.dodeca.core.StandaloneServer
import dev.dodeca.core.StandaloneServer.Companion.SYSTEM
import dev.dodeca.core.VanishingGraph
import java.nio.file.Path
import java.util.Optional
import java.util.function.Function
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.dbms.systemgraph.SystemDatabaseProvider
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Transaction
import org.neo4j.logging.NullLogProvider

/**
 * Reading the users, roles and privileges while other transactions change them, as a DROP ROLE
 * applied from the Raft log does between the scan that finds a role and the read of its name.
 */
class SystemGraphSecurityTest {
    // Kept when a test fails, for the logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private lateinit var server: StandaloneServer

    @BeforeEach
    fun start() {
        server = StandaloneServer(root).also { it.start() }
        listOf("readers", "writers").forEach { server.administer("CREATE ROLE $it") }
    }

    @AfterEach
    fun stop() {
        server.close()
    }

    private fun provider(graph: GraphDatabaseService) =
        object : SystemDatabaseProvider {
            override fun optionalDatabaseContext():
                Optional<SystemDatabaseProvider.SystemDatabaseContext> = Optional.empty()

            override fun <T> query(function: Function<Transaction, T>): T =
                graph.beginTx().use { function.apply(it) }
        }

    @Test
    fun `the roles read while one is dropped are those after the drop`() {
        val system =
            VanishingGraph(server.database(SYSTEM), PrivilegeRecords.ROLE) {
                it.getProperty(PrivilegeRecords.ROLE_NAME, null) == "readers"
            }
        val security = SystemGraphSecurity(provider(system), NullLogProvider.getInstance())

        val roles = security.current().roles.keys

        assertThat(system.deleted.get()).isEqualTo(1)
        assertThat(roles).contains("writers").doesNotContain("readers")
    }
}
