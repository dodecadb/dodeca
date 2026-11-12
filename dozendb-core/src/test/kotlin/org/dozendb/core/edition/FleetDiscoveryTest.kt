package org.dozendb.core.edition

import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.dozendb.core.StandaloneServer
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.Config
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.graphdb.config.Setting

/**
 * Neo4j's fleet discovery, which would announce a DozenDB server as Neo4j Enterprise on the local
 * network.
 */
class FleetDiscoveryTest {
    @TempDir lateinit var root: Path

    private fun announces(settings: Map<Setting<*>, Any> = emptyMap()): Boolean =
        StandaloneServer(root, settings).use { server ->
            server.start()
            server
                .database(StandaloneServer.SYSTEM)
                .dependencyResolver
                .resolveDependency(Config::class.java)
                .get(BoltConnector.enable_discovery)
        }

    @Test
    fun `is off unless the operator turns it on`() {
        assertThat(announces()).isFalse()
    }

    @Test
    fun `stays on when the operator turns it on`() {
        assertThat(announces(mapOf(BoltConnector.enable_discovery to true))).isTrue()
    }
}
