package org.dozendb.core.edition

import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.dozendb.core.StandaloneServer
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.graphdb.config.Setting

/** Neo4j's usage data collector, which would report a DozenDB server as Neo4j Enterprise. */
class UsageDataTest {
    @TempDir lateinit var root: Path

    private fun reportsUsage(settings: Map<Setting<*>, Any> = emptyMap()): Boolean =
        StandaloneServer(root, settings).use { server ->
            server.start()
            server
                .database(StandaloneServer.SYSTEM)
                .dependencyResolver
                .resolveDependency(Config::class.java)
                .get(GraphDatabaseSettings.udc_enabled)
        }

    @Test
    fun `is off unless the operator turns it on`() {
        assertThat(reportsUsage()).isFalse()
    }

    @Test
    fun `stays on when the operator turns it on`() {
        assertThat(reportsUsage(mapOf(GraphDatabaseSettings.udc_enabled to true))).isTrue()
    }
}
