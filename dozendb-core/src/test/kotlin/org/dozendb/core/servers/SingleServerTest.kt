package org.dozendb.core.servers

import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.dozendb.core.DozenDbSettings
import org.dozendb.core.StandaloneServer
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.kernel.internal.Version

/** SHOW SERVERS without clustering: this server alone, as a single Neo4j Enterprise server. */
class SingleServerTest {
    // Kept when a test fails, for the server's logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path

    @Test
    fun `the server is enabled, with the tags it started with`() {
        StandaloneServer(root, mapOf(DozenDbSettings.initialServerTags to listOf("eu", "big")))
            .use { server ->
                server.start()
                val rows = server.rows("SHOW SERVERS YIELD *", database = StandaloneServer.SYSTEM)

                val row = rows.single()
                assertThat(row["name"]).isEqualTo(row["serverId"])
                assertThat(row["state"]).isEqualTo("Enabled")
                assertThat(row["health"]).isEqualTo("Available")
                assertThat(row["tags"]).isEqualTo(listOf("eu", "big"))
                assertThat(row["modeConstraint"]).isEqualTo("NONE")
                assertThat(row["hosting"]).isEqualTo(listOf("system", "neo4j"))
                assertThat(row["version"]).isEqualTo(Version.getNeo4jVersion())
                assertThat(
                        server
                            .rows("SHOW SERVERS", database = StandaloneServer.SYSTEM)
                            .single()
                            .keys
                    )
                    .containsExactlyInAnyOrder("name", "address", "state", "health", "hosting")
            }
    }

    @Test
    fun `the server can be renamed and its options changed, which a restart keeps`() {
        StandaloneServer(root, mapOf(DozenDbSettings.initialServerTags to listOf("eu"))).use {
            server ->
            server.start()
            val id =
                server
                    .rows("SHOW SERVERS YIELD serverId", database = StandaloneServer.SYSTEM)
                    .single()["serverId"]

            server.administer("RENAME SERVER '$id' TO 'only'")
            server.administer("ALTER SERVER 'only' SET OPTIONS {tags: ['us']}")
            server.administer("ENABLE SERVER 'only' OPTIONS {tags: ['us']}")
            server.restart()

            val row =
                server.rows("SHOW SERVERS YIELD *", database = StandaloneServer.SYSTEM).single()
            assertThat(row["name"]).isEqualTo("only")
            assertThat(row["tags"]).isEqualTo(listOf("us"))
        }
    }
}
