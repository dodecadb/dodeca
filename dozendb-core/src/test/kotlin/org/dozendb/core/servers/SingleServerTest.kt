package org.dozendb.core.servers

import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
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
    fun `options that would leave a database without a server are refused`() {
        StandaloneServer(root).use { server ->
            server.start()
            val id =
                server
                    .rows("SHOW SERVERS YIELD serverId", database = StandaloneServer.SYSTEM)
                    .single()["serverId"]

            val error = catchThrowable {
                server.administer("ALTER SERVER '$id' SET OPTIONS {deniedDatabases: ['neo4j']}")
            }

            assertThat(error)
                .hasMessageStartingWith("Could not alter server '$id'")
                .hasMessageContaining("no other server can host 'neo4j'")
        }
    }

    @Test
    fun `DEALLOCATE fails, as no other server can host the databases`() {
        StandaloneServer(root).use { server ->
            server.start()
            val id =
                server
                    .rows("SHOW SERVERS YIELD serverId", database = StandaloneServer.SYSTEM)
                    .single()["serverId"]

            val error = catchThrowable {
                server.administer("DEALLOCATE DATABASES FROM SERVER '$id'")
            }

            assertThat(error)
                .hasMessageStartingWith("Could not deallocate databases from server '$id'")
                .hasMessageContaining("no other server can host 'neo4j'")
        }
    }

    @Test
    fun `DROP SERVER fails, as the server is enabled`() {
        StandaloneServer(root).use { server ->
            server.start()
            val id =
                server
                    .rows("SHOW SERVERS YIELD serverId", database = StandaloneServer.SYSTEM)
                    .single()["serverId"]

            val error = catchThrowable { server.administer("DROP SERVER '$id'") }

            assertThat(error)
                .hasMessageStartingWith("Could not drop server '$id'")
                .hasMessageContaining("it is Enabled")
        }
    }

    @Test
    fun `REALLOCATE moves nothing, as no other server can host a copy`() {
        StandaloneServer(root).use { server ->
            server.start()

            val moves =
                server.rows("DRYRUN REALLOCATE DATABASES", database = StandaloneServer.SYSTEM)
            server.administer("REALLOCATE DATABASES")

            assertThat(moves).isEmpty()
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
