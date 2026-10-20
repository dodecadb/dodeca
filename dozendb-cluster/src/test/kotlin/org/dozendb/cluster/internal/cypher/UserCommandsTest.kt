package org.dozendb.cluster.internal.cypher

import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.dozendb.cluster.internal.StandaloneServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir

/** User management is Neo4j Community's, with clustering disabled too. */
class UserCommandsTest {
    // Kept when a test fails, for the server's logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private lateinit var server: StandaloneServer

    @BeforeEach
    fun start() {
        server = StandaloneServer(root).also { it.start() }
    }

    @AfterEach
    fun stop() {
        server.close()
    }

    private fun passwordChangeRequired(user: String): Boolean =
        server
            .rows(
                "SHOW USERS YIELD user, passwordChangeRequired WHERE user = \$user",
                StandaloneServer.SYSTEM,
                mapOf("user" to user),
            )
            .single()["passwordChangeRequired"]
            as Boolean

    @Test
    fun `alters users`() {
        server.administer("CREATE USER alice SET PASSWORD 'alice-secret-1' CHANGE NOT REQUIRED")

        server.administer("ALTER USER alice SET PASSWORD 'alice-secret-2' CHANGE REQUIRED")

        assertThat(passwordChangeRequired("alice")).isTrue()
    }

    @Test
    fun `refuses a status or home database, which nothing would act on`() {
        assertThat(
                catchThrowable {
                    server.administer(
                        "CREATE USER alice SET PASSWORD 'alice-secret-1' SET STATUS SUSPENDED"
                    )
                }
            )
            .hasMessageContaining("'SET STATUS' is not available")
        assertThat(
                catchThrowable {
                    server.administer(
                        "CREATE USER alice SET PASSWORD 'alice-secret-1' SET HOME DATABASE films"
                    )
                }
            )
            .hasMessageContaining("'HOME DATABASE' is not available")
    }
}
