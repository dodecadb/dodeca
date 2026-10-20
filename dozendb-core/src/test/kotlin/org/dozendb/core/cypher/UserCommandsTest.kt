package org.dozendb.core.cypher

import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.dozendb.core.StandaloneServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.exceptions.AuthenticationException
import org.neo4j.driver.exceptions.ClientException

class UserCommandsTest {
    // Kept when a test fails, for the server's logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private lateinit var server: StandaloneServer
    private val drivers = mutableListOf<Driver>()

    @BeforeEach
    fun start() {
        server =
            StandaloneServer(
                    root,
                    mapOf(
                        GraphDatabaseSettings.auth_enabled to true,
                        BoltConnector.enabled to true,
                    ),
                )
                .also { it.start() }
    }

    @AfterEach
    fun stop() {
        drivers.forEach(Driver::close)
        server.close()
    }

    private fun user(name: String): Map<String, Any?> =
        server
            .rows(
                "SHOW USERS YIELD user, passwordChangeRequired, suspended, home WHERE user = \$user",
                StandaloneServer.SYSTEM,
                mapOf("user" to name),
            )
            .single()

    private fun driver(user: String, password: String): Driver =
        GraphDatabase.driver("bolt://${server.boltAddress()}", AuthTokens.basic(user, password))
            .also { drivers += it }

    /** Runs [cypher] in a new session without a database, which uses the user's home database. */
    private fun Driver.run(cypher: String, database: String? = null): List<Map<String, Any>> {
        val config =
            if (database == null) SessionConfig.defaultConfig()
            else SessionConfig.forDatabase(database)
        return session(config).use { session -> session.run(cypher).list { it.asMap() } }
    }

    @Test
    fun `alters users`() {
        server.administer("CREATE USER alice SET PASSWORD 'alice-secret-1' CHANGE NOT REQUIRED")

        server.administer("ALTER USER alice SET PASSWORD 'alice-secret-2' CHANGE REQUIRED")

        assertThat(user("alice")["passwordChangeRequired"]).isEqualTo(true)
    }

    @Test
    fun `stores and shows the status and home database`() {
        server.administer(
            "CREATE USER alice SET PASSWORD 'alice-secret-1' SET STATUS SUSPENDED " +
                "SET HOME DATABASE Films"
        )
        server.administer("CREATE USER bob SET PASSWORD 'bob-secret-1'")

        assertThat(user("alice")).containsEntry("suspended", true).containsEntry("home", "films")
        assertThat(user("bob")).containsEntry("suspended", false).containsEntry("home", null)

        server.administer("ALTER USER alice REMOVE HOME DATABASE SET STATUS ACTIVE")
        server.administer("ALTER USER bob SET STATUS SUSPENDED SET HOME DATABASE neo4j")

        assertThat(user("alice")).containsEntry("suspended", false).containsEntry("home", null)
        assertThat(user("bob")).containsEntry("suspended", true).containsEntry("home", "neo4j")
    }

    @Test
    fun `suspended users cannot log in, and lose access in sessions they have`() {
        server.administer("CREATE USER alice SET PASSWORD 'alice-secret-1' CHANGE NOT REQUIRED")
        val alice = driver("alice", "alice-secret-1")
        assertThat(alice.run("RETURN 1 AS one")).containsExactly(mapOf("one" to 1L))
        val session = alice.session()
        session.run("RETURN 1").consume()

        server.administer("ALTER USER alice SET STATUS SUSPENDED")

        assertThat(catchThrowable { session.run("RETURN 1").consume() })
            .isInstanceOf(ClientException::class.java)
            .hasMessageContaining("alice")
            .hasMessageContaining("suspended")
        session.close()
        assertThat(catchThrowable { driver("alice", "alice-secret-1").run("RETURN 1") })
            .isInstanceOf(AuthenticationException::class.java)
        assertThat(catchThrowable { driver("alice", "wrong-password").run("RETURN 1") })
            .isInstanceOf(AuthenticationException::class.java)

        server.administer("ALTER USER alice SET STATUS ACTIVE")

        assertThat(driver("alice", "alice-secret-1").run("RETURN 1 AS one")).hasSize(1)
    }

    @Test
    fun `sessions without a database use the user's home database`() {
        server.administer("CREATE DATABASE films")
        server.administer(
            "CREATE USER alice SET PASSWORD 'alice-secret-1' CHANGE NOT REQUIRED " +
                "SET HOME DATABASE films"
        )
        server.administer("CREATE USER bob SET PASSWORD 'bob-secret-1' CHANGE NOT REQUIRED")
        val alice = driver("alice", "alice-secret-1")
        val bob = driver("bob", "bob-secret-1")
        val database = "CALL db.info() YIELD name"

        assertThat(alice.run(database)).containsExactly(mapOf("name" to "films"))
        assertThat(bob.run(database)).containsExactly(mapOf("name" to "neo4j"))
        assertThat(alice.run("SHOW HOME DATABASE YIELD name", "system"))
            .containsExactly(mapOf("name" to "films"))

        server.administer("ALTER USER alice SET HOME DATABASE missing")

        assertThat(catchThrowable { alice.run(database) })
            .isInstanceOf(ClientException::class.java)
            .hasMessageContaining("missing")
        server.administer("ALTER USER alice REMOVE HOME DATABASE")
        assertThat(alice.run(database)).containsExactly(mapOf("name" to "neo4j"))
    }

    @Test
    fun `refuses external authentication, which DozenDB does not support`() {
        assertThat(
                catchThrowable {
                    server.administer("CREATE USER alice SET AUTH 'oidc-sso' { SET ID 'alice' }")
                }
            )
            .hasMessage(
                "Failed to create the specified user 'alice': `SET AUTH 'oidc-sso'` is not supported."
            )
        server.administer("CREATE USER bob SET PASSWORD 'bob-secret-1'")
        assertThat(catchThrowable { server.administer("ALTER USER bob REMOVE AUTH 'native'") })
            .hasMessage("Failed to alter the specified user 'bob': `REMOVE AUTH` is not supported.")
    }
}
