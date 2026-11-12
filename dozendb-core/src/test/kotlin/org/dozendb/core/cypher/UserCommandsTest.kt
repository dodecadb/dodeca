package org.dozendb.core.cypher

import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.dozendb.core.StandaloneServer
import org.dozendb.core.StandaloneServer.Companion.SYSTEM
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
import org.neo4j.gqlstatus.ErrorGqlStatusObject

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
        val external = catchThrowable {
            server.administer("CREATE USER alice SET AUTH 'oidc-sso' { SET ID 'alice' }")
        }
        assertThat(external)
            .hasMessage(
                "Failed to create the specified user 'alice': `SET AUTH 'oidc-sso'` is not supported."
            )
        assertThat(gqlStatusesOf(external)).contains("51N31")
        server.administer("CREATE USER bob SET PASSWORD 'bob-secret-1'")
        val remove = catchThrowable { server.administer("ALTER USER bob REMOVE AUTH 'native'") }
        assertThat(remove)
            .hasMessage("Failed to alter the specified user 'bob': `REMOVE AUTH` is not supported.")
        assertThat(gqlStatusesOf(remove)).contains("51N31")
    }

    @Test
    fun `lists users as commands that recreate them, with their passwords`() {
        server.administer(
            "CREATE USER alice SET PASSWORD 'alice-secret-1' SET STATUS SUSPENDED " +
                "SET HOME DATABASE films"
        )
        server.administer("CREATE USER `bob.smith` SET PASSWORD 'bob-secret-1' CHANGE NOT REQUIRED")
        val password = "SET ENCRYPTED PASSWORD '1,[0-9a-fA-F]+,[0-9a-fA-F]+'"

        val commands =
            server.rows("CYPHER 25 SHOW USERS AS COMMANDS YIELD user, command", SYSTEM).associate {
                it["user"] to it["command"] as String
            }

        assertThat(commands.getValue("alice"))
            .matches(
                "CREATE USER alice SET STATUS SUSPENDED SET HOME DATABASE films " +
                    "SET AUTH PROVIDER 'native' \\{ $password }"
            )
        assertThat(commands.getValue("bob.smith"))
            .matches(
                "CREATE USER `bob.smith` SET AUTH PROVIDER 'native' " +
                    "\\{ $password SET PASSWORD CHANGE NOT REQUIRED }"
            )
        assertThat(server.rows("CYPHER 25 SHOW USERS AS COMMANDS", SYSTEM).map { it.keys })
            .containsOnly(setOf("command"))
        assertThat(
                server.rows(
                    "CYPHER 25 SHOW USERS WITH AUTH AS COMMANDS WHERE command CONTAINS 'bob'",
                    SYSTEM,
                )
            )
            .containsExactly(
                mapOf(
                    "command" to commands.getValue("bob.smith"),
                    "provider" to "native",
                    "auth" to mapOf("password" to "***", "changeRequired" to false),
                )
            )

        // The command recreates bob with his password, on this server as on another.
        server.administer("DROP USER `bob.smith`")
        server.administer(commands.getValue("bob.smith"))
        assertThat(driver("bob.smith", "bob-secret-1").run("SHOW CURRENT USER YIELD user", SYSTEM))
            .containsExactly(mapOf("user" to "bob.smith"))
    }

    @Test
    fun `lists users as commands with SHOW USER and SHOW USER CREDENTIALS, or USER MANAGEMENT`() {
        server.administer("CREATE USER carol SET PASSWORD 'carol-secret-1' CHANGE NOT REQUIRED")
        server.administer("CREATE ROLE viewer")
        server.administer("GRANT ROLE viewer TO carol")
        server.administer("GRANT SHOW USER ON DBMS TO viewer")
        val carol = driver("carol", "carol-secret-1")
        fun commands() = carol.run("CYPHER 25 SHOW USERS AS COMMANDS", SYSTEM)

        assertThat(carol.run("SHOW USERS", SYSTEM)).hasSize(2)
        assertThat(catchThrowable { commands() })
            .hasMessageStartingWith("Permission has not been granted for SHOW USER CREDENTIALS.")

        server.administer("CYPHER 25 GRANT SHOW USER CREDENTIALS ON DBMS TO viewer")
        assertThat(commands()).hasSize(2)
        server.administer("REVOKE SHOW USER ON DBMS FROM viewer")
        assertThat(catchThrowable { commands() })
            .hasMessageStartingWith("Permission has not been granted for SHOW USER.")

        server.administer("CYPHER 25 REVOKE SHOW USER CREDENTIALS ON DBMS FROM viewer")
        server.administer("GRANT USER MANAGEMENT ON DBMS TO viewer")
        assertThat(commands()).hasSize(2)
        server.administer("CYPHER 25 DENY SHOW USER CREDENTIALS ON DBMS TO viewer")
        assertThat(catchThrowable { commands() })
            .hasMessageStartingWith("Permission denied for SHOW USER CREDENTIALS.")
        assertThat(
                server.rows("CYPHER 25 SHOW ROLE viewer PRIVILEGES AS COMMANDS", SYSTEM).map {
                    it["command"]
                }
            )
            .containsExactlyInAnyOrder(
                "GRANT USER MANAGEMENT ON DBMS TO `viewer`",
                "DENY SHOW USER CREDENTIALS ON DBMS TO `viewer`",
            )
        assertThat(
                server.rows(
                    "SHOW SUPPORTED PRIVILEGES YIELD action, target " +
                        "WHERE action = 'show user credentials'",
                    SYSTEM,
                )
            )
            .containsExactly(mapOf("action" to "show user credentials", "target" to "dbms"))
    }

    /** The GQL statuses of [error] and of its causes, outermost first. */
    private fun gqlStatusesOf(error: Throwable): List<String> =
        generateSequence(error) { it.cause }
            .filterIsInstance<ErrorGqlStatusObject>()
            .flatMap { status -> generateSequence(status) { it.cause().orElse(null) } }
            .map { it.gqlStatus() }
            .toList()
}
