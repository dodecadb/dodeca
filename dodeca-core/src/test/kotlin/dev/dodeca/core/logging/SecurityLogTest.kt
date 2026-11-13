package dev.dodeca.core.logging

import dev.dodeca.core.DodecaSettings
import dev.dodeca.core.StandaloneServer
import dev.dodeca.core.StandaloneServer.Companion.SYSTEM
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.exists
import kotlin.io.path.readLines
import kotlin.io.path.readText
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig
import org.neo4j.graphdb.config.Setting

/** `security.log`, as users log in and run commands through Bolt. */
class SecurityLogTest {
    @TempDir lateinit var root: Path
    private val drivers = mutableListOf<Driver>()
    private var server: StandaloneServer? = null

    @AfterEach
    fun stop() {
        drivers.forEach(Driver::close)
        server?.close()
    }

    private fun start(vararg settings: Pair<Setting<*>, Any>): StandaloneServer =
        StandaloneServer(
                root,
                mapOf<Setting<*>, Any>(
                    GraphDatabaseSettings.auth_enabled to true,
                    BoltConnector.enabled to true,
                ) + settings,
            )
            .also {
                server = it
                it.start()
                it.administer("ALTER USER neo4j SET PASSWORD 'admin-secret-1' CHANGE NOT REQUIRED")
                it.administer("CREATE USER alice SET PASSWORD 'alice-secret-1' CHANGE NOT REQUIRED")
            }

    private fun StandaloneServer.driver(user: String, password: String): Driver =
        GraphDatabase.driver("bolt://${boltAddress()}", AuthTokens.basic(user, password)).also {
            drivers += it
        }

    private fun Driver.run(query: String, database: String = "neo4j", impersonate: String? = null) {
        val config =
            SessionConfig.builder()
                .withDatabase(database)
                .apply { impersonate?.let { withImpersonatedUser(it) } }
                .build()
        session(config).use { it.run(query).consume() }
    }

    private fun Driver.fails(
        query: String,
        database: String = "neo4j",
        impersonate: String? = null,
    ) = assertThat(catchThrowable { run(query, database, impersonate) }).isNotNull()

    private val StandaloneServer.securityLog: Path
        get() = home.resolve("logs/security.log")

    /** The security log's lines once [action] ran, without the timestamps. */
    private fun StandaloneServer.linesOf(action: () -> Unit): List<String> {
        val before = if (securityLog.exists()) securityLog.readLines().size else 0
        action()
        var size = -1L
        var still = 0
        while (still < 4) {
            Thread.sleep(50)
            val now = if (securityLog.exists()) Files.size(securityLog) else 0
            still = if (now == size) still + 1 else 0
            size = now
        }
        return securityLog.readLines().drop(before).map { it.substringAfter("+0000 ") }
    }

    @Test
    fun `logins, successful or not`() {
        val server = start()
        val lines = server.linesOf {
            server.driver("alice", "alice-secret-1").verifyConnectivity()
            assertThat(catchThrowable { server.driver("alice", "wrong-1").verifyConnectivity() })
                .isNotNull()
            server.administer("ALTER USER alice SET STATUS SUSPENDED")
            assertThat(
                    catchThrowable {
                        server.driver("alice", "alice-secret-1").verifyConnectivity()
                    }
                )
                .isNotNull()
        }

        assertThat(lines)
            .containsSubsequence(
                "INFO  [alice]: logged in",
                "ERROR [alice]: failed to log in: invalid principal or credentials",
                "ERROR [alice]: failed to log in: the user is suspended",
            )
        assertThat(lines.joinToString("\n")).doesNotContain("secret", "wrong-1")
        assertThat(server.home.resolve("logs/debug.log").readText())
            .doesNotContain("failed to log in")
    }

    @Test
    fun `successful logins are left out when asked`() {
        val server = start(DodecaSettings.logSuccessfulAuthentication to false)
        val lines = server.linesOf { server.driver("alice", "alice-secret-1").verifyConnectivity() }

        assertThat(lines).noneMatch { "logged in" in it }
    }

    @Test
    fun `administration commands that change something, as they committed`() {
        val server = start()
        val admin = server.driver("neo4j", "admin-secret-1")
        val lines = server.linesOf {
            admin.run("CREATE USER bob SET PASSWORD 'bob-secret-1' CHANGE NOT REQUIRED", SYSTEM)
            admin.run("CREATE ROLE critic", SYSTEM)
            admin.run("CREATE ROLE critic IF NOT EXISTS", SYSTEM)
            admin.fails("CREATE ROLE critic", SYSTEM)
            admin.run("GRANT ROLE critic TO bob", SYSTEM)
            admin.run("SHOW USERS", SYSTEM)
        }
        val commands = lines.filter { it.startsWith("INFO  [neo4j]: ") && "logged in" !in it }

        assertThat(commands).hasSize(3)
        assertThat(commands[0])
            .startsWith("INFO  [neo4j]: CREATE USER bob SET PASSWORD")
            .doesNotContain("bob-secret-1")
        assertThat(commands[1]).isEqualTo("INFO  [neo4j]: CREATE ROLE critic")
        assertThat(commands[2]).isEqualTo("INFO  [neo4j]: GRANT ROLE critic TO bob")
    }

    @Test
    fun `authorization failures`() {
        val server = start()
        server.administer("CREATE DATABASE films WAIT")
        val alice = server.driver("alice", "alice-secret-1")
        val lines = server.linesOf {
            alice.fails("CREATE (:Movie)")
            alice.fails("MATCH (n) RETURN n", "films")
            alice.fails("CALL dbms.setConfigValue('db.logs.query.enabled', 'OFF')", SYSTEM)
            alice.fails("CREATE USER carol SET PASSWORD 'carol-secret-1'", SYSTEM)
        }
        val errors = lines.filter { it.startsWith("ERROR [alice]: ") }

        assertThat(errors.joinToString("\n"))
            .contains("Creating new node label on database 'neo4j' is not allowed")
            .contains("Database access is not allowed for user 'alice' with roles [PUBLIC]")
            .contains("Executing admin procedure 'dbms.setConfigValue' permission has not been")
            .contains("Permission has not been granted for CREATE USER")
            .doesNotContain("carol-secret-1")
    }

    @Test
    fun `impersonation, allowed or refused`() {
        val server = start()
        server.administer("CREATE USER bob SET PASSWORD 'bob-secret-1' CHANGE NOT REQUIRED")
        val alice = server.driver("alice", "alice-secret-1")
        val refused = server.linesOf { alice.fails("RETURN 1", impersonate = "bob") }
        assertThat(refused).anyMatch {
            it.startsWith("ERROR [alice]: Impersonating user 'bob' was refused: ") &&
                "IMPERSONATE" in it
        }

        server.administer("CREATE ROLE impersonator")
        server.administer("GRANT IMPERSONATE (bob) ON DBMS TO impersonator")
        server.administer("GRANT ROLE impersonator TO alice")
        val allowed = server.linesOf { alice.run("RETURN 1", impersonate = "bob") }
        assertThat(allowed).contains("INFO  [alice:bob]: impersonating user bob logged in")
    }

    @Test
    fun `nothing is logged with authentication disabled`() {
        val server =
            StandaloneServer(root, mapOf(BoltConnector.enabled to true)).also {
                this.server = it
                it.start()
            }
        server.administer("CREATE ROLE critic")
        GraphDatabase.driver("bolt://${server.boltAddress()}").use { it.verifyConnectivity() }

        assertThat(!server.securityLog.exists() || server.securityLog.readText().isEmpty()).isTrue()
    }
}
