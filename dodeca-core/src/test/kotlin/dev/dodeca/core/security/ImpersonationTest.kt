package dev.dodeca.core.security

import dev.dodeca.core.StandaloneServer
import dev.dodeca.core.StandaloneServer.Companion.SYSTEM
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowableOfType
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
import org.neo4j.driver.exceptions.ClientException

/** Users with IMPERSONATE act as other users through drivers, with their privileges. */
class ImpersonationTest {
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
        server.administer("CREATE DATABASE films")
        server.rows("CREATE (:Movie {title: 'Alien'})", "films")
        server.rows("CREATE (:Book {title: 'Dune'})")
        for (user in listOf("alice", "bob", "carol")) {
            server.administer("CREATE USER $user SET PASSWORD '$user-secret-1' CHANGE NOT REQUIRED")
        }
        server.administer("CREATE ROLE impersonator")
        server.administer("GRANT ROLE impersonator, editor TO alice")
        server.administer("GRANT ROLE reader TO bob")
        server.administer("ALTER USER bob SET HOME DATABASE films")
    }

    @AfterEach
    fun stop() {
        drivers.forEach(Driver::close)
        server.close()
    }

    private fun driver(user: String): Driver =
        GraphDatabase.driver(
                "bolt://${server.boltAddress()}",
                AuthTokens.basic(user, "$user-secret-1"),
            )
            .also { drivers += it }

    private val alice by lazy { driver("alice") }

    /** Runs [command] as the admin, with authentication disabled. */
    private fun grant(command: String) = server.administer(command)

    private fun config(user: String, database: String?): SessionConfig =
        SessionConfig.builder()
            .withImpersonatedUser(user)
            .apply { if (database != null) withDatabase(database) }
            .build()

    /** Runs [cypher] as [user], in [database] or the user's home database. */
    private fun Driver.run(
        user: String,
        cypher: String,
        database: String? = null,
    ): List<Map<String, Any?>> =
        session(config(user, database)).use { session -> session.run(cypher).list { it.asMap() } }

    private fun Driver.refused(user: String, cypher: String, database: String? = null): String =
        checkNotNull(
                catchThrowableOfType(ClientException::class.java) { run(user, cypher, database) }
            ) {
                "$cypher as $user succeeded"
            }
            .message!!

    @Test
    fun `impersonates the users IMPERSONATE names, but those it denies`() {
        assertThat(alice.refused("bob", "SHOW CURRENT USER", SYSTEM))
            .isEqualTo("Permission has not been granted for IMPERSONATE. $CHECK_PRIVILEGES")

        grant("GRANT IMPERSONATE (*) ON DBMS TO impersonator")
        grant("DENY IMPERSONATE (carol) ON DBMS TO impersonator")

        assertThat(alice.run("bob", "SHOW CURRENT USER YIELD user, roles", SYSTEM))
            .containsExactly(mapOf("user" to "bob", "roles" to listOf("reader", "PUBLIC")))
        assertThat(alice.refused("carol", "SHOW CURRENT USER", SYSTEM))
            .isEqualTo("Permission denied for IMPERSONATE. $CHECK_PRIVILEGES")
        assertThat(alice.refused("dave", "SHOW CURRENT USER", SYSTEM))
            .isEqualTo("Cannot impersonate user 'dave': the user does not exist.")

        grant("ALTER USER bob SET STATUS SUSPENDED")

        assertThat(alice.refused("bob", "SHOW CURRENT USER", SYSTEM))
            .isEqualTo("Cannot impersonate user 'bob': the user is suspended.")

        grant("REVOKE DENY IMPERSONATE (carol) ON DBMS FROM impersonator")

        assertThat(alice.run("carol", "SHOW CURRENT USER YIELD user", SYSTEM))
            .containsExactly(mapOf("user" to "carol"))
    }

    @Test
    fun `acts with the privileges and home database of the user, and administers nothing`() {
        grant("GRANT IMPERSONATE (bob) ON DBMS TO impersonator")
        grant("CREATE ROLE manager")
        grant("GRANT ROLE manager TO bob")
        grant("GRANT CREATE ROLE ON DBMS TO manager")
        grant("GRANT SHOW ROLE ON DBMS TO manager")

        val home =
            alice.session(config("bob", null)).use { session ->
                session.run("MATCH (n) RETURN n.title AS title").let {
                    it.list { row -> row["title"].asString() } to it.consume().database().name()
                }
            }
        assertThat(home).isEqualTo(listOf("Alien") to "films")
        assertThat(alice.run("bob", "MATCH (n) RETURN n.title AS title", "neo4j"))
            .containsExactly(mapOf("title" to "Dune"))
        assertThat(alice.refused("bob", "CREATE (:Book)", "neo4j"))
            .isEqualTo(
                "Create node with labels 'Book' on database 'neo4j' is not allowed for user " +
                    "'bob' with roles [PUBLIC, manager, reader]."
            )
        assertThat(alice.run("bob", "SHOW ROLES YIELD role WHERE role = 'manager'", SYSTEM))
            .hasSize(1)
        assertThat(alice.refused("bob", "CREATE ROLE curator", SYSTEM))
            .isEqualTo("Not allowed to run updating system commands when impersonating a user.")
        // Alice herself still writes.
        alice.session(SessionConfig.forDatabase("neo4j")).use { it.run("CREATE (:Book)").consume() }
    }

    @Test
    fun `refuses to impersonate while the password must change`() {
        server.administer("CREATE USER dora SET PASSWORD 'dora-secret-1' CHANGE REQUIRED")
        grant("GRANT ROLE impersonator TO dora")
        grant("GRANT IMPERSONATE (*) ON DBMS TO impersonator")

        assertThat(driver("dora").refused("bob", "SHOW CURRENT USER", SYSTEM))
            .isEqualTo("Cannot impersonate a user while password change required.")
    }

    private companion object {
        const val CHECK_PRIVILEGES =
            "Try executing SHOW USER PRIVILEGES to determine the missing or denied privileges. " +
                "In case of missing privileges, they need to be granted (See GRANT). In case of " +
                "denied privileges, they need to be revoked (See REVOKE) and granted."
    }
}
