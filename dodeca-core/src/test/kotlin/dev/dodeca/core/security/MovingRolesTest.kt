package dev.dodeca.core.security

import dev.dodeca.core.DodecaSettings
import dev.dodeca.core.StandaloneServer
import dev.dodeca.core.StandaloneServer.Companion.SYSTEM
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig
import org.neo4j.graphdb.config.Setting

/**
 * Users, roles, auth rules and privileges moved to another DBMS as the operator guide's "Moving
 * roles from or to Neo4j Enterprise" says: with the commands that `SHOW ... AS COMMANDS` gives.
 */
class MovingRolesTest {
    // Kept when a test fails, for the servers' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private val servers = mutableListOf<StandaloneServer>()
    private val drivers = mutableListOf<Driver>()

    @AfterEach
    fun stop() {
        drivers.forEach(Driver::close)
        servers.forEach(StandaloneServer::close)
    }

    private fun server(name: String): StandaloneServer =
        StandaloneServer(
                root.resolve(name),
                mapOf<Setting<*>, Any>(
                    GraphDatabaseSettings.auth_enabled to true,
                    BoltConnector.enabled to true,
                    DodecaSettings.abacAuthorizationProviders to listOf("native"),
                ),
            )
            .also {
                servers += it
                it.start()
            }

    private fun StandaloneServer.commands(show: String): List<String> =
        rows("CYPHER 25 $show", SYSTEM).map { it["command"] as String }

    private fun StandaloneServer.security(): Map<String, Set<Any?>> =
        mapOf(
            "users" to
                rows(
                        "SHOW USERS YIELD user, roles, passwordChangeRequired, suspended, home, tags",
                        SYSTEM,
                    )
                    .toSet(),
            "roles" to rows("SHOW ROLES WITH USERS YIELD *", SYSTEM).toSet(),
            "auth rules" to rows("CYPHER 25 SHOW AUTH RULES YIELD *", SYSTEM).toSet(),
            "roles of auth rules" to
                rows("CYPHER 25 SHOW ROLES WITH AUTH RULES YIELD *", SYSTEM).toSet(),
            "privileges" to commands("SHOW PRIVILEGES AS COMMANDS").toSet(),
        )

    @Test
    fun `moves users with their passwords and tags, roles, auth rules and privileges to another DBMS`() {
        val from = server("from")
        from.administer("CREATE ROLE curator")
        from.administer("GRANT MATCH {title} ON GRAPH neo4j NODES Movie TO curator")
        from.administer("DENY READ {budget} ON GRAPH * NODES * TO curator")
        from.administer("CYPHER 25 GRANT SHOW USER CREDENTIALS ON DBMS TO curator")
        from.administer(
            "CYPHER 25 CREATE USER alice SET PASSWORD 'alice-secret-1' CHANGE NOT REQUIRED " +
                "SET HOME DATABASE neo4j SET TAGS ['finance', 'auditor']"
        )
        from.administer("CREATE USER bob SET PASSWORD 'bob-secret-1' SET STATUS SUSPENDED")
        from.administer("GRANT ROLE curator, reader TO alice")
        from.administer("GRANT ROLE editor TO bob")
        from.administer("CREATE ROLE auditing")
        from.administer("GRANT READ {*} ON GRAPH neo4j TO auditing")
        from.administer(
            "CYPHER 25 CREATE AUTH RULE auditors SET CONDITION " +
                "'auditor' IN abac.native.user_tags()"
        )
        from.administer(
            "CYPHER 25 CREATE AUTH RULE `night shift` SET CONDITION false SET ENABLED false"
        )
        from.administer("CYPHER 25 GRANT ROLE auditing, editor TO AUTH RULE auditors")
        val script =
            listOf(
                    "SHOW ROLES AS COMMANDS",
                    "SHOW USERS AS COMMANDS",
                    "SHOW ROLES WITH USERS AS COMMANDS",
                    "SHOW AUTH RULES AS COMMANDS",
                    "SHOW ROLES WITH AUTH RULES AS COMMANDS",
                    "SHOW PRIVILEGES AS COMMANDS",
                )
                .flatMap { from.commands(it) }

        // The new DBMS has its built-in roles and neo4j, so their commands are left out.
        val to = server("to")
        val existing = to.commands("SHOW ROLES AS COMMANDS").toSet()
        for (command in
            script.filterNot { it in existing || it.startsWith("CREATE USER neo4j ") }) {
            to.administer("CYPHER 25 $command")
        }

        assertThat(to.security()).isEqualTo(from.security())
        assertThat(to.rows("CYPHER 25 SHOW AUTH RULES YIELD name", SYSTEM))
            .extracting<Any?> { it["name"] }
            .containsExactly("auditors", "night shift")
        val alice =
            GraphDatabase.driver(
                    "bolt://${to.boltAddress()}",
                    AuthTokens.basic("alice", "alice-secret-1"),
                )
                .also { drivers += it }
        assertThat(
                alice.session(SessionConfig.forDatabase(SYSTEM)).use {
                    it.run("SHOW CURRENT USER YIELD user, roles").single().asMap()
                }
            )
            .isEqualTo(mapOf("user" to "alice", "roles" to listOf("curator", "reader", "PUBLIC")))
        // With the roles of the auth rule that holds for her tags.
        assertThat(
                alice.session(SessionConfig.forDatabase(SYSTEM)).use {
                    it.run("CALL dbms.showCurrentUser() YIELD roles RETURN roles")
                        .single()["roles"]
                        .asList()
                }
            )
            .containsExactlyInAnyOrder("curator", "reader", "PUBLIC", "auditing", "editor")
    }
}
