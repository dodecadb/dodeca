package org.dozendb.core.security

import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.dozendb.core.StandaloneServer
import org.dozendb.core.StandaloneServer.Companion.SYSTEM
import org.dozendb.core.security.MetadataScript.Include
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig
import org.neo4j.logging.NullLog

/**
 * The roles, privileges and users of one database, as a backup keeps them and a DBMS takes them.
 */
class MetadataScriptTest {
    // Kept when a test fails, for the servers' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private lateinit var source: StandaloneServer
    private val servers = mutableListOf<StandaloneServer>()

    @BeforeEach
    fun start() {
        source = server("source")
        for (command in
            listOf(
                "CREATE DATABASE films",
                "CREATE ROLE curator",
                "GRANT ACCESS ON DATABASE films TO curator",
                "GRANT MATCH {*} ON GRAPH films NODES Movie TO curator",
                "DENY READ {secret} ON GRAPH films FOR (n:Movie) WHERE n.rating > 5 TO curator",
                "GRANT TRAVERSE ON GRAPH * NODES Person TO curator",
                "GRANT CREATE ROLE ON DBMS TO curator",
                // On the DBMS in Cypher 5, which Neo4j keeps as on every database; on films, only
                // Cypher 25 grants it.
                "GRANT ALTER DATABASE ON DBMS TO curator",
                "CYPHER 25 GRANT SET DATABASE ACCESS ON DATABASE films TO curator",
                "GRANT MATCH {*} ON GRAPH neo4j TO curator",
                "CREATE ROLE `film_fan`",
                "GRANT ACCESS ON DATABASE films TO `film_fan`",
                "CREATE ROLE other",
                "GRANT MATCH {*} ON GRAPH neo4j TO other",
                "CREATE USER alice SET PASSWORD 'alice-secret-1' CHANGE NOT REQUIRED " +
                    "SET HOME DATABASE films",
                "GRANT ROLE curator, other, `film_fan` TO alice",
                "CREATE USER bob SET PASSWORD 'bob-secret-1' CHANGE NOT REQUIRED",
                "GRANT ROLE other TO bob",
                "CREATE USER carol SET PASSWORD 'carol-secret-1' SET STATUS SUSPENDED",
                "GRANT ROLE curator TO carol",
            )) {
            source.administer(command)
        }
    }

    @AfterEach
    fun stop() {
        servers.forEach(StandaloneServer::close)
    }

    private fun server(name: String): StandaloneServer =
        StandaloneServer(
                root.resolve(name),
                mapOf(GraphDatabaseSettings.auth_enabled to true, BoltConnector.enabled to true),
            )
            .also { servers += it }
            .also { it.start() }

    private fun script(
        include: Include,
        database: String = "films",
        users: Set<String>? = null,
    ): String? =
        source.database(SYSTEM).beginTx().use { tx ->
            MetadataScript.generate(tx, database, include, NullLog.getInstance(), users)
        }

    private fun statements(script: String?): List<String> =
        checkNotNull(script).split(";\n").filter { it.isNotBlank() }

    @Test
    fun `keeps the roles that concern the database, their privileges there and their users`() {
        val statements = statements(script(Include.ALL))
        val credentials = { user: String ->
            statements.single { it.startsWith("CREATE USER `$user`") }
        }

        assertThat(statements)
            // PUBLIC first, and the built-in roles too, with their privileges on every database.
            .startsWith("GRANT ACCESS ON HOME DATABASE TO `PUBLIC`")
            .contains(
                "CREATE ROLE `admin` IF NOT EXISTS",
                "GRANT ACCESS ON DATABASE * TO `admin`",
                "CREATE ROLE `curator` IF NOT EXISTS",
                "GRANT ACCESS ON DATABASE \$database TO `curator`",
                "GRANT MATCH {*} ON GRAPH \$database NODE Movie TO `curator`",
                "DENY READ {secret} ON GRAPH \$database FOR (n:Movie) WHERE n.rating > 5 TO `curator`",
                "GRANT TRAVERSE ON GRAPH * NODE Person TO `curator`",
                "CREATE ROLE `film_fan` IF NOT EXISTS",
                "GRANT ACCESS ON DATABASE \$database TO `film_fan`",
                "GRANT ROLE `curator` TO `alice`",
                "GRANT ROLE `film_fan` TO `alice`",
                "GRANT ROLE `curator` TO `carol`",
            )
            .noneMatch { "ON DBMS" in it || " neo4j" in it || "`other`" in it }
            .noneMatch { "`bob`" in it }
        assertThat(credentials("alice"))
            .matches(
                "CREATE USER `alice` IF NOT EXISTS SET ENCRYPTED PASSWORD '1,[0-9a-f]+,[0-9a-f]+' " +
                    "CHANGE NOT REQUIRED SET STATUS ACTIVE SET HOME DATABASE \\\$database"
            )
        assertThat(credentials("carol"))
            .contains("CHANGE REQUIRED SET STATUS SUSPENDED")
            .doesNotContain("HOME")
        // Prefixed where only Cypher 25 has the command; ALTER DATABASE on the DBMS is left out.
        assertThat(statements)
            .contains("CYPHER 25 GRANT SET DATABASE ACCESS ON DATABASE \$database TO `curator`")
            .noneMatch { "ALTER DATABASE" in it }
    }

    @Test
    fun `gives a DBMS back the roles, privileges and users, under another name, more than once`() {
        val script = statements(script(Include.ALL))
        val target = server("target")
        target.administer("CREATE DATABASE archive")
        repeat(2) {
            script.forEach { target.administer(it, mapOf("database" to "archive")) }
        }
        fun commands(server: StandaloneServer, role: String) =
            server
                // In Cypher 5, which names all privileges of the DBMS so.
                .rows("CYPHER 5 SHOW ROLE `$role` PRIVILEGES AS COMMANDS", SYSTEM)
                .map { it["command"] as String }
                .filter { "DBMS" !in it && "neo4j" !in it }
                .map { it.replace("archive", "films") }

        assertThat(commands(target, "curator"))
            .containsExactlyInAnyOrderElementsOf(commands(source, "curator"))
        assertThat(
                target.rows(
                    "SHOW USERS YIELD user, roles, suspended, home RETURN * ORDER BY user",
                    SYSTEM,
                )
            )
            .containsExactly(
                mapOf(
                    "user" to "alice",
                    "roles" to listOf("curator", "film_fan", "PUBLIC"),
                    "suspended" to false,
                    "home" to "archive",
                ),
                mapOf(
                    "user" to "carol",
                    "roles" to listOf("curator", "PUBLIC"),
                    "suspended" to true,
                    "home" to null,
                ),
                mapOf(
                    "user" to "neo4j",
                    "roles" to listOf("admin", "PUBLIC"),
                    "suspended" to false,
                    "home" to null,
                ),
            )
        // The encrypted password logs alice in.
        GraphDatabase.driver(
                "bolt://${target.boltAddress()}",
                AuthTokens.basic("alice", "alice-secret-1"),
            )
            .use { driver ->
                driver.session(SessionConfig.forDatabase("archive")).use {
                    assertThat(it.run("RETURN 1 AS one").single()["one"].asLong()).isEqualTo(1)
                }
            }
    }

    @Test
    fun `keeps what is asked for, and nothing for system or no database`() {
        assertThat(statements(script(Include.ROLES))).noneMatch { it.startsWith("CREATE USER") }
        assertThat(statements(script(Include.USERS)))
            .noneMatch { it.startsWith("CREATE ROLE") || it.startsWith("GRANT ACCESS") }
            .anyMatch { it.startsWith("CREATE USER `alice`") }
            .contains("GRANT ROLE `curator` TO `alice`")
        assertThat(script(Include.NONE)).isNull()
        // Only the users listed, as --include-metadata=users=alice,dave asks: dave does not exist.
        assertThat(statements(script(Include.USERS, users = setOf("alice", "dave"))))
            .anyMatch { it.startsWith("CREATE USER `alice`") }
            .noneMatch { it.startsWith("CREATE USER `carol`") || it.contains("`dave`") }
        assertThat(script(Include.ALL, SYSTEM)).isNull()
        assertThat(script(Include.ALL, "nothing")).isNull()
    }
}
