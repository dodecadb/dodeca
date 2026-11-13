package dev.dodeca.core.cypher

import dev.dodeca.core.StandaloneServer
import dev.dodeca.core.StandaloneServer.Companion.SYSTEM
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
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

/**
 * Tags of native users, as Neo4j Enterprise 2026.06 has them: `SET`, `ADD` and `REMOVE [ALL] TAGS`
 * of `CREATE USER`, `ALTER USER` and `ALTER USERS`, shown with `SHOW USER METADATA`.
 */
class UserTagsTest {
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

    private fun tags(user: String): Any? =
        server
            .rows("SHOW USERS YIELD user, tags WHERE user = \$user", SYSTEM, mapOf("user" to user))
            .single()["tags"]

    /** Runs [cypher] on `system` in Cypher 25, as Neo4j's parser has tags in Cypher 25 only. */
    private fun administer(cypher: String, parameters: Map<String, Any> = emptyMap()) =
        server.administer("CYPHER 25 $cypher", parameters)

    private fun driver(user: String, password: String): Driver =
        GraphDatabase.driver("bolt://${server.boltAddress()}", AuthTokens.basic(user, password))
            .also { drivers += it }

    private fun Driver.run(cypher: String): List<Map<String, Any?>> =
        session(SessionConfig.forDatabase(SYSTEM)).use { session ->
            session.run("CYPHER 25 ${cypher.removePrefix("CYPHER 25 ")}").list { it.asMap() }
        }

    @Test
    fun `sets, adds and removes tags, in the order given and each once`() {
        administer("CREATE USER jim SET PASSWORD 'jim-secret-1' SET TAGS ['finance', 'auditor']")
        assertThat(tags("jim")).isEqualTo(listOf("finance", "auditor"))

        administer("ALTER USER jim REMOVE TAG 'auditor' ADD TAG 'on-call'")
        assertThat(tags("jim")).isEqualTo(listOf("finance", "on-call"))

        administer("ALTER USER jim REMOVE TAG 'x' ADD TAGS ['finance', 'audit', 'audit']")
        assertThat(tags("jim")).isEqualTo(listOf("finance", "on-call", "audit"))

        administer("ALTER USER jim SET TAGS 'Finance'")
        assertThat(tags("jim")).isEqualTo(listOf("Finance"))

        administer("ALTER USER jim SET TAGS \$tags", mapOf("tags" to listOf("a", "b", "a")))
        assertThat(tags("jim")).isEqualTo(listOf("a", "b"))

        administer("ALTER USER jim REMOVE ALL TAGS ADD TAG 'c'")
        assertThat(tags("jim")).isEqualTo(listOf("c"))

        administer("ALTER USER jim SET TAGS []")
        assertThat(tags("jim")).isEqualTo(emptyList<String>())

        // Neo4j's parser has tags in Cypher 25 only.
        assertThat(catchThrowable { server.administer("CYPHER 5 ALTER USER jim SET TAGS 'x'") })
            .hasMessageContaining("Invalid input 'TAGS'")
        administer("CREATE OR REPLACE USER jim SET PASSWORD 'jim-secret-1' SET TAGS 'x'")
        assertThat(tags("jim")).isEqualTo(listOf("x"))
        administer("ALTER USER missing IF EXISTS ADD TAG 'x'")
        assertThat(server.rows("SHOW USERS YIELD user", SYSTEM).map { it["user"] })
            .containsExactlyInAnyOrder("neo4j", "jim")
    }

    @Test
    fun `alters the tags of many users at once, all or none`() {
        administer("CREATE USER bob SET PASSWORD 'bob-secret-1' SET TAGS ['finance']")
        administer("CREATE USER alice SET PASSWORD 'alice-secret-1'")

        administer("ALTER USERS bob, alice ADD TAG 'on-call'")
        assertThat(tags("bob")).isEqualTo(listOf("finance", "on-call"))
        assertThat(tags("alice")).isEqualTo(listOf("on-call"))

        assertThat(catchThrowable { administer("ALTER USERS bob, carol SET TAGS 'x'") })
            .rootCause()
            .hasMessage("Failed to alter the specified user 'carol': User does not exist.")
        assertThat(tags("bob")).isEqualTo(listOf("finance", "on-call"))

        administer(
            "ALTER USERS \$bob, carol, alice IF EXISTS SET TAGS ['finance', 'auditor']",
            mapOf("bob" to "bob"),
        )
        assertThat(tags("bob")).isEqualTo(listOf("finance", "auditor"))
        assertThat(tags("alice")).isEqualTo(listOf("finance", "auditor"))
        assertThat(server.rows("SHOW USERS YIELD user", SYSTEM).map { it["user"] })
            .doesNotContain("carol")
    }

    @Test
    fun `refuses tag parameters that are not strings`() {
        administer("CREATE USER jim SET PASSWORD 'jim-secret-1'")
        fun refusal(value: Any?): String? {
            // Neo4j takes null parameters, which Kotlin's types do not show.
            @Suppress("UNCHECKED_CAST") val parameters = mapOf("tags" to value) as Map<String, Any>
            return generateSequence(
                    catchThrowable {
                        administer("ALTER USER jim ADD TAGS \$tags", parameters)
                    }
                ) {
                    it.cause
                }
                .last()
                .message
        }

        for (value in listOf(null, 1L, listOf("a", 1L), listOf("a", null), "", listOf(""))) {
            assertThat(refusal(value))
                .describedAs("$value")
                .startsWith(
                    "Failed to alter the specified user 'jim': ADD TAGS takes a non-empty String " +
                        "or a List of non-empty Strings, not "
                )
        }
        assertThat(tags("jim")).isEqualTo(emptyList<String>())
    }

    @Test
    fun `shows and lists tags as commands only with SHOW USER METADATA`() {
        administer("ALTER USER neo4j SET PASSWORD 'neo4j-secret-1' CHANGE NOT REQUIRED")
        administer(
            "CREATE USER jim SET PASSWORD 'jim-secret-1' CHANGE NOT REQUIRED " +
                "SET TAGS ['finance', \"it's\"]"
        )
        administer("CREATE ROLE viewer")
        administer("GRANT ROLE viewer TO jim")
        administer("GRANT SHOW USER ON DBMS TO viewer")
        administer("GRANT SHOW USER CREDENTIALS ON DBMS TO viewer")
        val jim = driver("jim", "jim-secret-1")
        fun shown() =
            jim.run("SHOW USERS YIELD user, tags WHERE user = 'jim'").single()["tags"] to
                jim.run("SHOW CURRENT USER YIELD tags").single()["tags"]
        fun command() =
            jim.run("CYPHER 25 SHOW USERS AS COMMANDS YIELD user, command, tags WHERE user = 'jim'")
                .single()

        assertThat(shown()).isEqualTo(null to null)
        assertThat(command()["tags"]).isNull()
        assertThat(command()["command"] as String).doesNotContain("TAGS")

        administer("GRANT SHOW USER METADATA ON DBMS TO viewer")
        assertThat(shown()).isEqualTo(listOf("finance", "it's") to listOf("finance", "it's"))
        assertThat(command()["tags"]).isEqualTo(listOf("finance", "it's"))
        assertThat(command()["command"] as String).endsWith(" SET TAGS ['finance', \"it's\"]")

        administer("REVOKE SHOW USER METADATA ON DBMS FROM viewer")
        administer("GRANT USER METADATA MANAGEMENT ON DBMS TO viewer")
        assertThat(shown()).isEqualTo(listOf("finance", "it's") to listOf("finance", "it's"))
        administer("DENY SHOW USER METADATA ON DBMS TO viewer")
        assertThat(shown()).isEqualTo(null to null)

        val admin = driver("neo4j", "neo4j-secret-1")
        assertThat(admin.run("SHOW USERS YIELD user, tags WHERE user = 'jim'").single()["tags"])
            .isEqualTo(listOf("finance", "it's"))
    }

    @Test
    fun `sets tags only with SET USER METADATA, which USER MANAGEMENT does not include`() {
        administer("CREATE USER carol SET PASSWORD 'carol-secret-1' CHANGE NOT REQUIRED")
        administer("CREATE USER jim SET PASSWORD 'jim-secret-1'")
        administer("CREATE ROLE manager")
        administer("GRANT ROLE manager TO carol")
        administer("GRANT USER MANAGEMENT ON DBMS TO manager")
        val carol = driver("carol", "carol-secret-1")
        fun refusal(cypher: String) = catchThrowable { carol.run(cypher) }?.message

        assertThat(refusal("CREATE USER tim SET PASSWORD 'tim-secret-1' SET TAGS 'a'"))
            .startsWith("Permission has not been granted for SET USER METADATA.")
        assertThat(refusal("ALTER USER jim ADD TAG 'a'"))
            .startsWith("Permission has not been granted for SET USER METADATA.")
        assertThat(refusal("ALTER USERS jim SET TAGS 'a'"))
            .startsWith("Permission has not been granted for SET USER METADATA.")
        assertThat(tags("jim")).isEqualTo(emptyList<String>())

        administer("GRANT SET USER METADATA ON DBMS TO manager")
        carol.run("ALTER USERS jim ADD TAG 'a'")
        carol.run("ALTER USER jim ADD TAG 'b'")
        carol.run("CREATE USER tim SET PASSWORD 'tim-secret-1' SET TAGS 'c'")
        assertThat(tags("jim")).isEqualTo(listOf("a", "b"))
        assertThat(tags("tim")).isEqualTo(listOf("c"))
        assertThat(
                server.rows("SHOW ROLE manager PRIVILEGES AS COMMANDS", SYSTEM).map {
                    it["command"]
                }
            )
            .containsExactlyInAnyOrder(
                "GRANT USER MANAGEMENT ON DBMS TO `manager`",
                "GRANT SET USER METADATA ON DBMS TO `manager`",
            )
    }
}
