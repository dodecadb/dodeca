package org.dozendb.core.security

import java.nio.file.Path
import java.time.LocalDate
import java.time.ZoneOffset
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.assertj.core.api.SoftAssertions
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
import org.neo4j.gqlstatus.ErrorGqlStatusObject

/**
 * Property-based rules granted and denied with Cypher, as a user finds and reads nodes through
 * Bolt, against the admin's own WHERE on the same nodes (P1-P3).
 */
class PropertyRulesTest {
    // Kept when a test fails, for the server's logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private lateinit var server: StandaloneServer
    private lateinit var alice: Driver

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
        server.rows(
            "CREATE (:Doc {id: 1, level: 0, name: 'a', public: true, score: 1.5, " +
                "due: date('2026-01-01'), at: datetime('2026-01-01T12:00:00Z'), " +
                "place: point({x: 1, y: 2}), span: duration('P1D'), secret: 's1'}), " +
                "(:Doc {id: 2, level: 1, name: 'b', public: false, score: 2.5, " +
                "due: date('2026-12-01'), secret: 's2'}), " +
                "(:Doc {id: 3, level: 2, name: 'c', secret: 's3'}), " +
                "(:Doc {id: 4, level: '1', secret: 's4'}), " +
                "(:Doc {id: 5, secret: 's5'}), " +
                "(:Doc:Other {id: 6, level: 3, secret: 's6'}), " +
                "(:Other {id: 7, level: 1})"
        )
        server.administer("CREATE USER alice SET PASSWORD 'alice-secret-1' CHANGE NOT REQUIRED")
        alice =
            GraphDatabase.driver(
                "bolt://${server.boltAddress()}",
                AuthTokens.basic("alice", "alice-secret-1"),
            )
    }

    @AfterEach
    fun stop() {
        alice.close()
        server.close()
    }

    private fun role(vararg commands: String, parameters: Map<String, Any> = emptyMap()) {
        server.administer("DROP ROLE r IF EXISTS")
        server.administer("CREATE ROLE r")
        server.administer("GRANT ROLE r TO alice")
        commands.forEach { server.administer(it, parameters) }
    }

    private fun Driver.ids(cypher: String, parameters: Map<String, Any> = emptyMap()): List<Any?> =
        session(SessionConfig.forDatabase("neo4j")).use { session ->
            session.run(cypher, parameters).list { it.values().single().asObject() }
        }

    /** The ids of the nodes for which [predicate] is true, as the admin finds them. */
    private fun where(predicate: String, parameters: Map<String, Any> = emptyMap()): List<Any?> =
        server
            .rows(
                "MATCH (n) WHERE coalesce($predicate, false) RETURN n.id AS id ORDER BY id",
                parameters = parameters,
            )
            .map { it["id"] }

    /** A rule, `(n:Doc) WHERE ...`, and the predicate that tells which nodes it applies to. */
    private data class Case(val pattern: String, val applies: String)

    @Test
    fun `finds the nodes that GRANT rules allow and DENY rules do not, failing open`() {
        val parameters = mapOf("level" to 1L, "levels" to listOf(0L, 2L))
        val cases =
            listOf(
                    "n.level = 1",
                    "n.level <> 1",
                    "n.level > 1",
                    "n.level >= 1",
                    "n.level < 1",
                    "n.level <= 1",
                    "n.level IN [0, 2]",
                    "NOT n.level IN [0, 2]",
                    "n.level IN []",
                    "NOT n.level IN []",
                    "n.level IS NULL",
                    "n.level IS NOT NULL",
                    "NOT n.level = 1",
                    "NOT n.level > 1",
                    "NOT n.level IS NULL",
                    "n.name = 'a'",
                    "n.name >= 'b'",
                    "n.public = true",
                    "n.score >= 2.0",
                    "n.due < date('2026-06-01')",
                    "n.at >= datetime('2026-01-01T00:00:00Z')",
                    "n.place = point({x: 1, y: 2})",
                    "n.span = duration('P1D')",
                    "n.level = \$level",
                    "n.level IN \$levels",
                )
                .map { Case("(n:Doc) WHERE $it", "n:Doc AND $it") } +
                listOf(
                    Case("(n:Doc {level: 1})", "n:Doc AND n.level = 1"),
                    Case("(n) WHERE n.level = 1", "n.level = 1"),
                    Case("(n:Doc|Other) WHERE n.level >= 1", "(n:Doc OR n:Other) AND n.level >= 1"),
                )
        val soft = SoftAssertions()
        for (case in cases) {
            role(
                "GRANT READ {id} ON GRAPH * TO r",
                "GRANT TRAVERSE ON GRAPH * FOR ${case.pattern} TO r",
                parameters = parameters,
            )
            val granted = where(case.applies, parameters)
            soft
                .assertThat(alice.ids("MATCH (n) RETURN n.id AS id ORDER BY id"))
                .describedAs("GRANT ${case.pattern}")
                .isEqualTo(granted)
            soft
                .assertThat(alice.ids("MATCH (n:Doc) RETURN count(n)").single())
                .describedAs("count of GRANT ${case.pattern}")
                .isEqualTo(granted.count { it != 7L }.toLong())

            role(
                "GRANT READ {id} ON GRAPH * TO r",
                "GRANT TRAVERSE ON GRAPH * NODES * TO r",
                "DENY TRAVERSE ON GRAPH * FOR ${case.pattern} TO r",
                parameters = parameters,
            )
            val left = where("NOT coalesce(${case.applies}, false)", parameters)
            soft
                .assertThat(alice.ids("MATCH (n) RETURN n.id AS id ORDER BY id"))
                .describedAs("DENY ${case.pattern}")
                .isEqualTo(left)
            soft
                .assertThat(alice.ids("MATCH (n:Doc) RETURN count(n)").single())
                .describedAs("count of DENY ${case.pattern}")
                .isEqualTo(left.count { it != 7L }.toLong())
        }
        soft.assertAll()
    }

    @Test
    fun `reads the properties that rules allow, through indexes too`() {
        server.rows("CREATE INDEX doc_secret FOR (n:Doc) ON (n.secret)")
        server.rows("CALL db.awaitIndexes()")
        fun secrets() = alice.ids("MATCH (n:Doc) RETURN n.secret AS s ORDER BY n.id")

        role(
            "GRANT TRAVERSE ON GRAPH * TO r",
            "GRANT READ {id} ON GRAPH * TO r",
            "GRANT READ {secret} ON GRAPH * FOR (n:Doc) WHERE n.public = true TO r",
        )

        assertThat(secrets()).containsExactly("s1", null, null, null, null, null)

        role(
            "GRANT TRAVERSE ON GRAPH * TO r",
            "GRANT READ {*} ON GRAPH * TO r",
            "DENY READ {secret} ON GRAPH * FOR (n) WHERE n.level >= 2 TO r",
        )

        assertThat(secrets()).containsExactly("s1", "s2", null, "s4", "s5", null)
        assertThat(alice.ids("MATCH (n:Doc) RETURN properties(n).secret ORDER BY n.id"))
            .containsExactly("s1", "s2", null, "s4", "s5", null)
        assertThat(alice.ids("MATCH (n:Doc) RETURN n {.*}.secret ORDER BY n.id"))
            .containsExactly("s1", "s2", null, "s4", "s5", null)
        assertThat(alice.ids("MATCH (n:Doc) WHERE n.secret = 's3' RETURN n.id")).isEmpty()
        assertThat(
                alice.ids("MATCH (n:Doc) WHERE n.secret STARTS WITH 's' RETURN n.id ORDER BY n.id")
            )
            .containsExactly(1L, 2L, 4L, 5L)

        role("GRANT MATCH {*} ON GRAPH * FOR (n:Doc) WHERE n.level = 1 TO r")

        assertThat(alice.ids("MATCH (n) RETURN n.secret")).containsExactly("s2")
    }

    @Test
    fun `rules about names that do not exist yet apply once they do`() {
        role(
            "GRANT READ {id} ON GRAPH * TO r",
            "GRANT TRAVERSE ON GRAPH * NODES * TO r",
            "DENY TRAVERSE ON GRAPH * FOR (n:Doc) WHERE n.clearance IS NOT NULL TO r",
            "GRANT TRAVERSE ON GRAPH * FOR (n:Later) WHERE n.level = 1 TO r",
        )

        assertThat(alice.ids("MATCH (n) RETURN n.id ORDER BY n.id"))
            .containsExactly(1L, 2L, 3L, 4L, 5L, 6L, 7L)

        server.rows("MATCH (n {id: 2}) SET n.clearance = 1")
        server.rows("CREATE (:Later {id: 8, level: 1})")

        assertThat(alice.ids("MATCH (n) RETURN n.id ORDER BY n.id"))
            .containsExactly(1L, 3L, 4L, 5L, 6L, 7L, 8L)
    }

    @Test
    fun `shows rules as commands that give them back, with values evaluated when granted`() {
        val before = LocalDate.now(ZoneOffset.UTC)
        role(
            "GRANT TRAVERSE ON GRAPH * FOR (n:Doc) WHERE n.due <= date() TO r",
            "GRANT TRAVERSE ON GRAPH * FOR (n:Doc) WHERE n.at > datetime('2026-01-01T00:00:00Z') TO r",
            "GRANT TRAVERSE ON GRAPH * FOR (n) WHERE n.place = point({x: 1, y: 2}) TO r",
            "GRANT TRAVERSE ON GRAPH * FOR (n) WHERE n.span <> duration('P1D') TO r",
            "GRANT READ {secret} ON GRAPH * FOR (n:Doc|Other) WHERE n.name IN ['a', 'it\\'s'] TO r",
            "DENY MATCH {*} ON GRAPH neo4j FOR (n:Doc) WHERE NOT n.score > 1.5 TO r",
            "DENY READ {*} ON GRAPH * FOR (n) WHERE n.public IS NULL TO r",
            "GRANT TRAVERSE ON GRAPH * FOR (n:Doc {level: 1}) TO r",
        )
        val after = LocalDate.now(ZoneOffset.UTC)
        val commands =
            server
                .rows("SHOW ROLE r PRIVILEGES AS COMMANDS", StandaloneServer.SYSTEM)
                .map { it["command"] as String }
                .filter { "FOR" in it }

        assertThat(commands)
            .contains(
                "GRANT TRAVERSE ON GRAPH * FOR (n:Doc) WHERE n.level = 1 TO `r`",
                "DENY MATCH {*} ON GRAPH neo4j FOR (n:Doc) WHERE n.score <= 1.5 TO `r`",
                "GRANT READ {secret} ON GRAPH * FOR (n:Doc|Other) WHERE n.name IN ['a', 'it\\'s'] TO `r`",
            )
            .anyMatch {
                it ==
                    "GRANT TRAVERSE ON GRAPH * FOR (n:Doc) WHERE n.due <= date('$before') TO `r`" ||
                    it ==
                        "GRANT TRAVERSE ON GRAPH * FOR (n:Doc) WHERE n.due <= date('$after') TO `r`"
            }
            .hasSize(8)
        fun rows(role: String) =
            server.rows(
                "SHOW ROLE $role PRIVILEGES YIELD access, action, resource, graph, segment " +
                    "RETURN access, action, resource, graph, segment ORDER BY segment, action",
                StandaloneServer.SYSTEM,
            )
        val shown = rows("r")
        assertThat(shown.map { it["segment"] })
            .contains("FOR((n:Doc) WHERE n.level = 1)", "FOR((n) WHERE n.public IS NULL)")

        server.administer("CREATE ROLE copy")
        commands.forEach { server.administer(it.replace("TO `r`", "TO copy")) }

        assertThat(rows("copy")).isEqualTo(shown)

        server.administer("REVOKE TRAVERSE ON GRAPH * FOR (n:Doc {level: 1}) FROM r")
        server.administer("REVOKE DENY READ {*} ON GRAPH * FOR (n) WHERE n.public IS NULL FROM r")

        assertThat(rows("r")).hasSize(shown.size - 2)
    }

    @Test
    fun `refuses rule values that no property can be compared with`() {
        server.administer("CREATE ROLE r")
        /** The message and GQL statuses, outermost first, of the refusal of [predicate]. */
        fun refusal(predicate: String, value: Any?): Pair<String?, List<String>> {
            // Neo4j takes null parameters, which Kotlin's types do not show.
            @Suppress("UNCHECKED_CAST") val parameters = mapOf("v" to value) as Map<String, Any>
            val error = catchThrowable {
                server.administer(
                    "GRANT TRAVERSE ON GRAPH * FOR (n) WHERE $predicate TO r",
                    parameters,
                )
            }
            val refusal = generateSequence(error) { e -> e.cause }.last()
            val statuses =
                generateSequence((refusal as ErrorGqlStatusObject)) { it.cause().orElse(null) }
                    .map { it.gqlStatus() }
                    .toList()
            return refusal.message to statuses
        }

        assertThat(refusal("n.p = \$v", null))
            .isEqualTo(
                "Failed to administer property rule. The property value access rule pattern " +
                    "`p = NULL` always evaluates to `NULL`. Use `IS NULL` instead." to
                    listOf("22NA0", "22NA5")
            )
        assertThat(refusal("n.p <> \$v", null))
            .isEqualTo(
                "Failed to administer property rule. The property value access rule pattern " +
                    "`p <> NULL` always evaluates to `NULL`. Use `IS NOT NULL` instead." to
                    listOf("22NA0", "22NA6")
            )
        assertThat(refusal("n.p >= \$v", null))
            .isEqualTo(
                "Failed to administer property rule. The property value access rule pattern " +
                    "`p >= NULL` always evaluates to `NULL`." to listOf("22NA0", "22NA4")
            )
        assertThat(refusal("n.p > \$v", Double.NaN))
            .isEqualTo(
                "Failed to administer property rule. `NaN` is not supported for property-based " +
                    "access control." to listOf("22NA0", "22NA3")
            )
        assertThat(refusal("n.p IN \$v", listOf(1L, "a")))
            .isEqualTo(
                "Failed to administer property rule. All elements in a list must be literals of " +
                    "the same type for property-based access control." to listOf("22NA0")
            )
        assertThat(refusal("n.p IN \$v", 1L))
            .isEqualTo(
                "Failed to administer property rule. The value `Long(1)` is not supported for " +
                    "property-based access control: IN needs a list." to listOf("22NA0")
            )
    }
}
