package org.dozendb.core.security

import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.dozendb.core.DozenDbSettings
import org.dozendb.core.StandaloneServer
import org.dozendb.core.StandaloneServer.Companion.SYSTEM
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
 * Auth rules, as Neo4j Enterprise 2026.10 has them with the native provider: roles that users get
 * at the start of each transaction from the rules whose conditions hold for them.
 */
class AuthRulesTest {
    // Kept when a test fails, for the server's logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private lateinit var server: StandaloneServer
    private val drivers = mutableListOf<Driver>()

    @AfterEach
    fun stop() {
        drivers.forEach(Driver::close)
        if (::server.isInitialized) server.close()
    }

    private fun start(providers: List<String> = listOf("native")) {
        val settings: Map<Setting<*>, Any> =
            mapOf(
                GraphDatabaseSettings.auth_enabled to true,
                BoltConnector.enabled to true,
                DozenDbSettings.abacAuthorizationProviders to providers,
            )
        server = StandaloneServer(root, settings).also { it.start() }
        server.rows(
            "CREATE (:Doc {id: 1, classification: 'public'}), " +
                "(:Doc {id: 2, classification: 'finance'}), " +
                "(:Doc {id: 3, classification: 'hr'})"
        )
    }

    /** Runs [cypher] on `system` in Cypher 25, which has auth rules. */
    private fun administer(cypher: String, parameters: Map<String, Any> = emptyMap()) =
        server.administer("CYPHER 25 $cypher", parameters)

    private fun rows(cypher: String): List<Map<String, Any?>> =
        server.rows("CYPHER 25 $cypher", SYSTEM)

    private fun refusal(cypher: String): String? =
        generateSequence(catchThrowable { administer(cypher) }) { it.cause }.last().message

    private fun user(name: String, vararg tags: String) {
        administer(
            "CREATE USER $name SET PASSWORD '$name-secret-1' CHANGE NOT REQUIRED" +
                if (tags.isEmpty()) "" else " SET TAGS \$tags",
            mapOf("tags" to tags.toList()),
        )
    }

    private fun driver(user: String): Driver =
        GraphDatabase.driver(
                "bolt://${server.boltAddress()}",
                AuthTokens.basic(user, "$user-secret-1"),
            )
            .also { drivers += it }

    private fun Driver.run(cypher: String, database: String = "neo4j"): List<Map<String, Any?>> =
        session(SessionConfig.forDatabase(database)).use { session ->
            session.run(cypher).list { it.asMap() }
        }

    /** The roles [this] has in a transaction, as `dbms.showCurrentUser()` shows them. */
    private fun Driver.roles(): Any? =
        run("CALL dbms.showCurrentUser() YIELD roles RETURN roles", SYSTEM).single()["roles"]

    private fun Driver.docs(): List<Any?> =
        run("MATCH (d:Doc) RETURN d.id AS id ORDER BY id").map { it["id"] }

    @Test
    fun `creates, shows, alters, renames and drops auth rules`() {
        start()
        administer("CREATE AUTH RULE finance SET CONDITION 'finance' IN abac.native.user_tags()")
        administer(
            "CREATE AUTH RULE `night shift` SET CONDITION time.transaction('UTC').hour < 6 OR " +
                "time.transaction('UTC').hour >= 22 SET ENABLED false"
        )
        administer("CREATE ROLE analyst")
        administer("GRANT ROLE analyst TO AUTH RULE finance")
        val finance =
            mapOf(
                "name" to "finance",
                "condition" to "'finance' IN abac.native.user_tags()",
                "enabled" to true,
                "roles" to listOf("analyst"),
            )
        val night =
            mapOf(
                "name" to "night shift",
                "condition" to
                    "time.transaction('UTC').hour < 6 OR time.transaction('UTC').hour >= 22",
                "enabled" to false,
                "roles" to emptyList<String>(),
            )
        assertThat(rows("SHOW AUTH RULES")).containsExactly(finance, night)
        assertThat(rows("SHOW AUTH RULES AS COMMANDS YIELD *"))
            .containsExactly(
                mapOf(
                    "command" to
                        "CREATE AUTH RULE finance SET CONDITION " +
                            "'finance' IN abac.native.user_tags() SET ENABLED true",
                    "roles" to listOf("analyst"),
                ),
                mapOf(
                    "command" to
                        "CREATE AUTH RULE `night shift` SET CONDITION " +
                            "time.transaction('UTC').hour < 6 OR " +
                            "time.transaction('UTC').hour >= 22 SET ENABLED false",
                    "roles" to emptyList<String>(),
                ),
            )

        assertThat(refusal("CREATE AUTH RULE finance SET CONDITION true"))
            .isEqualTo(
                "Failed to create the specified auth rule 'finance': Auth rule already exists."
            )
        administer("CREATE AUTH RULE finance IF NOT EXISTS SET CONDITION true")
        assertThat(rows("SHOW AUTH RULES")).containsExactly(finance, night)
        assertThat(refusal("CREATE AUTH RULE x SET CONDITION \$c")).contains("auth rule condition")

        administer("ALTER AUTH RULE finance SET ENABLED false")
        administer("ALTER AUTH RULE finance SET CONDITION 'audit' IN abac.native.user_tags()")
        administer("ALTER AUTH RULE missing IF EXISTS SET ENABLED false")
        assertThat(refusal("ALTER AUTH RULE missing SET ENABLED false"))
            .isEqualTo(
                "Failed to alter the specified auth rule 'missing': Auth rule does not exist."
            )
        val audit =
            finance + mapOf("condition" to "'audit' IN abac.native.user_tags()", "enabled" to false)
        assertThat(rows("SHOW AUTH RULES")).containsExactly(audit, night)

        administer("RENAME AUTH RULE finance TO money")
        administer("RENAME AUTH RULE missing IF EXISTS TO other")
        assertThat(refusal("RENAME AUTH RULE missing TO other"))
            .isEqualTo(
                "Failed to rename the specified auth rule 'missing' to 'other': " +
                    "The auth rule 'missing' does not exist."
            )
        assertThat(refusal("RENAME AUTH RULE money TO `night shift`"))
            .isEqualTo(
                "Failed to rename the specified auth rule 'money' to 'night shift': " +
                    "Auth rule 'night shift' already exists."
            )
        assertThat(rows("SHOW AUTH RULES")).containsExactly(audit + ("name" to "money"), night)

        // Replacing a rule leaves its roles behind, as Neo4j's docs say.
        administer("CREATE OR REPLACE AUTH RULE money SET CONDITION true")
        administer("DROP AUTH RULE `night shift`")
        administer("DROP AUTH RULE missing IF EXISTS")
        assertThat(refusal("DROP AUTH RULE missing"))
            .isEqualTo(
                "Failed to delete the specified auth rule 'missing': Auth rule does not exist."
            )
        assertThat(rows("SHOW AUTH RULES"))
            .containsExactly(
                mapOf(
                    "name" to "money",
                    "condition" to "true",
                    "enabled" to true,
                    "roles" to emptyList<String>(),
                )
            )
    }

    @Test
    fun `the commands of SHOW AUTH RULES and SHOW ROLES WITH AUTH RULES give them back`() {
        start()
        administer("CREATE ROLE analyst")
        administer("CREATE ROLE oncall_team")
        administer(
            "CREATE AUTH RULE finance SET CONDITION all(tag IN ['finance', 'audit'] WHERE " +
                "tag IN abac.native.user_tags())"
        )
        administer(
            "CREATE AUTH RULE `on call` SET CONDITION NOT ('off' IN abac.native.user_tags()) AND " +
                "size(abac.native.user_tags()) > 0 SET ENABLED false"
        )
        administer("GRANT ROLES analyst, oncall_team TO AUTH RULE finance")
        administer("GRANT ROLE oncall_team TO AUTH RULES `on call`")
        fun state() =
            mapOf(
                "rules" to rows("SHOW AUTH RULES"),
                "roles" to rows("SHOW ROLES WITH AUTH RULES YIELD *"),
            )
        val before = state()
        assertThat(rows("SHOW ROLES WITH AUTH RULES WHERE authRule IS NOT NULL"))
            .containsExactly(
                mapOf("role" to "analyst", "authRule" to "finance"),
                mapOf("role" to "oncall_team", "authRule" to "finance"),
                mapOf("role" to "oncall_team", "authRule" to "on call"),
            )
        assertThat(rows("SHOW ROLES WITH AUTH RULES WHERE role = 'reader'"))
            .containsExactly(mapOf("role" to "reader", "authRule" to null))
        val commands =
            rows("SHOW AUTH RULES AS COMMANDS").map { it["command"] as String } +
                rows("SHOW ROLES WITH AUTH RULES AS COMMANDS").map { it["command"] as String }
        assertThat(rows("SHOW ROLES WITH AUTH RULES AS COMMANDS").map { it["command"] })
            .containsExactly(
                "GRANT ROLE analyst TO AUTH RULE finance",
                "GRANT ROLE oncall_team TO AUTH RULE finance",
                "GRANT ROLE oncall_team TO AUTH RULE `on call`",
            )

        administer("DROP AUTH RULE finance")
        administer("DROP AUTH RULE `on call`")
        commands.forEach { administer(it) }
        assertThat(state()).isEqualTo(before)
    }

    @Test
    fun `gives the roles of rules that hold for a user from their next transaction`() {
        start()
        administer("CREATE ROLE analyst")
        administer("GRANT ACCESS ON DATABASE neo4j TO analyst")
        administer("GRANT MATCH {*} ON GRAPH neo4j NODES Doc TO analyst")
        administer("CREATE AUTH RULE finance SET CONDITION 'finance' IN abac.native.user_tags()")
        administer("GRANT ROLE analyst TO AUTH RULE finance")
        user("alice", "finance", "audit")
        user("bob")
        val alice = driver("alice")
        val bob = driver("bob")

        assertThat(alice.roles()).isEqualTo(listOf("analyst", "PUBLIC"))
        assertThat(alice.docs()).containsExactly(1L, 2L, 3L)
        assertThat(bob.roles()).isEqualTo(listOf("PUBLIC"))
        assertThat(bob.docs()).isEmpty()
        // SHOW USERS and SHOW CURRENT USER show the roles granted to users, as Neo4j's docs say.
        assertThat(rows("SHOW USERS YIELD user, roles WHERE user = 'alice'").single()["roles"])
            .isEqualTo(listOf("PUBLIC"))

        administer("ALTER USER alice REMOVE TAG 'finance'")
        assertThat(alice.roles()).isEqualTo(listOf("PUBLIC"))
        administer("ALTER USER bob ADD TAG 'finance'")
        assertThat(bob.docs()).containsExactly(1L, 2L, 3L)

        administer("ALTER AUTH RULE finance SET ENABLED false")
        assertThat(bob.roles()).isEqualTo(listOf("PUBLIC"))
        administer("ALTER AUTH RULE finance SET ENABLED true")
        administer("REVOKE ROLE analyst FROM AUTH RULE finance")
        assertThat(bob.roles()).isEqualTo(listOf("PUBLIC"))

        // A role granted both ways stays, and suspended users get none.
        administer("GRANT ROLE analyst TO AUTH RULE finance")
        administer("GRANT ROLE analyst TO bob")
        assertThat(bob.roles()).isEqualTo(listOf("analyst", "PUBLIC"))
        administer("ALTER USER bob SET STATUS SUSPENDED")
        assertThat(catchThrowable { bob.roles() }).isNotNull()
    }

    @Test
    fun `evaluates conditions with the functions Neo4j allows in them`() {
        start()
        user("alice", "finance", "Audit")
        val alice = driver("alice")
        val conditions =
            mapOf(
                "true" to true,
                "false" to false,
                "null" to false,
                "'x' IN abac.native.user_tags()" to false,
                "NOT 'x' IN abac.native.user_tags()" to true,
                "'audit' IN [tag IN abac.native.user_tags() | toLower(tag)]" to true,
                "any(tag IN abac.native.user_tags() WHERE tag STARTS WITH 'fin')" to true,
                "size(abac.native.user_tags()) = 2 AND head(abac.native.user_tags()) = 'finance'" to
                    true,
                "time.transaction('UTC').hour >= 0" to true,
                "date.transaction('UTC') > date('2020-01-01')" to true,
                "duration.between(date('2026-01-01'), date('2026-01-31')).days = 30" to true,
                "datetime.fromEpochMillis(0).year = 1970" to true,
                "reduce(n = 0, x IN range(1, 3) | n + x) = 6" to true,
                "abac.oidc.user_attribute('department') = 'finance'" to false,
                "abac.oidc.user_attribute('department') IS NULL" to false,
            )
        for ((index, condition) in conditions.keys.withIndex()) {
            administer("CREATE ROLE r$index")
            administer("CREATE AUTH RULE rule$index SET CONDITION $condition")
            administer("GRANT ROLE r$index TO AUTH RULE rule$index")
        }
        val roles = alice.roles() as List<*>
        for ((index, entry) in conditions.entries.withIndex()) {
            assertThat("r$index" in roles).describedAs(entry.key).isEqualTo(entry.value)
        }
    }

    @Test
    fun `gives no roles and leaves out tag rules without the native provider`() {
        start(providers = emptyList())
        administer("CREATE ROLE reader2")
        administer("GRANT ACCESS ON DATABASE neo4j TO reader2")
        administer("GRANT TRAVERSE ON GRAPH neo4j NODES Doc TO reader2")
        administer(
            "GRANT READ {id} ON GRAPH neo4j FOR (d:Doc) WHERE d.classification IN " +
                "abac.native.user_tags() TO reader2"
        )
        administer("CREATE AUTH RULE always SET CONDITION true")
        administer("CREATE ROLE analyst")
        administer("GRANT ROLE analyst TO AUTH RULE always")
        user("alice", "finance")
        administer("GRANT ROLE reader2 TO alice")
        val alice = driver("alice")

        assertThat(alice.roles()).isEqualTo(listOf("reader2", "PUBLIC"))
        assertThat(alice.docs()).containsExactly(null, null, null)
    }

    @Test
    fun `refuses denied privileges on roles that auth rules give`() {
        start()
        administer("CREATE ROLE analyst")
        administer("CREATE ROLE denied")
        administer("DENY READ {secret} ON GRAPH * NODES * TO denied")
        administer("CREATE AUTH RULE finance SET CONDITION true")

        assertThat(refusal("GRANT ROLE denied TO AUTH RULE finance"))
            .contains("GRANT ROLE denied TO AUTH RULE finance")
            .contains("is not allowed for roles with DENY privileges")
        administer("GRANT ROLE analyst TO AUTH RULE finance")
        assertThat(refusal("DENY TRAVERSE ON GRAPH * TO analyst"))
            .contains("DENY TRAVERSE ON GRAPH * NODES * TO analyst")
            .contains("is not allowed for roles that are granted to an AUTH RULE")
        assertThat(refusal("DENY SHOW USER ON DBMS TO analyst"))
            .contains("is not allowed for roles that are granted to an AUTH RULE")
        assertThat(refusal("DENY ACCESS ON DATABASE * TO analyst"))
            .contains("is not allowed for roles that are granted to an AUTH RULE")
        assertThat(rows("SHOW ROLE analyst PRIVILEGES")).isEmpty()

        administer("GRANT TRAVERSE ON GRAPH * TO analyst")
        administer("REVOKE ROLE analyst FROM AUTH RULE finance")
        administer("DENY TRAVERSE ON GRAPH * TO analyst")
        assertThat(rows("SHOW ROLE analyst PRIVILEGES YIELD access").map { it["access"] })
            .contains("DENIED")
    }

    @Test
    fun `refuses missing roles and rules, and PUBLIC, and tells when nothing changes`() {
        start()
        administer("CREATE ROLE analyst")
        administer("CREATE AUTH RULE finance SET CONDITION true")

        assertThat(refusal("GRANT ROLE analyst TO AUTH RULE missing"))
            .isEqualTo(
                "Failed to grant role 'analyst' to auth rule 'missing': Auth rule does not exist."
            )
        assertThat(refusal("GRANT ROLE missing TO AUTH RULE finance"))
            .isEqualTo(
                "Failed to grant role 'missing' to auth rule 'finance': Role does not exist."
            )
        assertThat(refusal("GRANT ROLE PUBLIC TO AUTH RULE finance")).contains("PUBLIC")
        assertThat(refusal("REVOKE ROLE analyst FROM AUTH RULE missing")).contains("missing")
        assertThat(refusal("REVOKE ROLE missing FROM AUTH RULE finance")).contains("missing")

        /** The GQL statuses of [cypher] other than success. */
        fun statuses(cypher: String): List<String> =
            server.database(SYSTEM).executeTransactionally("CYPHER 25 $cypher", emptyMap()) { result
                ->
                result.forEachRemaining {}
                result.gqlStatusObjects.map { it.gqlStatus() }.filterNot { it.startsWith("0000") }
            }
        assertThat(statuses("GRANT ROLE analyst TO AUTH RULE finance")).isEmpty()
        assertThat(statuses("GRANT ROLE analyst TO AUTH RULE finance")).containsExactly("00N70")
        assertThat(statuses("REVOKE ROLE analyst FROM AUTH RULE finance")).isEmpty()
        assertThat(statuses("REVOKE ROLE analyst FROM AUTH RULE finance")).containsExactly("00N71")
    }

    @Test
    fun `manages auth rules with the auth rule privileges`() {
        start()
        user("carol")
        administer("CREATE ROLE manager")
        administer("GRANT ROLE manager TO carol")
        administer("CREATE ROLE analyst")
        val carol = driver("carol")
        fun refusal(cypher: String) = catchThrowable {
            carol.run("CYPHER 25 $cypher", SYSTEM)
        }
            ?.message

        assertThat(refusal("CREATE AUTH RULE finance SET CONDITION true"))
            .startsWith("Permission has not been granted for CREATE AUTH RULE.")
        assertThat(refusal("SHOW AUTH RULES"))
            .startsWith("Permission has not been granted for SHOW AUTH RULE.")

        administer("GRANT CREATE AUTH RULE ON DBMS TO manager")
        administer("GRANT SHOW AUTH RULE ON DBMS TO manager")
        carol.run("CYPHER 25 CREATE AUTH RULE finance SET CONDITION true", SYSTEM)
        administer("GRANT ROLE analyst TO AUTH RULE finance")
        // The roles of rules show with SHOW ROLE only.
        assertThat(carol.run("CYPHER 25 SHOW AUTH RULES YIELD name, roles", SYSTEM))
            .containsExactly(mapOf("name" to "finance", "roles" to null))
        assertThat(refusal("ALTER AUTH RULE finance SET ENABLED false"))
            .startsWith("Permission has not been granted for ALTER AUTH RULE.")
        assertThat(refusal("GRANT ROLE analyst TO AUTH RULE finance"))
            .startsWith("Permission has not been granted for ASSIGN ROLE.")

        administer("GRANT AUTH RULE MANAGEMENT ON DBMS TO manager")
        administer("GRANT SHOW ROLE ON DBMS TO manager")
        carol.run("CYPHER 25 ALTER AUTH RULE finance SET ENABLED false", SYSTEM)
        carol.run("CYPHER 25 RENAME AUTH RULE finance TO money", SYSTEM)
        assertThat(carol.run("CYPHER 25 SHOW AUTH RULES YIELD name, roles", SYSTEM))
            .containsExactly(mapOf("name" to "money", "roles" to listOf("analyst")))
        carol.run("CYPHER 25 DROP AUTH RULE money", SYSTEM)

        administer("REVOKE AUTH RULE MANAGEMENT ON DBMS FROM manager")
        administer("REVOKE CREATE AUTH RULE ON DBMS FROM manager")
        administer("GRANT ALL DBMS PRIVILEGES ON DBMS TO manager")
        carol.run("CYPHER 25 CREATE AUTH RULE finance SET CONDITION true", SYSTEM)
        carol.run("CYPHER 25 GRANT ROLE analyst TO AUTH RULE finance", SYSTEM)

        assertThat(rows("SHOW ROLE manager PRIVILEGES AS COMMANDS").map { it["command"] })
            .containsExactlyInAnyOrder(
                "GRANT SHOW AUTH RULE ON DBMS TO `manager`",
                "GRANT SHOW ROLE ON DBMS TO `manager`",
                "GRANT ALL DBMS PRIVILEGES ON DBMS TO `manager`",
            )
    }

    @Test
    fun `finds the nodes and relationships whose property is one of the user's tags`() {
        start()
        server.rows(
            "MATCH (a:Doc {id: 1}), (b:Doc {id: 2}), (c:Doc {id: 3}) " +
                "CREATE (a)-[:LINK {id: 10, classification: 'finance'}]->(b), " +
                "(b)-[:LINK {id: 11, classification: 'hr'}]->(c)"
        )
        administer("CREATE ROLE tagged")
        administer("GRANT ACCESS ON DATABASE neo4j TO tagged")
        administer(
            "GRANT MATCH {*} ON GRAPH neo4j FOR (d:Doc) WHERE d.classification IN " +
                "abac.native.user_tags() TO tagged"
        )
        administer(
            "GRANT MATCH {*} ON GRAPH neo4j FOR ()-[l:LINK]-() WHERE l.classification IN " +
                "abac.native.user_tags() TO tagged"
        )
        user("alice", "finance", "hr")
        user("bob", "public")
        user("carol")
        for (name in listOf("alice", "bob", "carol")) administer("GRANT ROLE tagged TO $name")
        val alice = driver("alice")
        val bob = driver("bob")
        val carol = driver("carol")
        fun Driver.links() =
            run("MATCH ()-[l:LINK]->() RETURN l.id AS id ORDER BY id").map { it["id"] }

        assertThat(alice.docs()).containsExactly(2L, 3L)
        assertThat(alice.links()).containsExactly(11L)
        assertThat(bob.docs()).containsExactly(1L)
        assertThat(bob.links()).isEmpty()
        assertThat(carol.docs()).isEmpty()
        administer("ALTER USER bob ADD TAGS ['finance', 'hr']")
        assertThat(bob.docs()).containsExactly(1L, 2L, 3L)
        assertThat(bob.links()).containsExactly(10L, 11L)

        val commands =
            rows("SHOW ROLE tagged PRIVILEGES AS COMMANDS").map { it["command"] as String }
        assertThat(commands)
            .contains(
                "GRANT MATCH {*} ON GRAPH neo4j FOR (n:Doc) WHERE n.classification IN " +
                    "abac.native.user_tags() TO `tagged`",
                "GRANT MATCH {*} ON GRAPH neo4j FOR ()-[r:LINK]-() WHERE r.classification IN " +
                    "abac.native.user_tags() TO `tagged`",
            )
        val before = rows("SHOW ROLE tagged PRIVILEGES")
        administer("DROP ROLE tagged")
        administer("CREATE ROLE tagged")
        commands.forEach { administer(it) }
        assertThat(rows("SHOW ROLE tagged PRIVILEGES")).isEqualTo(before)
        // Neo4j refuses the forms that could deny by tags.
        assertThat(
                refusal(
                    "DENY READ {*} ON GRAPH * FOR (d:Doc) WHERE d.classification IN " +
                        "abac.native.user_tags() TO tagged"
                )
            )
            .contains("DENY")
        assertThat(
                refusal(
                    "GRANT READ {*} ON GRAPH * FOR (d:Doc) WHERE NOT d.classification IN " +
                        "abac.native.user_tags() TO tagged"
                )
            )
            .contains("NOT")
    }

    @Test
    fun `shows the privileges that auth rules give`() {
        start()
        administer("CREATE ROLE analyst")
        administer("CREATE ROLE auditor")
        administer("GRANT ACCESS ON DATABASE neo4j TO analyst")
        administer("GRANT TRAVERSE ON GRAPH neo4j NODES Doc TO auditor")
        administer("CREATE AUTH RULE finance SET CONDITION true")
        administer("CREATE AUTH RULE audit SET CONDITION false")
        administer("GRANT ROLES analyst, auditor TO AUTH RULE finance")
        administer("GRANT ROLE auditor TO AUTH RULE audit")

        assertThat(rows("SHOW AUTH RULE finance PRIVILEGES YIELD action, role, authRule"))
            .containsExactly(
                mapOf("action" to "access", "role" to "analyst", "authRule" to "finance"),
                mapOf("action" to "traverse", "role" to "auditor", "authRule" to "finance"),
            )
        assertThat(rows("SHOW AUTH RULES finance, audit PRIVILEGES AS COMMANDS"))
            .containsExactlyInAnyOrder(
                mapOf(
                    "command" to "GRANT ACCESS ON DATABASE neo4j TO \$role",
                    "authRules" to listOf("finance"),
                ),
                mapOf(
                    "command" to "GRANT TRAVERSE ON GRAPH neo4j NODE Doc TO \$role",
                    "authRules" to listOf("audit", "finance"),
                ),
            )
    }
}
