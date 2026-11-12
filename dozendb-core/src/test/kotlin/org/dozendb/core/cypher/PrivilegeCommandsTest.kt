package org.dozendb.core.cypher

import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowableOfType
import org.dozendb.core.StandaloneServer
import org.dozendb.core.StandaloneServer.Companion.SYSTEM
import org.dozendb.core.security.Access
import org.dozendb.core.security.Privilege
import org.dozendb.core.security.PrivilegeRecords
import org.dozendb.core.security.PrivilegeScope
import org.dozendb.core.security.SecurityGraph
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
import org.neo4j.internal.kernel.api.security.PrivilegeAction
import org.neo4j.logging.NullLog

/**
 * GRANT, DENY and REVOKE of DBMS, database and LOAD privileges, over Bolt as drivers see them, by
 * the admin `neo4j` unless said otherwise.
 */
class PrivilegeCommandsTest {
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
        server.administer("ALTER USER neo4j SET PASSWORD 'neo4j-secret-1' CHANGE NOT REQUIRED")
        server.administer("CREATE USER alice SET PASSWORD 'alice-secret-1' CHANGE NOT REQUIRED")
        server.administer("CREATE DATABASE films")
        server.administer("CREATE ROLE manager")
        server.administer("GRANT ROLE manager TO alice")
    }

    @AfterEach
    fun stop() {
        drivers.forEach(Driver::close)
        server.close()
    }

    private fun driver(user: String, password: String): Driver =
        GraphDatabase.driver("bolt://${server.boltAddress()}", AuthTokens.basic(user, password))
            .also { drivers += it }

    private val admin by lazy { driver("neo4j", "neo4j-secret-1") }
    private val alice by lazy { driver("alice", "alice-secret-1") }

    private fun Driver.run(
        cypher: String,
        parameters: Map<String, Any> = emptyMap(),
        database: String = SYSTEM,
    ): List<Map<String, Any?>> =
        session(SessionConfig.forDatabase(database)).use { session ->
            session.run(cypher, parameters).list { it.asMap() }
        }

    /**
     * The notifications of [cypher], as their code and their title with their description: the
     * driver's legacy notifications, which Neo4j still sends next to the GQL statuses.
     */
    @Suppress("DEPRECATION")
    private fun Driver.notifications(
        cypher: String,
        parameters: Map<String, Any> = emptyMap(),
    ): List<Pair<String, String>> =
        session(SessionConfig.forDatabase(SYSTEM)).use { session ->
            session.run(cypher, parameters).consume().notifications().map {
                it.code() to "${it.title()} ${it.description()}"
            }
        }

    private fun Driver.refused(
        cypher: String,
        parameters: Map<String, Any> = emptyMap(),
        database: String = SYSTEM,
    ): ClientException =
        checkNotNull(
            catchThrowableOfType(ClientException::class.java) { run(cypher, parameters, database) }
        ) {
            "$cypher succeeded"
        }

    private fun security(): SecurityGraph =
        server.database(SYSTEM).beginTx().use { SecurityGraph.read(it, NullLog.getInstance()) }

    private fun privileges(role: String = "manager"): List<Privilege> =
        security().roles.getValue(role).privileges

    private fun databaseId(name: String) = checkNotNull(security().databaseId(name))

    @Test
    fun `grants, denies and revokes DBMS privileges, which apply from the next transaction`() {
        assertThat(alice.refused("CREATE ROLE curator"))
            .hasMessageStartingWith("Permission has not been granted for CREATE ROLE.")

        admin.run("GRANT CREATE ROLE ON DBMS TO manager")

        assertThat(privileges())
            .containsExactly(
                Privilege(Access.GRANTED, PrivilegeAction.CREATE_ROLE, PrivilegeScope.Dbms)
            )
        alice.run("CREATE ROLE curator")

        admin.run("DENY CREATE ROLE ON DBMS TO \$role", mapOf("role" to "manager"))

        assertThat(alice.refused("CREATE ROLE keeper"))
            .hasMessageStartingWith("Permission denied for CREATE ROLE.")

        admin.run("REVOKE DENY CREATE ROLE ON DBMS FROM manager")
        alice.run("CREATE ROLE keeper")
        admin.run("REVOKE CREATE ROLE ON DBMS FROM manager")

        assertThat(privileges()).isEmpty()
        assertThat(alice.refused("CREATE ROLE archivist"))
            .hasMessageStartingWith("Permission has not been granted for CREATE ROLE.")
    }

    @Test
    fun `keeps the qualifiers of DBMS privileges`() {
        admin.run("GRANT EXECUTE PROCEDURE db.* ON DBMS TO manager")
        admin.run("GRANT EXECUTE BOOSTED FUNCTION apoc.coll.* ON DBMS TO manager")
        admin.run("GRANT SHOW SETTING dbms.security.* ON DBMS TO manager")
        admin.run("DENY IMPERSONATE (\$user, carol) ON DBMS TO manager", mapOf("user" to "bob"))
        admin.run("GRANT EXECUTE ADMIN PROCEDURES ON DBMS TO manager")

        assertThat(privileges().map { "${it.access} ${it.action.name} ${it.segment}" })
            .containsExactlyInAnyOrder(
                "GRANTED EXECUTE PROCEDURE(db.*)",
                "GRANTED EXECUTE_BOOSTED FUNCTION(apoc.coll.*)",
                "GRANTED SHOW_SETTING SETTING(dbms.security.*)",
                "DENIED IMPERSONATE USER(bob)",
                "DENIED IMPERSONATE USER(carol)",
                "GRANTED EXECUTE_ADMIN database",
            )
    }

    @Test
    fun `grants database privileges on a database, all of them or the home database`() {
        assertThat(alice.refused("RETURN 1", database = "films"))
            .hasMessage(
                "Database access is not allowed for user 'alice' with roles [PUBLIC, manager]."
            )

        admin.run("GRANT ACCESS ON DATABASE \$database TO manager", mapOf("database" to "Films"))

        assertThat(alice.run("RETURN 1 AS one", database = "films"))
            .containsExactly(mapOf("one" to 1L))

        admin.run("DENY ACCESS ON DATABASE * TO manager")
        admin.run("GRANT TRANSACTION MANAGEMENT (bob) ON HOME DATABASE TO manager")
        admin.run("GRANT SHOW INDEX ON DATABASES films, neo4j TO manager")

        assertThat(alice.refused("RETURN 1", database = "films"))
            .hasMessage(
                "Database access is not allowed for user 'alice' with roles [PUBLIC, manager]."
            )
        assertThat(privileges().map { "${it.access} ${it.action.name} ${it.scope} ${it.segment}" })
            .containsExactlyInAnyOrder(
                "GRANTED ACCESS database ${databaseId("films")} database",
                "DENIED ACCESS * database",
                "GRANTED TRANSACTION_MANAGEMENT HOME USER(bob)",
                "GRANTED SHOW_INDEX database ${databaseId("films")} database",
                "GRANTED SHOW_INDEX database ${databaseId("neo4j")} database",
            )
    }

    @Test
    fun `refuses to grant on a database or to a role that does not exist, revoking is a warning`() {
        assertThat(admin.refused("GRANT ACCESS ON DATABASE nowhere TO manager"))
            .hasMessage(
                "Failed to grant privilege to role 'manager': Database `nowhere` does not exist."
            )
            .extracting { it.gqlStatus() }
            .isEqualTo("42001")
        assertThat(admin.refused("DENY CREATE ROLE ON DBMS TO missing"))
            .hasMessage("Failed to deny privilege to role 'missing': Role does not exist.")
            .extracting { it.gqlStatus() }
            .isEqualTo("42001")
        assertThat(
                admin.refused(
                    "GRANT ACCESS ON DATABASE \$database TO manager",
                    mapOf("database" to "*"),
                )
            )
            .hasMessage(
                "Failed to grant privilege to role 'manager': Parameterized database and graph " +
                    "names do not support wildcards."
            )

        assertThat(admin.notifications("REVOKE GRANT ACCESS ON DATABASE nowhere FROM manager"))
            .containsExactly(
                IMPOSSIBLE_REVOKE to
                    impossible(
                        "REVOKE GRANT ACCESS ON DATABASE nowhere FROM manager",
                        "Database `nowhere` does not exist.",
                    )
            )
        assertThat(admin.notifications("REVOKE GRANT CREATE ROLE ON DBMS FROM missing"))
            .containsExactly(
                IMPOSSIBLE_REVOKE to
                    impossible(
                        "REVOKE GRANT CREATE ROLE ON DBMS FROM missing",
                        "Role does not exist.",
                    )
            )
        assertThat(
                admin.notifications(
                    "REVOKE GRANT ACCESS ON DATABASE \$database FROM manager",
                    mapOf("database" to "*"),
                )
            )
            .containsExactly(
                IMPOSSIBLE_REVOKE to
                    impossible(
                        "REVOKE GRANT ACCESS ON DATABASE \$database FROM manager",
                        "Parameterized database and graph names do not support wildcards.",
                    )
            )
        assertThat(privileges()).isEmpty()
    }

    @Test
    fun `notifies what has no effect`() {
        admin.run("GRANT CREATE ROLE ON DBMS TO manager")

        assertThat(admin.notifications("GRANT CREATE ROLE ON DBMS TO manager"))
            .containsExactly(
                HAS_NO_EFFECT to
                    noEffect(
                        "GRANT CREATE ROLE ON DBMS TO manager",
                        "The role already has the privilege.",
                    )
            )
        assertThat(admin.notifications("REVOKE CREATE ROLE ON DBMS FROM manager"))
            .containsExactly(
                HAS_NO_EFFECT to
                    noEffect(
                        "REVOKE DENY CREATE ROLE ON DBMS FROM manager",
                        "The role does not have the privilege.",
                    )
            )
        assertThat(admin.notifications("REVOKE GRANT CREATE ROLE ON DBMS FROM manager"))
            .containsExactly(
                HAS_NO_EFFECT to
                    noEffect(
                        "REVOKE GRANT CREATE ROLE ON DBMS FROM manager",
                        "The role does not have the privilege.",
                    )
            )
    }

    private fun noEffect(command: String, why: String) =
        "`$command` has no effect. $why See Status Codes documentation for more information."

    private fun impossible(command: String, why: String) =
        noEffect(
            command,
            "$why Make sure nothing is misspelled. This notification will become an error in a " +
                "future major version.",
        )

    @Test
    fun `keeps LOAD privileges for all data or CIDR ranges`() {
        admin.run("GRANT LOAD ON CIDR \$range TO manager", mapOf("range" to "10.0.0.0/8"))
        admin.run("DENY LOAD ON CIDR '2001:db8::/32' TO manager")
        admin.run("DENY LOAD ON ALL DATA TO manager")

        assertThat(privileges().map { "${it.access} ${it.action.name} ${it.segment}" })
            .containsExactlyInAnyOrder(
                "GRANTED LOAD_CIDR CIDR('10.0.0.0/8')",
                "DENIED LOAD_CIDR CIDR('2001:db8::/32')",
                "DENIED LOAD ALL DATA",
            )
        assertThat(admin.refused("GRANT LOAD ON CIDR '10.0.0.1' TO manager"))
            .hasMessage("'10.0.0.1' is not a CIDR range, such as '10.0.0.0/8' or '2001:db8::/32'.")
    }

    @Test
    fun `keeps immutable privileges as they are while authentication is enabled`() {
        assertThat(admin.refused("GRANT IMMUTABLE CREATE ROLE ON DBMS TO manager"))
            .hasMessage(
                "Immutable privileges cannot be granted. " +
                    "Try granting the privilege without the IMMUTABLE keyword."
            )
        // Embedded transactions run with authentication disabled.
        server.administer("GRANT IMMUTABLE CREATE ROLE ON DBMS TO manager")
        admin.run("GRANT CREATE ROLE ON DBMS TO manager")

        assertThat(privileges().map { it.immutable }).containsExactlyInAnyOrder(true, false)
        val immutable =
            "Immutable privileges cannot be revoked. Use " +
                "`SHOW ROLE manager PRIVILEGES AS COMMANDS YIELD *` to inspect manager's privileges."
        assertThat(admin.refused("REVOKE CREATE ROLE ON DBMS FROM manager")).hasMessage(immutable)
        assertThat(admin.refused("REVOKE IMMUTABLE CREATE ROLE ON DBMS FROM manager"))
            .hasMessage(immutable)
        alice.run("CREATE ROLE curator")

        server.administer("REVOKE IMMUTABLE CREATE ROLE ON DBMS FROM manager")

        assertThat(privileges().map { it.immutable }).containsExactly(false)
        server.administer("GRANT IMMUTABLE CREATE ROLE ON DBMS TO manager")
        server.administer("REVOKE CREATE ROLE ON DBMS FROM manager")
        assertThat(privileges()).isEmpty()
    }

    @Test
    fun `gives immutable roles only immutable privileges`() {
        server.administer("CREATE IMMUTABLE ROLE locked")

        assertThat(admin.refused("GRANT CREATE ROLE ON DBMS TO locked"))
            .hasMessage(
                "Only immutable privileges can be assigned to an immutable role. Try " +
                    "`GRANT/DENY IMMUTABLE` instead."
            )
        assertThat(
                catchThrowableOfType(Exception::class.java) {
                    server.administer("DENY ACCESS ON DATABASE * TO locked")
                }
            )
            .hasMessageContaining("Only immutable privileges can be assigned to an immutable role.")

        server.administer("DENY IMMUTABLE ACCESS ON DATABASE * TO locked")

        assertThat(privileges("locked"))
            .containsExactly(
                Privilege(
                    Access.DENIED,
                    PrivilegeAction.ACCESS,
                    PrivilegeScope.AllDatabases,
                    immutable = true,
                )
            )
    }

    @Test
    fun `needs the privileges to assign and remove privileges`() {
        admin.run("GRANT CREATE ROLE ON DBMS TO reader")

        assertThat(alice.refused("GRANT CREATE ROLE ON DBMS TO manager"))
            .hasMessageStartingWith("Permission has not been granted for ASSIGN PRIVILEGE.")
        assertThat(alice.refused("REVOKE CREATE ROLE ON DBMS FROM reader"))
            .hasMessageStartingWith("Permission has not been granted for REMOVE PRIVILEGE.")

        admin.run("GRANT ASSIGN PRIVILEGE ON DBMS TO manager")
        alice.run("GRANT CREATE ROLE ON DBMS TO manager")

        assertThat(privileges().map { it.action })
            .containsExactlyInAnyOrder(
                PrivilegeAction.ASSIGN_PRIVILEGE,
                PrivilegeAction.CREATE_ROLE,
            )
    }

    @Test
    fun `drops the privileges of a database with it`() {
        admin.run("GRANT ACCESS ON DATABASE films TO manager")
        admin.run("GRANT ACCESS ON DATABASE * TO manager")
        val films = databaseId("films")

        admin.run("DROP DATABASE films")

        assertThat(privileges())
            .containsExactly(
                Privilege(Access.GRANTED, PrivilegeAction.ACCESS, PrivilegeScope.AllDatabases)
            )
        admin.run("CREATE DATABASE films")
        admin.run("GRANT ACCESS ON DATABASE films TO manager")
        admin.run("CREATE OR REPLACE DATABASE films")
        assertThat(privileges().map { it.scope }).containsExactly(PrivilegeScope.AllDatabases)
        assertThat(databaseId("films")).isNotEqualTo(films)
        server.database(SYSTEM).beginTx().use { tx ->
            assertThat(
                    tx.findNodes(PrivilegeRecords.PRIVILEGE).stream().filter {
                        !it.hasRelationship()
                    }
                )
                .isEmpty()
        }
    }

    private companion object {
        const val HAS_NO_EFFECT = "Neo.ClientNotification.Security.CommandHasNoEffect"
        const val IMPOSSIBLE_REVOKE = "Neo.ClientNotification.Security.ImpossibleRevokeCommand"
    }
}
