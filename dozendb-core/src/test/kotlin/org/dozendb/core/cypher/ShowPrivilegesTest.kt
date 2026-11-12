package org.dozendb.core.cypher

import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowableOfType
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
import org.neo4j.driver.exceptions.ClientException

/**
 * SHOW PRIVILEGES, SHOW SUPPORTED PRIVILEGES and the roles columns, compared with the examples of
 * Neo4j's docs, over Bolt as the admin `neo4j` unless said otherwise.
 */
class ShowPrivilegesTest {
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
        server.administer("CREATE USER jake SET PASSWORD 'jake-secret-1' CHANGE NOT REQUIRED")
        server.administer("CREATE USER joe SET PASSWORD 'joe-secret-1' CHANGE NOT REQUIRED")
        server.administer("CREATE ROLE regularUsers")
        server.administer("CREATE ROLE noAccessUsers")
        server.administer("GRANT ACCESS ON DATABASE neo4j TO regularUsers")
        server.administer("DENY ACCESS ON DATABASE neo4j TO noAccessUsers")
        server.administer("GRANT ROLE regularUsers TO jake")
        server.administer("GRANT ROLE noAccessUsers TO joe")
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
    private val jake by lazy { driver("jake", "jake-secret-1") }

    private fun Driver.run(
        cypher: String,
        parameters: Map<String, Any> = emptyMap(),
    ): List<Map<String, Any?>> =
        session(SessionConfig.forDatabase(SYSTEM)).use { session ->
            session.run(cypher, parameters).list { it.asMap() }
        }

    private fun Driver.column(cypher: String, parameters: Map<String, Any> = emptyMap()) =
        run(cypher, parameters).map { it.values.single() }

    /** A row of Cypher 25's SHOW USERS PRIVILEGES AS COMMANDS YIELD * of an access privilege. */
    private fun command(command: String, access: String, role: String, vararg users: String) =
        mapOf(
            "command" to command,
            "access" to access,
            "action" to "access",
            "roles" to listOf(role),
            "immutable" to false,
            "users" to users.toList(),
        )

    @Test
    fun `shows the privileges of roles as the docs show them`() {
        assertThat(admin.run("SHOW ROLES regularUsers, noAccessUsers PRIVILEGES"))
            .containsExactly(
                row("DENIED", "access", "database", "neo4j", "database", "noAccessUsers"),
                row("GRANTED", "access", "database", "neo4j", "database", "regularUsers"),
            )
        assertThat(
                admin.column(
                    "SHOW PRIVILEGES YIELD role, access, action, segment ORDER BY action " +
                        "WHERE role = 'admin' RETURN action + ' ' + segment"
                )
            )
            .containsExactly(
                "access database",
                "constraint database",
                "dbms_actions database",
                "index database",
                "load ALL DATA",
                "match NODE(*)",
                "match RELATIONSHIP(*)",
                "show_constraint database",
                "show_index database",
                "start_database database",
                "stop_database database",
                "token database",
                "transaction_management USER(*)",
                "write NODE(*)",
                "write RELATIONSHIP(*)",
            )
        assertThat(
                admin.run("SHOW PRIVILEGES WHERE graph <> '*'").map { it["graph"] to it["role"] }
            )
            .containsExactly(
                "HOME" to "PUBLIC",
                "neo4j" to "noAccessUsers",
                "neo4j" to "regularUsers",
            )
        assertThat(admin.run("SHOW ROLE \$role PRIVILEGES", mapOf("role" to "regularUsers")))
            .hasSize(1)
    }

    private fun row(
        access: String,
        action: String,
        resource: String,
        graph: String,
        segment: String,
        role: String,
    ) =
        mapOf(
            "access" to access,
            "action" to action,
            "resource" to resource,
            "graph" to graph,
            "segment" to segment,
            "role" to role,
            "immutable" to false,
        )

    @Test
    fun `shows privileges as commands as the docs show them`() {
        assertThat(admin.column("SHOW ROLE PUBLIC PRIVILEGES AS COMMANDS"))
            .containsExactly(
                "GRANT ACCESS ON HOME DATABASE TO `PUBLIC`",
                "GRANT EXECUTE FUNCTION * ON DBMS TO `PUBLIC`",
                "GRANT EXECUTE PROCEDURE * ON DBMS TO `PUBLIC`",
                "GRANT LOAD ON ALL DATA TO `PUBLIC`",
            )
        assertThat(admin.column("SHOW ROLE reader PRIVILEGES AS REVOKE COMMANDS"))
            .containsExactly(
                "REVOKE GRANT ACCESS ON DATABASE * FROM `reader`",
                "REVOKE GRANT MATCH {*} ON GRAPH * NODE * FROM `reader`",
                "REVOKE GRANT MATCH {*} ON GRAPH * RELATIONSHIP * FROM `reader`",
                "REVOKE GRANT SHOW CONSTRAINT ON DATABASE * FROM `reader`",
                "REVOKE GRANT SHOW INDEX ON DATABASE * FROM `reader`",
            )
        assertThat(admin.column("SHOW PRIVILEGES AS COMMANDS WHERE command CONTAINS 'WRITE'"))
            .containsExactly(
                "GRANT WRITE ON GRAPH * TO `admin`",
                "GRANT WRITE ON GRAPH * TO `architect`",
                "GRANT WRITE ON GRAPH * TO `editor`",
                "GRANT WRITE ON GRAPH * TO `publisher`",
            )
        assertThat(admin.run("CYPHER 5 SHOW ROLE regularUsers PRIVILEGES AS COMMANDS YIELD *"))
            .containsExactly(
                mapOf(
                    "command" to "GRANT ACCESS ON DATABASE neo4j TO `regularUsers`",
                    "immutable" to false,
                )
            )
        assertThat(admin.run("CYPHER 25 SHOW ROLE regularUsers PRIVILEGES AS COMMANDS YIELD *"))
            .containsExactly(
                mapOf(
                    "command" to "GRANT ACCESS ON DATABASE neo4j TO `regularUsers`",
                    "access" to "GRANTED",
                    "action" to "access",
                    "roles" to listOf("regularUsers"),
                    "immutable" to false,
                )
            )
    }

    @Test
    fun `shows the privileges of users, their own without privileges`() {
        assertThat(
                admin.run("SHOW USERS jake, joe PRIVILEGES").map {
                    "${it["user"]} ${it["role"]} ${it["action"]}"
                }
            )
            .containsExactly(
                "jake PUBLIC load",
                "jake PUBLIC execute",
                "jake PUBLIC execute",
                "jake PUBLIC access",
                "jake regularUsers access",
                "joe PUBLIC load",
                "joe PUBLIC execute",
                "joe PUBLIC execute",
                "joe PUBLIC access",
                "joe noAccessUsers access",
            )
        assertThat(admin.column("CYPHER 5 SHOW USER jake PRIVILEGES AS COMMANDS"))
            .containsExactly(
                "GRANT ACCESS ON DATABASE neo4j TO \$role",
                "GRANT ACCESS ON HOME DATABASE TO \$role",
                "GRANT EXECUTE FUNCTION * ON DBMS TO \$role",
                "GRANT EXECUTE PROCEDURE * ON DBMS TO \$role",
                "GRANT LOAD ON ALL DATA TO \$role",
            )
        assertThat(
                jake.column(
                    "CYPHER 5 SHOW USER PRIVILEGES AS REVOKE COMMANDS " +
                        "WHERE command CONTAINS 'EXECUTE'"
                )
            )
            .containsExactly(
                "REVOKE GRANT EXECUTE FUNCTION * ON DBMS FROM \$role",
                "REVOKE GRANT EXECUTE PROCEDURE * ON DBMS FROM \$role",
            )
        // Cypher 25 shows each command once, with the roles and users that have it.
        assertThat(
                admin.run(
                    "CYPHER 25 SHOW USERS jake, joe PRIVILEGES AS COMMANDS YIELD * " +
                        "WHERE action = 'access'"
                )
            )
            .containsExactly(
                command(
                    "DENY ACCESS ON DATABASE neo4j TO \$role",
                    "DENIED",
                    "noAccessUsers",
                    "joe",
                ),
                command(
                    "GRANT ACCESS ON DATABASE neo4j TO \$role",
                    "GRANTED",
                    "regularUsers",
                    "jake",
                ),
                command(
                    "GRANT ACCESS ON HOME DATABASE TO \$role",
                    "GRANTED",
                    "PUBLIC",
                    "jake",
                    "joe",
                ),
            )
        assertThat(
                jake.run(
                    "CYPHER 25 SHOW USER PRIVILEGES AS REVOKE COMMANDS WHERE command CONTAINS 'LOAD'"
                )
            )
            .containsExactly(
                mapOf(
                    "command" to "REVOKE GRANT LOAD ON ALL DATA FROM \$role",
                    "users" to listOf("jake"),
                )
            )
        assertThat(jake.run("SHOW USER jake PRIVILEGES")).hasSize(5)
        assertThat(
                catchThrowableOfType(ClientException::class.java) {
                    jake.run("SHOW USER joe PRIVILEGES")
                }
            )
            .hasMessageStartingWith("Permission has not been granted for SHOW PRIVILEGE")
        assertThat(
                catchThrowableOfType(ClientException::class.java) { jake.run("SHOW PRIVILEGES") }
            )
            .hasMessageStartingWith("Permission has not been granted for SHOW PRIVILEGE")
    }

    @Test
    fun `shows the supported privileges as the docs show them`() {
        assertThat(
                admin.run(
                    "SHOW SUPPORTED PRIVILEGES YIELD * ORDER BY action DESC LIMIT 10 " +
                        "RETURN action, qualifier, target, scope, description"
                )
            )
            .containsExactly(
                supported(
                    "write",
                    null,
                    "graph",
                    null,
                    "allows all WRITE operations on an entire graph",
                ),
                supported(
                    "user management",
                    null,
                    "dbms",
                    null,
                    "enables the specified roles to create, delete, modify, and list users",
                ),
                supported(
                    "traverse",
                    null,
                    "graph",
                    listOf("elements", "nodes", "pattern", "relationships"),
                    "enables the specified entities to be found",
                ),
                supported(
                    "transaction management",
                    "username",
                    "database",
                    null,
                    "allows listing and ending transactions and queries for the specified users " +
                        "on the specified database",
                ),
                supported(
                    "terminate transactions",
                    "username",
                    "database",
                    null,
                    "allows ending transactions and queries for the specified users on the " +
                        "specified database",
                ),
                supported(
                    "stop",
                    null,
                    "database",
                    null,
                    "allows the specified database to be stopped",
                ),
                supported(
                    "start",
                    null,
                    "database",
                    null,
                    "allows the specified database to be started",
                ),
                supported(
                    "show user",
                    null,
                    "dbms",
                    null,
                    "enables the specified roles to list users",
                ),
                supported(
                    "show transactions",
                    "username",
                    "database",
                    null,
                    "allows listing transactions and queries for the specified users on the " +
                        "specified database",
                ),
                supported(
                    "show settings",
                    "setting",
                    "dbms",
                    null,
                    "enables the specified roles to query given configuration settings",
                ),
            )
    }

    private fun supported(
        action: String,
        qualifier: String?,
        target: String,
        scope: List<String>?,
        description: String,
    ) =
        mapOf(
            "action" to action,
            "qualifier" to qualifier,
            "target" to target,
            "scope" to scope,
            "description" to description,
        )

    @Test
    fun `shows the roles of users`() {
        server.administer("GRANT ROLE reader TO jake")

        assertThat(
                admin.run("SHOW USERS YIELD user, roles").associate { it["user"] to it["roles"] }
            )
            .containsEntry("neo4j", listOf("admin", "PUBLIC"))
            .containsEntry("jake", listOf("reader", "regularUsers", "PUBLIC"))
            .containsEntry("joe", listOf("noAccessUsers", "PUBLIC"))
        assertThat(jake.run("SHOW CURRENT USER YIELD user, roles"))
            .containsExactly(
                mapOf("user" to "jake", "roles" to listOf("reader", "regularUsers", "PUBLIC"))
            )
        assertThat(jake.run("CALL dbms.showCurrentUser()"))
            .containsExactly(
                mapOf(
                    "username" to "jake",
                    "roles" to listOf("reader", "regularUsers", "PUBLIC"),
                    "flags" to emptyList<String>(),
                )
            )
    }

    @Test
    fun `shows who may execute procedures and functions`() {
        server.administer("CREATE ROLE curator")
        server.administer("GRANT EXECUTE BOOSTED PROCEDURE db.* ON DBMS TO curator")

        val labels =
            admin
                .run(
                    "SHOW PROCEDURES YIELD name, rolesExecution, rolesBoostedExecution " +
                        "WHERE name = 'db.labels'"
                )
                .single()
        // EXECUTE BOOSTED alone runs nothing.
        assertThat(labels["rolesExecution"] as List<*>).containsExactlyInAnyOrder("PUBLIC", "admin")
        assertThat(labels["rolesBoostedExecution"] as List<*>)
            .containsExactlyInAnyOrder("admin", "curator")
        assertThat(
                admin.column(
                    "SHOW PROCEDURES EXECUTABLE BY jake YIELD name WHERE name = 'db.labels'"
                )
            )
            .containsExactly("db.labels")

        server.administer("DENY EXECUTE PROCEDURE db.labels ON DBMS TO regularUsers")

        assertThat(
                admin.column(
                    "SHOW PROCEDURES EXECUTABLE BY jake YIELD name WHERE name = 'db.labels'"
                )
            )
            .isEmpty()
        assertThat(
                admin.column(
                    "SHOW FUNCTIONS YIELD name, rolesExecution WHERE name = 'apoc.coll.sum' RETURN rolesExecution"
                )
            )
            .isEmpty()
    }
}
