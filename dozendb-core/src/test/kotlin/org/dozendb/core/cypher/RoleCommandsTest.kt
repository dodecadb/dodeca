package org.dozendb.core.cypher

import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowableOfType
import org.dozendb.core.StandaloneServer
import org.dozendb.core.StandaloneServer.Companion.SYSTEM
import org.dozendb.core.security.Access
import org.dozendb.core.security.PrivilegeRecords
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
import org.neo4j.driver.exceptions.Neo4jException
import org.neo4j.logging.NullLog

/** The role commands, over Bolt as drivers see them, by the admin `neo4j` unless said otherwise. */
class RoleCommandsTest {
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
        server.administer("CREATE USER bob SET PASSWORD 'bob-secret-1' CHANGE NOT REQUIRED")
        server.rows("CREATE (:Movie {title: 'Alien'})")
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
    private fun Driver.notifications(cypher: String): List<Pair<String, String>> =
        session(SessionConfig.forDatabase(SYSTEM)).use { session ->
            session.run(cypher).consume().notifications().map {
                it.code() to "${it.title()} ${it.description()}"
            }
        }

    private fun Driver.refused(
        cypher: String,
        parameters: Map<String, Any> = emptyMap(),
    ): ClientException =
        checkNotNull(
            catchThrowableOfType(ClientException::class.java) { run(cypher, parameters) }
        ) {
            "$cypher succeeded"
        }

    /** The GQL statuses of [this] and of its causes, outermost first. */
    private fun Neo4jException.gqlChain(): List<String> =
        generateSequence(this) { it.gqlCause().orElse(null) }.map { it.gqlStatus() }.toList()

    /**
     * The outermost GQL status of [this] and the one at the root of its causes: Neo4j gives 42001
     * for a role that exists or does not, caused by the code saying which.
     */
    private fun Neo4jException.gqlStatuses(): Pair<String, String> =
        generateSequence(this) { it.gqlCause().orElse(null) }
            .map { it.gqlStatus() }
            .toList()
            .let { it.first() to it.last() }

    private fun roles(): List<String> = admin.run("SHOW ROLES").map { it["role"] as String }

    private fun security(): SecurityGraph =
        server.database(SYSTEM).beginTx().use { SecurityGraph.read(it, NullLog.getInstance()) }

    /** Privileges that no role has, which dropping or replacing roles must not leave. */
    private fun orphans(): Long =
        server.database(SYSTEM).beginTx().use { tx ->
            tx.findNodes(PrivilegeRecords.PRIVILEGE)
                .stream()
                .filter { !it.hasRelationship() }
                .count()
        }

    @Test
    fun `creates, renames and drops roles`() {
        admin.run("CREATE ROLE curator")
        admin.run("CREATE ROLE \$name", mapOf("name" to "archivist"))

        assertThat(roles())
            .containsExactly(
                "PUBLIC",
                "admin",
                "architect",
                "archivist",
                "curator",
                "editor",
                "publisher",
                "reader",
            )

        admin.run("RENAME ROLE curator TO keeper")
        admin.run("DROP ROLE \$name", mapOf("name" to "archivist"))

        assertThat(roles()).contains("keeper").doesNotContain("curator", "archivist")
    }

    @Test
    fun `refuses names that exist or do not, unless asked not to mind`() {
        admin.run("CREATE ROLE curator")
        admin.run("CREATE ROLE keeper")

        assertThat(admin.refused("CREATE ROLE curator"))
            .hasMessage("Failed to create the specified role 'curator': Role already exists.")
            .extracting { it.gqlStatuses() }
            .isEqualTo("42001" to "42N13")
        assertThat(admin.refused("DROP ROLE missing"))
            .hasMessage("Failed to delete the specified role 'missing': Role does not exist.")
            .extracting { it.gqlStatuses() }
            .isEqualTo("42001" to "42N10")
        assertThat(admin.refused("RENAME ROLE missing TO other"))
            .hasMessage(
                "Failed to rename the specified role 'missing' to 'other': " +
                    "The role 'missing' does not exist."
            )
        assertThat(admin.refused("RENAME ROLE curator TO keeper"))
            .hasMessage(
                "Failed to rename the specified role 'curator' to 'keeper': " +
                    "Role 'keeper' already exists."
            )
            .extracting { it.gqlStatuses() }
            .isEqualTo("42001" to "42N13")

        admin.run("CREATE ROLE curator IF NOT EXISTS")
        admin.run("DROP ROLE missing IF EXISTS")
        admin.run("RENAME ROLE missing IF EXISTS TO other")

        assertThat(roles()).containsOnlyOnce("curator").contains("keeper").doesNotContain("other")
    }

    @Test
    fun `refuses invalid names, and PUBLIC`() {
        admin.run("CREATE ROLE curator")

        assertThat(admin.refused("CREATE ROLE `cu-rator`"))
            .hasMessageStartingWith("Role name 'cu-rator' contains illegal characters.")
        assertThat(admin.refused("CREATE ROLE _curator"))
            .hasMessage("Role name '_curator' must begin with an ascii letter.")
        assertThat(admin.refused("RENAME ROLE curator TO `1curator`"))
            .hasMessage("Role name '1curator' must begin with an ascii letter.")
        assertThat(admin.refused("CREATE ROLE PUBLIC"))
            .hasMessage(
                "Failed to create the specified role 'PUBLIC': 'PUBLIC' is a reserved role."
            )
        assertThat(admin.refused("CREATE OR REPLACE ROLE PUBLIC"))
            .hasMessage(
                "Failed to replace the specified role 'PUBLIC': 'PUBLIC' is a reserved role."
            )
        assertThat(admin.refused("DROP ROLE PUBLIC"))
            .hasMessage(
                "Failed to delete the specified role 'PUBLIC': 'PUBLIC' is a reserved role."
            )
        assertThat(admin.refused("RENAME ROLE PUBLIC TO everyone"))
            .hasMessage(
                "Failed to rename the specified role 'PUBLIC': 'PUBLIC' is a reserved role."
            )
        assertThat(admin.refused("RENAME ROLE curator TO PUBLIC"))
            .hasMessage(
                "Failed to rename the specified role 'curator' to 'PUBLIC': " +
                    "'PUBLIC' is a reserved role."
            )
        assertThat(admin.refused("GRANT ROLE PUBLIC TO alice"))
            .hasMessage("Failed to grant the specified role 'PUBLIC': 'PUBLIC' is a reserved role.")
        assertThat(admin.refused("REVOKE ROLE PUBLIC FROM alice"))
            .hasMessage(
                "Failed to revoke the specified role 'PUBLIC': 'PUBLIC' is a reserved role."
            )

        assertThat(roles()).containsOnlyOnce("PUBLIC").contains("curator")
    }

    @Test
    fun `gives users the privileges of the roles granted to them, until revoked`() {
        assertThat(alice.run("MATCH (m:Movie) RETURN m.title AS title", database = "neo4j"))
            .isEmpty()

        admin.run("GRANT ROLE reader TO alice")

        assertThat(alice.run("MATCH (m:Movie) RETURN m.title AS title", database = "neo4j"))
            .containsExactly(mapOf("title" to "Alien"))

        admin.run("REVOKE ROLE reader FROM alice")

        assertThat(alice.run("MATCH (m:Movie) RETURN m.title AS title", database = "neo4j"))
            .isEmpty()
    }

    @Test
    fun `grants and revokes roles, with notifications for what has no effect`() {
        admin.run("CREATE ROLE curator")

        assertThat(admin.notifications("GRANT ROLES reader, curator TO alice, bob")).isEmpty()
        assertThat(security().users.mapValues { it.value.roles })
            .containsEntry("alice", setOf("curator", "reader"))
            .containsEntry("bob", setOf("curator", "reader"))
        assertThat(admin.notifications("GRANT ROLE reader TO alice"))
            .containsExactly(
                HAS_NO_EFFECT to
                    noEffect("GRANT ROLE reader TO alice", "The user already has the role.")
            )
        assertThat(admin.refused("GRANT ROLE missing TO alice"))
            .hasMessage("Failed to grant role 'missing' to user 'alice': Role does not exist.")
            .extracting { it.gqlStatuses() }
            .isEqualTo("42001" to "42N10")
        assertThat(admin.refused("GRANT ROLE reader TO nobody"))
            .hasMessage("Failed to grant role 'reader' to user 'nobody': User does not exist.")

        assertThat(admin.notifications("REVOKE ROLE reader FROM alice")).isEmpty()
        // What has no effect is a notification in both Cypher versions.
        for (version in listOf("CYPHER 5", "CYPHER 25")) {
            assertThat(admin.notifications("$version REVOKE ROLE reader FROM alice"))
                .containsExactly(
                    HAS_NO_EFFECT to
                        noEffect(
                            "REVOKE ROLE reader FROM alice",
                            "The user does not have the role.",
                        )
                )
        }
        // A role or user that does not exist is an error in Cypher 25, as in Neo4j Enterprise,
        // and the command changes nothing.
        assertThat(admin.refused("CYPHER 25 REVOKE ROLE missing FROM bob"))
            .satisfies({
                assertThat(it.gqlChain()).containsExactly("42001", "42NA8", "42N10")
                assertThat(it.message)
                    .contains("Invalid reference in command 'REVOKE ROLE missing FROM bob'.")
                    .contains("A role with the name `missing` was not found.")
            })
        assertThat(admin.refused("CYPHER 25 REVOKE ROLES curator FROM bob, nobody"))
            .satisfies({
                assertThat(it.gqlChain()).containsExactly("42001", "42NA8", "42N09")
                assertThat(it.message).contains("A user with the name `nobody` was not found.")
            })
        assertThat(security().users.mapValues { it.value.roles })
            .containsEntry("bob", setOf("curator", "reader"))
        assertThat(admin.notifications("CYPHER 5 REVOKE ROLES missing, curator FROM nobody, bob"))
            .containsExactlyInAnyOrder(
                IMPOSSIBLE_REVOKE to impossible("REVOKE ROLE missing FROM nobody", "Role"),
                IMPOSSIBLE_REVOKE to impossible("REVOKE ROLE missing FROM bob", "Role"),
                IMPOSSIBLE_REVOKE to impossible("REVOKE ROLE curator FROM nobody", "User"),
            )
        assertThat(security().users.mapValues { it.value.roles })
            .containsEntry("alice", setOf("curator"))
            .containsEntry("bob", setOf("reader"))
    }

    private fun noEffect(command: String, why: String) =
        "`$command` has no effect. $why See Status Codes documentation for more information."

    private fun impossible(command: String, missing: String) =
        noEffect(
            command,
            "$missing does not exist. Make sure nothing is misspelled. This notification will " +
                "become an error in a future major version.",
        )

    @Test
    fun `shows all roles or populated ones, with their users or not`() {
        admin.run("CREATE ROLE curator")
        admin.run("GRANT ROLE reader TO alice")

        assertThat(admin.run("SHOW POPULATED ROLES").map { it["role"] })
            .containsExactly("PUBLIC", "admin", "reader")
        assertThat(admin.run("SHOW ROLES WITH USERS WHERE role IN ['PUBLIC', 'curator', 'reader']"))
            .containsExactly(
                mapOf("role" to "PUBLIC", "member" to "alice"),
                mapOf("role" to "PUBLIC", "member" to "bob"),
                mapOf("role" to "PUBLIC", "member" to "neo4j"),
                mapOf("role" to "curator", "member" to null),
                mapOf("role" to "reader", "member" to "alice"),
            )
        assertThat(admin.run("SHOW POPULATED ROLES WITH USERS").map { it["role"] to it["member"] })
            .containsExactly(
                "PUBLIC" to "alice",
                "PUBLIC" to "bob",
                "PUBLIC" to "neo4j",
                "admin" to "neo4j",
                "reader" to "alice",
            )
        assertThat(admin.run("SHOW ROLES YIELD * WHERE role = 'curator'"))
            .containsExactly(mapOf("role" to "curator", "immutable" to false))
        assertThat(alice.refused("SHOW ROLES"))
            .hasMessageStartingWith("Permission has not been granted for SHOW ROLE.")
    }

    @Test
    fun `copies the privileges of roles, but not their users`() {
        admin.run("GRANT ROLE reader TO bob")

        admin.run("CREATE ROLE curator AS COPY OF reader")

        val roles = security().roles
        assertThat(roles.getValue("curator").privileges)
            .isNotEmpty
            .containsExactlyInAnyOrderElementsOf(roles.getValue("reader").privileges)
        assertThat(admin.run("SHOW ROLES WITH USERS WHERE role = 'curator'"))
            .containsExactly(mapOf("role" to "curator", "member" to null))
        admin.run("GRANT ROLE curator TO alice")
        assertThat(alice.run("MATCH (m:Movie) RETURN m.title AS title", database = "neo4j"))
            .containsExactly(mapOf("title" to "Alien"))
        assertThat(admin.refused("CREATE ROLE keeper AS COPY OF missing"))
            .hasMessage("Failed to create a role as copy of 'missing': Role does not exist.")
        assertThat(roles()).doesNotContain("keeper")
    }

    @Test
    fun `replaces and drops roles with their privileges and users`() {
        admin.run("CREATE ROLE curator AS COPY OF reader")
        admin.run("GRANT ROLE curator TO alice")

        admin.run("CREATE OR REPLACE ROLE curator")

        assertThat(security().roles.getValue("curator").privileges).isEmpty()
        assertThat(security().users.getValue("alice").roles).isEmpty()
        admin.run("CREATE OR REPLACE ROLE keeper AS COPY OF reader")
        admin.run("GRANT ROLE keeper TO alice")

        admin.run("DROP ROLE keeper")

        assertThat(roles()).doesNotContain("keeper")
        assertThat(security().users.getValue("alice").roles).isEmpty()
        assertThat(orphans()).isZero()
    }

    @Test
    fun `keeps immutable roles as they are while authentication is enabled`() {
        assertThat(admin.refused("CREATE IMMUTABLE ROLE locked"))
            .hasMessage(
                "Immutable roles cannot be created. Try creating the role without the IMMUTABLE " +
                    "keyword."
            )
        // Embedded transactions run with authentication disabled.
        server.administer("CREATE IMMUTABLE ROLE locked")
        immutablePrivilege("locked")

        assertThat(admin.run("SHOW ROLES YIELD * WHERE role = 'locked'"))
            .containsExactly(mapOf("role" to "locked", "immutable" to true))
        assertThat(admin.refused("DROP ROLE locked"))
            .hasMessage(
                "Immutable roles cannot be dropped. Use `SHOW ROLES YIELD *` to see which roles " +
                    "are immutable."
            )
        assertThat(admin.refused("RENAME ROLE locked TO unlocked"))
            .hasMessageStartingWith("Immutable roles cannot be renamed.")
        assertThat(admin.refused("CREATE OR REPLACE ROLE locked"))
            .hasMessageStartingWith("Immutable roles cannot be replaced.")
        assertThat(admin.refused("CREATE ROLE unlocked AS COPY OF locked"))
            .hasMessage(
                "'locked' cannot be copied because it has one or more immutable privileges " +
                    "assigned to it and immutable privileges cannot be copied. Use " +
                    "`SHOW ROLE locked PRIVILEGES AS COMMANDS YIELD *` to inspect locked's " +
                    "privileges."
            )
        admin.run("GRANT ROLE locked TO alice")
        admin.run("REVOKE ROLE locked FROM alice")
        assertThat(roles()).contains("locked").doesNotContain("unlocked")

        server.administer("CREATE IMMUTABLE ROLE sealed AS COPY OF locked")
        server.administer("CREATE ROLE unlocked AS COPY OF locked")
        assertThat(
                catchThrowableOfType(Exception::class.java) {
                    server.administer("CREATE IMMUTABLE ROLE sealed_reader AS COPY OF reader")
                }
            )
            .hasMessageContaining(
                "'reader' cannot be copied to an immutable role because 'reader' has one or more " +
                    "non-immutable privileges."
            )
        assertThat(security().roles.filterKeys { it in setOf("locked", "sealed", "unlocked") })
            .allSatisfy { _, role ->
                assertThat(role.privileges).singleElement().matches { it.immutable }
            }
        server.administer("RENAME ROLE sealed TO resealed")
        server.administer("DROP ROLE resealed")
        server.administer("CREATE OR REPLACE ROLE locked")
        assertThat(security().roles.getValue("locked").privileges).isEmpty()
        assertThat(roles()).doesNotContain("sealed", "resealed", "sealed_reader")
    }

    /** Gives [role] an immutable privilege, as GRANT IMMUTABLE will. */
    private fun immutablePrivilege(role: String) {
        val privilege =
            security().roles.getValue("reader").privileges.first().copy(immutable = true)
        server.database(SYSTEM).beginTx().use { tx ->
            val node = tx.findNode(PrivilegeRecords.ROLE, PrivilegeRecords.ROLE_NAME, role)
            val record = tx.createNode(PrivilegeRecords.PRIVILEGE)
            PrivilegeRecords.write(
                record,
                node.getProperty(PrivilegeRecords.ROLE_ID) as String,
                privilege,
            )
            node.createRelationshipTo(record, PrivilegeRecords.relationship(Access.GRANTED))
            tx.commit()
        }
    }

    @Test
    fun `needs the privileges of each command`() {
        assertThat(alice.refused("CREATE ROLE curator"))
            .hasMessageStartingWith("Permission has not been granted for CREATE ROLE.")
        assertThat(alice.refused("GRANT ROLE admin TO alice"))
            .hasMessageStartingWith("Permission has not been granted for ASSIGN ROLE.")
        assertThat(alice.refused("DROP ROLE reader"))
            .hasMessageStartingWith("Permission has not been granted for DROP ROLE.")
        assertThat(security().users.getValue("alice").roles).isEmpty()
        assertThat(roles()).contains("reader").doesNotContain("curator")
    }

    private companion object {
        const val HAS_NO_EFFECT = "Neo.ClientNotification.Security.CommandHasNoEffect"
        const val IMPOSSIBLE_REVOKE = "Neo.ClientNotification.Security.ImpossibleRevokeCommand"
    }
}
