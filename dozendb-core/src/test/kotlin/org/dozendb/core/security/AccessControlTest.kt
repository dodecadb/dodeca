package org.dozendb.core.security

import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
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
import org.neo4j.driver.exceptions.ClientException
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo
import org.neo4j.internal.kernel.api.security.AccessMode
import org.neo4j.internal.kernel.api.security.AdminAccessMode
import org.neo4j.internal.kernel.api.security.AdminActionOnResource
import org.neo4j.internal.kernel.api.security.CommunitySecurityLog
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.internal.kernel.api.security.PermissionState
import org.neo4j.internal.kernel.api.security.PrivilegeAction
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.internal.kernel.api.security.Segment
import org.neo4j.kernel.api.security.AuthManager
import org.neo4j.kernel.api.security.AuthToken
import org.neo4j.kernel.database.DatabaseReferenceRepository

/** Users have the privileges of their roles, over Bolt as drivers see them. */
class AccessControlTest {
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
        server.rows("CREATE (:Movie {title: 'Alien'})-[:SEQUEL]->(:Movie {title: 'Aliens'})")
        server.administer("CREATE USER alice SET PASSWORD 'alice-secret-1' CHANGE NOT REQUIRED")
    }

    @AfterEach
    fun stop() {
        drivers.forEach(Driver::close)
        server.close()
    }

    private val alice by lazy {
        GraphDatabase.driver(
                "bolt://${server.boltAddress()}",
                AuthTokens.basic("alice", "alice-secret-1"),
            )
            .also { drivers += it }
    }

    private fun Driver.run(cypher: String, database: String? = null): List<Map<String, Any?>> {
        val config =
            if (database == null) SessionConfig.defaultConfig()
            else SessionConfig.forDatabase(database)
        return session(config).use { session -> session.run(cypher).list { it.asMap() } }
    }

    private fun Driver.refused(cypher: String, database: String? = null): String? = catchThrowable {
        run(cypher, database)
    }
        .also { assertThat(it).describedAs(cypher).isInstanceOf(ClientException::class.java) }
        .message

    /** Gives [user] the [role], or takes it away, as GRANT and REVOKE ROLE will. */
    private fun role(user: String, role: String, given: Boolean = true) =
        server.database(StandaloneServer.SYSTEM).beginTx().use { tx ->
            val node = tx.findNode(org.neo4j.graphdb.Label.label("User"), "name", user)
            if (given) {
                node.createRelationshipTo(
                    tx.findNode(PrivilegeRecords.ROLE, PrivilegeRecords.ROLE_NAME, role),
                    PrivilegeRecords.HAS_ROLE,
                )
            } else {
                node.getRelationships(PrivilegeRecords.HAS_ROLE).forEach { it.delete() }
            }
            tx.commit()
        }

    @Test
    fun `with PUBLIC alone, a user reaches their home database and procedures, not data`() {
        assertThat(alice.run("RETURN 1 AS one")).containsExactly(mapOf("one" to 1L))
        assertThat(alice.run("CALL db.info() YIELD name")).containsExactly(mapOf("name" to "neo4j"))
        assertThat(alice.run("MATCH (n) RETURN count(n) AS count"))
            .containsExactly(mapOf("count" to 0L))
        assertThat(alice.run("MATCH (n:Movie) RETURN n.title")).isEmpty()
        assertThat(alice.refused("CREATE (:Movie {title: 'Prometheus'})"))
            .isEqualTo(
                "Create node with labels 'Movie' on database 'neo4j' is not allowed for user " +
                    "'alice' with roles [PUBLIC]."
            )
        assertThat(alice.refused("RETURN 1", "films"))
            .isEqualTo("Database access is not allowed for user 'alice' with roles [PUBLIC].")
        assertThat(alice.run("SHOW DATABASES YIELD name", "system").map { it["name"] })
            .containsExactlyInAnyOrder("neo4j", "system")
        assertThat(alice.refused("CREATE USER bob SET PASSWORD 'bob-secret-1'", "system"))
            .startsWith("Permission has not been granted for CREATE USER.")
    }

    @Test
    fun `the built-in roles give what Neo4j's docs list`() {
        role("alice", "reader")
        assertThat(alice.run("MATCH (n:Movie) RETURN count(n) AS count"))
            .containsExactly(mapOf("count" to 2L))
        assertThat(alice.run("MATCH (n:Movie) RETURN n.title AS title ORDER BY title"))
            .containsExactly(mapOf("title" to "Alien"), mapOf("title" to "Aliens"))
        assertThat(alice.run("MATCH ()-[r:SEQUEL]->() RETURN count(r) AS count"))
            .containsExactly(mapOf("count" to 1L))
        assertThat(alice.run("RETURN 1 AS one", "films")).hasSize(1)
        assertThat(alice.run("SHOW DATABASES YIELD name", "system").map { it["name"] })
            .containsExactlyInAnyOrder("films", "neo4j", "system")
        assertThat(alice.refused("CREATE (:Movie)")).contains("is not allowed for user 'alice'")

        role("alice", "editor")
        alice.run("CREATE (:Movie {title: 'Prometheus'})")
        assertThat(alice.refused("CREATE (:Series)"))
            .startsWith(
                "Creating new node label on database 'neo4j' is not allowed for user 'alice' " +
                    "with roles [PUBLIC, editor, reader]."
            )

        role("alice", "publisher")
        alice.run("CREATE (:Series {title: 'Alien: Earth'})")
        assertThat(alice.refused("CREATE INDEX FOR (m:Movie) ON (m.title)"))
            .contains("is not allowed for user 'alice'")

        role("alice", "architect")
        alice.run("CREATE INDEX FOR (m:Movie) ON (m.title)")
        assertThat(alice.refused("STOP DATABASE films", "system"))
            .startsWith("Permission has not been granted for STOP DATABASE.")

        role("alice", "admin")
        alice.run("STOP DATABASE films", "system")
        alice.run("CREATE USER bob SET PASSWORD 'bob-secret-1'", "system")
        assertThat(alice.run("SHOW DATABASES YIELD name", "system")).hasSize(3)
    }

    @Test
    fun `a change of roles applies from the next transaction`() {
        role("alice", "reader")
        alice.session().use { session ->
            session.beginTransaction().use { tx ->
                val count = "MATCH (n:Movie) RETURN count(n) AS count"
                assertThat(tx.run(count).single()["count"].asLong()).isEqualTo(2)

                role("alice", "reader", given = false)

                assertThat(tx.run(count).single()["count"].asLong()).isEqualTo(2)
                tx.commit()
            }
            assertThat(
                    session
                        .run("MATCH (n:Movie) RETURN count(n) AS count")
                        .single()["count"]
                        .asLong()
                )
                .isEqualTo(0)
        }
    }

    @Test
    fun `a user whose password must change can only change it`() {
        server.administer("CREATE USER carol SET PASSWORD 'carol-secret-1' CHANGE REQUIRED")
        val carol =
            GraphDatabase.driver(
                    "bolt://${server.boltAddress()}",
                    AuthTokens.basic("carol", "carol-secret-1"),
                )
                .also { drivers += it }

        val expired = "must be changed before you can use this instance"
        assertThat(carol.refused("RETURN 1")).contains(expired)
        role("carol", "admin")
        assertThat(carol.refused("SHOW USERS", "system")).contains(expired)
        carol.run(
            "ALTER CURRENT USER SET PASSWORD FROM 'carol-secret-1' TO 'carol-secret-2'",
            "system",
        )

        val renewed =
            GraphDatabase.driver(
                    "bolt://${server.boltAddress()}",
                    AuthTokens.basic("carol", "carol-secret-2"),
                )
                .also { drivers += it }
        assertThat(renewed.run("RETURN 1 AS one")).hasSize(1)
    }

    @Test
    fun `a security context keeps its restrictions through withMode and authorize`() {
        val dependencies = server.database(StandaloneServer.SYSTEM).dependencyResolver
        val login =
            dependencies
                .resolveDependency(AuthManager::class.java)
                .login(
                    AuthToken.newBasicAuthToken("alice", "alice-secret-1"),
                    ClientConnectionInfo.EMBEDDED_CONNECTION,
                )
        val references = dependencies.resolveDependency(DatabaseReferenceRepository::class.java)
        fun reference(name: String) = references.getByAlias(name).orElseThrow()
        val log = CommunitySecurityLog.NULL_LOG
        val context = login.authorize(LoginContext.IdLookup.EMPTY, reference("neo4j"), log)
        val createUser =
            AdminActionOnResource(
                PrivilegeAction.CREATE_USER,
                AdminActionOnResource.DatabaseScope.ALL,
                Segment.ALL,
            )

        for (variant in
            listOf(
                context,
                context.withMode(AccessMode.Static.FULL),
                context.withMode(AccessMode.Static.READ).withMode(AccessMode.Static.FULL),
            )) {
            assertThat(variant).isInstanceOf(DozenDbSecurityContext::class.java)
            assertThat(variant.allowsAdminAction(createUser)).isEqualTo(PermissionState.NOT_GRANTED)
            assertThat(variant.allowExecuteAdminProcedure(0)).isEqualTo(PermissionState.NOT_GRANTED)
            assertThat(variant.databaseAccessMode().canAccessDatabase(reference("films"))).isFalse
            assertThat(variant.databaseAccessMode().canSeeDatabase(reference("films"))).isFalse
            assertThat(variant.databaseAccessMode().canSeeDatabase(reference("neo4j"))).isTrue
            assertThat(variant.roles()).containsExactly("PUBLIC")
            assertThat(variant.isAuthDisabled).isFalse
            assertThat(variant.authorize(LoginContext.IdLookup.EMPTY, reference("neo4j"), log))
                .isSameAs(variant)
            assertThat(
                    catchThrowable {
                        variant.authorize(LoginContext.IdLookup.EMPTY, reference("films"), log)
                    }
                )
                .hasMessage("Database access is not allowed for user 'alice' with roles [PUBLIC].")
        }
        // Boosted procedures run with every administration action, and only them.
        val boosted = context.withMode(AdminAccessMode.FULL)
        assertThat(boosted.allowsAdminAction(createUser)).isEqualTo(PermissionState.EXPLICIT_GRANT)
        assertThat(boosted.withMode(AccessMode.Static.READ).allowsAdminAction(createUser))
            .isEqualTo(PermissionState.EXPLICIT_GRANT)
        assertThat(boosted.databaseAccessMode().canAccessDatabase(reference("films"))).isFalse
    }

    @Test
    fun `the security context overrides everything Neo4j's would grant`() {
        val inherited =
            setOf(
                "subject",
                "connectionInfo",
                "mode",
                "database",
                "impersonating",
                "nativelyAuthenticated",
                "assertCredentialsNotExpired",
                "description",
            )
        val declared =
            DozenDbSecurityContext::class.java.declaredMethods.map {
                it.name to it.parameterTypes.toList()
            }
        for (method in SecurityContext::class.java.methods) {
            if (method.declaringClass == Any::class.java || method.name in inherited) continue
            if (java.lang.reflect.Modifier.isStatic(method.modifiers)) continue
            assertThat(declared)
                .describedAs("SecurityContext.${method.name}")
                .contains(method.name to method.parameterTypes.toList())
        }
    }
}
