package dev.dodeca.cluster.internal.forwarding

import dev.dodeca.cluster.internal.ClusterHarness
import dev.dodeca.consensus.testing.TestAuthority
import dev.dodeca.core.security.MemberLogins
import java.net.URI
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assertions.catchThrowable
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.configuration.connectors.ConnectorPortRegister
import org.neo4j.configuration.connectors.ConnectorType
import org.neo4j.driver.AuthToken
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.ClientCertificateManagers
import org.neo4j.driver.ClientCertificates
import org.neo4j.driver.Config
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.exceptions.AuthenticationException
import org.neo4j.driver.exceptions.Neo4jException
import org.neo4j.graphdb.config.Setting

/** Writes that reach a member which does not lead their database run on the leader. */
class ForwardedWritesTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    private val drivers = mutableListOf<Driver>()

    @AfterEach
    fun stop() {
        drivers.forEach(Driver::close)
        harness?.close()
    }

    private fun cluster(
        settings: Map<Setting<*>, Any> = BOLT,
        authority: TestAuthority? = TestAuthority(),
    ): ClusterHarness =
        ClusterHarness(root, 3, settings = settings, authority = authority).also {
            harness = it
            it.start()
        }

    /** The routing address of each member, as this member sees it, by Raft port. */
    private fun ClusterHarness.Member.routingAddresses(): List<String?> =
        database("system").executeTransactionally(
            "CALL dodeca.cluster.members() YIELD raftAddress, routingAddress " +
                "RETURN routingAddress ORDER BY raftAddress",
            emptyMap(),
        ) { result ->
            result.stream().map { it["routingAddress"] as String? }.toList()
        }

    /** Waits until this member knows every member's Bolt address, which its refusals name. */
    private fun ClusterHarness.Member.awaitBoltAddresses() {
        await withPollInterval
            Duration.ofMillis(100) atMost
            ClusterHarness.AWAIT untilAsserted
            {
                assertThat(
                        database("system").executeTransactionally(
                            "CALL dodeca.cluster.members() YIELD boltAddress RETURN boltAddress",
                            emptyMap(),
                        ) { result ->
                            result.stream().map { it["boltAddress"] }.toList()
                        }
                    )
                    .hasSize(3)
                    .doesNotContainNull()
            }
    }

    private fun ClusterHarness.Member.boundRoutingAddress(): String {
        val address =
            checkNotNull(
                database("system")
                    .dependencyResolver
                    .resolveDependency(ConnectorPortRegister::class.java)
                    .getLocalAddress(ConnectorType.INTRA_BOLT)
            ) {
                "$this has no routing connector"
            }
        return "127.0.0.1:${address.port}"
    }

    /**
     * A client of [member]'s routing connector as [auth], with a member certificate the cluster's
     * authority issued if [asMember].
     */
    private fun ClusterHarness.routingClient(
        member: ClusterHarness.Member,
        auth: AuthToken,
        asMember: Boolean = true,
    ): Driver {
        val authority = authority
        val config = Config.builder().withoutEncryption()
        if (authority == null) {
            return GraphDatabase.driver(
                    "bolt://${member.boundRoutingAddress()}",
                    auth,
                    config.build(),
                )
                .also { drivers += it }
        }
        val certificate = authority.issue()
        val directory = Files.createTempDirectory(root, "client")
        certificate.writeTo(directory)
        val trust =
            Config.TrustStrategy.trustCustomCertificateSignedBy(
                directory.resolve("trusted/authority-0.crt").toFile()
            )
        val tls = Config.builder().withEncryption().withTrustStrategy(trust).build()
        val uri = URI("bolt://${member.boundRoutingAddress()}")
        return if (asMember) {
                GraphDatabase.driver(
                    uri,
                    auth,
                    ClientCertificateManagers.rotating(
                        ClientCertificates.of(
                            directory.resolve("public.crt").toFile(),
                            directory.resolve("private.key").toFile(),
                        )
                    ),
                    tls,
                )
            } else {
                GraphDatabase.driver(uri, auth, tls)
            }
            .also { drivers += it }
    }

    private fun Driver.currentUser(): String =
        session(SessionConfig.forDatabase("system")).use { session ->
            session.run("SHOW CURRENT USER YIELD user").single()["user"].asString()
        }

    /** Creates [user] on the system leader and waits until every member has them. */
    private fun ClusterHarness.createUser(user: String, vararg roles: String) {
        val system = awaitLeader("system").database("system")
        system.executeTransactionally(
            "CREATE USER $user SET PASSWORD '$user-secret-1' CHANGE NOT REQUIRED"
        )
        roles.forEach { system.executeTransactionally("GRANT ROLE $it TO $user") }
        for (member in members) {
            await withPollInterval
                Duration.ofMillis(100) atMost
                ClusterHarness.AWAIT untilAsserted
                {
                    assertThat(
                            member.database("system").executeTransactionally(
                                "SHOW USERS YIELD user WHERE user = \$user RETURN user",
                                mapOf("user" to user),
                            ) {
                                it.stream().count()
                            }
                        )
                        .isEqualTo(1L)
                }
        }
    }

    @Test
    fun `members log in on the routing connector as the user they name`() {
        val cluster = cluster(AUTH)
        cluster.createUser("alice")
        cluster.createUser("bob")
        cluster
            .awaitLeader("system")
            .database("system")
            .executeTransactionally("ALTER USER bob SET STATUS SUSPENDED")
        val member = cluster.members[1]
        await withPollInterval
            Duration.ofMillis(100) atMost
            ClusterHarness.AWAIT untilAsserted
            {
                assertThat(
                        member.database("system").executeTransactionally(
                            "SHOW USERS YIELD user, suspended WHERE user = 'bob' RETURN suspended",
                            emptyMap(),
                        ) {
                            it.next()["suspended"]
                        }
                    )
                    .isEqualTo(true)
            }

        assertThat(cluster.routingClient(member, memberToken("alice")).currentUser())
            .isEqualTo("alice")
        for (user in listOf("mallory", "bob")) {
            assertThatThrownBy { cluster.routingClient(member, memberToken(user)).currentUser() }
                .isInstanceOf(AuthenticationException::class.java)
        }
        assertThatThrownBy {
                cluster
                    .routingClient(member, AuthTokens.basic("alice", "alice-secret-1"))
                    .verifyConnectivity()
            }
            .isInstanceOf(AuthenticationException::class.java)
            .hasMessageContaining("takes logins of cluster members only, not 'basic'")
        // Only clients with a member's certificate get in.
        assertThatThrownBy {
                cluster
                    .routingClient(member, memberToken("alice"), asMember = false)
                    .verifyConnectivity()
            }
            .isNotInstanceOf(AuthenticationException::class.java)
    }

    @Test
    fun `members in plaintext refuse logins on the routing connector`() {
        val cluster = cluster(AUTH, authority = null)

        assertThatThrownBy {
                cluster.routingClient(cluster.members[0], memberToken("neo4j")).verifyConnectivity()
            }
            .isInstanceOf(AuthenticationException::class.java)
            .hasMessageContaining("members talk in plaintext")
    }

    @Test
    fun `members with mutual TLS report where they take forwarded writes`() {
        val cluster = cluster()

        val expected = cluster.members.sortedBy { it.raftPort }.map { it.boundRoutingAddress() }
        for (member in cluster.members) {
            await withPollInterval
                Duration.ofMillis(100) atMost
                ClusterHarness.AWAIT untilAsserted
                {
                    assertThat(member.routingAddresses()).isEqualTo(expected)
                }
        }
    }

    @Test
    fun `members in plaintext take no forwarded writes`() {
        val cluster = cluster(authority = null)

        for (member in cluster.members) {
            assertThat(member.routingAddresses()).containsOnlyNulls().hasSize(3)
        }
    }

    @Test
    fun `a member in plaintext that refuses a write says where to write and why`() {
        val cluster = cluster(authority = null)
        val leader = cluster.awaitLeader()
        val follower = cluster.members.first { it != leader }
        follower.awaitBoltAddresses()

        val refused =
            catchThrowable {
                GraphDatabase.driver("bolt://${follower.boltAddress()}").use { driver ->
                    driver.session(SessionConfig.forDatabase("neo4j")).use {
                        it.run("CREATE (:Refused)").consume()
                    }
                }
            }
                as Neo4jException

        assertThat(refused.code()).isEqualTo("Neo.ClientError.Cluster.NotALeader")
        val others = cluster.members.filter { it != follower }.map { it.boltAddress() }
        assertThat(refused.message)
            .matches(
                "Member \\S+ at (${others.joinToString("|") { Regex.escape(it) }}) leads database 'neo4j', not this member\\..*"
            )
            .contains("This member forwards no writes to the leader: members talk in plaintext")
    }

    private companion object {
        val BOLT: Map<Setting<*>, Any> = mapOf(BoltConnector.enabled to true)
        val AUTH: Map<Setting<*>, Any> = BOLT + mapOf(GraphDatabaseSettings.auth_enabled to true)

        fun memberToken(user: String): AuthToken =
            AuthTokens.custom(user, "", null, MemberLogins.SCHEME)
    }
}
