package org.dozendb.cluster.internal

import java.nio.file.Path
import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.dozendb.core.security.DozenDbSecurityComponent
import org.dozendb.core.security.PrivilegeRecords
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.driver.AuthToken
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.exceptions.AuthenticationException
import org.neo4j.driver.exceptions.ClientException
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.config.Setting

/** User status, home databases and roles on every member, which learns of them through Raft. */
class UsersTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null
    private val drivers = mutableListOf<Driver>()

    @AfterEach
    fun stop() {
        drivers.forEach(Driver::close)
        harness?.close()
    }

    private fun ClusterHarness.administer(command: String) {
        awaitLeader(SYSTEM).database(SYSTEM).executeTransactionally(command)
    }

    private fun ClusterHarness.Member.driver(user: String, password: String): Driver =
        GraphDatabase.driver("bolt://${boltAddress()}", AuthTokens.basic(user, password)).also {
            drivers += it
        }

    /** A routing driver, as cypher-shell and applications use, that starts at this member. */
    private fun ClusterHarness.Member.routing(auth: AuthToken): Driver =
        GraphDatabase.driver("neo4j://${boltAddress()}", auth).also { drivers += it }

    /** The nodes in [database], read by a reader and then by the writer. */
    private fun Driver.nodes(database: String): List<Long> =
        session(SessionConfig.forDatabase(database)).use { session ->
            val count = "MATCH (n) RETURN count(n) AS count"
            listOf(
                session.executeRead { it.run(count).single()["count"].asLong() },
                session.executeWrite { it.run(count).single()["count"].asLong() },
            )
        }

    /** The database a session without one uses for [user] on this member. */
    private fun ClusterHarness.Member.homeOf(user: String, password: String): String =
        driver(user, password).session().use { session ->
            session.run("CALL db.info() YIELD name").single()["name"].asString()
        }

    private fun ClusterHarness.Member.movies(user: String, password: String): Long =
        driver(user, password).session().use { session ->
            session.run("MATCH (m:Movie) RETURN count(m) AS movies").single()["movies"].asLong()
        }

    private fun eventually(assertion: () -> Unit) {
        // A member that has yet to copy films fails sessions in it for a moment.
        await.ignoreExceptions() withPollInterval
            Duration.ofMillis(200) atMost
            ClusterHarness.AWAIT untilAsserted
            {
                assertion()
            }
    }

    @Test
    fun `status and home databases apply on every member`() {
        val cluster =
            ClusterHarness(
                    root,
                    3,
                    settings =
                        mapOf(
                            GraphDatabaseSettings.auth_enabled to true,
                            BoltConnector.enabled to true,
                        ),
                )
                .also { harness = it }
        cluster.start()
        cluster.administer("CREATE DATABASE films")
        cluster.administer(
            "CREATE USER alice SET PASSWORD 'alice-secret-1' CHANGE NOT REQUIRED " +
                "SET HOME DATABASE films"
        )

        for (member in cluster.members) {
            eventually { assertThat(member.homeOf("alice", "alice-secret-1")).isEqualTo("films") }
        }

        cluster.administer("ALTER USER alice SET STATUS SUSPENDED")

        for (member in cluster.members) {
            eventually {
                assertThat(catchThrowable { member.homeOf("alice", "alice-secret-1") })
                    .isInstanceOf(AuthenticationException::class.java)
            }
        }
    }

    @Test
    fun `roles granted and revoked apply on every member`() {
        val cluster =
            ClusterHarness(
                    root,
                    3,
                    settings =
                        mapOf(
                            GraphDatabaseSettings.auth_enabled to true,
                            BoltConnector.enabled to true,
                        ),
                )
                .also { harness = it }
        cluster.start()
        cluster.awaitLeader("neo4j").database("neo4j").executeTransactionally("CREATE (:Movie)")
        cluster.administer("CREATE USER alice SET PASSWORD 'alice-secret-1' CHANGE NOT REQUIRED")
        cluster.administer("CREATE ROLE curator AS COPY OF reader")

        cluster.administer("GRANT ROLE curator TO alice")

        for (member in cluster.members) {
            eventually { assertThat(member.movies("alice", "alice-secret-1")).isEqualTo(1) }
        }

        cluster.administer("REVOKE ROLE curator FROM alice")

        for (member in cluster.members) {
            eventually { assertThat(member.movies("alice", "alice-secret-1")).isZero() }
        }
    }

    @Test
    fun `a cluster without roles gets them from its system leader, refusing logins until then`() {
        val cluster =
            ClusterHarness(
                    root,
                    3,
                    settings =
                        mapOf(
                            GraphDatabaseSettings.auth_enabled to true,
                            BoltConnector.enabled to true,
                        ),
                )
                .also { harness = it }
        cluster.start()
        cluster.administer("CREATE USER alice SET PASSWORD 'alice-secret-1' CHANGE NOT REQUIRED")
        fun ClusterHarness.Member.roles(): Any? =
            driver("alice", "alice-secret-1").session().use { session ->
                session.run("SHOW CURRENT USER YIELD roles").single()["roles"].asList()
            }
        for (member in cluster.members) {
            eventually { assertThat(member.roles()).isEqualTo(listOf("PUBLIC")) }
        }

        // As in a cluster formed before DozenDB had roles.
        val leader = cluster.awaitLeader(SYSTEM)
        leader.database(SYSTEM).beginTx().use { tx ->
            for (label in listOf(PrivilegeRecords.PRIVILEGE, PrivilegeRecords.ROLE)) {
                tx.findNodes(label).forEach { node ->
                    node.relationships.forEach { it.delete() }
                    node.delete()
                }
            }
            tx.findNodes(Label.label("Version")).forEach {
                it.removeProperty(DozenDbSecurityComponent.NAME.name())
            }
            tx.commit()
        }

        for (member in cluster.members) {
            eventually {
                assertThat(catchThrowable { member.roles() })
                    .hasMessage(
                        "Database access is not allowed for user 'alice' yet: this server does " +
                            "not have the roles of the cluster yet. Try again in a moment."
                    )
            }
        }

        leader.stop()

        for (member in cluster.running()) {
            eventually { assertThat(member.roles()).isEqualTo(listOf("PUBLIC")) }
        }
        assertThat(
                cluster.awaitLeader(SYSTEM).database(SYSTEM).executeTransactionally(
                    "SHOW USERS YIELD user, roles WHERE user = 'neo4j'",
                    emptyMap(),
                ) {
                    it.next()["roles"]
                }
            )
            .isEqualTo(listOf("admin", "PUBLIC"))

        leader.start()

        eventually { assertThat(leader.roles()).isEqualTo(listOf("PUBLIC")) }
    }

    @Test
    fun `a routing driver reaches the databases a user may access, on readers and the writer`() {
        val cluster =
            ClusterHarness(
                    root,
                    3,
                    settings =
                        mapOf(
                            GraphDatabaseSettings.auth_enabled to true,
                            BoltConnector.enabled to true,
                        ),
                )
                .also { harness = it }
        cluster.start()
        cluster.administer("CREATE DATABASE films")
        cluster.awaitLeader("films").database("films").executeTransactionally("CREATE (:Movie)")
        cluster.administer("CREATE USER alice SET PASSWORD 'alice-secret-1' CHANGE NOT REQUIRED")
        cluster.administer("CREATE ROLE viewer")
        cluster.administer("GRANT ROLE viewer TO alice")
        val alice = cluster.members[0].routing(AuthTokens.basic("alice", "alice-secret-1"))

        eventually { assertThat(alice.nodes("neo4j")).containsExactly(0, 0) }
        val refused = catchThrowable { alice.nodes("films") }
        assertThat(refused)
            .isInstanceOf(ClientException::class.java)
            .hasMessage(
                "Database access is not allowed for user 'alice' with roles [PUBLIC, viewer]."
            )

        cluster.administer("GRANT ACCESS ON DATABASE films TO viewer")
        cluster.administer("GRANT MATCH {*} ON GRAPH films TO viewer")

        eventually { assertThat(alice.nodes("films")).containsExactly(1, 1) }

        cluster.administer("DENY ACCESS ON DATABASE films TO viewer")

        eventually {
            assertThat(catchThrowable { alice.nodes("films") })
                .isInstanceOf(ClientException::class.java)
                .hasMessage(
                    "Database access is not allowed for user 'alice' with roles [PUBLIC, viewer]."
                )
        }
    }

    @Test
    fun `an admin locked out gets the role back with auth disabled on every member`() {
        val cluster =
            ClusterHarness(
                    root,
                    3,
                    settings =
                        mapOf(
                            GraphDatabaseSettings.auth_enabled to true,
                            BoltConnector.enabled to true,
                        ),
                )
                .also { harness = it }
        cluster.start()
        cluster.administer("ALTER USER neo4j SET PASSWORD 'neo4j-secret-1' CHANGE NOT REQUIRED")
        cluster.administer("REVOKE ROLE admin FROM neo4j")
        fun Driver.createUser(name: String) {
            session(SessionConfig.forDatabase(SYSTEM)).use { session ->
                session
                    .run("CREATE USER $name SET PASSWORD '$name-secret-1' CHANGE NOT REQUIRED")
                    .consume()
            }
        }
        val locked = cluster.members[0].routing(AuthTokens.basic("neo4j", "neo4j-secret-1"))
        eventually {
            assertThat(catchThrowable { locked.createUser("bob") })
                .isInstanceOf(ClientException::class.java)
                .hasMessageStartingWith("Permission has not been granted for CREATE USER.")
        }

        // As the operations manual has it: auth disabled on every member, then the command on one.
        fun restart(settings: Map<Setting<*>, Any>) {
            drivers.forEach(Driver::close)
            drivers.clear()
            cluster.members.forEach(ClusterHarness.Member::stop)
            cluster.members.forEach { it.settings = settings }
            cluster.start()
        }
        restart(mapOf(GraphDatabaseSettings.auth_enabled to false))
        val unauthenticated = cluster.members[0].routing(AuthTokens.none())
        // Until the members have elected a system leader, there is no writer to route to.
        eventually {
            unauthenticated.session(SessionConfig.forDatabase(SYSTEM)).use {
                it.run("GRANT ROLE admin TO neo4j").consume()
            }
        }
        restart(emptyMap())

        val admin = cluster.members[0].routing(AuthTokens.basic("neo4j", "neo4j-secret-1"))
        eventually { admin.createUser("bob") }
        for (member in cluster.members) {
            eventually {
                assertThat(member.homeOf("bob", "bob-secret-1")).isEqualTo("neo4j")
                member
                    .driver("neo4j", "neo4j-secret-1")
                    .session(SessionConfig.forDatabase(SYSTEM))
                    .use { session ->
                        assertThat(
                                session
                                    .run("SHOW CURRENT USER YIELD roles")
                                    .single()["roles"]
                                    .asList()
                            )
                            .containsExactly("admin", "PUBLIC")
                    }
            }
        }
    }

    private companion object {
        const val SYSTEM = "system"
    }
}
