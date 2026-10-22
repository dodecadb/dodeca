package org.dozendb.cluster.internal

import java.nio.file.Path
import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
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
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.exceptions.AuthenticationException

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

    private companion object {
        const val SYSTEM = "system"
    }
}
