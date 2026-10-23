package org.dozendb.cluster.internal.forwarding

import java.nio.file.Path
import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.dozendb.cluster.internal.ClusterHarness
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig

/** Forwarded transactions whose leader stops. */
class ForwardingFailoverTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    private fun ClusterHarness.Member.orders(): Long =
        database().executeTransactionally("MATCH (o:Order) RETURN count(o) AS c", emptyMap()) {
            it.next()["c"] as Long
        }

    private fun eventually(assertion: () -> Unit) {
        await withPollInterval
            Duration.ofMillis(100) atMost
            ClusterHarness.AWAIT untilAsserted
            {
                assertion()
            }
    }

    @Test
    fun `a write whose leader stops before it commits runs again on the new leader, once`() {
        val cluster =
            ClusterHarness(
                    root,
                    3,
                    settings =
                        mapOf(
                            BoltConnector.enabled to true,
                            GraphDatabaseSettings.routing_default_router to
                                GraphDatabaseSettings.RoutingMode.SERVER,
                        ),
                )
                .also { harness = it }
        cluster.start()
        val leader = cluster.awaitLeader()
        val follower = cluster.members.first { it != leader }
        var attempts = 0

        GraphDatabase.driver("neo4j://${follower.boltAddress()}", AuthTokens.none()).use { driver ->
            driver.session(SessionConfig.forDatabase("neo4j")).use { session ->
                session.executeWrite { tx ->
                    attempts++
                    tx.run("CREATE (:Order {id: 1})").consume()
                    if (attempts == 1) {
                        leader.stop()
                        // The others have a new leader before the driver commits.
                        cluster.awaitLeader()
                    }
                }
            }
        }

        assertThat(attempts).isEqualTo(2)
        for (member in cluster.running()) eventually { assertThat(member.orders()).isEqualTo(1) }
        leader.start()
        eventually { assertThat(leader.orders()).isEqualTo(1) }
    }
}
