package org.dozendb.cluster.internal.routing

import java.nio.file.Path
import java.time.Duration
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.dozendb.cluster.internal.ClusterHarness
import org.dozendb.cluster.internal.ClusterSettings
import org.dozendb.cluster.internal.replication.ReplicatedDatabase
import org.dozendb.cluster.internal.replication.ReplicationService
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.connectors.BoltConnector

/**
 * A new leader tells the other members at once, so that one that does not host the database sends
 * drivers to it within moments, also when the previous leader is gone and still counts as leading
 * as it last reported. Members probe each other only every hour here, so that only the
 * announcements can tell them.
 */
class LeaderAnnouncementsTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private lateinit var cluster: ClusterHarness
    private lateinit var hosts: List<ClusterHarness.Member>
    /** A Free server, which hosts only `system`. */
    private lateinit var router: ClusterHarness.Member

    @BeforeEach
    fun start() {
        cluster =
            ClusterHarness(
                root,
                3,
                settings =
                    mapOf(
                        ClusterSettings.autoEnableFreeServers to false,
                        ClusterSettings.memberRefreshInterval to Duration.ofHours(1),
                        BoltConnector.enabled to true,
                    ),
            )
        cluster.start()
        hosts = cluster.members.take(3)
        administer("CREATE DATABASE films TOPOLOGY 3 PRIMARIES WAIT")
        router =
            cluster.newMember().also {
                it.start()
                administer("CALL dozendb.cluster.addMember('127.0.0.1:${it.raftPort}')")
            }
        routesWritesToTheLeader()
    }

    @AfterEach
    fun stop() {
        cluster.close()
    }

    @Test
    fun `the router sends writes to a member that took over the leadership`() {
        val leader = cluster.awaitLeader(FILMS) { hosts }
        val successor = hosts.first { it != leader }

        assertThat(
                runBlocking {
                    leader
                        .replica(FILMS)
                        .transferLeadership(
                            successor.cluster.self.id,
                            "a test",
                            Duration.ofSeconds(5),
                        )
                }
            )
            .isTrue()

        assertThat(cluster.awaitLeader(FILMS) { hosts }).isEqualTo(successor)
        routesWritesToTheLeader()
    }

    @Test
    fun `the router sends writes to the member elected after the leader stopped`() {
        val leader = cluster.awaitLeader(FILMS) { hosts }

        leader.stop()

        assertThat(cluster.awaitLeader(FILMS) { hosts.filter { it.running } }).isNotEqualTo(leader)
        routesWritesToTheLeader()
    }

    /** Waits until the router's routing table of films names its leader, as the hosts know it. */
    private fun routesWritesToTheLeader() {
        await withPollInterval
            Duration.ofMillis(100) atMost
            ClusterHarness.AWAIT untilAsserted
            {
                val leader = cluster.awaitLeader(FILMS) { hosts.filter { it.running } }
                assertThat(writers()).containsExactly(leader.boltAddress())
            }
    }

    /** The addresses the router's routing table of films sends writes to. */
    private fun writers(): List<Any?> =
        router.database(SYSTEM).executeTransactionally(
            "CALL dbms.routing.getRoutingTable({}, '$FILMS') YIELD servers",
            emptyMap(),
        ) { result ->
            (result.next()["servers"] as List<*>)
                .map { it as Map<*, *> }
                .filter { it["role"] == "WRITE" }
                .flatMap { it["addresses"] as List<*> }
        }

    private fun administer(query: String) {
        cluster
            .awaitLeader(SYSTEM) { hosts.filter { it.running } }
            .database(SYSTEM)
            .executeTransactionally(query)
    }

    private fun ClusterHarness.Member.replica(name: String): ReplicatedDatabase {
        val database = database(name)
        return checkNotNull(
            database.dependencyResolver
                .resolveDependency(ReplicationService::class.java)
                .replica(database.databaseId())
        )
    }

    private companion object {
        const val SYSTEM = "system"
        const val FILMS = "films"
    }
}
