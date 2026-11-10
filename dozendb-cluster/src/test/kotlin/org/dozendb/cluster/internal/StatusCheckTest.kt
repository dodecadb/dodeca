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
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.monitoring.DatabaseHealth

/**
 * `dbms.cluster.statusCheck`: whether the leader of each database replicates a status check entry
 * to a majority, and which primaries apply it, as Neo4j Enterprise reports with a dummy
 * transaction.
 */
class StatusCheckTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    private fun cluster(): ClusterHarness =
        ClusterHarness(root, 3, settings = mapOf(BoltConnector.enabled to true)).also {
            harness = it
            it.start()
        }

    private fun ClusterHarness.Member.rows(cypher: String) =
        database("system").executeTransactionally(cypher, emptyMap()) { result ->
            result.asSequence().map { it.toMap() }.toList()
        }

    private fun ClusterHarness.Member.statusCheck(
        databases: String = "[]",
        timeoutMillis: Long? = null,
    ) = rows("CALL dbms.cluster.statusCheck($databases${timeoutMillis?.let { ", $it" }.orEmpty()})")

    /** Waits until this member knows the Bolt address of every member, which rows give. */
    private fun ClusterHarness.Member.awaitBoltAddresses() {
        await withPollInterval
            Duration.ofMillis(100) atMost
            ClusterHarness.AWAIT untilAsserted
            {
                assertThat(
                        rows("CALL dozendb.cluster.members() YIELD boltAddress").map {
                            it["boltAddress"]
                        }
                    )
                    .hasSize(3)
                    .doesNotContainNull()
            }
    }

    @Test
    fun `every primary applies the status check of each database it hosts`() {
        val cluster = cluster()
        val leaders = listOf("neo4j", "system").associateWith { cluster.awaitLeader(it) }
        val asking = cluster.members.first { it != leaders["neo4j"] }
        asking.awaitBoltAddresses()

        val rows = asking.statusCheck()

        assertThat(rows.map { it["database"] to it["serverId"] })
            .containsExactlyInAnyOrderElementsOf(
                leaders.keys.flatMap { database -> cluster.members.map { database to it.serverId } }
            )
        for (row in rows) {
            val member = cluster.members.single { it.serverId == row["serverId"] }
            val leader = leaders.getValue(row["database"] as String)
            assertThat(row)
                .describedAs("$row")
                .containsEntry("serverName", member.serverId)
                .containsEntry("address", member.boltAddress())
                .containsEntry("replicationSuccessful", true)
                .containsEntry("memberStatus", "APPLYING")
                .containsEntry("recognisedLeader", leader.serverId)
                .containsEntry(
                    "recognisedLeaderTerm",
                    leader.status(row["database"] as String)?.term,
                )
                .containsEntry("requester", member == asking)
                .containsEntry("error", null)
        }
    }

    @Test
    fun `checks the databases named, and tells which this server does not host as a primary`() {
        val cluster = cluster()
        cluster.awaitLeader()
        val asking = cluster.members[0]

        val rows = asking.statusCheck("['NEO4J', 'films']")

        assertThat(rows.filter { it["database"] == "neo4j" }.map { it["memberStatus"] })
            .containsExactly("APPLYING", "APPLYING", "APPLYING")
        assertThat(rows.single { it["database"] == "films" })
            .containsEntry("serverId", asking.serverId)
            .containsEntry("requester", true)
            .containsEntry("replicationSuccessful", false)
            .containsEntry("memberStatus", null)
            .containsEntry("error", "Database 'films' is not hosted on this server as a primary.")
        val invalid = catchThrowable { asking.statusCheck("[]", 0) }
        assertThat(generateSequence(invalid) { it.cause }.mapNotNull { it.message }.toList())
            .anySatisfy { assertThat(it).contains("'timeoutMilliseconds' must be positive") }
    }

    @Test
    fun `a primary whose database failed replicates the check without applying it`() {
        val cluster = cluster()
        val leader = cluster.awaitLeader()
        val failed = cluster.members.first { it != leader }
        val health =
            failed.database().dependencyResolver.resolveDependency(DatabaseHealth::class.java)

        health.panic(IllegalStateException("failed on purpose"))
        // It stops replicating in the background, keeping its place in the group.
        await atMost
            ClusterHarness.AWAIT untilAsserted
            {
                assertThat(failed.cluster.status().map { it.database.name }).doesNotContain("neo4j")
            }
        val rows = leader.statusCheck("['neo4j']")

        assertThat(rows.associate { it["serverId"] to it["memberStatus"] })
            .isEqualTo(
                cluster.members.associate {
                    it.serverId to if (it == failed) "REPLICATING" else "APPLYING"
                }
            )
        assertThat(rows.map { it["replicationSuccessful"] }).containsOnly(true)
    }

    @Test
    fun `members that do not answer are unavailable, and without a majority the check fails`() {
        val cluster = cluster()
        val leader = cluster.awaitLeader()
        val (first, second) = cluster.members.filter { it != leader }
        val firstId = first.serverId
        val secondId = second.serverId

        first.stop()
        val rows = leader.statusCheck("['neo4j']")

        assertThat(rows.associate { it["serverId"] to it["memberStatus"] })
            .isEqualTo(
                mapOf(
                    leader.serverId to "APPLYING",
                    secondId to "APPLYING",
                    firstId to "UNAVAILABLE",
                )
            )
        assertThat(rows.map { it["replicationSuccessful"] }).containsOnly(true)
        assertThat(rows.single { it["serverId"] == firstId }["error"] as String)
            .startsWith("Server $firstId did not answer: ")

        second.stop()
        val alone = leader.statusCheck("['neo4j']", 500)

        assertThat(alone.map { it["serverId"] })
            .containsExactlyInAnyOrder(leader.serverId, firstId, secondId)
        for (row in alone) {
            assertThat(row)
                .describedAs("$row")
                .containsEntry("replicationSuccessful", false)
                .containsEntry("memberStatus", "UNAVAILABLE")
            assertThat(row["error"] as String?).isNotBlank()
        }
    }
}
