package org.dozendb.cluster.internal

import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.dozendb.cluster.internal.ClusterHarness.Companion.AWAIT
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.connectors.BoltConnector

/**
 * `dbms.quarantineDatabase` in a cluster: the copy on the member it runs on stops there only, and
 * stays stopped, while the others serve the database, until its quarantine is lifted.
 */
class ClusterQuarantineTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    private fun cluster(): ClusterHarness =
        ClusterHarness(root, 3, settings = mapOf(BoltConnector.enabled to true))
            .also { harness = it }
            .apply {
                start()
                administer("CREATE DATABASE films WAIT")
                write("films")
            }

    private fun ClusterHarness.administer(cypher: String) {
        awaitLeader("system").database("system").executeTransactionally(cypher)
    }

    private fun ClusterHarness.write(
        database: String,
        among: () -> List<ClusterHarness.Member> = ::running,
    ) {
        awaitLeader(database, among).database(database).executeTransactionally("CREATE (:Node)")
    }

    private fun ClusterHarness.Member.count(database: String): Long =
        database(database).executeTransactionally(
            "MATCH (n:Node) RETURN count(n) AS count",
            emptyMap(),
        ) {
            it.next()["count"] as Long
        }

    /** Calls `dbms.quarantineDatabase` on this member, on a database it runs. */
    private fun ClusterHarness.Member.quarantine(
        database: String,
        set: Boolean,
        reason: String = "",
        on: String = "neo4j",
    ): Map<String, Any?> =
        database(on).executeTransactionally(
            "CALL dbms.quarantineDatabase(\$database, \$set, \$reason)",
            mapOf("database" to database, "set" to set, "reason" to reason),
        ) {
            it.next().toMap()
        }

    /** The rows of SHOW DATABASE [database] on this member, by server id. */
    private fun ClusterHarness.Member.shown(database: String): Map<String, Map<String, Any?>> =
        database("system").executeTransactionally(
            "SHOW DATABASE $database YIELD serverID, currentStatus, statusMessage, role, writer",
            emptyMap(),
        ) { result ->
            result.asSequence().associate { it["serverID"] as String to it.toMap() }
        }

    /** The ports of the Bolt addresses in this member's routing table of [database], by role. */
    private fun ClusterHarness.Member.routing(database: String): Map<String, List<String>> =
        database().executeTransactionally(
            "CALL dbms.routing.getRoutingTable({}, \$database) YIELD servers",
            mapOf("database" to database),
        ) { result ->
            @Suppress("UNCHECKED_CAST")
            (result.next()["servers"] as List<Map<String, Any>>).associate { server ->
                server["role"] as String to
                    (server["addresses"] as List<String>).map { it.substringAfterLast(':') }
            }
        }

    /** Whether this member's copy of [database] runs; its Raft group may run without it. */
    private fun ClusterHarness.Member.runs(database: String): Boolean =
        checkNotNull(dbms).database(database).isAvailable

    @Test
    fun `a follower's copy in quarantine stops there only, and catches up once lifted`() {
        val cluster = cluster()
        val member = cluster.members.first { it != cluster.awaitLeader("films") }
        val others = { cluster.running().filter { it != member } }

        val result = member.quarantine("films", true, "Inspecting its files")

        assertThat(result).containsEntry("databaseName", "films").containsEntry("quarantined", true)
        val message = result["result"] as String
        assertThat(message).matches("By anonymous at .*Z: Inspecting its files")
        assertThat(member.runs("films")).isFalse()
        await atMost
            AWAIT untilAsserted
            {
                for (asked in cluster.running()) {
                    val rows = asked.shown("films")
                    assertThat(rows.getValue(member.serverId))
                        .describedAs("on $asked")
                        .containsEntry("currentStatus", "quarantined")
                        .containsEntry("statusMessage", message)
                        .containsEntry("role", "unknown")
                        .containsEntry("writer", false)
                    assertThat(others().map { rows.getValue(it.serverId)["currentStatus"] })
                        .containsOnly("online")
                }
            }
        // The others serve it; routing tables send drivers to them only.
        cluster.write("films", others)
        val refused = member.boltAddress().substringAfterLast(':')
        await atMost
            AWAIT untilAsserted
            {
                for (router in cluster.running()) {
                    val table = router.routing("films")
                    assertThat(table["WRITE"].orEmpty() + table["READ"].orEmpty())
                        .describedAs("$router's routing table of films: $table")
                        .doesNotContain(refused)
                }
            }

        // START DATABASE does not start it there; the others follow commands as usual.
        cluster.administer("STOP DATABASE films")
        cluster.administer("START DATABASE films WAIT")
        cluster.write("films", others)
        assertThat(member.runs("films")).isFalse()
        assertThat(member.shown("films").getValue(member.serverId))
            .containsEntry("currentStatus", "quarantined")

        val lifted = member.quarantine("films", false)

        assertThat(lifted).containsEntry("quarantined", false)
        await atMost
            AWAIT untilAsserted
            {
                assertThat(member.shown("films").getValue(member.serverId))
                    .containsEntry("currentStatus", "online")
                assertThat(member.count("films")).isEqualTo(3)
            }
    }

    @Test
    fun `the leader's copy in quarantine hands its leadership over`() {
        val cluster = cluster()
        val leader = cluster.awaitLeader("films")
        val others = { cluster.running().filter { it != leader } }

        leader.quarantine("films", true)

        assertThat(cluster.awaitLeader("films", others)).isNotEqualTo(leader)
        cluster.write("films", others)
        leader.quarantine("films", false)
        await atMost
            AWAIT untilAsserted
            {
                assertThat(leader.runs("films")).isTrue()
                assertThat(leader.count("films")).isEqualTo(2)
            }
    }

    @Test
    fun `a quarantine is kept across a restart, and system's is lifted by one`() {
        val cluster = cluster()
        val member = cluster.members.first { it != cluster.awaitLeader("films") }
        val message = member.quarantine("films", true)["result"]
        member.quarantine("system", true)
        assertThat(member.runs("system")).isFalse()
        // The others administer the cluster.
        cluster.administer("CREATE DATABASE books")

        member.stop()
        member.start()

        await atMost
            AWAIT untilAsserted
            {
                assertThat(member.shown("books").getValue(member.serverId))
                    .containsEntry("currentStatus", "online")
            }
        assertThat(member.shown("films").getValue(member.serverId))
            .containsEntry("currentStatus", "quarantined")
            .containsEntry("statusMessage", message)
        assertThat(member.runs("films")).isFalse()
    }

    @Test
    fun `lifting the quarantine of system brings the member's system graph up to date`() {
        val cluster = cluster()
        val member = cluster.members.first { it != cluster.awaitLeader("system") }
        member.quarantine("system", true)
        cluster.administer("CREATE DATABASE books")

        member.quarantine("system", false)

        await atMost
            AWAIT untilAsserted
            {
                assertThat(member.shown("books").getValue(member.serverId))
                    .containsEntry("currentStatus", "online")
            }
    }
}
