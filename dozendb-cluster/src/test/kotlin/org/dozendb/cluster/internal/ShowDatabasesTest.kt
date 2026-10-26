package org.dozendb.cluster.internal

import java.nio.file.Path
import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.dozendb.cluster.internal.ClusterHarness.Companion.DEFAULT_DATABASE
import org.dozendb.cluster.internal.replication.ReplicationService
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir

/** SHOW DATABASES as Neo4j Enterprise shows a cluster's databases: a row for each member. */
class ShowDatabasesTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    private fun cluster(): ClusterHarness =
        ClusterHarness(root, 3).also {
            harness = it
            it.start()
        }

    private fun ClusterHarness.Member.showDatabases(columns: String): List<Map<String, Any?>> =
        database(SYSTEM).executeTransactionally("SHOW DATABASES YIELD $columns", emptyMap()) {
            result ->
            result.asSequence().map { it.toMap() }.toList()
        }

    /** The member whose row for [database] on [on] says it is the writer, or null. */
    private fun ClusterHarness.writer(on: ClusterHarness.Member, database: String): String? =
        on.showDatabases("name, serverID, writer")
            .filter { it["name"] == database && it["writer"] == true }
            .map { it["serverID"] as String }
            .singleOrNull()

    @Test
    fun `every member lists each database on every member, the leader as the writer`() {
        val cluster = cluster()
        val leader = cluster.awaitLeader()

        for (member in cluster.running()) {
            val rows = member.showDatabases("name, serverID, writer, currentStatus, role")
            for (database in listOf(SYSTEM, DEFAULT_DATABASE)) {
                val copies = rows.filter { it["name"] == database }
                assertThat(copies.map { it["serverID"] })
                    .describedAs("$database on $member")
                    .containsExactlyInAnyOrderElementsOf(cluster.running().map { it.serverId })
                assertThat(copies.map { it["currentStatus"] }).containsOnly("online")
                assertThat(copies.map { it["role"] }).containsOnly("primary")
            }
            assertThat(cluster.writer(member, DEFAULT_DATABASE)).isEqualTo(leader.serverId)
        }
    }

    @Test
    fun `the writer follows the leadership`() {
        val cluster = cluster()
        val leader = cluster.awaitLeader()
        val target = cluster.running().first { it != leader }

        val replica =
            leader
                .database()
                .dependencyResolver
                .resolveDependency(ReplicationService::class.java)
                .replica(leader.database().databaseId())!!
        assertThat(replica.transferLeadership(target.cluster.self.id, "a test", TRANSFER)).isTrue()

        await atMost
            ClusterHarness.AWAIT untilAsserted
            {
                cluster.running().forEach {
                    assertThat(cluster.writer(it, DEFAULT_DATABASE)).isEqualTo(target.serverId)
                }
            }
    }

    @Test
    fun `a member that stopped shows as unknown, the others with their last transactions`() {
        val cluster = cluster()
        val leader = cluster.awaitLeader()
        leader.database().executeTransactionally("CREATE (:Film)")
        val stopped = cluster.running().first { it != leader }
        val stoppedId = stopped.serverId

        stopped.stop()

        val rows =
            leader
                .showDatabases(
                    "name, serverID, currentStatus, role, lastCommittedTxn, replicationLag"
                )
                .filter { it["name"] == DEFAULT_DATABASE }
                .associateBy { it["serverID"] }
        val gone = rows.getValue(stoppedId)
        assertThat(gone["currentStatus"]).isEqualTo("unknown")
        assertThat(gone["role"]).isEqualTo("unknown")
        assertThat(gone["lastCommittedTxn"]).isNull()
        val led = rows.getValue(leader.serverId)
        assertThat(led["lastCommittedTxn"] as Long).isPositive()
        assertThat(led["replicationLag"]).isEqualTo(0L)
    }

    private companion object {
        const val SYSTEM = "system"
        val TRANSFER: Duration = Duration.ofSeconds(10)
    }
}
