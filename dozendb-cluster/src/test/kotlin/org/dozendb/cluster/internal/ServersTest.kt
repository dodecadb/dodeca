package org.dozendb.cluster.internal

import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.dozendb.core.DozenDbSettings
import org.dozendb.core.servers.ServerOptions
import org.dozendb.core.servers.ServerRecord
import org.dozendb.core.servers.ServerRecords
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.kernel.internal.Version

/** SHOW SERVERS and the server commands, as Neo4j Enterprise has them, on a cluster. */
class ServersTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    /** A cluster of three whose members are tagged with their index and `eu`. */
    private fun tagged(): ClusterHarness =
        ClusterHarness(root, 3).also { cluster ->
            harness = cluster
            cluster.members.forEach {
                it.settings = mapOf(DozenDbSettings.initialServerTags to it.tags)
            }
            cluster.start()
        }

    private val ClusterHarness.Member.tags: List<String>
        get() = listOf("member-$index", "eu")

    private val ClusterHarness.Member.serverId: String
        get() = cluster.self.id.uuid.toString()

    /** The server records in this member's system graph, and whether the initial ones are in. */
    private fun ClusterHarness.Member.records(): Pair<Set<ServerRecord>, Boolean> =
        database(SYSTEM).beginTx().use { tx ->
            ServerRecords.records(tx).values.toSet() to ServerRecords.initialServersEnabled(tx)
        }

    private fun ClusterHarness.Member.showServers(): List<Map<String, Any?>> =
        database(SYSTEM).executeTransactionally("SHOW SERVERS YIELD *", emptyMap()) { result ->
            result.asSequence().map { it.toMap() }.toList()
        }

    @Test
    fun `every member of a new cluster is an enabled server, with the tags it started with`() {
        val cluster = tagged()
        val servers = cluster.running().associateBy { it.serverId }

        for (member in cluster.running()) {
            await atMost
                ClusterHarness.AWAIT untilAsserted
                {
                    val rows = member.showServers()
                    assertThat(rows.map { it["serverId"] })
                        .describedAs("on $member")
                        .containsExactlyInAnyOrderElementsOf(servers.keys)
                    for (row in rows) {
                        val server = servers.getValue(row["serverId"] as String)
                        assertThat(row["name"]).isEqualTo(row["serverId"])
                        assertThat(row["state"]).isEqualTo("Enabled")
                        assertThat(row["health"]).isEqualTo("Available")
                        assertThat(row["tags"]).isEqualTo(server.tags)
                        assertThat(row["modeConstraint"]).isEqualTo("NONE")
                        assertThat(row["allowedDatabases"]).isEqualTo(emptyList<String>())
                        assertThat(row["deniedDatabases"]).isEqualTo(emptyList<String>())
                        assertThat(row["hosting"]).isEqualTo(listOf(SYSTEM, NEO4J))
                        assertThat(row["requestedHosting"]).isEqualTo(listOf(SYSTEM, NEO4J))
                        assertThat(row["version"]).isEqualTo(Version.getNeo4jVersion())
                    }
                    assertThat(member.records())
                        .isEqualTo(
                            servers.values
                                .map {
                                    ServerRecord(
                                        it.cluster.self.id.uuid,
                                        options = ServerOptions(it.tags),
                                    )
                                }
                                .toSet() to true
                        )
                }
        }
    }

    @Test
    fun `a member that stopped is unavailable`() {
        val cluster = tagged()
        val leader = cluster.awaitLeader(SYSTEM)
        val stopped = cluster.running().first { it != leader }
        val stoppedId = stopped.serverId

        stopped.stop()

        await atMost
            ClusterHarness.AWAIT untilAsserted
            {
                val rows = leader.showServers().associateBy { it["serverId"] }
                assertThat(rows.getValue(stoppedId)["health"]).isEqualTo("Unavailable")
                assertThat(rows.getValue(stoppedId)["state"]).isEqualTo("Enabled")
                assertThat(rows.getValue(stoppedId)["hosting"]).isEqualTo(emptyList<String>())
                assertThat(rows.getValue(leader.serverId)["health"]).isEqualTo("Available")
            }
    }

    private companion object {
        const val SYSTEM = "system"
        const val NEO4J = "neo4j"
    }
}
