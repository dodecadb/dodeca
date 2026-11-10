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

/**
 * `dbms.cluster.checkConnectivity`: whether this member reaches each other member at the ports
 * Neo4j Enterprise names, DozenDB's Raft port as CLUSTER and RAFT, and its routing connector as
 * INTRA_BOLT.
 */
class ConnectivityTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    private fun ClusterHarness.Member.rows(cypher: String) =
        database("system").executeTransactionally(cypher, emptyMap()) { result ->
            result.asSequence().map { it.toMap() }.toList()
        }

    @Test
    fun `reports each port of each other member, and which do not answer`() {
        val cluster =
            ClusterHarness(root, 3, settings = mapOf(BoltConnector.enabled to true)).also {
                harness = it
            }
        cluster.start()
        val (asking, other, stopped) = cluster.members
        // The routing connectors of the others, which take writes once they reported them.
        await withPollInterval
            Duration.ofMillis(100) atMost
            ClusterHarness.AWAIT untilAsserted
            {
                assertThat(
                        asking.rows("CALL dozendb.cluster.members() YIELD routingAddress").map {
                            it["routingAddress"]
                        }
                    )
                    .hasSize(3)
                    .doesNotContainNull()
            }

        val rows = asking.rows("CALL dbms.cluster.checkConnectivity()")

        assertThat(rows.map { Triple(it["serverId"], it["port-name"], it["result"]) })
            .containsExactlyInAnyOrder(
                *listOf(other, stopped)
                    .flatMap { member ->
                        listOf("CLUSTER", "RAFT", "INTRA_BOLT").map {
                            Triple(member.serverId, it, "OK")
                        }
                    }
                    .toTypedArray()
            )
        val raft = rows.single { it["serverId"] == other.serverId && it["port-name"] == "RAFT" }
        assertThat(raft)
            .containsEntry("name", other.serverId)
            .containsEntry("mode-constraint", "NONE")
            .containsEntry("port-address", "127.0.0.1:${other.raftPort}")

        val stoppedId = stopped.serverId
        stopped.stop()

        val filtered = asking.rows("CALL dbms.cluster.checkConnectivity('RAFT', '$stoppedId')")
        assertThat(filtered).hasSize(1)
        assertThat(filtered.single()["result"]).isNotEqualTo("OK")
        val routing = asking.rows("CALL dbms.cluster.checkConnectivity('INTRA_BOLT', '$stoppedId')")
        assertThat(routing.map { it["result"] }).hasSize(1).doesNotContain("OK")
        val unknown = catchThrowable {
            asking.rows("CALL dbms.cluster.checkConnectivity('DISCOVERY')")
        }
        assertThat(generateSequence(unknown) { it.cause }.mapNotNull { it.message }.toList())
            .anySatisfy { assertThat(it).contains("CLUSTER, INTRA_BOLT, RAFT") }
    }
}
