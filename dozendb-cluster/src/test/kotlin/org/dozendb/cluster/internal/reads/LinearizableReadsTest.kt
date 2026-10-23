package org.dozendb.cluster.internal.reads

import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.dozendb.cluster.internal.ClusterHarness
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.TransactionConfig
import org.neo4j.driver.exceptions.Neo4jException

/** Linearizable reads of Bolt clients, which Neo4j's query router runs. */
class LinearizableReadsTest {
    // Kept when the test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path

    @Test
    fun `a follower that cannot reach a majority refuses linearizable reads, not causal ones`() {
        val linearizable =
            TransactionConfig.builder()
                .withMetadata(mapOf("dozendb.consistency" to "linearizable"))
                .build()
        ClusterHarness(root, 3, settings = mapOf(BoltConnector.enabled to true)).use { cluster ->
            cluster.start()
            val leader = cluster.awaitLeader()
            val follower = cluster.members.first { it != leader }
            GraphDatabase.driver("bolt://${follower.boltAddress()}").use { driver ->
                fun read(config: TransactionConfig): Long =
                    driver.session(SessionConfig.forDatabase("neo4j")).use {
                        it.run("RETURN 1 AS one", config).single()["one"].asLong()
                    }
                assertThat(read(linearizable)).isEqualTo(1)

                cluster.members.filter { it != follower }.forEach { it.stop() }

                val refused = catchThrowable { read(linearizable) } as Neo4jException
                assertThat(refused.code())
                    .isEqualTo("Neo.TransientError.Cluster.ReplicationFailure")
                assertThat(refused).hasMessageContaining("could not be confirmed with the leader")
                assertThat(read(TransactionConfig.empty())).isEqualTo(1)
            }
        }
    }
}
