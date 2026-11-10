package org.dozendb.cluster.internal.http

import java.nio.file.Path
import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.dozendb.cluster.internal.ClusterHarness
import org.dozendb.core.DozenDbSettings
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.dbms.systemgraph.InstanceModeConstraint
import org.neo4j.graphdb.config.Setting
import org.neo4j.storageengine.api.TransactionIdStore

/**
 * What the cluster status endpoints of the HTTP server report of a database on each member: the
 * leader takes its writes, the other primaries and the secondaries serve reads, and only the
 * primaries vote. The endpoints themselves run in Neo4j's web server, which `RoutingIT` calls.
 */
class ClusterStatusesTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    private fun ClusterHarness.Member.statuses(): Map<String, DatabaseClusterStatus> =
        database(SYSTEM)
            .dependencyResolver
            .resolveDependency(ClusterStatuses::class.java)
            .statuses()
            .associateBy { it.name }

    private fun ClusterHarness.Member.lastTransactionIndex(): Long =
        database(FILMS)
            .dependencyResolver
            .resolveDependency(TransactionIdStore::class.java)
            .lastCommittedTransaction
            .consensusIndex()

    private fun eventually(assertion: () -> Unit) {
        await withPollInterval
            Duration.ofMillis(100) atMost
            ClusterHarness.AWAIT untilAsserted
            {
                assertion()
            }
    }

    @Test
    fun `primaries and secondaries report the leader, the voters and what they serve`() {
        // The first two members host primaries, the last one only secondaries.
        val cluster = ClusterHarness(root, 3).also { harness = it }
        cluster.members[2].settings =
            mapOf<Setting<*>, Any>(
                DozenDbSettings.initialServerModeConstraint to InstanceModeConstraint.SECONDARY
            )
        cluster.start()
        val primaries = cluster.members.take(2)
        val secondary = cluster.members[2]
        cluster
            .awaitLeader(SYSTEM)
            .database(SYSTEM)
            .executeTransactionally("CREATE DATABASE $FILMS TOPOLOGY 2 PRIMARIES 1 SECONDARY WAIT")
        val leader = cluster.awaitLeader(FILMS) { primaries }
        leader.database(FILMS).executeTransactionally("CREATE (:Film)")
        val id = leader.database(FILMS).databaseId().databaseId().uuid()
        val voters = primaries.map { it.serverId }.sorted()

        // Again until the secondary pulled the write, and every member knows the leader.
        eventually {
            for (member in cluster.members) {
                val core = member != secondary
                val status = member.statuses().getValue(FILMS)
                assertThat(status.copy(votingMembers = status.votingMembers.sorted()))
                    .describedAs("$member")
                    .isEqualTo(
                        DatabaseClusterStatus(
                            name = FILMS,
                            id = id,
                            core = core,
                            running = true,
                            writable = member == leader,
                            lastAppliedRaftIndex = status.lastAppliedRaftIndex,
                            participatingInRaftGroup = core,
                            votingMembers = voters,
                            healthy = true,
                            memberId = member.serverId,
                            leader = leader.serverId,
                        )
                    )
                assertThat(status.lastAppliedRaftIndex)
                    .describedAs("$member")
                    .isGreaterThanOrEqualTo(leader.lastTransactionIndex())
            }
            assertThat(secondary.statuses().getValue(FILMS).lastAppliedRaftIndex)
                .describedAs("the secondary's, the index of its last transaction")
                .isEqualTo(leader.lastTransactionIndex())
        }

        cluster
            .awaitLeader(SYSTEM)
            .database(SYSTEM)
            .executeTransactionally("STOP DATABASE $FILMS WAIT")
        eventually {
            for (member in cluster.members) {
                val status = member.statuses().getValue(FILMS)
                assertThat(status.running to status.writable)
                    .describedAs("$member: running, writable")
                    .isEqualTo(false to false)
            }
        }
    }

    private companion object {
        const val SYSTEM = "system"
        const val FILMS = "films"
    }
}
