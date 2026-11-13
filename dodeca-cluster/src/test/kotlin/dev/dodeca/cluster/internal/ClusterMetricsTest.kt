package dev.dodeca.cluster.internal

import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.graphdb.config.Setting

/** Each member reports the Raft progress and replication of each database it runs. */
class ClusterMetricsTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    private fun cluster(settings: Map<Setting<*>, Any> = ClusterHarness.METRICS): ClusterHarness =
        ClusterHarness(root, 3, settings = settings).also {
            harness = it
            it.start()
        }

    private fun eventually(assertion: () -> Unit) {
        await.ignoreExceptions() atMost ClusterHarness.AWAIT untilAsserted { assertion() }
    }

    @Test
    fun `every member reports each database's Raft progress, and one of them leads it`() {
        val cluster = cluster()
        val leader = cluster.awaitLeader()
        repeat(3) { leader.database().executeTransactionally("CREATE (:Movie)") }
        val committed = checkNotNull(leader.status()).commitIndex

        for (database in listOf("system", ClusterHarness.DEFAULT_DATABASE)) {
            eventually {
                val samples = cluster.members.associateWith { it.samples() }
                fun ClusterHarness.Member.raft(name: String): Double? =
                    samples.getValue(this)["neo4j_database_${database}_cluster_raft_$name"]
                fun ClusterHarness.Member.dodeca(name: String): Double? =
                    samples.getValue(this)["neo4j_database_${database}_dodeca_cluster_raft_$name"]

                val leaders = cluster.members.filter { it.raft("is_leader") == 1.0 }
                assertThat(leaders).containsExactly(cluster.awaitLeader(database))
                assertThat(cluster.members.map { it.raft("is_leader") }).containsOnly(0.0, 1.0)
                assertThat(cluster.members.map { it.raft("term") }.distinct()).hasSize(1)
                for (member in cluster.members) {
                    val applied = member.raft("applied_index")!!
                    assertThat(applied).isLessThanOrEqualTo(member.raft("commit_index")!!)
                    assertThat(member.raft("commit_index")!!)
                        .isLessThanOrEqualTo(member.raft("append_index")!!)
                    assertThat(member.raft("prune_index")!!)
                        .isLessThanOrEqualTo(member.raft("append_index")!!)
                    assertThat(member.raft("term")).isPositive()
                    assertThat(member.dodeca("leader_changes_total")).isPositive()
                    assertThat(samples.getValue(member)["neo4j_dbms_dodeca_cluster_members"])
                        .isEqualTo(3.0)
                    if (database == ClusterHarness.DEFAULT_DATABASE) {
                        assertThat(applied).isGreaterThanOrEqualTo(committed.toDouble())
                    }
                }

                val lead = leaders.single()
                assertThat(lead.raft("last_leader_message")).isZero()
                assertThat(lead.dodeca("followers")).isEqualTo(2.0)
                assertThat(lead.dodeca("follower_lag_max")).isZero()
                for (follower in cluster.members - lead) {
                    // Leaders send heartbeats every few hundred milliseconds here.
                    assertThat(follower.raft("last_leader_message")).isBetween(0.0, 5_000.0)
                    assertThat(follower.dodeca("followers")).isNull()
                    assertThat(follower.dodeca("follower_lag_max")).isNull()
                }
            }
        }
    }

    @Test
    fun `the leader counts the transactions it replicates and those it cannot`() {
        val cluster = cluster(ClusterHarness.METRICS + SMALL_ENTRIES)
        val leader = cluster.awaitLeader()
        val follower = cluster.members.first { it != leader }
        // Creates the label and property tokens, in transactions of their own.
        leader.database().executeTransactionally("CREATE (:Movie {title: 'first'})")
        fun ClusterHarness.Member.counts(): Map<String, Double> {
            val samples = samples()
            return REPLICATION.associateWith { samples.getValue("neo4j_database_neo4j_$it") }
        }
        fun ClusterHarness.Member.since(before: Map<String, Double>): List<Double> {
            val now = counts()
            return REPLICATION.map { now.getValue(it) - before.getValue(it) }
        }

        val leaderBefore = leader.counts()
        val followerBefore = follower.counts()
        repeat(5) {
            leader.database().executeTransactionally("CREATE (:Movie {title: 'more'})")
        }
        assertThatThrownBy {
                leader
                    .database()
                    .executeTransactionally(
                        "CREATE (:Movie {title: \$title})",
                        mapOf("title" to "x".repeat(2 * MAX_ENTRY_SIZE)),
                    )
            }
            .hasStackTraceContaining("too large to replicate")
        // A follower refuses writes before they reach Raft, as it holds no lease.
        assertThatThrownBy {
            follower.database().executeTransactionally("CREATE (:Movie {title: 'refused'})")
        }

        // New, attempt, success, fail, maybe, then the latency samples.
        assertThat(leader.since(leaderBefore)).containsExactly(6.0, 6.0, 5.0, 1.0, 0.0, 5.0)
        assertThat(follower.since(followerBefore)).containsOnly(0.0)
    }

    companion object {
        private const val MAX_ENTRY_SIZE = 64 shl 10

        /** Raft entries as small as allowed, so that a transaction is easily too large. */
        private val SMALL_ENTRIES: Map<Setting<*>, Any> =
            mapOf(ClusterSettings.raftMaxEntrySize to MAX_ENTRY_SIZE.toLong())

        /** Replication counts of the default database, in Prometheus. */
        private val REPLICATION =
            listOf(
                "cluster_raft_replication_new_total",
                "cluster_raft_replication_attempt_total",
                "cluster_raft_replication_success_total",
                "cluster_raft_replication_fail_total",
                "cluster_raft_replication_maybe_total",
                "dodeca_cluster_replication_latency_millis_count",
            )
    }
}
