package org.dozendb.core.checkpoint

import java.nio.file.Path
import java.time.Duration
import kotlin.io.path.readText
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.dozendb.core.StandaloneServer
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.GraphDatabaseSettings.CheckpointPolicy
import org.neo4j.io.ByteUnit
import org.neo4j.logging.NullLogProvider
import org.neo4j.storageengine.api.TransactionIdStore
import org.neo4j.time.Clocks
import org.neo4j.wal.LogPosition
import org.neo4j.wal.WALService
import org.neo4j.wal.checkpoint.CheckPointThreshold
import org.neo4j.wal.checkpoint.CheckPointer
import org.neo4j.wal.pruning.LogPruning

/** Neo4j Enterprise's checkpoint policies that Neo4j Community lacks: CONTINUOUS and VOLUMETRIC. */
class CheckpointPoliciesTest {
    @TempDir lateinit var root: Path

    /** Log pruning that might prune when [prunable] says so, and records what it is asked. */
    private class Pruning : LogPruning {
        var prunable = false
        val asked = mutableListOf<Long>()

        override fun pruneLogs(currentVersion: Long) = throw UnsupportedOperationException()

        override fun mightHaveLogsToPrune(upperVersion: Long): Boolean {
            asked += upperVersion
            return prunable
        }

        override fun describeCurrentStrategy() = "test"
    }

    private fun threshold(policy: CheckpointPolicy, pruning: LogPruning = Pruning()) =
        CheckPointThreshold.createThreshold(
            Config.defaults(GraphDatabaseSettings.check_point_policy, policy),
            Clocks.nanoClock(),
            pruning,
            NullLogProvider.getInstance(),
            false,
        )

    private fun CheckPointThreshold.needed(appendIndex: Long, version: Long = 0): String? {
        var reason: String? = null
        val needed = isCheckPointingNeeded(appendIndex, LogPosition(version, 64)) { reason = it }
        assertThat(needed).isEqualTo(reason != null)
        return reason
    }

    @Test
    fun `Neo4j loads both policies by their names`() {
        assertThat(threshold(CheckpointPolicy.CONTINUOUS))
            .isInstanceOf(ContinuousThreshold::class.java)
        assertThat(threshold(CheckpointPolicy.VOLUMETRIC))
            .isInstanceOf(VolumetricThreshold::class.java)
    }

    @Test
    fun `continuous checkpoints whenever transactions were appended since the last checkpoint`() {
        val threshold = threshold(CheckpointPolicy.CONTINUOUS)
        threshold.initialize(5, LogPosition(0, 64))

        assertThat(threshold.needed(5)).isNull()
        assertThat(threshold.needed(6)).isEqualTo("continuous checkpointing")
        threshold.checkPointHappened(6, LogPosition(0, 128))
        assertThat(threshold.needed(6)).isNull()
        assertThat(threshold.needed(7)).isNotNull()
        assertThat(threshold.checkFrequencyMillis()).isEqualTo(100)
    }

    @Test
    fun `volumetric checkpoints when a checkpoint would let the pruning delete logs`() {
        val pruning = Pruning()
        val threshold = threshold(CheckpointPolicy.VOLUMETRIC, pruning)
        threshold.initialize(5, LogPosition(0, 64))

        assertThat(threshold.needed(6, version = 3)).isNull()
        assertThat(pruning.asked).containsExactly(3L)
        pruning.prunable = true
        assertThat(threshold.needed(6, version = 3)).isEqualTo("transaction logs to prune")
        threshold.checkPointHappened(6, LogPosition(3, 64))
        assertThat(threshold.needed(6, version = 3))
            .describedAs("nothing appended since the checkpoint")
            .isNull()
        assertThat(threshold.checkFrequencyMillis()).isEqualTo(10_000)
    }

    @Test
    fun `a database on the continuous policy checkpoints soon after each transaction`() {
        StandaloneServer(
                root,
                mapOf(GraphDatabaseSettings.check_point_policy to CheckpointPolicy.CONTINUOUS),
            )
            .use { server ->
                server.start()
                val database = server.database().dependencyResolver
                val checkPointer = database.resolveDependency(CheckPointer::class.java)
                val committed = database.resolveDependency(TransactionIdStore::class.java)

                server.rows("CREATE (:Film)")

                // The periodic policy would wait 15 minutes.
                await atMost
                    Duration.ofSeconds(30) untilAsserted
                    {
                        assertThat(checkpointed(checkPointer))
                            .isEqualTo(committed.lastCommittedTransactionId)
                    }
                assertThat(server.home.resolve("logs/debug.log").readText())
                    .contains("Scheduled checkpoint for continuous checkpointing")
                    .doesNotContain("Could not load check point policy")
            }
    }

    @Test
    fun `a database on the volumetric policy checkpoints once logs can be pruned`() {
        StandaloneServer(
                root,
                mapOf(
                    GraphDatabaseSettings.check_point_policy to CheckpointPolicy.VOLUMETRIC,
                    GraphDatabaseSettings.logical_log_rotation_threshold to ByteUnit.kibiBytes(128),
                    GraphDatabaseSettings.keep_logical_logs to "keep_none",
                ),
            )
            .use { server ->
                server.start()
                val logFile =
                    server
                        .database()
                        .dependencyResolver
                        .resolveDependency(WALService::class.java)
                        .logFile

                while (logFile.currentLogVersion == 0L) {
                    server.rows("CREATE (:Film {title: \$title})", parameters = big())
                }
                // keep_none keeps the logs from the one holding the entry before the last: log 0
                // can go once log 1 holds two.
                repeat(2) { server.rows("CREATE (:Film)") }

                await atMost
                    Duration.ofSeconds(60) untilAsserted
                    {
                        assertThat(logFile.matchedFiles.minOf(logFile::getLogVersion))
                            .isGreaterThan(0)
                    }
                assertThat(server.home.resolve("logs/debug.log").readText())
                    .contains("Scheduled checkpoint for transaction logs to prune")
                    .doesNotContain("Could not load check point policy")
            }
    }

    private fun big(): Map<String, Any> = mapOf("title" to "x".repeat(200 * 1024))

    private fun checkpointed(checkPointer: CheckPointer) =
        checkPointer.latestCheckPointInfo().highestObservedClosedTransactionId().id()
}
