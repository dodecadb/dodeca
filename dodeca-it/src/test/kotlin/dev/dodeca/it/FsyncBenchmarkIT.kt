package dev.dodeca.it

import java.nio.file.Files
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilNotNull
import org.awaitility.kotlin.withPollInterval
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.condition.EnabledIfSystemProperty
import org.testcontainers.containers.Network
import org.testcontainers.lifecycle.Startables

/**
 * Write throughput of three members with single fsync (the default) and with double fsync. Run on
 * demand, with `-Ddodeca.benchmark=true`; results go to the console and to `fsync-benchmark.txt` in
 * the IT logs directory. Docker on a laptop gives indicative numbers only. It may run for longer
 * than the 10 minutes a test of the suites may take.
 */
@EnabledIfSystemProperty(named = "dodeca.benchmark", matches = "true")
@Timeout(value = 1, unit = TimeUnit.HOURS)
class FsyncBenchmarkIT {
    private data class Result(
        val mode: String,
        val sequential: Double,
        val concurrent: Double,
        val entriesPerCommit: Double,
    )

    @Test
    fun `single fsync against double fsync`() {
        val results = listOf(measure(singleFsync = true), measure(singleFsync = false))
        val report = buildString {
            appendLine(
                "mode, sequential commits/s (1 writer), concurrent commits/s ($WRITERS writers), " +
                    "Raft entries per sequential commit"
            )
            results.forEach {
                appendLine(
                    "${it.mode}, %.0f, %.0f, %.2f"
                        .format(it.sequential, it.concurrent, it.entriesPerCommit)
                )
            }
        }
        print(report)
        val directory = Files.createDirectories(DodecaContainer.logsDirectory)
        Files.writeString(directory.resolve("fsync-benchmark.txt"), report)
    }

    private fun measure(singleFsync: Boolean): Result {
        val aliases = listOf("core1", "core2", "core3")
        Network.newNetwork().use { network ->
            val members = aliases.map { alias ->
                DodecaContainer(DodecaContainer.dodecaImage, alias)
                    .inNetwork(network)
                    .clusterMember(aliases.map { "$it:7700" })
                    .withEnv("NEO4J_server_dodeca_cluster_single__fsync", singleFsync.toString())
            }
            try {
                Startables.deepStart(members).join()
                val leader = awaitLeader(members)
                leader.rows("CREATE (:Warmup)")

                val indexBefore = commitIndex(leader)
                val sequentialStart = System.nanoTime()
                repeat(SEQUENTIAL) { leader.rows("CREATE (:Sequential {i: $it})") }
                val sequential = SEQUENTIAL / seconds(sequentialStart)
                val entriesPerCommit = (commitIndex(leader) - indexBefore).toDouble() / SEQUENTIAL

                val committed = AtomicLong()
                val concurrentStart = System.nanoTime()
                val deadline = concurrentStart + CONCURRENT_DURATION.toNanos()
                List(WRITERS) { writer ->
                        thread {
                            var i = 0
                            while (System.nanoTime() < deadline) {
                                leader.rows("CREATE (:Concurrent {writer: $writer, i: ${i++}})")
                                committed.incrementAndGet()
                            }
                        }
                    }
                    .forEach { it.join() }
                val concurrent = committed.get() / seconds(concurrentStart)
                return Result(
                    if (singleFsync) "single" else "double",
                    sequential,
                    concurrent,
                    entriesPerCommit,
                )
            } finally {
                members.forEach { it.stop() }
            }
        }
    }

    private fun seconds(since: Long): Double = (System.nanoTime() - since) / 1e9

    private fun commitIndex(member: DodecaContainer): Long =
        member
            .rows("CALL dodeca.cluster.status()", "system")
            .single {
                it["database"] == "neo4j"
            }["commitIndex"]
            as Long

    private fun awaitLeader(members: List<DodecaContainer>): DodecaContainer =
        await.pollDelay(Duration.ZERO) withPollInterval
            Duration.ofMillis(200) atMost
            Duration.ofSeconds(60) untilNotNull
            {
                members.singleOrNull { member ->
                    when (val outcome = member.run("CALL dodeca.cluster.status()", "system")) {
                        is Outcome.Rows ->
                            outcome.rows.any { it["database"] == "neo4j" && it["role"] == "LEADER" }
                        is Outcome.Failed -> false
                    }
                }
            }

    private companion object {
        const val SEQUENTIAL = 2_000
        const val WRITERS = 16
        val CONCURRENT_DURATION: Duration = Duration.ofSeconds(30)
    }
}
