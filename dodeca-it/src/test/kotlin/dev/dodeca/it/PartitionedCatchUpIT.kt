package dev.dodeca.it

import java.time.Duration
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.untilNotNull
import org.awaitility.kotlin.withPollInterval
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestMethodOrder
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.GraphDatabase
import org.testcontainers.containers.Network
import org.testcontainers.lifecycle.Startables

/**
 * Members cut off from Raft while the others discard their logs catch up with a copy once the
 * partition heals, and keep serving reads while the copy is under way. Members run with small Raft
 * log segments and frequent checkpoints, so that logs are discarded after a few megabytes.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class PartitionedCatchUpIT {
    private val network = Network.newNetwork()
    private val aliases = listOf("core1", "core2", "core3")
    private val members = aliases.map { alias ->
        DodecaContainer(DodecaContainer.dodecaImage, alias)
            .inNetwork(network)
            .clusterMember(aliases.map { "$it:7700" })
            // Snapshots follow checkpoints; Neo4j's default interval is 15 minutes.
            .withEnv("NEO4J_db_checkpoint_interval_time", "1s")
            // Segments are raised to twice the largest entry: about 2.1 MB.
            .withEnv("NEO4J_internal_dbms_dodeca_cluster_raft__max__entry__size", "1m")
            .withEnv("NEO4J_internal_dbms_dodeca_cluster_raft__log__segment__size", "1")
    }
    private val faults = mutableListOf<NetworkFaults>()
    private val probes = mutableListOf<() -> List<Read>>()
    private val blob = "x".repeat(200_000)

    @BeforeAll
    fun start() {
        Startables.deepStart(members).join()
    }

    @AfterEach
    fun stopProbes() {
        // Also when a test fails: a probe left running fails whatever test this JVM runs next.
        probes.forEach { it() }
        probes.clear()
    }

    @AfterEach
    fun heal() {
        faults.forEach { it.close() }
        faults.clear()
    }

    @AfterAll
    fun stop() {
        members.forEach {
            it.saveLogs(DodecaContainer.logsDirectory.resolve(javaClass.simpleName))
            it.stop()
        }
        network.close()
    }

    private fun faultsOf(member: DodecaContainer) = NetworkFaults(member).also { faults += it }

    private fun status(member: DodecaContainer, database: String = "neo4j"): Map<String, Any?>? =
        when (val outcome = member.run("CALL dodeca.cluster.status()", "system")) {
            is Outcome.Rows -> outcome.rows.singleOrNull { it["database"] == database }
            is Outcome.Failed -> null
        }

    private fun awaitLeader(database: String = "neo4j"): DodecaContainer =
        await.pollDelay(Duration.ZERO) withPollInterval
            Duration.ofMillis(200) atMost
            Duration.ofSeconds(30) untilNotNull
            {
                members.singleOrNull { status(it, database)?.get("role") == "LEADER" }
            }

    private fun count(member: DodecaContainer, label: String): Long =
        member.rows("MATCH (n:$label) RETURN count(n) AS c").single()["c"] as Long

    private fun awaitSameData(member: DodecaContainer, leader: DodecaContainer) {
        await withPollInterval
            Duration.ofMillis(500) atMost
            Duration.ofMinutes(3) untilAsserted
            {
                for (label in listOf("Blob", "Marker")) {
                    val expected = count(leader, label)
                    assertThat(member.run("MATCH (n:$label) RETURN count(n) AS c"))
                        .describedAs("${member.alias} $label")
                        .isEqualTo(Outcome.Rows(listOf(mapOf("c" to expected))))
                }
            }
    }

    /** Writes blobs on [leader] until its snapshot is [entries] past [index]; returns it. */
    private fun discardLogPast(leader: DodecaContainer, index: Long, entries: Int): Long {
        var written = 0
        return await withPollInterval
            Duration.ofMillis(100) atMost
            Duration.ofMinutes(2) untilNotNull
            {
                leader.rows(
                    "CREATE (:Blob {i: ${written++}, data: \$data})",
                    parameters = mapOf("data" to blob),
                )
                (status(leader)?.get("snapshotIndex") as Long?)?.takeIf { it > index + entries }
            }
    }

    /** A read on one member: when it finished and the count it returned, null if it failed. */
    private data class Read(val at: Long, val count: Long?)

    /** Runs [read] every 100 ms until the returned stop function is called. */
    private fun probe(read: () -> Long?): () -> List<Read> {
        val reads = ConcurrentLinkedQueue<Read>()
        val probing = AtomicBoolean(true)
        val prober = thread {
            while (probing.get()) {
                val count = read()
                reads += Read(System.nanoTime(), count)
                Thread.sleep(100)
            }
        }
        return {
            probing.set(false)
            prober.join()
            reads.toList()
        }
            .also { probes += it }
    }

    /** Reads [label]s on [member] every 100 ms until the returned stop function is called. */
    private fun probe(member: DodecaContainer, label: String): () -> List<Read> = probe {
        val outcome = member.run("MATCH (n:$label) RETURN count(n) AS c")
        (outcome as? Outcome.Rows)?.rows?.single()?.get("c") as Long?
    }

    /** The time of a line of Neo4j's debug log. */
    /** The first index of the system database's Raft log on [member]. */
    private fun firstSystemIndex(member: DodecaContainer): Long =
        member
            .execInContainer("ls", "/data/raft/$SYSTEM_GROUP/current")
            .stdout
            .lines()
            .mapNotNull { SEGMENT.matchEntire(it)?.groupValues?.get(1)?.toLong() }
            .min()

    private fun userCount(member: DodecaContainer): Long? =
        (member.run("SHOW USERS YIELD user RETURN count(*) AS c", "system") as? Outcome.Rows)
            ?.rows
            ?.single()
            ?.get("c") as Long?

    /** What [reads] returned, in ms since [since], where it changed or a read took long. */
    private fun changes(reads: List<Read>, since: Long): List<String> =
        reads
            .filterIndexed { i, read ->
                i == 0 ||
                    read.count != reads[i - 1].count ||
                    read.at - reads[i - 1].at > 1_000_000_000
            }
            .map { "${(it.at - since) / 1_000_000} ms: ${it.count ?: "failed"}" }

    /** The longest time from a failed read to the next successful one, in milliseconds. */
    private fun longestOutage(reads: List<Read>): Long {
        var longest = 0L
        var failedSince: Long? = null
        for (read in reads) {
            if (read.count == null) {
                if (failedSince == null) failedSince = read.at
            } else if (failedSince != null) {
                longest = maxOf(longest, (read.at - failedSince) / 1_000_000)
                failedSince = null
            }
        }
        return longest
    }

    @Test
    @Order(1)
    fun `a follower copies the database over a slow link while it keeps serving reads`() {
        val leader = awaitLeader()
        val behind = members.first { it != leader }
        leader.rows("CREATE (:Marker {at: 'before'})")
        awaitSameData(behind, leader)
        val partition = faultsOf(behind)
        partition.cutRaft()
        val cutAt = status(leader)?.get("commitIndex") as Long
        discardLogPast(leader, cutAt, 50)
        // Copies come from the leader first, then from the other member: slow both.
        for (source in members - behind) {
            faultsOf(source).shapeTo(behind, "rate", "40mbit", "delay", "20ms")
        }

        val reads = probe(behind, "Blob")
        val healedAt = System.nanoTime()
        partition.healRaft()
        awaitSameData(behind, leader)
        val caughtUpAt = System.nanoTime()
        val afterHealing = reads().filter { it.at > healedAt }

        assertThat(behind.debugLog()).contains("Database neo4j now holds Raft snapshot")
        // Blobs were written only while the member was cut off.
        val servedStale = afterHealing.filter { it.count == 0L }.maxOfOrNull { it.at - healedAt }
        assertThat((servedStale ?: 0) / 1_000_000)
            .describedAs(
                "served stale reads while copying, of %d ms until caught up; read %s",
                (caughtUpAt - healedAt) / 1_000_000,
                changes(afterHealing, healedAt),
            )
            .isGreaterThan(5_000)
        assertThat(longestOutage(afterHealing))
            .describedAs(
                "unavailable only to move the copy into place; read %s",
                changes(afterHealing, healedAt),
            )
            .isLessThan(3_000)
        leader.rows("CREATE (:Marker {at: 'after'})")
        awaitSameData(behind, leader)
    }

    @Test
    @Order(2)
    fun `a follower copies system over a slow link while logins and administration keep working`() {
        val leader = awaitLeader("system")
        val behind = members.first { it != leader }
        leader.rows("CREATE USER probe SET PASSWORD 'probe-secret' CHANGE NOT REQUIRED", "system")
        await atMost
            Duration.ofSeconds(30) untilAsserted
            {
                assertThat(userCount(behind)).isEqualTo(userCount(leader))
            }
        val partition = faultsOf(behind)
        partition.cutRaft()
        val cutAt = status(leader, "system")?.get("commitIndex") as Long
        leader.rows("CREATE DATABASE docs", "system")
        // About 2,000 users fill a segment of the system log; four at a time.
        var created = 0
        while (firstSystemIndex(leader) <= cutAt + 1) {
            (0 until 4)
                .map { worker ->
                    thread {
                        repeat(100) {
                            leader.rows(
                                "CREATE USER u${created}_${worker}_$it SET PASSWORD " +
                                    "'user-secret' CHANGE NOT REQUIRED",
                                "system",
                            )
                        }
                    }
                }
                .forEach { it.join() }
            created++
        }
        for (source in members - behind) {
            faultsOf(source).shapeTo(behind, "rate", "4mbit", "delay", "20ms")
        }

        val probeUrl = "bolt://${behind.host}:${behind.getMappedPort(DodecaContainer.BOLT_PORT)}"
        val logins = probe {
            try {
                GraphDatabase.driver(probeUrl, AuthTokens.basic("probe", "probe-secret")).use {
                    it.verifyConnectivity()
                }
                1L
            } catch (e: Exception) {
                null
            }
        }
        val administration = probe { userCount(behind) }
        val healedAt = System.nanoTime()
        partition.healRaft()
        await withPollInterval
            Duration.ofMillis(500) atMost
            Duration.ofMinutes(3) untilAsserted
            {
                assertThat(userCount(behind)).isEqualTo(userCount(leader))
                assertThat(
                        behind.run(
                            "SHOW DATABASE docs YIELD serverID, currentStatus " +
                                "WHERE serverID = \$self RETURN currentStatus",
                            "system",
                            mapOf("self" to behind.memberId()),
                        )
                    )
                    .isEqualTo(Outcome.Rows(listOf(mapOf("currentStatus" to "online"))))
            }
        val usersAfter = checkNotNull(userCount(leader))
        val loginsAfterHealing = logins().filter { it.at > healedAt }
        val readsAfterHealing = administration().filter { it.at > healedAt }

        val log = behind.debugEntries()
        val copyStarted = log.last { "Database system needs Raft entries" in it.message }.time
        val copied = log.last { "Copied database system holding" in it.message }.time
        assertThat(copied.toEpochMilli() - copyStarted.toEpochMilli())
            .describedAs("copy time, long enough to tell copying from being stopped")
            .isGreaterThan(3_000)
        // Stale reads have fewer users than the leader, though not necessarily as few as before
        // the cut: appends the leader sent while the link was cut, which TCP kept, still arrive
        // once it heals, and the member applies them before it finds it needs a copy.
        assertThat(readsAfterHealing.count { it.count != null && it.count < usersAfter })
            .describedAs(
                "stale administration reads while copying; read %s",
                changes(readsAfterHealing, healedAt),
            )
            .isGreaterThan(10)
        assertThat(longestOutage(loginsAfterHealing))
            .describedAs(
                "logins unavailable only to move the copy into place; logged in %s",
                changes(loginsAfterHealing, healedAt),
            )
            .isLessThan(3_000)
        assertThat(longestOutage(readsAfterHealing))
            .describedAs("administration reads unavailable only to move the copy into place")
            .isLessThan(3_000)
    }

    private companion object {
        const val SYSTEM_GROUP = "00000000-0000-0000-0000-000000000001"
        val SEGMENT = Regex("log_(?:inprogress_)?(\\d+)(?:-\\d+)?")
    }
}
