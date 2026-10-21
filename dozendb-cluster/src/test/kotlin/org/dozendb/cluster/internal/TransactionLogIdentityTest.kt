package org.dozendb.cluster.internal

import java.io.ByteArrayOutputStream
import java.nio.file.Path
import java.security.MessageDigest
import java.time.Duration
import java.time.LocalDate
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread
import kotlin.random.Random
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.dozendb.cluster.internal.durability.PowerLossFileSystem
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.io.ByteUnit
import org.neo4j.io.fs.OutputStreamWritableChannel
import org.neo4j.kernel.impl.transaction.CompleteBatchRepresentation
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo
import org.neo4j.kernel.impl.transaction.log.files.LogFiles
import org.neo4j.storageengine.api.TransactionIdStore

/**
 * Every member's transaction log holds the same transactions, field for field and command for
 * command, with the same checksums, as each comes from the same Raft entry: through leader changes,
 * restarts, catch-up by store copy and power losses. Differential backups depend on it: one from
 * any member continues a backup from any other. Where a log file ends may differ: Neo4j rotates
 * after a batch of appends, and members batch differently.
 */
class TransactionLogIdentityTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    /** What a member's transaction log holds of a transaction. */
    private data class Entry(
        val transactionId: Long,
        val appendIndex: Long,
        val kernelVersion: Byte,
        val timeStarted: Long,
        val latestCommittedWhenStarted: Long,
        val previousChecksum: Int,
        val header: String,
        val commandCount: Int,
        val commands: String,
        val timeCommitted: Long,
        val checksum: Int,
    )

    private fun sha(bytes: ByteArray): String =
        MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { "%02x".format(it) }

    private fun ClusterHarness.Member.checkpoint(name: String) {
        database(name)
            .dependencyResolver
            .resolveDependency(CheckPointer::class.java)
            .forceCheckPoint(SimpleTriggerInfo("test"))
    }

    private fun ClusterHarness.Member.lastTransactionId(name: String): Long =
        database(name)
            .dependencyResolver
            .resolveDependency(TransactionIdStore::class.java)
            .lastCommittedTransactionId

    private fun ClusterHarness.Member.entries(name: String): List<Entry> {
        val resolver = database(name).dependencyResolver
        val logFile = resolver.resolveDependency(LogFiles::class.java).logFile
        val store = resolver.resolveDependency(LogicalTransactionStore::class.java)
        val start = logFile.extractHeader(logFile.lowestLogVersion).startPosition
        val entries = mutableListOf<Entry>()
        store.getCommandBatches(start).use { cursor ->
            while (cursor.next()) {
                val batch = cursor.get() as CompleteBatchRepresentation
                val commands = ByteArrayOutputStream()
                val channel = OutputStreamWritableChannel(commands)
                batch.commandBatch().accept { command ->
                    command.serialize(channel)
                    false
                }
                channel.close()
                val started = batch.startEntry()
                val committed = batch.commitEntry()
                entries +=
                    Entry(
                        committed.txId,
                        started.appendIndex,
                        started.kernelVersion().version(),
                        started.timeWritten,
                        started.lastCommittedTxWhenTransactionStarted,
                        started.previousChecksum,
                        started.additionalHeader?.let { sha(it) } ?: "none",
                        batch.commandBatch().commandCount(),
                        sha(commands.toByteArray()),
                        committed.timeWritten,
                        committed.checksum,
                    )
            }
        }
        return entries
    }

    private fun ClusterHarness.write(
        cypher: String,
        parameters: Map<String, Any> = emptyMap(),
        database: String = ClusterHarness.DEFAULT_DATABASE,
    ) {
        await.ignoreExceptions() withPollInterval
            Duration.ofMillis(200) atMost
            Duration.ofSeconds(60) untilAsserted
            {
                awaitLeader(database).database(database).executeTransactionally(cypher, parameters)
            }
    }

    /** Creates, updates, relabels and deletes nodes and relationships of many property types. */
    private fun ClusterHarness.workload(round: String) {
        write(
            "CREATE (:Person {name: ${'$'}name, born: ${'$'}born, tags: ['a', 'b'], round: ${'$'}round})",
            mapOf("name" to "p-$round", "born" to LocalDate.of(1970, 1, 1), "round" to round),
        )
        repeat(20) { i ->
            write(
                "UNWIND range(1, 50) AS k " +
                    "CREATE (a:A {i: ${'$'}i, k: k, s: ${'$'}s})-[:R {w: k * 0.5}]->(:B {k: k})",
                mapOf("i" to i, "s" to "x".repeat(i * 37)),
            )
        }
        write("MATCH (a:A) WHERE a.k % 7 = 0 SET a:Seven, a.updated = true")
        write("MATCH (a:A) WHERE a.k % 11 = 0 REMOVE a:A")
        write("MATCH (b:B) WHERE b.k % 13 = 0 DETACH DELETE b")
        write("UNWIND range(1, 8000) AS i CREATE (:Big {i: i, s: 'big-' + i})")
        write("CREATE (:Point {p: point({x: 1.5, y: 2.5}), list: [1, 2, 3]})")
    }

    /** Waits until every running member has the same last transaction in each of [names]. */
    private fun ClusterHarness.awaitSame(names: List<String>) {
        await.ignoreExceptions() withPollInterval
            Duration.ofMillis(200) atMost
            Duration.ofSeconds(90) untilAsserted
            {
                for (name in names) {
                    assertThat(running().map { it.lastTransactionId(name) }.distinct())
                        .describedAs("last transactions of $name")
                        .hasSize(1)
                }
            }
    }

    private val blob = "x".repeat(200_000)

    /** Writes and checkpoints until the leader's Raft log of [database] starts after [index]. */
    private fun ClusterHarness.discardLogUpTo(index: Long, database: UUID) {
        var written = 0
        await withPollInterval
            Duration.ofMillis(300) atMost
            Duration.ofSeconds(90) untilAsserted
            {
                write(
                    "CREATE (:Blob {i: ${'$'}i, data: ${'$'}data})",
                    mapOf("i" to written++, "data" to blob),
                )
                running().forEach { it.checkpoint(ClusterHarness.DEFAULT_DATABASE) }
                assertThat(awaitLeader().firstLogIndex(database)).isGreaterThan(index + 1)
            }
    }

    private fun assertIdentical(cluster: ClusterHarness, names: List<String>) {
        cluster.awaitSame(names)
        for (name in names) {
            val members = cluster.running()
            members.forEach { it.checkpoint(name) }
            val (first, others) = members.first() to members.drop(1)
            val entries = first.entries(name)
            assertThat(entries.last().transactionId).isEqualTo(first.lastTransactionId(name))
            for (member in others) {
                assertThat(member.entries(name))
                    .describedAs("the transactions of $name on $member, as on $first")
                    .isEqualTo(entries)
            }
        }
    }

    @Test
    fun `through leader changes, restarts and catch-up by store copy`() {
        val cluster =
            ClusterHarness(
                    root,
                    3,
                    settings =
                        mapOf(
                            GraphDatabaseSettings.logical_log_rotation_threshold to
                                ByteUnit.kibiBytes(256)
                        ),
                ) {
                    it.copy(
                        maxEntrySize = 1 shl 20,
                        logSegmentSize = 1,
                        snapshotInterval = Duration.ofMillis(200),
                    )
                }
                .also {
                    harness = it
                    it.start()
                }

        // Schema and system commands too, and a database created at runtime.
        cluster.workload("1")
        cluster.write("CREATE INDEX person_name FOR (p:Person) ON (p.name)")
        cluster.write("CREATE CONSTRAINT b_k IF NOT EXISTS FOR (b:B) REQUIRE b.k IS NOT NULL")
        cluster.write(
            "CREATE USER alice SET PASSWORD 'secret-secret' CHANGE NOT REQUIRED",
            database = "system",
        )
        cluster.write("CREATE DATABASE other WAIT", database = "system")
        cluster.write("CREATE (:Other {x: 1})", database = "other")

        // A new leader takes writes while the old one is down, then the old one catches up.
        val firstLeader = cluster.awaitLeader()
        firstLeader.stop()
        cluster.workload("2")
        firstLeader.start()
        cluster.awaitSame(listOf("neo4j", "system", "other"))

        // A follower misses entries its leader's Raft log no longer holds, and copies the store.
        val leader = cluster.awaitLeader()
        val behind = cluster.members.first { it != leader }
        val id = behind.database().databaseId().databaseId().uuid()
        behind.stop()
        cluster.discardLogUpTo(checkNotNull(leader.status()).commitIndex, id)
        behind.start()
        cluster.workload("3")

        assertIdentical(cluster, listOf("neo4j", "system", "other"))
    }

    @Test
    fun `through power losses`() {
        val fileSystems = ConcurrentHashMap<Int, PowerLossFileSystem>()
        val cluster =
            ClusterHarness(
                    root,
                    3,
                    { member -> PowerLossFileSystem().also { fileSystems[member.index] = it } },
                    settings =
                        mapOf(
                            GraphDatabaseSettings.logical_log_rotation_threshold to
                                ByteUnit.kibiBytes(256)
                        ),
                ) {
                    it.copy(logSegmentSize = 1, snapshotInterval = Duration.ofMillis(200))
                }
                .also {
                    harness = it
                    it.start()
                }
        val random = Random(42)
        repeat(4) { round ->
            val writing = AtomicBoolean(true)
            val ids = AtomicLong(round * 1_000_000L)
            val writers =
                List(2) {
                    thread {
                        while (writing.get()) {
                            val id = ids.incrementAndGet()
                            runCatching {
                                cluster
                                    .awaitLeader()
                                    .database()
                                    .executeTransactionally(
                                        "CREATE (:Written:Label${id % 50} {id: $id, key${id % 30}: 1})"
                                    )
                            }
                        }
                    }
                }
            Thread.sleep(random.nextLong(500, 1_500))
            // Leaders and followers in turn; the last time with a hole before unforced data.
            val leader = cluster.awaitLeader()
            val victim = if (round % 2 == 0) leader else cluster.running().first { it != leader }
            val fileSystem = fileSystems.getValue(victim.index)
            fileSystem.cutWithUnforcedWrites(
                ClusterHarness.DEFAULT_DATABASE,
                if (round == 3) 16 * 1024L else 1L,
                Duration.ofSeconds(20),
            )
            runCatching { victim.stop() }
            if (round == 3) fileSystem.loseFirstUnforcedSector()
            else fileSystem.loseUnforcedWrites(random)
            victim.start()
            Thread.sleep(500)
            writing.set(false)
            writers.forEach { it.join() }
        }

        assertIdentical(cluster, listOf("neo4j", "system"))
    }
}
