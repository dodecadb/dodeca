package org.dozendb.cluster.internal

import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
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
import org.neo4j.configuration.Config
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import org.neo4j.io.pagecache.PageCache
import org.neo4j.io.pagecache.tracing.DatabaseFlushEvent
import org.neo4j.kernel.BinarySupportedKernelVersions
import org.neo4j.kernel.impl.transaction.log.LogEntryCursor
import org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder
import org.neo4j.storageengine.api.StorageEngineFactory
import org.neo4j.storageengine.api.TransactionIdStore

/**
 * Members that lose power during writes, with damage to what their transaction logs had not forced,
 * recover every acknowledged write: from their own log up to the damage, from the Raft log after
 * it.
 */
class PowerLossTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    /** Frequent snapshots and small segments: the Raft logs are compacted too. */
    private fun powerLossCluster(systems: MutableMap<Int, PowerLossFileSystem>): ClusterHarness =
        ClusterHarness(
                root,
                3,
                { member -> PowerLossFileSystem().also { systems[member.index] = it } },
            ) {
                it.copy(logSegmentSize = 1, snapshotInterval = Duration.ofMillis(200))
            }
            .also {
                harness = it
                it.start()
            }

    /**
     * Writes on the leader from two threads until closed. Every write creates a label and a
     * property key, so token records change all the time.
     */
    private class Writers(
        private val cluster: ClusterHarness,
        private val ids: AtomicLong = AtomicLong(),
    ) : AutoCloseable {
        val acknowledged: MutableSet<Long> = ConcurrentHashMap.newKeySet()
        private val leader = AtomicReference(cluster.awaitLeader())
        private val running = AtomicBoolean(true)
        private val threads = List(2) { thread(name = "writer-$it") { write() } }

        private fun write() {
            while (running.get()) {
                val id = ids.incrementAndGet()
                try {
                    leader
                        .get()
                        .database()
                        .executeTransactionally("CREATE (:Written:Label$id {id: $id, key$id: 1})")
                    acknowledged += id
                } catch (_: Exception) {
                    // Unknown outcome: it may or may not have committed.
                    runCatching { leader.set(cluster.awaitLeader()) }
                }
            }
        }

        override fun close() {
            running.set(false)
            threads.forEach { it.join() }
        }
    }

    /** Cuts the power of [victim], damages its unforced log writes with [lose], restarts it. */
    private fun ClusterHarness.Member.losePower(
        fs: PowerLossFileSystem,
        minimumUnforcedBytes: Long,
        lose: (PowerLossFileSystem) -> Long,
    ) {
        val unforced =
            fs.cutWithUnforcedWrites(
                ClusterHarness.DEFAULT_DATABASE,
                minimumUnforcedBytes,
                Duration.ofSeconds(20),
            )
        runCatching { stop() }
        val lost = lose(fs)
        assertThat(unforced).describedAs("$this had unforced writes").isTrue
        assertThat(lost).describedAs("bytes $this lost").isPositive()
        start()
    }

    @Test
    fun `members that lose power during writes recover every acknowledged write`() {
        val seed = System.getProperty("dozendb.seed")?.toLong() ?: System.nanoTime()
        // Printed first, for failures anywhere: the random choices repeat with
        // -Ddozendb.seed=<seed>, though the threads' timing does not.
        println("PowerLossTest seed $seed")
        val random = Random(seed)
        val systems = ConcurrentHashMap<Int, PowerLossFileSystem>()
        val cluster = powerLossCluster(systems)
        val acknowledged = mutableSetOf<Long>()
        val ids = AtomicLong()
        repeat(ROUNDS) { round ->
            val writers = Writers(cluster, ids)
            val running = AtomicBoolean(true)
            val checkpoints =
                thread(name = "checkpoints") {
                    while (running.get()) {
                        cluster.running().forEach { member ->
                            runCatching { member.checkpoint() }
                        }
                        Thread.sleep(random.nextLong(100, 1_000))
                    }
                }
            Thread.sleep(random.nextLong(300, 1_500))
            val victim =
                if (round % 2 == 0) cluster.awaitLeader()
                else cluster.running().first { it != cluster.awaitLeader() }

            victim.losePower(checkNotNull(systems[victim.index]), 1) {
                it.loseUnforcedWrites(random)
            }

            Thread.sleep(500)
            writers.close()
            running.set(false)
            checkpoints.join()
            acknowledged += writers.acknowledged
            cluster.awaitSameData(acknowledged, "round $round, seed $seed")
        }
        cluster.assertConsistentStores()
    }

    @Test
    fun `a member whose unforced log has a hole replays the transactions after it from Raft`() {
        val systems = ConcurrentHashMap<Int, PowerLossFileSystem>()
        val cluster = powerLossCluster(systems)
        val writers = Writers(cluster)
        val victim = cluster.running().first { it != cluster.awaitLeader() }

        // A hole right after the last force, with written transactions behind it: recovery
        // stops at the hole and the transactions after it come from the Raft log.
        victim.losePower(checkNotNull(systems[victim.index]), 16 * 1024) {
            it.loseFirstUnforcedSector()
        }

        Thread.sleep(500)
        writers.close()
        cluster.awaitSameData(writers.acknowledged, "after the hole")
        cluster.assertConsistentStores()
    }

    @Test
    fun `a member whose log lost a few bytes right after its checkpoint replays the rest from Raft`() {
        val systems = ConcurrentHashMap<Int, PowerLossFileSystem>()
        val cluster = powerLossCluster(systems)
        val leader = cluster.awaitLeader()
        val victim = cluster.running().first { it != leader }
        val fs = checkNotNull(systems[victim.index])
        val acknowledged = mutableSetOf<Long>()
        fun write(ids: LongRange) {
            for (id in ids) {
                leader.database().executeTransactionally("CREATE (:Written {id: $id})")
                acknowledged += id
            }
        }
        write(1L..5L)
        cluster.awaitSameData(acknowledged, "before the checkpoint")
        // The checkpoint forces the log first, up to the checkpoint; what comes after is unforced.
        victim.checkpoint()
        write(6L..20L)
        cluster.awaitSameData(acknowledged, "after the checkpoint")

        // A hole right after the checkpoint, shorter than the stretch beyond the end of a log in
        // which Neo4j looks for stray bytes, with written transactions behind it.
        fs.cut()
        runCatching { victim.stop() }
        assertThat(fs.loseUnforcedBytes(ClusterHarness.DEFAULT_DATABASE, HOLE)).isEqualTo(HOLE)
        victim.start()

        cluster.awaitSameData(acknowledged, "after the hole")
        cluster.assertConsistentStores()
    }

    @Test
    fun `a member whose log has a hole that Neo4j reads past into garbage replays the rest from Raft`() =
        replaysPastHoleBefore(TOKEN_LOOKALIKE)

    @Test
    fun `a member whose log has a hole before a property command lookalike replays the rest from Raft`() =
        replaysPastHoleBefore(PROPERTY_LOOKALIKE)

    /**
     * Writes [lookalike] as a property value, then makes a hole in a member's log that has Neo4j
     * read it as a command when the member starts, and checks that the member recovers every
     * acknowledged write.
     */
    private fun replaysPastHoleBefore(lookalike: ByteArray) {
        val systems = ConcurrentHashMap<Int, PowerLossFileSystem>()
        val cluster = powerLossCluster(systems)
        val leader = cluster.awaitLeader()
        val victim = cluster.running().first { it != leader }
        val fs = checkNotNull(systems[victim.index])
        val acknowledged = mutableSetOf<Long>()
        fun write(ids: LongRange, value: ByteArray = byteArrayOf()) {
            for (id in ids) {
                leader
                    .database()
                    .executeTransactionally(
                        "CREATE (:Written {id: \$id, value: \$value})",
                        mapOf("id" to id, "value" to value),
                    )
                acknowledged += id
            }
        }
        write(1L..5L)
        cluster.awaitSameData(acknowledged, "before the checkpoint")
        victim.checkpoint()
        write(6L..6L, lookalike)
        write(7L..20L)
        cluster.awaitSameData(acknowledged, "after the checkpoint")

        // A hole from just after the header of the log entry that holds the lookalike up to it,
        // with written transactions behind: Neo4j skips the zeros where the entry's command begins
        // and reads the lookalike as one, as a power loss leaves it now and then (about one sector
        // in a hundred of a log of small transactions). Neo4j asserts on what it reads there,
        // which only tests enable; it stops at the entry as at any garbage.
        fs.cut()
        runCatching { victim.stop() }
        val log = checkNotNull(fs.newestTransactionLog(ClusterHarness.DEFAULT_DATABASE))
        val (from, until) = holeBefore(log, lookalike)
        assertThat(fs.loseUnforcedRange(log, from, until)).isEqualTo(until - from)
        victim.start()

        cluster.awaitSameData(acknowledged, "after the hole")
        cluster.assertConsistentStores()
    }

    /**
     * The bytes of [log] from just after the header of the entry that holds [lookalike], verbatim,
     * up to it: without them Neo4j skips zeros where the entry's command begins and reads
     * [lookalike] as the command.
     */
    private fun holeBefore(log: Path, lookalike: ByteArray): Pair<Long, Long> {
        val bytes = Files.readAllBytes(log)
        val at =
            (0..bytes.size - lookalike.size).firstOrNull { offset ->
                lookalike.indices.all { bytes[offset + it] == lookalike[it] }
            }
        checkNotNull(at) { "$log does not hold the lookalike verbatim" }
        val factory = StorageEngineFactory.defaultStorageEngine().commandReaderFactory()
        val config = Config.defaults()
        val logFile =
            LogFilesBuilder.logFilesBasedOnlyBuilder(log.parent, DefaultFileSystemAbstraction())
                .withCommandReaderFactory(factory)
                .withConfig(config)
                .build()
                .logFile
        val version = log.fileName.toString().substringAfterLast('.').toLong()
        var entry = -1L
        logFile.getReader(logFile.extractHeader(version).startPosition, NO_MORE_CHANNELS).use {
            reader ->
            val entries = VersionAwareLogEntryReader(factory, BinarySupportedKernelVersions(config))
            LogEntryCursor(entries, reader).use { cursor ->
                var position = reader.currentLogPosition.byteOffset
                while (position < at && cursor.next()) {
                    entry = position
                    position = reader.currentLogPosition.byteOffset
                }
            }
        }
        check(entry >= 0) { "no log entry of $log holds the lookalike" }
        return entry + ENTRY_HEADER to at.toLong()
    }

    @Test
    fun `a member that wrote store pages back just before losing power recovers`() {
        val systems = ConcurrentHashMap<Int, PowerLossFileSystem>()
        val cluster = powerLossCluster(systems)
        val writers = Writers(cluster)
        val victim = cluster.running().first { it != cluster.awaitLeader() }
        val fs = checkNotNull(systems[victim.index])

        // Write back the store pages of transactions whose log is not forced yet, then lose the
        // power. The write-back forces the log first, so the store holds nothing that recovery
        // does not find in the log.
        assertThat(
                fs.awaitUnforcedWrites(
                    ClusterHarness.DEFAULT_DATABASE,
                    8 * 1024,
                    Duration.ofSeconds(20),
                )
            )
            .describedAs("unforced writes")
            .isTrue
        victim
            .database()
            .dependencyResolver
            .resolveDependency(PageCache::class.java)
            .flushAndForce(DatabaseFlushEvent.NULL)
        fs.cut()
        runCatching { victim.stop() }
        fs.loseUnforcedWrites(Random(0))
        victim.start()

        Thread.sleep(500)
        writers.close()
        cluster.awaitSameData(writers.acknowledged, "after the write-back")
        cluster.assertConsistentStores()
    }

    private fun ClusterHarness.Member.checkpoint() {
        database()
            .dependencyResolver
            .resolveDependency(CheckPointer::class.java)
            .forceCheckPoint(SimpleTriggerInfo("test"))
    }

    private fun ClusterHarness.Member.written(): List<Long> =
        database().executeTransactionally("MATCH (n:Written) RETURN n.id AS id", emptyMap()) {
            it.stream().map { row -> row["id"] as Long }.toList()
        }

    private fun ClusterHarness.Member.lastTransaction(): Pair<Long, Long> {
        val transaction =
            database()
                .dependencyResolver
                .resolveDependency(TransactionIdStore::class.java)
                .lastCommittedTransaction
        return transaction.id() to transaction.consensusIndex()
    }

    /**
     * Waits until every member holds the same writes, each once, including every acknowledged one,
     * with the same last transaction id and consensus index.
     */
    private fun ClusterHarness.awaitSameData(acknowledged: Set<Long>, description: String) {
        await withPollInterval
            Duration.ofMillis(300) atMost
            Duration.ofSeconds(90) untilAsserted
            {
                val leader = awaitLeader()
                val expected = leader.written()
                assertThat(expected).describedAs("$description: duplicates").doesNotHaveDuplicates()
                assertThat(expected)
                    .describedAs("$description: acknowledged writes")
                    .containsAll(acknowledged)
                for (member in members) {
                    assertThat(member.written())
                        .describedAs("$description: $member")
                        .containsExactlyInAnyOrderElementsOf(expected)
                    assertThat(member.lastTransaction())
                        .describedAs("$description: $member last transaction id and Raft index")
                        .isEqualTo(leader.lastTransaction())
                }
            }
    }

    /** Stops every member and runs Neo4j's consistency check on each of its stores. */
    private fun ClusterHarness.assertConsistentStores() {
        members.forEach { it.stop() }
        for (member in members) {
            for (database in listOf(ClusterHarness.DEFAULT_DATABASE, "system")) {
                val result = member.checkConsistency(database)
                assertThat(result.isSuccessful)
                    .describedAs(
                        "$member $database: ${result.summary()}, see ${result.reportFile()}"
                    )
                    .isTrue
            }
        }
    }

    private companion object {
        const val ROUNDS = 6

        /** Bytes lost after a checkpoint: less than the 1 KiB Neo4j checks beyond a log's end. */
        const val HOLE = 600L

        /** A log entry's version and type, before its command. */
        const val ENTRY_HEADER = 2

        /**
         * Bytes that Neo4j reads as a label token command whose name record has -1 bytes: the
         * command type, the token id, then the record before the change: in use, its name id, one
         * name record (id, type, in use, -1 bytes, no next record). Its integers read the same in
         * either byte order. A byte array property holds them verbatim.
         */
        val TOKEN_LOOKALIKE =
            byteArrayOf(8) +
                byteArrayOf(1, 0, 0, 1) +
                byteArrayOf(1) +
                byteArrayOf(1, 0, 0, 1) +
                byteArrayOf(1, 0, 0, 1) +
                byteArrayOf(1, 0, 0, 0, 0, 0, 0, 1) +
                byteArrayOf(1, 0, 0, 1) +
                byteArrayOf(1) +
                ByteArray(4) { -1 } +
                ByteArray(8) { -1 }

        /**
         * Bytes that Neo4j reads as a property command whose record before the change, not in use
         * and without blocks, lists a deleted name record that is in use, which
         * `PropertyRecord.addDeletedRecord` asserts against: the command type, the record id, then
         * that record (flags, next, previous and owner -1, no blocks, deleted records, the first:
         * id, type, in use, no bytes, no next block). Its integers read the same in either byte
         * order.
         */
        val PROPERTY_LOOKALIKE =
            byteArrayOf(2) +
                byteArrayOf(1, 0, 0, 0, 0, 0, 0, 1) +
                byteArrayOf(0) +
                ByteArray(24) { -1 } +
                byteArrayOf(0) +
                byteArrayOf(1, 0, 0, 1) +
                byteArrayOf(1, 0, 0, 0, 0, 0, 0, 1) +
                byteArrayOf(1, 0, 0, 1) +
                byteArrayOf(1) +
                ByteArray(4) +
                ByteArray(8) { -1 }
    }
}
