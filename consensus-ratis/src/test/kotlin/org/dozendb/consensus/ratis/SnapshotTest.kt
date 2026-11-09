package org.dozendb.consensus.ratis

import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.during
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.untilNotNull
import org.dozendb.consensus.CommitResult
import org.dozendb.consensus.LogPosition
import org.dozendb.consensus.ReplicatedStateMachine
import org.dozendb.consensus.Snapshot
import org.dozendb.consensus.fake.RecordingStateMachine
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

/** Snapshots, log compaction and catching up from a copy, on real Ratis members. */
class SnapshotTest {
    @TempDir lateinit var root: Path
    private var cluster: RatisTestCluster? = null

    /**
     * Small segments, so that logs are discarded after a few entries. The group is formed by the
     * first [initial] of [size] members.
     */
    private fun formCluster(size: Int = 3, initial: Int = size): RatisTestCluster =
        RatisTestCluster(root, size) {
                it.copy(maxEntrySize = 1 shl 20, logSegmentSize = 1)
            }
            .also {
                cluster = it
                it.form(it.nodes.take(initial))
            }

    /** Writes until the leader discarded the start of its log, past [index]. */
    private fun RatisTestCluster.discardLogBeyond(leader: RatisTestCluster.Node, index: Long) {
        // Each entry fills a fifth of a segment.
        var written = 0
        await atMost
            RatisTestCluster.AWAIT untilAsserted
            {
                runBlocking { leader.write("entry-${written++}", size = 400_000) }
                assertThat(firstIndex(logDirectory(leader))).isGreaterThan(index + 1)
            }
    }

    @AfterEach
    fun stopCluster() {
        cluster?.close()
    }

    private suspend fun RatisTestCluster.Node.write(text: String, size: Int = 0): Long {
        val payload = text.encodeToByteArray() + ByteArray(size)
        val result = consensus.submit(cluster!!.database, payload)
        return (result as CommitResult.Committed).index
    }

    private fun segments(directory: Path): List<String> =
        Files.list(directory).use { files ->
            files.map { it.fileName.toString() }.filter { it.startsWith("log_") }.sorted().toList()
        }

    /** The index of the first entry in the log in [directory]. */
    private fun firstIndex(directory: Path): Long =
        segments(directory).minOf { name ->
            Regex("""log_(?:inprogress_)?(\d+).*""").matchEntire(name)!!.groupValues[1].toLong()
        }

    @Test
    fun `snapshots follow the second-to-last durable index and survive restarts`() =
        runBlocking<Unit> {
            val cluster = formCluster()
            val leader = cluster.awaitLeader()
            val node = cluster.nodes.first { it != leader }
            node.machine.durable = -1
            repeat(10) { leader.write("entry-$it") }
            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(node.machine.lastAppliedIndex()).isGreaterThanOrEqualTo(10)
                }

            node.machine.makeDurable(5)
            assertThat(node.consensus.latestSnapshot(cluster.database))
                .describedAs("one durable index observed")
                .isNull()
            node.machine.durable = 8
            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(node.consensus.latestSnapshot(cluster.database)?.position?.index)
                        .isEqualTo(5)
                }

            val snapshot = checkNotNull(node.consensus.latestSnapshot(cluster.database))
            assertThat(snapshot.appliedIndex).isEqualTo(5)
            node.stop()
            RatisConsensusLog(node.configForReading()).use {
                assertThat(it.latestSnapshot(cluster.database)).isEqualTo(snapshot)
            }
            node.start()
            node.consensus.startGroup(cluster.database, node.machine, emptyList())
            leader.write("after restart")
            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(node.machine.payloads().last()).isEqualTo("after restart")
                }
            assertThat(node.machine.payloads()).doesNotHaveDuplicates()
        }

    @Test
    fun `a member that stops while taking a snapshot logs no warning`() =
        runBlocking<Unit> {
            val cluster = formCluster(size = 1)
            val node = cluster.awaitLeader()
            node.machine.durable = -1
            val first = (1..3).map { node.write("first-$it") }.last()
            val consensus = node.consensus
            LogCapture().use { log ->
                val closing =
                    synchronized(node.machine) {
                        // The next entry waits to be applied, and a snapshot waits for it.
                        thread(isDaemon = true) {
                            runCatching {
                                runBlocking { consensus.submit(cluster.database, ByteArray(1)) }
                            }
                        }
                        await atMost
                            RatisTestCluster.AWAIT untilAsserted
                            {
                                assertThat(consensus.status(cluster.database).commitIndex)
                                    .isGreaterThan(first)
                            }
                        node.machine.makeDurable(first - 1)
                        // Not makeDurable: the snapshot request that follows holds up the next
                        // read.
                        node.machine.durable = first
                        val maintenance =
                            await atMost
                                RatisTestCluster.AWAIT untilNotNull
                                {
                                    Thread.getAllStackTraces()
                                        .filter { (thread, stack) ->
                                            thread.name == "dozendb-consensus-maintenance" &&
                                                stack.any { it.methodName == "snapshotManagement" }
                                        }
                                        .keys
                                        .singleOrNull()
                                }
                        val closing = thread { node.stop() }
                        maintenance.join(RatisTestCluster.AWAIT.toMillis())
                        assertThat(maintenance.isAlive).isFalse
                        closing
                    }
                closing.join()
                assertThat(log.lines().filter { "Snapshot of" in it }).isEmpty()
            }
        }

    @Test
    fun `a member behind the leader's log installs a copy and catches up`() =
        runBlocking<Unit> {
            val cluster = formCluster()
            val leader = cluster.awaitLeader()
            val behind = cluster.nodes.first { it != leader }
            val before = leader.write("before")
            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(behind.machine.payloads()).containsExactly("before")
                }
            behind.stop()
            cluster.discardLogBeyond(leader, before)

            behind.start()
            behind.consensus.startGroup(cluster.database, behind.machine, emptyList())
            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(behind.machine.snapshotRequests).hasSize(1)
                }
            val (reportedLeader, minimumIndex) = behind.machine.snapshotRequests.single()
            assertThat(reportedLeader?.id).isEqualTo(leader.id)
            assertThat(behind.machine.payloads()).containsExactly("before")

            behind.consensus.detachGroup(cluster.database)
            // As a store copy: the source's snapshot first, then a copy of its state.
            val snapshot = checkNotNull(leader.consensus.currentSnapshot(cluster.database))
            assertThat(snapshot.position.index).isGreaterThanOrEqualTo(minimumIndex)
            behind.machine.replaceWith(leader.machine)
            assertThat(behind.consensus.installSnapshot(cluster.database, snapshot)).isTrue()
            behind.consensus.startGroup(cluster.database, behind.machine, emptyList())
            leader.write("after")

            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(behind.machine.payloads())
                        .isEqualTo(leader.machine.payloads())
                        .endsWith("after")
                }
            assertThat(behind.consensus.latestSnapshot(cluster.database)?.position?.index)
                .isGreaterThanOrEqualTo(snapshot.position.index)
            assertThat(firstIndex(cluster.logDirectory(behind)))
                .isGreaterThan(snapshot.position.index)
        }

    @Test
    fun `a member awaiting a copy applies nothing from its own log meanwhile`() =
        runBlocking<Unit> {
            val cluster = formCluster()
            val leader = cluster.awaitLeader()
            val behind = cluster.nodes.first { it != leader }
            // No snapshot of its own: its whole log is applied again when it restarts.
            behind.machine.durable = -1
            val before = (1..3).map { leader.write("before-$it") }.last()
            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(behind.machine.lastAppliedIndex()).isEqualTo(before)
                }
            behind.stop()
            cluster.discardLogBeyond(leader, before)
            behind.start()
            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(behind.consensus.awaitingCopy(cluster.database)).isTrue
                }

            // Ratis truncates the log of a member awaiting a copy; applying it meanwhile once
            // closed the group there.
            val empty = RecordingStateMachine()
            behind.consensus.startGroup(cluster.database, empty, emptyList())
            assertThat(empty.snapshotRequests).hasSize(1)
            await during
                Duration.ofSeconds(1) atMost
                Duration.ofSeconds(2) untilAsserted
                {
                    assertThat(empty.entries()).isEmpty()
                }

            behind.consensus.detachGroup(cluster.database)
            val snapshot = checkNotNull(leader.consensus.currentSnapshot(cluster.database))
            empty.replaceWith(leader.machine)
            assertThat(behind.consensus.installSnapshot(cluster.database, snapshot)).isTrue()
            behind.consensus.startGroup(cluster.database, empty, emptyList())
            leader.write("after")
            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(empty.payloads()).isEqualTo(leader.machine.payloads())
                }
        }

    @Test
    fun `a member awaiting a copy asks for no snapshot of its own meanwhile`() =
        runBlocking<Unit> {
            val cluster = formCluster()
            val leader = cluster.awaitLeader()
            val behind = cluster.nodes.first { it != leader }
            val others = cluster.nodes - behind
            // No snapshot of its own until it awaits a copy.
            behind.machine.durable = -1
            val before = (1..3).map { leader.write("before-$it") }.last()
            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(behind.machine.lastAppliedIndex()).isEqualTo(before)
                }
            behind.stop()
            cluster.discardLogBeyond(leader, before)
            // Alone, it applies its own log again, so it awaits the copy with entries applied.
            others.forEach { it.stop() }
            behind.start()
            behind.consensus.startGroup(cluster.database, behind.machine, emptyList())
            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(behind.consensus.status(cluster.database).appliedIndex)
                        .isEqualTo(before)
                }
            others.forEach {
                it.start()
                it.consensus.startGroup(cluster.database, it.machine, emptyList())
            }
            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(behind.consensus.awaitingCopy(cluster.database)).isTrue
                }

            LogCapture().use { log ->
                // Its state becomes durable while it awaits the copy, as a database's does; a
                // snapshot would follow the second durable index observed.
                for (durable in listOf(before - 1, before)) {
                    behind.machine.durable = durable
                    await during
                        Duration.ofSeconds(1) atMost
                        Duration.ofSeconds(2) untilAsserted
                        {
                            assertThat(
                                    log.lines().filter {
                                        "Snapshot of" in it || "installation is in progress" in it
                                    }
                                )
                                .isEmpty()
                        }
                }
            }
            assertThat(behind.consensus.latestSnapshot(cluster.database)).isNull()

            behind.consensus.detachGroup(cluster.database)
            val source = cluster.awaitLeader()
            val snapshot = checkNotNull(source.consensus.currentSnapshot(cluster.database))
            behind.machine.replaceWith(source.machine)
            assertThat(behind.consensus.installSnapshot(cluster.database, snapshot)).isTrue()
            behind.machine.durable = null
            behind.consensus.startGroup(cluster.database, behind.machine, emptyList())
            // Snapshots follow the second-to-last durable index observed, so it needs two.
            var written = 0
            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    runBlocking { source.write("after-${written++}") }
                    assertThat(behind.consensus.latestSnapshot(cluster.database)?.position?.index)
                        .describedAs("a snapshot of its own after the copy")
                        .isGreaterThan(snapshot.position.index)
                }
            assertThat(behind.machine.payloads()).doesNotHaveDuplicates()
        }

    @Test
    fun `a member whose log ends just before the leader's installs a copy and catches up`() =
        runBlocking<Unit> {
            val cluster = formCluster()
            val previous = cluster.awaitLeader()
            previous.write("before")
            val behind = cluster.nodes.first { it != previous }
            // Every member holds the whole log, including the entry recording the last commit.
            val end =
                await atMost
                    RatisTestCluster.AWAIT untilNotNull
                    {
                        val applied = cluster.nodes.map { it.consensus.status(cluster.database) }
                        applied
                            .map { it.appliedIndex }
                            .distinct()
                            .singleOrNull()
                            ?.takeIf { index -> applied.all { it.commitIndex == index } }
                    }
            behind.stop()
            // The next term starts a log segment after the last entry the stopped member holds.
            previous.consensus.detachGroup(cluster.database, handOverLeadership = true)
            val leader = cluster.awaitLeaderOtherThan(previous)
            previous.consensus.startGroup(cluster.database, previous.machine, emptyList())
            // Writes until the leader discards that segment, with a snapshot beyond the entry
            // before its log.
            var written = 0
            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    runBlocking { leader.write("after-${written++}") }
                    assertThat(firstIndex(cluster.logDirectory(leader))).isEqualTo(end + 1)
                    assertThat(leader.consensus.latestSnapshot(cluster.database)?.position?.index)
                        .isGreaterThan(end)
                }

            behind.start()
            behind.consensus.startGroup(cluster.database, behind.machine, emptyList())
            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(behind.machine.snapshotRequests).hasSize(1)
                }
            behind.consensus.detachGroup(cluster.database)
            val snapshot = checkNotNull(leader.consensus.currentSnapshot(cluster.database))
            behind.machine.replaceWith(leader.machine)
            assertThat(behind.consensus.installSnapshot(cluster.database, snapshot)).isTrue()
            behind.consensus.startGroup(cluster.database, behind.machine, emptyList())
            leader.write("last")

            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(behind.machine.payloads())
                        .isEqualTo(leader.machine.payloads())
                        .endsWith("last")
                }
        }

    @Test
    fun `a member added after the leader discarded the start of its log installs a copy first`() =
        runBlocking<Unit> {
            val cluster = formCluster(size = 4, initial = 3)
            val leader = cluster.awaitLeader()
            cluster.discardLogBeyond(leader, leader.write("before"))
            val added = cluster.nodes.last().apply { start() }
            added.consensus.startGroup(
                cluster.database,
                added.machine,
                cluster.nodes.take(3).map { it.consensus.self },
            )

            val adding =
                async(Dispatchers.IO) {
                    leader.consensus.addMember(
                        cluster.database,
                        added.consensus.self,
                        CHANGE_TIMEOUT,
                    )
                }
            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(added.machine.snapshotRequests).hasSize(1)
                }
            assertThat(leader.consensus.members(cluster.database)).hasSize(3)
            added.consensus.detachGroup(cluster.database)
            val snapshot = checkNotNull(leader.consensus.currentSnapshot(cluster.database))
            added.machine.replaceWith(leader.machine)
            assertThat(added.consensus.installSnapshot(cluster.database, snapshot)).isTrue()
            added.consensus.startGroup(cluster.database, added.machine, emptyList())
            adding.await()
            leader.write("after")

            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(added.machine.payloads())
                        .isEqualTo(leader.machine.payloads())
                        .endsWith("after")
                }
            assertThat(leader.consensus.members(cluster.database)).hasSize(4)
        }

    @Test
    fun `a member added with a copy that covers the leader's log needs no other`() =
        runBlocking<Unit> {
            val cluster = formCluster(size = 4, initial = 3)
            val leader = cluster.awaitLeader()
            cluster.discardLogBeyond(leader, leader.write("before"))
            val added = cluster.nodes.last().apply { start() }
            // As a store copy: the source's snapshot first, then a copy of its state.
            val snapshot = checkNotNull(leader.consensus.currentSnapshot(cluster.database))
            added.machine.replaceWith(leader.machine)
            added.consensus.seed(cluster.database, snapshot)
            added.consensus.startGroup(
                cluster.database,
                added.machine,
                cluster.nodes.take(3).map { it.consensus.self },
            )
            // The copy's position survives a restart before the member is added.
            added.stop()
            added.start()
            added.consensus.startGroup(cluster.database, added.machine, emptyList())

            leader.consensus.addMember(cluster.database, added.consensus.self, CHANGE_TIMEOUT)
            leader.write("after")

            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(added.machine.payloads())
                        .isEqualTo(leader.machine.payloads())
                        .endsWith("after")
                }
            assertThat(added.machine.snapshotRequests).isEmpty()
            assertThat(added.consensus.latestSnapshot(cluster.database)?.position?.index)
                .isGreaterThanOrEqualTo(snapshot.position.index)
        }

    @Test
    fun `a member that fails to install its copy is asked again`() =
        runBlocking<Unit> {
            val cluster = formCluster(size = 4, initial = 3)
            val leader = cluster.awaitLeader()
            cluster.discardLogBeyond(leader, leader.write("before"))
            val added = cluster.nodes.last().apply { start() }
            val snapshot = checkNotNull(leader.consensus.currentSnapshot(cluster.database))
            added.machine.replaceWith(leader.machine)
            added.consensus.seed(cluster.database, snapshot)
            val failing = FailingOnce(added.machine)
            added.consensus.startGroup(
                cluster.database,
                failing,
                cluster.nodes.take(3).map { it.consensus.self },
            )
            // The next read of its state, when the copy is installed, fails.
            failing.armed.set(true)

            leader.consensus.addMember(cluster.database, added.consensus.self, CHANGE_TIMEOUT)
            leader.write("after")

            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(added.machine.payloads())
                        .isEqualTo(leader.machine.payloads())
                        .endsWith("after")
                }
            assertThat(failing.failures.get()).isEqualTo(1)
            assertThat(added.machine.snapshotRequests).isEmpty()
        }

    /** Delegates to [machine], except that [lastAppliedIndex] fails once after [armed] is set. */
    private class FailingOnce(private val machine: RecordingStateMachine) :
        ReplicatedStateMachine by machine {
        val armed = AtomicBoolean()
        val failures = AtomicInteger()

        override fun lastAppliedIndex(): Long {
            if (armed.compareAndSet(true, false)) {
                failures.incrementAndGet()
                throw IllegalStateException("the database is stopping")
            }
            return machine.lastAppliedIndex()
        }
    }

    @Test
    fun `snapshots are installed only into detached members behind the leader's log`() =
        runBlocking<Unit> {
            val cluster = formCluster()
            val leader = cluster.awaitLeader()
            val node = cluster.nodes.first { it != leader }
            repeat(3) { leader.write("entry-$it") }
            val snapshot = Snapshot(LogPosition(100, 1), 100)

            assertThatThrownBy { node.consensus.installSnapshot(cluster.database, snapshot) }
                .isInstanceOf(IllegalStateException::class.java)
            node.consensus.detachGroup(cluster.database)
            assertThat(node.consensus.installSnapshot(cluster.database, snapshot))
                .describedAs("the local log continues after the state")
                .isFalse()
            assertThat(node.consensus.latestSnapshot(cluster.database)).isNull()
        }

    @Test
    fun `a state machine older than the group's snapshot applies nothing and asks for a copy`() =
        runBlocking<Unit> {
            val cluster = formCluster()
            val leader = cluster.awaitLeader()
            val node = cluster.nodes.first { it != leader }
            // Nothing durable while the entries are applied: the durable index only moves up.
            node.machine.durable = -1
            repeat(4) { leader.write("entry-$it") }
            node.machine.makeDurable(2)
            node.machine.makeDurable(4)
            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(node.consensus.latestSnapshot(cluster.database)?.position?.index)
                        .isEqualTo(2)
                }
            val snapshot = checkNotNull(node.consensus.latestSnapshot(cluster.database))

            node.consensus.detachGroup(cluster.database)
            val restored = RecordingStateMachine()
            node.consensus.startGroup(cluster.database, restored, emptyList())
            leader.write("later")

            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(restored.snapshotRequests)
                        .containsExactly(null to snapshot.position.index)
                }
            assertThat(restored.payloads()).isEmpty()
        }

    /** A consensus log that is never started, to read stored snapshots of a stopped node. */
    private fun RatisTestCluster.Node.configForReading() =
        RatisConsensusConfig(
            memberId = id,
            listenHost = "127.0.0.1",
            listenPort = 0,
            advertisedAddress = null,
            storageDirectory = directory,
        )

    private companion object {
        val CHANGE_TIMEOUT: Duration = Duration.ofSeconds(20)
    }
}
