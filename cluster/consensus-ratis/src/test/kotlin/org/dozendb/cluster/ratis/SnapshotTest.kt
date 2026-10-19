package org.dozendb.cluster.ratis

import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.dozendb.cluster.api.CommitResult
import org.dozendb.cluster.api.LogPosition
import org.dozendb.cluster.api.Snapshot
import org.dozendb.cluster.api.fake.RecordingStateMachine
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
