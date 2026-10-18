package org.dozendb.cluster.ratis

import java.net.InetAddress
import java.net.ServerSocket
import java.nio.file.Path
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CountDownLatch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.dozendb.cluster.api.CommitResult
import org.dozendb.cluster.api.ConsensusUnavailableException
import org.dozendb.cluster.api.GroupStatus
import org.dozendb.cluster.api.HostPort
import org.dozendb.cluster.api.LogEntry
import org.dozendb.cluster.api.MemberId
import org.dozendb.cluster.api.ReplicatedStateMachine
import org.dozendb.cluster.api.Role
import org.dozendb.cluster.api.fake.RecordingStateMachine
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class RatisConsensusLogTest {
    @TempDir lateinit var root: Path
    private var cluster: RatisTestCluster? = null

    private fun formCluster(size: Int = 3): RatisTestCluster =
        RatisTestCluster(root, size).also {
            cluster = it
            it.form()
        }

    @AfterEach
    fun stopCluster() {
        cluster?.close()
    }

    private suspend fun RatisTestCluster.Node.write(text: String): CommitResult =
        consensus.submit(cluster!!.database, text.encodeToByteArray())

    @Test
    fun `forms a group and agrees on one leader`() {
        val cluster = formCluster()

        val leader = cluster.awaitLeader()

        cluster.nodes.forEach {
            val status = it.consensus.status(cluster.database)
            assertThat(status.leader).isEqualTo(leader.id)
            assertThat(status.role).isEqualTo(if (it == leader) Role.Leader else Role.Follower)
        }
        assertThat(leader.consensus.members(cluster.database).map { it.id })
            .containsExactlyInAnyOrderElementsOf(cluster.nodes.map { it.id })
    }

    @Test
    fun `the leader commits and every member applies in order`() =
        runBlocking<Unit> {
            val cluster = formCluster()
            val leader = cluster.awaitLeader()

            val results = (1..20).map { leader.write("entry-$it") }

            assertThat(results).allMatch { it is CommitResult.Committed }
            val indexes = results.map { (it as CommitResult.Committed).index }
            assertThat(indexes).isSorted.doesNotHaveDuplicates()
            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    cluster.nodes.forEach {
                        assertThat(it.machine.payloads())
                            .containsExactlyElementsOf((1..20).map { n -> "entry-$n" })
                        assertThat(it.machine.entries().map(LogEntry::index)).isEqualTo(indexes)
                    }
                }
            val term = (results.last() as CommitResult.Committed).term
            assertThat(leader.consensus.status(cluster.database).term).isEqualTo(term)
            assertThat(leader.consensus.status(cluster.database).commitIndex)
                .isGreaterThanOrEqualTo(indexes.last())
        }

    @Test
    fun `the submitter receives the response of the leader's apply`() =
        runBlocking<Unit> {
            val cluster = formCluster()
            val leader = cluster.awaitLeader()

            val result = leader.write("entry") as CommitResult.Committed

            assertThat(result.response.decodeToString()).isEqualTo("applied ${result.index}")
        }

    @Test
    fun `a new leader is writable only after applying the entries before its term`() =
        runBlocking<Unit> {
            val cluster = formCluster(size = 1)
            val node = cluster.nodes.single()
            cluster.awaitLeader()
            node.write("one")
            val last = node.write("two") as CommitResult.Committed
            node.stop()

            val replayed = RecordingStateMachine()
            val gate = CountDownLatch(1)
            val gated =
                object : ReplicatedStateMachine {
                    override fun apply(entry: LogEntry): ByteArray {
                        gate.await()
                        return replayed.apply(entry)
                    }

                    override fun lastAppliedIndex(): Long = replayed.lastAppliedIndex()
                }
            try {
                node.start()
                node.consensus.startGroup(cluster.database, gated, emptyList())

                // Elected, and its term's first entry is committed, but the old entries wait.
                await atMost
                    RatisTestCluster.AWAIT untilAsserted
                    {
                        val status = node.consensus.status(cluster.database)
                        assertThat(status.term).isGreaterThan(last.term)
                        assertThat(status.commitIndex).isGreaterThan(last.index)
                        assertThat(status.role).isEqualTo(Role.Candidate)
                        assertThat(status.leader).isNull()
                    }
            } finally {
                gate.countDown()
            }

            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(node.consensus.role(cluster.database)).isEqualTo(Role.Leader)
                }
            assertThat(replayed.payloads()).containsExactly("one", "two")
        }

    @Test
    fun `entries up to the maximum size commit and larger ones are refused`() =
        runBlocking<Unit> {
            val cluster = formCluster()
            val leader = cluster.awaitLeader()
            val max = leader.consensus.maxEntrySize

            val result = leader.consensus.submit(cluster.database, ByteArray(max))

            assertThat(result).isInstanceOf(CommitResult.Committed::class.java)
            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    cluster.nodes.forEach {
                        assertThat(it.machine.entries().single().payload).hasSize(max)
                    }
                }
            assertThatThrownBy {
                    runBlocking { leader.consensus.submit(cluster.database, ByteArray(max + 1)) }
                }
                .isInstanceOf(IllegalArgumentException::class.java)
        }

    @Test
    fun `followers reject writes and point at the leader`() =
        runBlocking<Unit> {
            val cluster = formCluster()
            val leader = cluster.awaitLeader()
            val follower = cluster.nodes.first { it != leader }

            assertThat(follower.write("rejected")).isEqualTo(CommitResult.NotLeader(leader.id))
            assertThat(cluster.nodes.flatMap { it.machine.payloads() }).isEmpty()
        }

    @Test
    fun `a follower read index covers every committed write`() =
        runBlocking<Unit> {
            val cluster = formCluster()
            val leader = cluster.awaitLeader()
            val follower = cluster.nodes.first { it != leader }
            val committed = (1..5).map { leader.write("entry-$it") as CommitResult.Committed }

            val readIndex = follower.consensus.readIndex(cluster.database)

            assertThat(readIndex).isGreaterThanOrEqualTo(committed.last().index)
            assertThat(follower.machine.lastAppliedIndex())
                .isGreaterThanOrEqualTo(committed.last().index)
        }

    @Test
    fun `probe reports identity, metadata and hosted groups`() =
        runBlocking<Unit> {
            val cluster = formCluster()
            val (a, b) = cluster.nodes

            val info = b.consensus.probe(a.consensus.self.raftAddress)

            assertThat(info.member).isEqualTo(a.consensus.self)
            assertThat(info.metadata).containsEntry("neo4j.version", "5.26.27")
            assertThat(info.groups.keys).containsExactly(cluster.database.id)
            assertThat(info.groups.getValue(cluster.database.id))
                .containsExactlyInAnyOrderElementsOf(cluster.nodes.map { it.consensus.self })
        }

    @Test
    fun `probing an unreachable address fails cleanly`() {
        val cluster = formCluster(size = 1)
        val unused = HostPort("127.0.0.1", 1)

        assertThatThrownBy { runBlocking { cluster.nodes[0].consensus.probe(unused) } }
            .isInstanceOf(ConsensusUnavailableException::class.java)
    }

    @Test
    fun `a new leader takes over writes after the leader stops`() =
        runBlocking<Unit> {
            val cluster = formCluster()
            val first = cluster.awaitLeader()
            first.write("before")

            first.stop()
            val second = cluster.awaitLeader()

            assertThat(second).isNotEqualTo(first)
            assertThat(second.write("after")).isInstanceOf(CommitResult.Committed::class.java)
            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    cluster.running().forEach {
                        assertThat(it.machine.payloads()).containsExactly("before", "after")
                    }
                }
        }

    @Test
    fun `a restarted member recovers its group and does not reapply entries`() =
        runBlocking<Unit> {
            val cluster = formCluster()
            val leader = cluster.awaitLeader()
            val follower = cluster.nodes.first { it != leader }
            leader.write("one")
            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(follower.machine.payloads()).hasSize(1)
                }

            follower.stop()
            leader.write("two")
            follower.start()

            assertThat(follower.consensus.localGroups()).containsExactly(cluster.database.id)
            follower.consensus.startGroup(cluster.database, follower.machine, emptyList())
            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(follower.machine.payloads()).containsExactly("one", "two")
                }
        }

    @Test
    fun `a write appended before leadership is lost has an unknown outcome`() =
        runBlocking<Unit> {
            val cluster = formCluster()
            val leader = cluster.awaitLeader()
            leader.write("committed")

            cluster.nodes.filter { it != leader }.forEach { it.stop() }
            val result = withTimeout(Duration.ofSeconds(20).toMillis()) { leader.write("stranded") }

            assertThat(result).isInstanceOf(CommitResult.OutcomeUnknown::class.java)
            assertThat(leader.consensus.status(cluster.database).role).isNotEqualTo(Role.Leader)
        }

    @Test
    fun `status listeners observe leadership changes`() {
        val cluster = formCluster()
        val leader = cluster.awaitLeader()
        val follower = cluster.nodes.first { it != leader }
        val seen = java.util.concurrent.CopyOnWriteArrayList<GroupStatus>()
        follower.consensus.addStatusListener(cluster.database) { seen += it }

        leader.stop()

        await atMost
            RatisTestCluster.AWAIT untilAsserted
            {
                assertThat(seen.mapNotNull { it.leader }.distinct()).anyMatch { it != leader.id }
            }
    }

    @Test
    fun `removing a member shrinks the group`() =
        runBlocking<Unit> {
            val cluster = formCluster()
            val leader = cluster.awaitLeader()
            val removed = cluster.nodes.first { it != leader }

            leader.consensus.removeMember(cluster.database, removed.id)

            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(leader.consensus.members(cluster.database).map { it.id })
                        .containsExactlyInAnyOrderElementsOf(
                            cluster.nodes.map { it.id } - removed.id
                        )
                }
            assertThat(leader.write("after removal"))
                .isInstanceOf(CommitResult.Committed::class.java)
        }

    @Test
    fun `an apply failure stops the group on that member only`() =
        runBlocking<Unit> {
            val cluster = RatisTestCluster(root, 3).also { cluster = it }
            val broken = cluster.nodes.first()
            val poisoned =
                object : ReplicatedStateMachine {
                    override fun apply(entry: LogEntry): ByteArray {
                        check(entry.payload.decodeToString() != "poison") { "disk on fire" }
                        return broken.machine.apply(entry)
                    }

                    override fun lastAppliedIndex(): Long = broken.machine.lastAppliedIndex()
                }
            cluster.form { if (it == broken) poisoned else it.machine }
            val healthy = cluster.nodes - broken

            // Whoever leads, the entry commits on the healthy majority.
            cluster.awaitLeader().write("poison")

            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(broken.consensus.status(cluster.database).role)
                        .isEqualTo(Role.Inactive)
                }
            val leader = cluster.awaitLeader()
            assertThat(leader).isIn(healthy)
            assertThat(leader.write("after")).isInstanceOf(CommitResult.Committed::class.java)
            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    healthy.forEach {
                        assertThat(it.machine.payloads()).containsExactly("poison", "after")
                    }
                }
            assertThat(broken.machine.payloads()).isEmpty()
        }

    @Test
    fun `a stopped group keeps its state and starts again`() =
        runBlocking<Unit> {
            val cluster = formCluster(size = 1)
            val node = cluster.nodes.single()
            cluster.awaitLeader()
            node.write("one")

            node.consensus.stopGroup(cluster.database)
            assertThat(node.consensus.status(cluster.database).role).isEqualTo(Role.Inactive)
            assertThat(node.consensus.localGroups()).containsExactly(cluster.database.id)

            node.consensus.startGroup(cluster.database, node.machine, emptyList())
            cluster.awaitLeader()
            assertThat(node.write("two")).isInstanceOf(CommitResult.Committed::class.java)
            assertThat(node.machine.payloads()).containsExactly("one", "two")
        }

    @Test
    fun `a port in use fails the start instead of ending the JVM`() {
        ServerSocket(0, 1, InetAddress.getLoopbackAddress()).use { taken ->
            val log =
                RatisConsensusLog(
                    RatisConsensusConfig(
                        memberId = MemberId(UUID.randomUUID()),
                        listenHost = "127.0.0.1",
                        listenPort = taken.localPort,
                        advertisedAddress = null,
                        storageDirectory = root.resolve("taken"),
                    )
                )
            try {
                assertThatThrownBy { log.start() }
                    .hasMessageContaining("Failed to start Grpc server")
            } finally {
                log.close()
            }
        }
        // Ratis' handler would end the JVM on any uncaught exception.
        assertThat(Thread.getDefaultUncaughtExceptionHandler()?.javaClass?.name.orEmpty())
            .doesNotContain("ratis")
    }

    @Test
    fun `starting a group without local state needs bootstrap members`() {
        val cluster = RatisTestCluster(root, 1).also { cluster = it }
        val node = cluster.nodes.single()
        node.start()
        val unknown = cluster.database.copy(id = UUID.randomUUID())

        assertThatThrownBy { node.consensus.startGroup(unknown, node.machine, emptyList()) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }
}
