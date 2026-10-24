package org.dozendb.consensus.ratis

import java.net.InetAddress
import java.net.ServerSocket
import java.nio.file.Path
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CountDownLatch
import kotlin.concurrent.thread
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.during
import org.awaitility.kotlin.untilAsserted
import org.dozendb.consensus.CommitResult
import org.dozendb.consensus.ConsensusUnavailableException
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.GroupStatus
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.LeaderPlacement
import org.dozendb.consensus.LogEntry
import org.dozendb.consensus.LogPosition
import org.dozendb.consensus.MemberId
import org.dozendb.consensus.ReplicatedStateMachine
import org.dozendb.consensus.Role
import org.dozendb.consensus.Snapshot
import org.dozendb.consensus.fake.RecordingStateMachine
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
    fun `progress shows each member's indexes and the followers' on the leader`() =
        runBlocking<Unit> {
            val cluster = formCluster()
            val leader = cluster.awaitLeader()
            val committed = (1..5).map { leader.write("entry-$it") as CommitResult.Committed }

            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    val progress = leader.consensus.progress(cluster.database)!!
                    assertThat(progress.leader).isTrue()
                    assertThat(progress.sinceLeaderContactMillis).isZero()
                    assertThat(progress.followerMatchIndexes!!.keys)
                        .containsExactlyInAnyOrderElementsOf(
                            cluster.nodes.filter { it != leader }.map { it.id }
                        )
                    assertThat(progress.followerMatchIndexes!!.values).allMatch {
                        it >= committed.last().index
                    }
                    for (node in cluster.nodes) {
                        val local = node.consensus.progress(cluster.database)!!
                        assertThat(local.appliedIndex)
                            .isGreaterThanOrEqualTo(committed.last().index)
                        assertThat(local.appliedIndex).isLessThanOrEqualTo(local.commitIndex)
                        assertThat(local.commitIndex).isLessThanOrEqualTo(local.appendIndex)
                        assertThat(local.pruneIndex).isLessThanOrEqualTo(local.appendIndex)
                        assertThat(local.term).isEqualTo(committed.last().term)
                        assertThat(local.leaderChanges).isPositive()
                    }
                }
            val follower =
                cluster.nodes.first { it != leader }.consensus.progress(cluster.database)!!
            assertThat(follower.leader).isFalse()
            assertThat(follower.followerMatchIndexes).isNull()
            assertThat(follower.sinceLeaderContactMillis).isNotNull().isLessThan(10_000)
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
                object : ReplicatedStateMachine by replayed {
                    override fun apply(entry: LogEntry): ByteArray {
                        gate.await()
                        return replayed.apply(entry)
                    }
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
                        assertThat(it.machine.entries().map { entry -> entry.payload.size })
                            .containsExactly(max)
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
            // One line that ends with why, without a stack trace.
            .hasMessageNotContaining("\n")
            .hasMessageContaining("Connection refused")
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

            leader.consensus.removeMember(cluster.database, removed.id, CHANGE_TIMEOUT)

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
    fun `an added member is brought up to date before it votes`() =
        runBlocking<Unit> {
            val cluster = RatisTestCluster(root, 4).also { cluster = it }
            val initial = cluster.nodes.take(3)
            cluster.form(initial)
            cluster.awaitLeader().write("before")
            val added = cluster.nodes.last()
            added.start()
            added.consensus.startGroup(
                cluster.database,
                added.machine,
                initial.map { it.consensus.self },
            )
            assertThat(added.consensus.status(cluster.database).role).isEqualTo(Role.Inactive)

            cluster
                .awaitLeader()
                .consensus
                .addMember(cluster.database, added.consensus.self, CHANGE_TIMEOUT)
            // Adding a member again changes nothing.
            initial
                .first()
                .consensus
                .addMember(cluster.database, added.consensus.self, CHANGE_TIMEOUT)
            cluster.awaitLeader().write("after")

            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(added.machine.payloads()).containsExactly("before", "after")
                    for (node in cluster.nodes) {
                        val membership = node.consensus.membership(cluster.database)
                        assertThat(membership.members.map { it.id })
                            .containsExactlyInAnyOrderElementsOf(cluster.nodes.map { it.id })
                        assertThat(membership.settled).isTrue()
                    }
                    assertThat(added.consensus.status(cluster.database).role)
                        .isIn(Role.Follower, Role.Leader)
                }
        }

    @Test
    fun `adding a member that does not run the group fails`() =
        runBlocking<Unit> {
            val cluster = RatisTestCluster(root, 4).also { cluster = it }
            cluster.form(cluster.nodes.take(3))
            val absent = cluster.nodes.last().apply { start() }

            assertThatThrownBy {
                    runBlocking {
                        cluster
                            .awaitLeader()
                            .consensus
                            .addMember(cluster.database, absent.consensus.self, CHANGE_TIMEOUT)
                    }
                }
                .isInstanceOf(ConsensusUnavailableException::class.java)
            assertThat(cluster.awaitLeader().consensus.members(cluster.database)).hasSize(3)
        }

    @Test
    fun `a member can be added to a group as soon as it shows the group`() =
        runBlocking<Unit> {
            // Ratis serves a group before it has started it, and can lose a snapshot notification
            // that arrives meanwhile; the member would then never finish joining.
            val cluster = RatisTestCluster(root, 4).also { cluster = it }
            val initial = cluster.nodes.take(3)
            cluster.form(initial)
            val joining = cluster.nodes.last().apply { start() }
            val members = initial.map { it.consensus.self }
            repeat(10) { n ->
                val database = DatabaseRef("db$n", UUID.randomUUID())
                initial.forEach {
                    it.consensus.startGroup(database, RecordingStateMachine(), members)
                }
                await atMost
                    RatisTestCluster.AWAIT untilAsserted
                    {
                        assertThat(initial.map { it.consensus.role(database) })
                            .contains(Role.Leader)
                    }
                // As a member being added does, it records a copy before it runs the group.
                joining.consensus.seed(database, Snapshot(LogPosition(0, 1), 0))
                val starting = thread {
                    joining.consensus.startGroup(database, RecordingStateMachine(), members)
                }
                try {
                    // Asked without pause, to add it as early as it shows the group.
                    val deadline = System.nanoTime() + RatisTestCluster.AWAIT.toNanos()
                    val peer = joining.consensus.self.raftAddress
                    while (database.id !in initial.first().consensus.probe(peer).groups) {
                        check(System.nanoTime() < deadline) { "$database never showed on $peer" }
                    }
                    initial
                        .first()
                        .consensus
                        .addMember(database, joining.consensus.self, CHANGE_TIMEOUT)
                } finally {
                    starting.join()
                }
            }
        }

    @Test
    fun `a removed follower stops running the group and knows it was removed`() =
        runBlocking<Unit> {
            val cluster = formCluster()
            val leader = cluster.awaitLeader()
            val removed = cluster.nodes.first { it != leader }

            cluster.nodes
                .first { it != removed }
                .consensus
                .removeMember(cluster.database, removed.id, CHANGE_TIMEOUT)

            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(removed.consensus.membership(cluster.database).removed).isTrue()
                    assertThat(removed.consensus.status(cluster.database).role)
                        .isEqualTo(Role.Inactive)
                    assertThat(
                            runBlocking {
                                removed.consensus.probe(removed.consensus.self.raftAddress).groups
                            }
                        )
                        .isEmpty()
                }
            assertThat(cluster.awaitLeader().write("after"))
                .isInstanceOf(CommitResult.Committed::class.java)
            assertThat(removed.machine.payloads()).isEmpty()
            assertThat(leader.consensus.membership(cluster.database).removed).isFalse()
        }

    @Test
    fun `a removed leader hands over to the remaining members`() =
        runBlocking<Unit> {
            val cluster = formCluster()
            val leader = cluster.awaitLeader()

            leader.consensus.removeMember(cluster.database, leader.id, CHANGE_TIMEOUT)

            val next = cluster.awaitLeaderOtherThan(leader)
            assertThat(next.write("after")).isInstanceOf(CommitResult.Committed::class.java)
            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(leader.consensus.membership(cluster.database).removed).isTrue()
                }
            assertThat(next.consensus.members(cluster.database).map { it.id })
                .containsExactlyInAnyOrderElementsOf(cluster.nodes.map { it.id } - leader.id)
        }

    @Test
    fun `an apply failure stops the group on that member only`() =
        runBlocking<Unit> {
            val cluster = RatisTestCluster(root, 3).also { cluster = it }
            val broken = cluster.nodes.first()
            val poisoned =
                object : ReplicatedStateMachine by broken.machine {
                    override fun apply(entry: LogEntry): ByteArray {
                        check(entry.payload.decodeToString() != "poison") { "disk on fire" }
                        return broken.machine.apply(entry)
                    }
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
    fun `a detached member keeps storing entries and applies them once attached again`() =
        runBlocking<Unit> {
            val cluster = formCluster()
            val leader = cluster.awaitLeader()
            val follower = cluster.nodes.first { it != leader }
            leader.write("one")
            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(follower.machine.payloads()).containsExactly("one")
                }

            follower.consensus.detachGroup(cluster.database)
            cluster.nodes.filter { it != follower }.forEach { it.stop() }
            // Alone, the detached member can neither commit nor apply; with it the leader can.
            val other = cluster.nodes.first { it != follower && it != leader }
            other.start()
            other.consensus.startGroup(cluster.database, other.machine, emptyList())
            // The detached member may win the election; it then hands leadership over.
            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(cluster.awaitLeader()).isEqualTo(other)
                }
            assertThat(other.write("two")).isInstanceOf(CommitResult.Committed::class.java)
            assertThat(follower.machine.payloads()).containsExactly("one")

            follower.consensus.startGroup(cluster.database, follower.machine, emptyList())
            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(follower.machine.payloads()).containsExactly("one", "two")
                }
        }

    @Test
    fun `a member that joined without a state machine applies the log once one is attached`() =
        runBlocking<Unit> {
            val cluster = RatisTestCluster(root, 3).also { cluster = it }
            cluster.nodes.forEach { it.start() }
            val members = cluster.nodes.map { it.consensus.self }
            val (joined, others) = cluster.nodes.first() to cluster.nodes.drop(1)
            // A leader without a state machine would answer writes only once one is attached.
            others.forEach { it.consensus.startGroup(cluster.database, it.machine, members) }
            val leader = cluster.awaitLeader()
            joined.consensus.joinGroup(cluster.database, members)
            assertThat(leader.write("one")).isInstanceOf(CommitResult.Committed::class.java)
            assertThat(joined.machine.payloads()).isEmpty()

            joined.consensus.startGroup(cluster.database, joined.machine, emptyList())

            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(joined.machine.payloads()).containsExactly("one")
                }
        }

    @Test
    fun `a removed group leaves no local state and the others continue`() =
        runBlocking<Unit> {
            val cluster = formCluster()
            val leader = cluster.awaitLeader()
            leader.write("one")
            val removed = cluster.nodes.first { it != leader }
            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(removed.machine.payloads()).containsExactly("one")
                }

            removed.consensus.removeGroup(cluster.database)

            assertThat(removed.consensus.localGroups()).doesNotContain(cluster.database.id)
            assertThat(removed.directory.resolve(cluster.database.id.toString())).doesNotExist()
            assertThat(leader.write("two")).isInstanceOf(CommitResult.Committed::class.java)
            assertThat(removed.machine.payloads()).containsExactly("one")
        }

    @Test
    fun `a detached leader hands leadership over`() =
        runBlocking<Unit> {
            val cluster = formCluster()
            val leader = cluster.awaitLeader()
            leader.write("one")

            leader.consensus.detachGroup(cluster.database)

            // Leadership is handed over asynchronously.
            val newLeader = cluster.awaitLeaderOtherThan(leader)
            assertThat(newLeader.write("two")).isInstanceOf(CommitResult.Committed::class.java)
            leader.consensus.startGroup(cluster.database, leader.machine, emptyList())
            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(leader.machine.payloads()).containsExactly("one", "two")
                }
        }

    @Test
    fun `a detached leader hands leadership to the member its placement ranks first`() =
        runBlocking<Unit> {
            val cluster = formCluster()
            val leader = cluster.awaitLeader()
            leader.write("one")
            // The member that configuration order tries last.
            val preferred =
                leader.consensus.members(cluster.database).map { it.id }.last { it != leader.id }
            leader.consensus.leaderPlacement = LeaderPlacement { _, _ -> listOf(preferred) }

            leader.consensus.detachGroup(cluster.database)

            assertThat(cluster.awaitLeaderOtherThan(leader).id).isEqualTo(preferred)
            assertThat(leader.consensus.progress(cluster.database)?.leadershipTransfers)
                .isEqualTo(1)
        }

    @Test
    fun `leadership transfers to a member that has caught up`() =
        runBlocking<Unit> {
            val cluster = formCluster()
            val leader = cluster.awaitLeader()
            leader.write("one")
            val target = cluster.nodes.first { it != leader }

            val transferred =
                leader.consensus.transferLeadership(
                    cluster.database,
                    target.id,
                    "a test",
                    Duration.ofSeconds(10),
                )

            assertThat(transferred).isTrue()
            assertThat(cluster.awaitLeaderOtherThan(leader)).isEqualTo(target)
            assertThat(target.write("two")).isInstanceOf(CommitResult.Committed::class.java)
            assertThat(leader.consensus.progress(cluster.database)?.leadershipTransfers)
                .isEqualTo(1)
            // A member that does not lead has nothing to transfer.
            assertThat(
                    leader.consensus.transferLeadership(
                        cluster.database,
                        target.id,
                        "a test",
                        Duration.ofSeconds(1),
                    )
                )
                .isFalse()
        }

    @Test
    fun `a leader detached without handover keeps leading`() =
        runBlocking<Unit> {
            val cluster = formCluster()
            val leader = cluster.awaitLeader()
            leader.write("one")

            leader.consensus.detachGroup(cluster.database, handOverLeadership = false)

            // Longer than the handover retry interval: a handover would have happened by then.
            await during
                Duration.ofSeconds(3) atMost
                Duration.ofSeconds(10) untilAsserted
                {
                    for (node in cluster.running()) {
                        assertThat(node.consensus.leader(cluster.database))
                            .describedAs("$node")
                            .isEqualTo(leader.id)
                    }
                }
            leader.consensus.startGroup(cluster.database, leader.machine, emptyList())
            assertThat(leader.write("two")).isInstanceOf(CommitResult.Committed::class.java)
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

    private companion object {
        val CHANGE_TIMEOUT: Duration = Duration.ofSeconds(20)
    }
}
