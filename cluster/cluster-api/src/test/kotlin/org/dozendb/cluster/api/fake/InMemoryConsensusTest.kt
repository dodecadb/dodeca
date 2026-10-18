package org.dozendb.cluster.api.fake

import java.util.UUID
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.dozendb.cluster.api.CommitResult
import org.dozendb.cluster.api.ConsensusUnavailableException
import org.dozendb.cluster.api.DatabaseRef
import org.dozendb.cluster.api.GroupStatus
import org.dozendb.cluster.api.LogPosition
import org.dozendb.cluster.api.Role
import org.dozendb.cluster.api.Snapshot
import org.junit.jupiter.api.Test

class InMemoryConsensusTest {
    private val consensus = InMemoryConsensus()
    private val database = DatabaseRef("neo4j", UUID.randomUUID())
    private val a = consensus.newMember(metadata = mapOf("neo4j.version" to "5.26.27"))
    private val b = consensus.newMember()
    private val c = consensus.newMember()
    private val all = listOf(a, b, c)
    private val machines = all.associate { it.self.id to RecordingStateMachine() }

    private fun startAll() {
        all.forEach {
            it.start()
            it.startGroup(database, machines.getValue(it.self.id), all.map { m -> m.self })
        }
    }

    @Test
    fun `leader commits and every running member applies in order`() =
        runBlocking<Unit> {
            startAll()
            consensus.electLeader(database, a.self.id)

            assertThat(a.submit(database, "one".encodeToByteArray()))
                .isEqualTo(CommitResult.Committed(1, 1, "applied 1".encodeToByteArray()))
            assertThat(a.submit(database, "two".encodeToByteArray()))
                .isEqualTo(CommitResult.Committed(2, 1, "applied 2".encodeToByteArray()))

            machines.values.forEach { assertThat(it.payloads()).containsExactly("one", "two") }
            assertThat(b.status(database))
                .isEqualTo(GroupStatus(database, Role.Follower, a.self.id, 1, 2, 2))
        }

    @Test
    fun `followers reject writes and name the leader`() =
        runBlocking<Unit> {
            startAll()
            consensus.electLeader(database, a.self.id)

            assertThat(b.submit(database, byteArrayOf(1)))
                .isEqualTo(CommitResult.NotLeader(a.self.id))
            assertThat(consensus.committedEntries(database)).isEmpty()
        }

    @Test
    fun `losing a majority makes the outcome unknown`() =
        runBlocking<Unit> {
            startAll()
            consensus.electLeader(database, a.self.id)
            consensus.crash(b.self.id)
            assertThat(a.submit(database, byteArrayOf(1)))
                .isInstanceOf(CommitResult.Committed::class.java)

            consensus.crash(c.self.id)

            assertThat(a.submit(database, byteArrayOf(2)))
                .isInstanceOf(CommitResult.OutcomeUnknown::class.java)
            assertThatThrownBy { runBlocking { a.readIndex(database) } }
                .isInstanceOf(ConsensusUnavailableException::class.java)
        }

    @Test
    fun `a restarted member replays the log without applying entries twice`() =
        runBlocking<Unit> {
            startAll()
            consensus.electLeader(database, a.self.id)
            a.submit(database, "one".encodeToByteArray())
            consensus.crash(c.self.id)
            a.submit(database, "two".encodeToByteArray())

            c.start()
            c.startGroup(database, machines.getValue(c.self.id), emptyList())

            assertThat(machines.getValue(c.self.id).payloads()).containsExactly("one", "two")
            assertThat(c.localGroups()).containsExactly(database.id)
        }

    @Test
    fun `status listeners observe elections and crashes`() {
        startAll()
        val seen = mutableListOf<Role>()
        b.addStatusListener(database) { seen += it.role }
        val removed = c.addStatusListener(database) { error("unregistered listener called") }
        removed.close()

        consensus.electLeader(database, b.self.id)
        consensus.crash(b.self.id)

        assertThat(seen).containsExactly(Role.Leader, Role.Inactive)
        assertThat(a.status(database).leader).isNull()
    }

    @Test
    fun `probe reports identity, metadata and hosted groups`() =
        runBlocking<Unit> {
            startAll()

            val info = b.probe(a.self.raftAddress)

            assertThat(info.member).isEqualTo(a.self)
            assertThat(info.metadata).containsEntry("neo4j.version", "5.26.27")
            assertThat(info.groups).containsOnlyKeys(database.id)
            assertThat(info.groups.getValue(database.id))
                .containsExactlyElementsOf(all.map { it.self })
        }

    @Test
    fun `a member refuses a bootstrap configuration that conflicts with the group`() {
        a.start()
        a.startGroup(database, RecordingStateMachine(), listOf(a.self, b.self, c.self))
        b.start()

        assertThatThrownBy { b.startGroup(database, RecordingStateMachine(), listOf(b.self)) }
            .isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("conflicting bootstrap configuration")
    }

    @Test
    fun `injected failures are returned once`() =
        runBlocking<Unit> {
            startAll()
            consensus.electLeader(database, a.self.id)
            val failure = CommitResult.OutcomeUnknown(RuntimeException("leader lost"))
            consensus.failNextSubmit(database, failure)

            assertThat(a.submit(database, byteArrayOf(1))).isSameAs(failure)
            assertThat(a.submit(database, byteArrayOf(2)))
                .isEqualTo(CommitResult.Committed(1, 1, "applied 1".encodeToByteArray()))
        }

    @Test
    fun `membership changes need a leader`() =
        runBlocking<Unit> {
            startAll()
            val d = consensus.newMember()
            assertThatThrownBy { runBlocking { a.addMember(database, d.self) } }
                .isInstanceOf(ConsensusUnavailableException::class.java)

            consensus.electLeader(database, a.self.id)
            a.addMember(database, d.self)
            a.removeMember(database, c.self.id)

            assertThat(a.members(database)).containsExactly(a.self, b.self, d.self)
        }

    @Test
    fun `bulk transfers stream a source's output and report missing sources`() {
        all.forEach { it.start() }
        a.registerBulkSource("echo") { request, out -> out.write(request + request) }

        val answer =
            b.fetchBulk(a.self.raftAddress, "echo", "ab".encodeToByteArray()) {
                it.readAllBytes().decodeToString()
            }

        assertThat(answer).isEqualTo("abab")
        assertThatThrownBy {
                b.fetchBulk(a.self.raftAddress, "missing", ByteArray(0)) { it.read() }
            }
            .isInstanceOf(ConsensusUnavailableException::class.java)
    }

    @Test
    fun `probes report the metadata current at the time of the probe`() =
        runBlocking<Unit> {
            all.forEach { it.start() }
            a.metadata = mapOf("dozendb.storesReady" to "true")

            assertThat(b.probe(a.self.raftAddress).metadata)
                .containsEntry("dozendb.storesReady", "true")
        }

    @Test
    fun `the current snapshot is the last entry delivered and the applied index`() =
        runBlocking<Unit> {
            startAll()
            consensus.electLeader(database, a.self.id)
            a.submit(database, "one".encodeToByteArray())
            a.submit(database, "two".encodeToByteArray())

            assertThat(b.currentSnapshot(database)).isEqualTo(Snapshot(LogPosition(2, 1), 2))
            b.detachGroup(database)
            assertThat(b.currentSnapshot(database)).isNull()
        }

    @Test
    fun `a member behind a compacted log is asked for a snapshot and catches up with one`() =
        runBlocking<Unit> {
            startAll()
            consensus.electLeader(database, a.self.id)
            consensus.crash(c.self.id)
            repeat(5) { a.submit(database, "e$it".encodeToByteArray()) }
            consensus.compact(database, 5)
            a.submit(database, "e5".encodeToByteArray())

            c.start()
            val behind = machines.getValue(c.self.id)
            c.startGroup(database, behind, emptyList())

            assertThat(behind.snapshotRequests).containsExactly(a.self to 5L)
            assertThat(behind.payloads()).isEmpty()

            c.detachGroup(database)
            behind.replaceWith(machines.getValue(a.self.id))
            assertThat(c.installSnapshot(database, checkNotNull(a.currentSnapshot(database))))
                .isTrue()
            c.startGroup(database, behind, emptyList())
            a.submit(database, "e6".encodeToByteArray())

            assertThat(behind.payloads()).containsExactly("e0", "e1", "e2", "e3", "e4", "e5", "e6")
            assertThat(c.latestSnapshot(database)).isEqualTo(Snapshot(LogPosition(6, 1), 6))
        }

    @Test
    fun `a restarted member that applied the discarded entries needs no snapshot`() =
        runBlocking<Unit> {
            startAll()
            consensus.electLeader(database, a.self.id)
            repeat(3) { a.submit(database, "e$it".encodeToByteArray()) }
            consensus.compact(database, 3)
            b.detachGroup(database)
            a.submit(database, "e3".encodeToByteArray())

            val machine = machines.getValue(b.self.id)
            b.startGroup(database, machine, emptyList())

            assertThat(machine.snapshotRequests).isEmpty()
            assertThat(machine.payloads()).containsExactly("e0", "e1", "e2", "e3")
        }

    @Test
    fun `snapshots are installed only into detached members that need one`() =
        runBlocking<Unit> {
            startAll()
            consensus.electLeader(database, a.self.id)
            val snapshot = Snapshot(LogPosition(4, 1), 3)

            assertThatThrownBy { b.installSnapshot(database, snapshot) }
                .isInstanceOf(IllegalStateException::class.java)
            b.detachGroup(database)
            assertThat(b.installSnapshot(database, snapshot)).isFalse()
            assertThat(b.latestSnapshot(database)).isNull()
        }
}
