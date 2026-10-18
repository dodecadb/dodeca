package org.dozendb.cluster.api.fake

import java.util.UUID
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.dozendb.cluster.api.CommitResult
import org.dozendb.cluster.api.ConsensusUnavailableException
import org.dozendb.cluster.api.DatabaseRef
import org.dozendb.cluster.api.GroupStatus
import org.dozendb.cluster.api.Role
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
                .isEqualTo(CommitResult.Committed(1, 1))
            assertThat(a.submit(database, "two".encodeToByteArray()))
                .isEqualTo(CommitResult.Committed(2, 1))

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
            assertThat(a.submit(database, byteArrayOf(2))).isEqualTo(CommitResult.Committed(1, 1))
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
}
