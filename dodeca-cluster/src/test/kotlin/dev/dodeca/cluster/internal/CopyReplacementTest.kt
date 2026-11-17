package dev.dodeca.cluster.internal

import dev.dodeca.cluster.DatabaseGroups
import dev.dodeca.cluster.internal.replication.FakeLocalDatabases
import dev.dodeca.consensus.ConsensusLog
import dev.dodeca.consensus.ConsensusUnavailableException
import dev.dodeca.consensus.DatabaseRef
import dev.dodeca.consensus.Member
import dev.dodeca.consensus.MemberId
import dev.dodeca.consensus.fake.InMemoryConsensus
import dev.dodeca.consensus.fake.RecordingStateMachine
import java.time.Duration
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.neo4j.kernel.database.DatabaseIdFactory
import org.neo4j.logging.NullLog

/**
 * Replacing the cluster state of a primary's copy by a new copy: this member deletes its state of
 * the database's group, then has the group remove it, trying again until the group did or the time
 * for it ran out.
 */
class CopyReplacementTest {
    private val consensus = InMemoryConsensus()
    private val here = consensus.newMember()
    private val leader = consensus.newMember()
    private val other = consensus.newMember()
    private val logs = listOf(here, leader, other)
    private val films = DatabaseIdFactory.from("films", UUID.randomUUID())
    private val group = DatabaseRef("films", films.databaseId().uuid())

    init {
        for (log in logs) {
            log.start()
            log.startGroup(group, RecordingStateMachine(), logs.map { it.self })
        }
        consensus.electLeader(group, leader.self.id)
    }

    /** The voters of films, as its leader knows them. */
    private fun voters(): List<MemberId> = leader.members(group).map { it.id }

    private fun replacement(through: ConsensusLog = here, timeout: Duration = AMPLE) =
        CopyReplacement(
            through,
            DatabaseGroups(),
            FakeLocalDatabases(),
            { logs.map { it.self } },
            timeout,
            NullLog.getInstance(),
        )

    /** [here], whose removal from a group runs [around] the removal. */
    private fun removing(around: suspend (suspend () -> Unit) -> Unit): ConsensusLog =
        object : ConsensusLog by here {
            override suspend fun removeMember(
                database: DatabaseRef,
                id: MemberId,
                timeout: Duration,
                members: List<Member>,
            ) = around { here.removeMember(database, id, timeout, members) }
        }

    @Test
    fun `the group removes this member, which no longer runs it`() {
        replacement().replace(films, keepStore = false)

        assertThat(here.localGroups()).doesNotContain(group.id)
        assertThat(voters()).containsExactlyInAnyOrder(leader.self.id, other.self.id)
    }

    @Test
    fun `while no majority of the group answers, it gives up once its time ran out`() {
        consensus.crash(other.self.id)
        val started = System.nanoTime()

        assertThatThrownBy { replacement(timeout = SHORT).replace(films, keepStore = false) }
            .isInstanceOf(IllegalStateException::class.java)
            .hasMessageStartingWith(
                "Database 'films': its Raft group did not remove the copy here, which joins it " +
                    "as a new member, within 2 seconds: no leader for films("
            )
            .hasMessageEndingWith(
                "The copy here is deleted and stays in quarantine; run the operation again once " +
                    "a majority of the database's primaries answers."
            )
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isGreaterThanOrEqualTo(SHORT)
        assertThat(here.localGroups()).doesNotContain(group.id)
        assertThat(voters()).contains(here.self.id)
    }

    @Test
    fun `the group removes it once a majority answers again within its time`() {
        consensus.crash(other.self.id)
        var attempts = 0
        val through = removing { remove ->
            attempts++
            try {
                remove()
            } finally {
                if (attempts == 1) {
                    other.start()
                    other.startGroup(group, RecordingStateMachine(), emptyList())
                }
            }
        }

        replacement(through).replace(films, keepStore = false)

        assertThat(attempts).isEqualTo(2)
        assertThat(voters()).containsExactlyInAnyOrder(leader.self.id, other.self.id)
    }

    // As when the leader that committed the change stepped down before it answered.
    @Test
    fun `an attempt whose answer was lost, which the group committed, is not made again`() {
        var attempts = 0
        val through = removing { remove ->
            attempts++
            remove()
            throw ConsensusUnavailableException("the answer was lost")
        }

        replacement(through).replace(films, keepStore = false)

        assertThat(attempts).isEqualTo(1)
        assertThat(voters()).containsExactlyInAnyOrder(leader.self.id, other.self.id)
    }

    private companion object {
        /** Longer than any of these tests waits for the group. */
        val AMPLE: Duration = Duration.ofSeconds(30)

        val SHORT: Duration = Duration.ofSeconds(2)
    }
}
