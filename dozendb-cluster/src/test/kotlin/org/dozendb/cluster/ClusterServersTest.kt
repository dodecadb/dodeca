package org.dozendb.cluster

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.dozendb.consensus.ConsensusUnavailableException
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.GroupMembership
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.Member
import org.dozendb.consensus.MemberId
import org.dozendb.consensus.fake.InMemoryConsensus
import org.dozendb.consensus.fake.RecordingStateMachine
import org.junit.jupiter.api.Test

class ClusterServersTest {
    private val consensus = InMemoryConsensus()
    private val system = DatabaseRef("system", UUID(0, 1))
    private val groups = DatabaseGroups()
    private val logs = List(3) { consensus.newMember() }
    private val secondary = Member(MemberId(UUID(9, 9)), HostPort("secondary", 7000))

    init {
        logs.forEach { it.start() }
        logs.forEach {
            it.startGroup(system, RecordingStateMachine(), logs.map { log -> log.self })
        }
    }

    @Test
    fun `the servers are the voters of system, then the system secondaries`() {
        val servers = ClusterServers(logs[0], groups, recorded = { listOf(secondary) })

        assertThat(servers.all())
            .containsExactly(*(logs.map { it.self } + secondary).toTypedArray())
        assertThat(servers.secondaries()).containsExactly(secondary)
    }

    @Test
    fun `a server that both votes and is recorded a secondary counts once, as a voter`() {
        val servers = ClusterServers(logs[0], groups, recorded = { listOf(logs[2].self) })

        assertThat(servers.all()).containsExactly(*logs.map { it.self }.toTypedArray())
    }

    @Test
    fun `the secondaries are read again only once system changed`() {
        val reads = AtomicInteger()
        var recorded = emptyList<Member>()
        val servers =
            ClusterServers(
                logs[0],
                groups,
                recorded = {
                    reads.incrementAndGet()
                    recorded
                },
            )
        assertThat(servers.secondaries()).isEmpty()
        recorded = listOf(secondary)

        assertThat(servers.secondaries()).isEmpty()
        servers.systemChanged()
        assertThat(servers.secondaries()).containsExactly(secondary)
        assertThat(servers.secondaries()).containsExactly(secondary)
        assertThat(reads.get()).isEqualTo(2)
    }

    @Test
    fun `while system cannot be read, the secondaries are as last read, and read once it can`() {
        var readable = true
        val servers =
            ClusterServers(
                logs[0],
                groups,
                recorded = {
                    check(readable) { "system is not running" }
                    listOf(secondary)
                },
            )
        assertThat(servers.secondaries()).containsExactly(secondary)
        readable = false
        servers.systemChanged()

        assertThat(servers.secondaries()).containsExactly(secondary)
        readable = true
        assertThat(servers.secondaries()).containsExactly(secondary)
    }

    @Test
    fun `a member that pulls system takes the voters from what they report`() {
        val outside = consensus.newMember().also { it.start() }
        var reported: GroupMembership? = null
        val servers =
            ClusterServers(
                outside,
                groups,
                recorded = { listOf(outside.self) },
                pullsSystem = { true },
                reportedVoters = { reported },
            )
        assertThatThrownBy { servers.all() }.isInstanceOf(ConsensusUnavailableException::class.java)

        reported = GroupMembership(logs.map { it.self }, settled = false, removed = false)

        assertThat(servers.all())
            .containsExactly(*(logs.map { it.self } + outside.self).toTypedArray())
        assertThat(servers.settled()).isNull()
    }
}
