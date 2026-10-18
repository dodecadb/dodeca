package org.dozendb.cluster.core

import java.time.Duration
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.dozendb.cluster.api.DatabaseRef
import org.dozendb.cluster.api.HostPort
import org.dozendb.cluster.api.fake.InMemoryConsensus
import org.dozendb.cluster.api.fake.RecordingStateMachine
import org.junit.jupiter.api.Test
import org.neo4j.logging.NullLog

class ClusterFormationTest {
    private val consensus = InMemoryConsensus()
    private val system = DatabaseRef("system", UUID(0, 1))
    private val version = mapOf(DozenDbMetadata.NEO4J_VERSION to "5.26.27")
    private val members = List(3) { consensus.newMember(metadata = version) }
    private val addresses = members.map { it.self.raftAddress }

    private fun formation(
        member: InMemoryConsensus.InMemoryConsensusLog,
        initial: List<HostPort> = addresses,
        sleep: (Duration) -> Unit = { error("unexpected wait") },
    ) = ClusterFormation(member, initial, "5.26.27", NullLog.getInstance(), sleep = sleep)

    @Test
    fun `every initial member computes the same ordered member list`() {
        members.forEach { it.start() }

        val decisions = members.map { formation(it).awaitInitialMembers(system.id) }

        assertThat(decisions.distinct()).hasSize(1)
        assertThat(decisions[0]).isEqualTo(members.map { it.self }.sortedBy { it.id })
    }

    @Test
    fun `waits until every initial member is reachable`() {
        members.take(2).forEach { it.start() }
        var waits = 0

        val decided =
            formation(members[0]) {
                    waits++
                    if (waits == 3) members[2].start()
                }
                .awaitInitialMembers(system.id)

        assertThat(waits).isEqualTo(3)
        assertThat(decided).hasSize(3)
    }

    @Test
    fun `adopts the members of a group that is already formed`() {
        members.forEach { it.start() }
        val formed = members.map { it.self }.sortedBy { it.id }
        members.take(2).forEach { it.startGroup(system, RecordingStateMachine(), formed) }
        members[1].close()

        assertThat(formation(members[2]).awaitInitialMembers(system.id)).isEqualTo(formed)
    }

    @Test
    fun `refuses to form when the cluster was formed without this member`() {
        members.forEach { it.start() }
        val wiped = consensus.newMember(metadata = version).apply { start() }
        val formed = members.map { it.self }
        members[0].startGroup(system, RecordingStateMachine(), formed)

        assertThatThrownBy {
                formation(wiped, initial = addresses.take(1) + wiped.self.raftAddress)
                    .awaitInitialMembers(system.id)
            }
            .isInstanceOf(ClusterFormationException::class.java)
            .hasMessageContaining("already formed")
    }

    @Test
    fun `refuses members that run another Neo4j version`() {
        val other =
            consensus.newMember(metadata = mapOf(DozenDbMetadata.NEO4J_VERSION to "5.26.32"))
        (members + other).forEach { it.start() }

        assertThatThrownBy {
                formation(members[0], initial = addresses + other.self.raftAddress)
                    .awaitInitialMembers(system.id)
            }
            .isInstanceOf(ClusterFormationException::class.java)
            .hasMessageContaining("5.26.32")
    }

    @Test
    fun `refuses an initial member list without this member`() {
        val outsider = consensus.newMember(metadata = version)
        (members + outsider).forEach { it.start() }

        assertThatThrownBy { formation(outsider).awaitInitialMembers(system.id) }
            .isInstanceOf(ClusterFormationException::class.java)
            .hasMessageContaining("is not one of")
    }

    @Test
    fun `refuses an initial member list that names one member twice`() {
        members.forEach { it.start() }

        assertThatThrownBy {
                formation(members[0], initial = addresses + addresses[1])
                    .awaitInitialMembers(system.id)
            }
            .isInstanceOf(ClusterFormationException::class.java)
            .hasMessageContaining("several addresses")
    }
}
