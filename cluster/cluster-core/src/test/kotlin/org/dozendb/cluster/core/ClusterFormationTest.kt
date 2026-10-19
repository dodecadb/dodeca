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

        val decisions = members.map { formation(it).awaitFormation(system.id).members }

        assertThat(decisions.distinct()).hasSize(1)
        assertThat(decisions[0]).isEqualTo(members.map { it.self }.sortedBy { it.id })
    }

    @Test
    fun `without existing databases the member with the lowest id is the store source`() {
        members.forEach { it.start() }

        val formations = members.map { formation(it).awaitFormation(system.id) }

        assertThat(formations.map { it.storeSource }.distinct())
            .containsExactly(members.map { it.self }.minBy { it.id })
    }

    @Test
    fun `the only member with existing databases is the store source`() {
        members[2].metadata = version + (DozenDbMetadata.HAS_STORES to "true")
        members.forEach { it.start() }

        val formations = members.map { formation(it).awaitFormation(system.id) }

        assertThat(formations.map { it.storeSource }.distinct()).containsExactly(members[2].self)
    }

    @Test
    fun `refuses to form when several members have existing databases`() {
        members.drop(1).forEach { it.metadata = version + (DozenDbMetadata.HAS_STORES to "true") }
        members.forEach { it.start() }

        assertThatThrownBy { formation(members[0]).awaitFormation(system.id) }
            .isInstanceOf(ClusterFormationException::class.java)
            .hasMessageContaining("existing databases")
    }

    @Test
    fun `joining a formed cluster has no store source`() {
        members.forEach { it.start() }
        val formed = members.map { it.self }.sortedBy { it.id }
        members.take(2).forEach { it.startGroup(system, RecordingStateMachine(), formed) }

        assertThat(formation(members[2]).awaitFormation(system.id).storeSource).isNull()
    }

    @Test
    fun `waits for the store source to serve its databases`() {
        members.forEach { it.start() }
        var waits = 0

        val address =
            formation(members[0]) {
                    waits++
                    if (waits == 2)
                        members[1].metadata = version + (DozenDbMetadata.STORES_READY to "true")
                }
                .awaitStoreSource(members[1].self)

        assertThat(address).isEqualTo(members[1].self.raftAddress)
        assertThat(waits).isEqualTo(2)
    }

    @Test
    fun `without a store source any other member that serves its databases qualifies`() {
        members.forEach { it.metadata = version + (DozenDbMetadata.STORES_READY to "true") }
        members.forEach { it.start() }
        members[1].close()

        // Itself and the stopped member do not qualify.
        assertThat(formation(members[0]).awaitStoreSource(null))
            .isEqualTo(members[2].self.raftAddress)
    }

    @Test
    fun `refuses a store source that runs another Neo4j version`() {
        members.forEach { it.start() }
        members[1].metadata =
            mapOf(
                DozenDbMetadata.NEO4J_VERSION to "5.26.32",
                DozenDbMetadata.STORES_READY to "true",
            )

        assertThatThrownBy { formation(members[0]).awaitStoreSource(members[1].self) }
            .isInstanceOf(ClusterFormationException::class.java)
            .hasMessageContaining("5.26.32")
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
                .awaitFormation(system.id)
                .members

        assertThat(waits).isEqualTo(3)
        assertThat(decided).hasSize(3)
    }

    @Test
    fun `adopts the members of a group that is already formed`() {
        members.forEach { it.start() }
        val formed = members.map { it.self }.sortedBy { it.id }
        members.take(2).forEach { it.startGroup(system, RecordingStateMachine(), formed) }
        members[1].close()

        assertThat(formation(members[2]).awaitFormation(system.id).members).isEqualTo(formed)
    }

    @Test
    fun `a member the cluster was formed without joins it`() {
        members.forEach { it.start() }
        val joining = consensus.newMember(metadata = version).apply { start() }
        val formed = members.map { it.self }
        members[0].startGroup(system, RecordingStateMachine(), formed)

        val formation =
            formation(joining, initial = addresses.take(1) + joining.self.raftAddress)
                .awaitFormation(system.id)

        assertThat(formation).isEqualTo(Formation(formed, storeSource = null))
    }

    @Test
    fun `refuses to join a cluster that runs another Neo4j version`() {
        members.forEach { it.start() }
        members[0].metadata = mapOf(DozenDbMetadata.NEO4J_VERSION to "5.26.32")
        members[0].startGroup(system, RecordingStateMachine(), members.map { it.self })
        val joining = consensus.newMember(metadata = version).apply { start() }

        assertThatThrownBy {
                formation(joining, initial = addresses.take(1)).awaitFormation(system.id)
            }
            .isInstanceOf(ClusterFormationException::class.java)
            .hasMessageContaining("5.26.32")
    }

    @Test
    fun `refuses members that run another Neo4j version`() {
        val other =
            consensus.newMember(metadata = mapOf(DozenDbMetadata.NEO4J_VERSION to "5.26.32"))
        (members + other).forEach { it.start() }

        assertThatThrownBy {
                formation(members[0], initial = addresses + other.self.raftAddress)
                    .awaitFormation(system.id)
                    .members
            }
            .isInstanceOf(ClusterFormationException::class.java)
            .hasMessageContaining("5.26.32")
    }

    @Test
    fun `refuses an initial member list without this member`() {
        val outsider = consensus.newMember(metadata = version)
        (members + outsider).forEach { it.start() }

        assertThatThrownBy { formation(outsider).awaitFormation(system.id).members }
            .isInstanceOf(ClusterFormationException::class.java)
            .hasMessageContaining("is not one of")
    }

    @Test
    fun `refuses an initial member list that names one member twice`() {
        members.forEach { it.start() }

        assertThatThrownBy {
                formation(members[0], initial = addresses + addresses[1])
                    .awaitFormation(system.id)
                    .members
            }
            .isInstanceOf(ClusterFormationException::class.java)
            .hasMessageContaining("several addresses")
    }
}
