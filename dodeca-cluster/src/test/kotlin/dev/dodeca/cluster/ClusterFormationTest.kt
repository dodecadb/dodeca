package dev.dodeca.cluster

import dev.dodeca.consensus.DatabaseRef
import dev.dodeca.consensus.HostPort
import dev.dodeca.consensus.fake.InMemoryConsensus
import dev.dodeca.consensus.fake.RecordingStateMachine
import java.time.Duration
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.neo4j.logging.NullLog

class ClusterFormationTest {
    private val consensus = InMemoryConsensus()
    private val system = DatabaseRef("system", UUID(0, 1))
    private val version = mapOf(MemberMetadata.NEO4J_VERSION to "5.26.27")
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
        members[2].metadata = version + (MemberMetadata.HAS_STORES to "true")
        members.forEach { it.start() }

        val formations = members.map { formation(it).awaitFormation(system.id) }

        assertThat(formations.map { it.storeSource }.distinct()).containsExactly(members[2].self)
    }

    @Test
    fun `refuses to form when several members have existing databases`() {
        members.drop(1).forEach { it.metadata = version + (MemberMetadata.HAS_STORES to "true") }
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
                        members[1].metadata = version + (MemberMetadata.STORES_READY to "true")
                }
                .awaitStoreSource(members[1].self)

        assertThat(address).isEqualTo(members[1].self.raftAddress)
        assertThat(waits).isEqualTo(2)
    }

    @Test
    fun `without a store source any other member that serves its databases qualifies`() {
        members.forEach { it.metadata = version + (MemberMetadata.STORES_READY to "true") }
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
                MemberMetadata.NEO4J_VERSION to "5.26.32",
                MemberMetadata.STORES_READY to "true",
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
        members[0].metadata = mapOf(MemberMetadata.NEO4J_VERSION to "5.26.32")
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
        val other = consensus.newMember(metadata = mapOf(MemberMetadata.NEO4J_VERSION to "5.26.32"))
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

    /** Reports [member] unbound from [old], with a store of `system` up to [last], or none. */
    private fun unbound(
        member: InMemoryConsensus.InMemoryConsensusLog,
        last: Long?,
        store: String = STORE,
        old: UUID = system.id,
    ) {
        member.metadata =
            version +
                mapOf(
                    MemberMetadata.SYSTEM_UNBOUND to "true",
                    MemberMetadata.SYSTEM_GROUP to old.toString(),
                    MemberMetadata.HAS_STORES to "true",
                ) +
                (last?.let {
                    mapOf(
                        MemberMetadata.SYSTEM_STORE to store,
                        MemberMetadata.SYSTEM_LAST_TRANSACTION to it.toString(),
                    )
                } ?: emptyMap())
    }

    /** Has [member] run `system` in [group] with [voters], as after re-forming it. */
    private fun runReformed(
        member: InMemoryConsensus.InMemoryConsensusLog,
        group: UUID,
        voters: List<dev.dodeca.consensus.Member>,
    ) {
        member.metadata =
            version +
                mapOf(
                    MemberMetadata.SYSTEM_GROUP to group.toString(),
                    MemberMetadata.SYSTEM_STORE to STORE,
                )
        member.startGroup(DatabaseRef("system", group), RecordingStateMachine(), voters)
    }

    @Test
    fun `the unbound member with the latest store of system re-forms it under a new group`() {
        unbound(members[0], 40)
        unbound(members[1], 42)
        unbound(members[2], 41)
        members.forEach { it.start() }

        val formation =
            formation(members[1]).awaitFormation(system.id, unbound = true, storeId = STORE)

        assertThat(formation.reforms).isTrue()
        assertThat(formation.storeSource).isEqualTo(members[1].self)
        assertThat(formation.members).isEqualTo(members.map { it.self }.sortedBy { it.id })
        assertThat(formation.group).isNotIn(system.id, DatabaseGroups.SYSTEM_DATABASE)
        // The same again, as after a restart before the others joined.
        assertThat(
                formation(members[1])
                    .awaitFormation(system.id, unbound = true, storeId = STORE)
                    .group
            )
            .isEqualTo(formation.group)
    }

    @Test
    fun `of unbound stores equally late the member with the lowest id re-forms system`() {
        members.forEach { unbound(it, 42) }
        members.forEach { it.start() }
        val lowest = members.minBy { it.self.id }

        assertThat(formation(lowest).awaitFormation(system.id, unbound = true).reforms).isTrue()
    }

    @Test
    fun `the others wait for the source to re-form system, then join its group`() {
        unbound(members[0], 42)
        unbound(members[1], 40)
        // A new server without databases takes part.
        members.forEach { it.start() }
        val group = UUID.randomUUID()
        val voters = members.map { it.self }.sortedBy { it.id }
        var waits = 0

        val formation =
            formation(members[1]) {
                    waits++
                    check(waits <= 2) { "still waiting" }
                    if (waits == 2) runReformed(members[0], group, voters)
                }
                .awaitFormation(system.id, unbound = true, storeId = STORE)

        assertThat(waits).isEqualTo(2)
        assertThat(formation).isEqualTo(Formation(voters, storeSource = null, group = group))
        // The new server too, which has no record and joins the group the others report.
        assertThat(formation(members[2]).awaitFormation(system.id, joinAny = true))
            .isEqualTo(formation)
    }

    @Test
    fun `a source still reported unbound that runs its new group is joined at once`() {
        unbound(members[1], 40)
        members.forEach { it.start() }
        val group = UUID.randomUUID()
        val voters = members.take(2).map { it.self }.sortedBy { it.id }
        // As between starting the group and recording it: unbound, but running the new group.
        unbound(members[0], 42, old = group)
        members[0].startGroup(DatabaseRef("system", group), RecordingStateMachine(), voters)

        assertThat(
                formation(members[1], initial = addresses.take(2))
                    .awaitFormation(system.id, unbound = true, storeId = STORE)
            )
            .isEqualTo(Formation(voters, storeSource = null, group = group))
    }

    @Test
    fun `a member that still runs the old group holds re-forming back`() {
        unbound(members[0], 42)
        unbound(members[1], 41)
        members.forEach { it.start() }
        members[2].startGroup(system, RecordingStateMachine(), members.map { it.self })
        var waits = 0

        val formation =
            formation(members[0]) {
                    waits++
                    check(waits <= 3) { "still waiting" }
                    if (waits == 3) {
                        members[2].removeGroup(system)
                        unbound(members[2], 40)
                    }
                }
                .awaitFormation(system.id, unbound = true, storeId = STORE)

        assertThat(waits).isEqualTo(3)
        assertThat(formation.reforms).isTrue()
    }

    @Test
    fun `refuses to re-form system with a member that has databases but was not unbound`() {
        unbound(members[0], 42)
        unbound(members[1], 41)
        members[2].metadata = version + (MemberMetadata.HAS_STORES to "true")
        members.forEach { it.start() }

        assertThatThrownBy {
                formation(members[0]).awaitFormation(system.id, unbound = true, storeId = STORE)
            }
            .isInstanceOf(ClusterFormationException::class.java)
            .hasMessageContaining("were not unbound")
    }

    @Test
    fun `refuses to re-form system from stores of different clusters`() {
        unbound(members[0], 42)
        unbound(members[1], 41, store = "1/2")
        members.forEach { it.start() }

        assertThatThrownBy { formation(members[2]).awaitFormation(system.id, joinAny = true) }
            .isInstanceOf(ClusterFormationException::class.java)
            .hasMessageContaining("different clusters")
    }

    @Test
    fun `refuses to re-form system without a store of it`() {
        unbound(members[0], null)
        members.forEach { it.start() }

        assertThatThrownBy { formation(members[1]).awaitFormation(system.id, joinAny = true) }
            .isInstanceOf(ClusterFormationException::class.java)
            .hasMessageContaining("holds a store of the system database")
    }

    @Test
    fun `an unbound member joins a re-formed group of its system only`() {
        val group = UUID.randomUUID()
        members.forEach { it.start() }
        runReformed(members[0], group, members.take(2).map { it.self })
        val rebinding = consensus.newMember(metadata = version).apply { start() }
        unbound(rebinding, 30)
        val initial = addresses.take(1)

        assertThat(
                formation(rebinding, initial)
                    .awaitFormation(system.id, unbound = true, storeId = STORE)
            )
            .isEqualTo(Formation(members.take(2).map { it.self }, null, group))
        assertThatThrownBy {
                formation(rebinding, initial)
                    .awaitFormation(system.id, unbound = true, storeId = "1/2")
            }
            .isInstanceOf(ClusterFormationException::class.java)
            .hasMessageContaining("another cluster")
    }

    @Test
    fun `a store source runs the group of system it is asked for`() {
        members.forEach { it.metadata = version + (MemberMetadata.STORES_READY to "true") }
        val group = UUID.randomUUID()
        members[2].metadata =
            members[2].metadata + (MemberMetadata.SYSTEM_GROUP to group.toString())
        members.forEach { it.start() }

        assertThat(formation(members[0]).awaitStoreSource(null, group))
            .isEqualTo(members[2].self.raftAddress)
    }

    private companion object {
        const val STORE = "1700000000000/42"
    }
}
