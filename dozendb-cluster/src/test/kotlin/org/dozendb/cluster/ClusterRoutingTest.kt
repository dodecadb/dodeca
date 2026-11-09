package org.dozendb.cluster

import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.Member
import org.dozendb.consensus.MemberId
import org.junit.jupiter.api.Test

class ClusterRoutingTest {
    private val members =
        (1..3).map { Member(MemberId(UUID(0, it.toLong())), HostPort("core$it", 7700)) }
    private val bolt = members.associate {
        it.id to HostPort("core${members.indexOf(it) + 1}", 7687)
    }

    private fun leader(member: Member?) = member?.id

    private fun address(index: Int) = HostPort("core$index", 7687)

    @Test
    fun `writes go to the leader and reads to every member`() {
        val endpoints =
            ClusterRouting.endpoints(
                leader(members[1]),
                members,
                bolt::get,
                readsOnLeader = true,
                failed = emptySet(),
            )

        assertThat(endpoints.writers).containsExactly(address(2))
        assertThat(endpoints.readers).containsExactly(address(1), address(3), address(2))
        assertThat(endpoints.routers).containsExactly(address(1), address(2), address(3))
    }

    @Test
    fun `a leader where the database is read-only takes no writes, but reads`() {
        val endpoints =
            ClusterRouting.endpoints(
                leader(members[1]),
                members,
                bolt::get,
                readsOnLeader = true,
                failed = emptySet(),
                readOnly = setOf(members[1].id, members[2].id),
            )

        assertThat(endpoints.writers).isEmpty()
        assertThat(endpoints.readers).containsExactly(address(1), address(3), address(2))
    }

    @Test
    fun `the leader can be kept free of reads`() {
        val endpoints =
            ClusterRouting.endpoints(
                leader(members[1]),
                members,
                bolt::get,
                readsOnLeader = false,
                failed = emptySet(),
            )

        assertThat(endpoints.readers).containsExactly(address(1), address(3))
    }

    @Test
    fun `a lone leader still takes reads`() {
        val endpoints =
            ClusterRouting.endpoints(
                leader(members[0]),
                members.take(1),
                bolt::get,
                readsOnLeader = false,
                failed = emptySet(),
            )

        assertThat(endpoints.writers).containsExactly(address(1))
        assertThat(endpoints.readers).containsExactly(address(1))
    }

    @Test
    fun `during an election there is no writer`() {
        val endpoints =
            ClusterRouting.endpoints(
                leader(null),
                members,
                bolt::get,
                readsOnLeader = true,
                failed = emptySet(),
            )

        assertThat(endpoints.writers).isEmpty()
        assertThat(endpoints.readers).containsExactly(address(1), address(2), address(3))
    }

    @Test
    fun `secondaries read and route, and followers read too`() {
        val secondaries = listOf(MemberId(UUID(0, 5)), MemberId(UUID(0, 4)))
        val addresses = bolt + secondaries.associateWith { HostPort("replica${it.uuid}", 7687) }

        val endpoints =
            ClusterRouting.endpoints(
                leader(members[1]),
                members,
                addresses::get,
                readsOnLeader = false,
                failed = setOf(secondaries[0]),
                secondaries = secondaries,
            )

        val replica4 = addresses.getValue(secondaries[1])
        val replica5 = addresses.getValue(secondaries[0])
        assertThat(endpoints.writers).containsExactly(address(2))
        assertThat(endpoints.readers).containsExactly(address(1), address(3), replica4)
        assertThat(endpoints.routers)
            .containsExactly(address(1), address(2), address(3), replica4, replica5)
    }

    @Test
    fun `without reads on primaries only secondaries read, while one can`() {
        val secondary = MemberId(UUID(0, 4))
        val addresses = bolt + (secondary to HostPort("replica", 7687))
        fun readers(failed: Set<MemberId>, readsOnLeader: Boolean) =
            ClusterRouting.endpoints(
                    leader(members[1]),
                    members,
                    addresses::get,
                    readsOnLeader = readsOnLeader,
                    failed = failed,
                    secondaries = listOf(secondary),
                    readsOnPrimaries = false,
                )
                .readers

        assertThat(readers(emptySet(), readsOnLeader = false))
            .containsExactly(HostPort("replica", 7687))
        assertThat(readers(emptySet(), readsOnLeader = true))
            .containsExactly(HostPort("replica", 7687), address(2))
        assertThat(readers(setOf(secondary), readsOnLeader = false))
            .describedAs("no secondary can read")
            .containsExactly(address(1), address(3))
    }

    @Test
    fun `members whose copy failed neither write nor read, but route`() {
        val endpoints =
            ClusterRouting.endpoints(
                leader(members[1]),
                members,
                bolt::get,
                readsOnLeader = true,
                failed = setOf(members[0].id),
            )

        assertThat(endpoints.writers).containsExactly(address(2))
        assertThat(endpoints.readers).containsExactly(address(3), address(2))
        assertThat(endpoints.routers).containsExactly(address(1), address(2), address(3))
    }

    @Test
    fun `a leader whose copy failed is no writer`() {
        val endpoints =
            ClusterRouting.endpoints(
                leader(members[1]),
                members,
                bolt::get,
                readsOnLeader = true,
                failed = setOf(members[1].id),
            )

        assertThat(endpoints.writers).isEmpty()
        assertThat(endpoints.readers).containsExactly(address(1), address(3))
    }

    @Test
    fun `members without a known address are left out`() {
        val known = bolt - members[2].id

        val endpoints =
            ClusterRouting.endpoints(
                leader(members[2]),
                members,
                known::get,
                readsOnLeader = true,
                failed = emptySet(),
            )

        assertThat(endpoints.writers).isEmpty()
        assertThat(endpoints.readers).containsExactly(address(1), address(2))
        assertThat(endpoints.routers).containsExactly(address(1), address(2))
    }

    @Test
    fun `a leader that is not a member is ignored`() {
        val stranger = Member(MemberId(UUID(0, 9)), HostPort("gone", 7700))

        val endpoints =
            ClusterRouting.endpoints(
                leader(stranger),
                members,
                bolt::get,
                readsOnLeader = true,
                failed = emptySet(),
            )

        assertThat(endpoints.writers).isEmpty()
    }
}
