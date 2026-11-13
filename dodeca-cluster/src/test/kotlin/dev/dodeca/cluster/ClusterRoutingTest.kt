package dev.dodeca.cluster

import dev.dodeca.consensus.HostPort
import dev.dodeca.consensus.Member
import dev.dodeca.consensus.MemberId
import dev.dodeca.core.servers.ServerRules
import java.util.UUID
import kotlin.random.Random
import org.assertj.core.api.Assertions.assertThat
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
                readsOnWriters = true,
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
                readsOnWriters = true,
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
                readsOnWriters = false,
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
                readsOnWriters = false,
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
                readsOnWriters = true,
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
                readsOnWriters = false,
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
        fun readers(failed: Set<MemberId>, readsOnWriters: Boolean) =
            ClusterRouting.endpoints(
                    leader(members[1]),
                    members,
                    addresses::get,
                    readsOnWriters = readsOnWriters,
                    failed = failed,
                    secondaries = listOf(secondary),
                    readsOnPrimaries = false,
                )
                .readers

        assertThat(readers(emptySet(), readsOnWriters = false))
            .containsExactly(HostPort("replica", 7687))
        assertThat(readers(emptySet(), readsOnWriters = true))
            .containsExactly(HostPort("replica", 7687), address(2))
        assertThat(readers(setOf(secondary), readsOnWriters = false))
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
                readsOnWriters = true,
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
                readsOnWriters = true,
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
                readsOnWriters = true,
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
                readsOnWriters = true,
                failed = emptySet(),
            )

        assertThat(endpoints.writers).isEmpty()
    }

    @Test
    fun `members and secondaries that do not answer are left out, the leader reading for them`() {
        val secondary = MemberId(UUID(0, 4))
        val addresses = bolt + (secondary to HostPort("replica", 7687))
        val down = setOf(members[0].id, members[2].id, secondary)

        val endpoints =
            ClusterRouting.endpoints(
                leader(members[1]),
                members,
                addresses::get,
                readsOnWriters = false,
                failed = emptySet(),
                secondaries = listOf(secondary),
                available = { it !in down },
            )

        assertThat(endpoints.writers).containsExactly(address(2))
        assertThat(endpoints.readers).containsExactly(address(2))
        assertThat(endpoints.routers).containsExactly(address(2))
    }

    /** Five members: 1 and 2 in north1, 3 in north2, all three in north, 4 and 5 in south. */
    private val regions =
        (1..5).map { Member(MemberId(UUID(0, it.toLong())), HostPort("core$it", 7700)) }
    private val regionTags =
        mapOf(
            1 to setOf("north1", "north"),
            2 to setOf("north1", "north"),
            3 to setOf("north2", "north"),
            4 to setOf("south"),
            5 to setOf("south"),
        )

    private fun regional(
        leader: Int?,
        policy: String,
        available: Set<Int> = (1..5).toSet(),
        readsOnWriters: Boolean = false,
    ) =
        ClusterRouting.endpoints(
            leader?.let { regions[it - 1].id },
            regions,
            { id -> HostPort("core${id.uuid.leastSignificantBits}", 7687) },
            readsOnWriters = readsOnWriters,
            failed = emptySet(),
            available = { it.uuid.leastSignificantBits.toInt() in available },
            policy = ServerRules.parse(policy),
            tags = { regionTags.getValue(it.uuid.leastSignificantBits.toInt()) },
        )

    @Test
    fun `a policy chooses readers and routers by their tags, the writer is the leader whatever it says`() {
        val endpoints = regional(leader = 4, policy = "tags(north1)->min(2); halt();")

        assertThat(endpoints.writers).containsExactly(address(4))
        assertThat(endpoints.readers).containsExactly(address(1), address(2))
        assertThat(endpoints.routers).containsExactly(address(1), address(2))
        assertThat(regional(leader = 1, policy = "tags(north1)->min(2); halt();").readers)
            .describedAs("the writer reads only if it may")
            .isEmpty()
        assertThat(
                regional(
                        leader = 1,
                        policy = "tags(north1)->min(2); halt();",
                        readsOnWriters = true,
                    )
                    .readers
            )
            .containsExactly(address(2), address(1))
    }

    @Test
    fun `a server that does not answer moves a policy to its next rule`() {
        val policy = "tags(north1)->min(2); tags(north); halt()"

        assertThat(regional(leader = 4, policy = policy, available = setOf(1, 3, 4, 5)).readers)
            .containsExactly(address(1), address(3))
        assertThat(
                regional(leader = 4, policy = "tags(north1)->min(2)", available = setOf(1, 4, 5))
                    .readers
            )
            .describedAs("all() follows the rules")
            .containsExactly(address(1), address(5))
    }

    @Test
    fun `a policy that passes no reader leaves none, and one that passes no router leaves the leader`() {
        val endpoints =
            regional(
                leader = 4,
                policy = "tags(north1)->min(2); halt();",
                available = setOf(1, 3, 4, 5),
            )

        assertThat(endpoints.writers).containsExactly(address(4))
        assertThat(endpoints.readers).isEmpty()
        assertThat(endpoints.routers).containsExactly(address(4))
        assertThat(
                regional(leader = null, policy = "tags(west); halt()", available = setOf(1, 4))
                    .routers
            )
            .describedAs("while there is no leader, the routers it was given")
            .containsExactly(address(1), address(4))
    }

    @Test
    fun `with a random, readers and routers are listed in varying orders`() {
        val random = Random(42)
        val orders =
            (1..20).map {
                val endpoints =
                    ClusterRouting.endpoints(
                        leader(regions[0]),
                        regions,
                        { id -> HostPort("core${id.uuid.leastSignificantBits}", 7687) },
                        readsOnWriters = false,
                        failed = emptySet(),
                        random = random,
                    )
                assertThat(endpoints.readers)
                    .containsExactlyInAnyOrder(address(2), address(3), address(4), address(5))
                assertThat(endpoints.routers)
                    .containsExactlyInAnyOrder(
                        address(1),
                        address(2),
                        address(3),
                        address(4),
                        address(5),
                    )
                endpoints.readers to endpoints.routers
            }

        assertThat(orders.map { it.first }.toSet()).hasSizeGreaterThan(1)
        assertThat(orders.map { it.second }.toSet()).hasSizeGreaterThan(1)
    }
}
