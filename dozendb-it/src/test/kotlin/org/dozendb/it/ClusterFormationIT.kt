package org.dozendb.it

import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestMethodOrder
import org.testcontainers.containers.Network
import org.testcontainers.lifecycle.Startables

/** Three members form a cluster from configuration alone and elect one leader for `system`. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class ClusterFormationIT {
    private val network = Network.newNetwork()
    private val aliases = listOf("core1", "core2", "core3")
    private val members = aliases.map { alias ->
        DozenDbContainer(DozenDbContainer.dozenDbImage, alias)
            .inNetwork(network)
            .clusterMember(aliases.map { "$it:7700" })
    }

    @BeforeAll
    fun start() {
        // Formation blocks startup until every initial member answers, so start them together.
        Startables.deepStart(members).join()
    }

    @AfterAll
    fun stop() {
        members.forEach {
            it.saveLogs(DozenDbContainer.logsDirectory.resolve(javaClass.simpleName))
            it.stop()
        }
        network.close()
    }

    private fun status(member: DozenDbContainer): Map<String, Any?> =
        member.rows("CALL dozendb.cluster.status()", "system").single { it["database"] == "system" }

    private fun awaitOneLeader(running: List<DozenDbContainer>): String {
        var leader: String? = null
        await withPollInterval
            Duration.ofMillis(200) atMost
            FAILOVER_TARGET untilAsserted
            {
                val statuses = running.map(::status)
                assertThat(statuses.map { it["role"] }).containsOnlyOnce("LEADER")
                assertThat(statuses.map { it["role"] }.filter { it != "LEADER" }).allMatch {
                    it == "FOLLOWER"
                }
                val leaders = statuses.map { it["leader"] }.distinct()
                assertThat(leaders).hasSize(1).doesNotContainNull()
                leader = leaders.single() as String
            }
        return leader!!
    }

    @Test
    @Order(1)
    fun `every member reports the same leader of the system group`() {
        val leader = awaitOneLeader(members)

        val memberIds = members.map { status(it)["memberId"] as String }
        assertThat(memberIds).doesNotHaveDuplicates().contains(leader)
        members.forEach { member ->
            val view = member.rows("CALL dozendb.cluster.members()", "system")
            assertThat(view.map { it["memberId"] }).containsExactlyInAnyOrderElementsOf(memberIds)
            assertThat(view.map { it["raftAddress"] })
                .containsExactlyInAnyOrderElementsOf(aliases.map { "$it:7700" })
            assertThat(view.single { it["role"] == "LEADER" }["memberId"]).isEqualTo(leader)
            assertThat(view.single { it["self"] == true }["memberId"])
                .isEqualTo(status(member)["memberId"])
        }
    }

    @Test
    @Order(2)
    fun `consensus logs reach the Neo4j debug log`() {
        val debugLog = members.first().debugEntries()

        assertThat(debugLog).anyMatch {
            it.category == "o.d.c.r.RatisConsensusLog" && it.message.startsWith("Raft member")
        }
        assertThat(debugLog).anyMatch { it.category.startsWith("o.a.r.") }
    }

    @Test
    @Order(2)
    fun `members talk to each other over mutual TLS with hostname verification`() {
        for (member in members) {
            assertThat(member.debugLog())
                .describedAs(member.alias)
                .contains("(TLS, client certificates required, hostnames verified)")
        }
    }

    @Test
    @Order(2)
    fun `one member forms the group and the others form it too or adopt it`() {
        val formed = "All 3 initial cluster members are reachable"
        val adopted = "Joining the already formed cluster"

        assertThat(members).anyMatch { formed in it.logs }
        assertThat(members).allMatch { formed in it.logs || adopted in it.logs }
    }

    @Test
    @Order(3)
    fun `the remaining members elect a new leader when the leader dies`() {
        val leaderId = awaitOneLeader(members)
        val leader = members.single { status(it)["memberId"] == leaderId }
        leader.saveLogs(DozenDbContainer.logsDirectory.resolve(javaClass.simpleName))

        leader.kill()

        val survivors = members - leader
        val newLeader = awaitOneLeader(survivors)
        assertThat(newLeader).isNotEqualTo(leaderId)
    }

    private companion object {
        /** The failover target from the design: a new leader within 10 seconds. */
        val FAILOVER_TARGET: Duration = Duration.ofSeconds(10)
    }
}
