package org.dozendb.cluster.it

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
        DozerDbContainer(DozerDbContainer.dozenDbImage, alias)
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
            it.saveLogs(DozerDbContainer.logsDirectory.resolve(javaClass.simpleName))
            it.stop()
        }
        network.close()
    }

    private fun status(member: DozerDbContainer): Map<String, Any?> =
        member.rows("CALL dozendb.cluster.status()", "system").single { it["database"] == "system" }

    private fun awaitOneLeader(running: List<DozerDbContainer>): String {
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
        val debugLog = members.first().debugLog()

        assertThat(debugLog).contains("[o.d.c.r.RatisConsensusLog] Raft member")
        assertThat(debugLog).containsPattern("\\[o\\.a\\.r\\.[^]]+]")
        assertThat(members.first().logs).contains("All 3 initial cluster members are reachable")
    }

    @Test
    @Order(3)
    fun `the remaining members elect a new leader when the leader dies`() {
        val leaderId = awaitOneLeader(members)
        val leader = members.single { status(it)["memberId"] == leaderId }
        leader.saveLogs(DozerDbContainer.logsDirectory.resolve(javaClass.simpleName))

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
