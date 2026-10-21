package org.dozendb.consensus.ratis

import java.nio.file.Path
import java.util.concurrent.CopyOnWriteArrayList
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.dozendb.consensus.CommitResult
import org.dozendb.consensus.LogEntry
import org.dozendb.consensus.ReplicatedStateMachine
import org.dozendb.consensus.Role
import org.dozendb.consensus.fake.RecordingStateMachine
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

/**
 * State machines attached with a state their group did not apply on that member, as when a
 * database's files were put in place by hand.
 */
class ForeignStateTest {
    @TempDir lateinit var root: Path
    private var cluster: RatisTestCluster? = null

    /** Takes no snapshots, so that a restarted member applies its whole log again. */
    private fun formCluster(): RatisTestCluster =
        RatisTestCluster(root, 3).also {
            cluster = it
            it.nodes.forEach { node -> node.machine.durable = -1 }
            it.form()
        }

    @AfterEach
    fun stopCluster() {
        cluster?.close()
    }

    private suspend fun RatisTestCluster.write(text: String): Long {
        val result = awaitLeader().consensus.submit(database, text.encodeToByteArray())
        return (result as CommitResult.Committed).index
    }

    /** A state machine holding one entry of another group, at [index]. */
    private fun foreignState(index: Long): RecordingStateMachine =
        RecordingStateMachine().apply { apply(LogEntry(index, 1, "foreign".encodeToByteArray())) }

    private fun RatisTestCluster.restart(
        node: RatisTestCluster.Node,
        stateMachine: ReplicatedStateMachine,
    ) {
        node.stop()
        node.start()
        node.consensus.startGroup(database, stateMachine, emptyList())
    }

    @Test
    fun `a state the group's entry at its applied index does not match is refused`() =
        runBlocking<Unit> {
            val cluster = formCluster()
            val leader = cluster.awaitLeader()
            val follower = cluster.nodes.first { it != leader }
            val written = (1..3).map { cluster.write("before-$it") }
            val foreign = foreignState(written[1])

            cluster.restart(follower, foreign)
            val last = cluster.write("after")

            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(foreign.foreignStates)
                        .containsExactly(
                            "it was applied up to Raft index ${written[1]}, but the group's " +
                                "entry there is a different one"
                        )
                }
            assertThat(foreign.payloads()).containsExactly("foreign")
            // The member keeps taking part in the group.
            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    val status = follower.consensus.status(cluster.database)
                    assertThat(status.role).isEqualTo(Role.Follower)
                    assertThat(status.commitIndex).isGreaterThanOrEqualTo(last)
                }

            // Refused again as it is, but a state from nothing applies the group's log after a
            // restart.
            follower.consensus.detachGroup(cluster.database, true)
            follower.consensus.startGroup(cluster.database, foreign, emptyList())
            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(foreign.foreignStates).hasSize(2)
                }
            val replacement = RecordingStateMachine()
            cluster.restart(follower, replacement)
            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(replacement.payloads()).isEqualTo(leader.machine.payloads())
                }
            assertThat(replacement.foreignStates).isEmpty()
        }

    @Test
    fun `a state applied beyond the end of the group's log is checked once the log reaches it`() =
        runBlocking<Unit> {
            val cluster = formCluster()
            val follower = cluster.nodes.first { it != cluster.awaitLeader() }
            val before = cluster.write("before")
            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(follower.machine.lastAppliedIndex()).isEqualTo(before)
                }
            val foreign = foreignState(before + 3)

            // The entry there may be a transaction or one of Ratis' own: refused either way.
            follower.consensus.detachGroup(cluster.database, true)
            follower.consensus.startGroup(cluster.database, foreign, emptyList())
            assertThat(foreign.foreignStates).isEmpty()
            repeat(5) { cluster.write("after-$it") }

            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(foreign.foreignStates)
                        .containsExactly(
                            "it was applied up to Raft index ${before + 3}, but the group's " +
                                "entry there is a different one"
                        )
                }
            assertThat(foreign.payloads()).containsExactly("foreign")
        }

    @Test
    fun `a state applied up to an entry that state machines do not apply is refused`() =
        runBlocking<Unit> {
            val cluster = formCluster()
            val leader = cluster.awaitLeader()
            val follower = cluster.nodes.first { it != leader }
            // The entries before the first write are Ratis' own and never reach state machines.
            val first = cluster.write("first")
            val foreign = foreignState(first - 1)

            cluster.restart(follower, foreign)
            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(foreign.foreignStates).hasSize(1)
                }
            // Again once Ratis has applied the entry.
            follower.consensus.detachGroup(cluster.database, true)
            follower.consensus.startGroup(cluster.database, foreign, emptyList())

            assertThat(foreign.foreignStates).hasSize(2)
            assertThat(foreign.payloads()).containsExactly("foreign")
        }

    @Test
    fun `a restarted member's own state is checked once and applied to`() =
        runBlocking<Unit> {
            val cluster = formCluster()
            val leader = cluster.awaitLeader()
            val follower = cluster.nodes.first { it != leader }
            val before = (1..3).map { cluster.write("before-$it") }.last()
            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(follower.machine.lastAppliedIndex()).isEqualTo(before)
                }
            val checked = CopyOnWriteArrayList<Long>()
            val checking =
                object : ReplicatedStateMachine by follower.machine {
                    override fun isLastApplied(entry: LogEntry): Boolean {
                        checked += entry.index
                        return follower.machine.isLastApplied(entry)
                    }
                }

            cluster.restart(follower, checking)
            cluster.write("after")

            await atMost
                RatisTestCluster.AWAIT untilAsserted
                {
                    assertThat(follower.machine.payloads()).last().isEqualTo("after")
                }
            assertThat(checked).containsExactly(before)
            assertThat(follower.machine.foreignStates).isEmpty()
        }
}
