package org.dozendb.cluster

import java.io.ByteArrayOutputStream
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.until
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.MemberId
import org.dozendb.consensus.fake.InMemoryConsensus
import org.dozendb.consensus.fake.RecordingStateMachine
import org.junit.jupiter.api.Test
import org.neo4j.logging.Log
import org.neo4j.logging.NullLog
import org.neo4j.logging.log4j.Log4jLogProvider

class MembershipReconcilerTest {
    private val consensus = InMemoryConsensus()
    private val system = DatabaseRef("system", UUID(0, 1))
    private val films = DatabaseRef("films", UUID.randomUUID())
    private val logs = List(3) { consensus.newMember() }
    private val initial = logs.map { it.self }
    private val leader = logs[0]

    /** The reconciler of [member], changing groups on the calling thread. */
    private fun reconciler(
        member: InMemoryConsensus.InMemoryConsensusLog = leader,
        log: Log = NullLog.getInstance(),
        hosts: (MemberId) -> Boolean = { true },
    ) =
        MembershipReconciler(
            member,
            system,
            { listOf(system, films) },
            log,
            changes = { it.run() },
            hosts = hosts,
        )

    private fun formCluster() {
        for (log in logs) {
            log.start()
            log.startGroup(system, RecordingStateMachine(), initial)
            log.startGroup(films, RecordingStateMachine(), initial)
        }
        consensus.electLeader(system, leader.self.id)
        consensus.electLeader(films, logs[1].self.id)
    }

    private fun changeCluster(change: suspend () -> Unit) = runBlocking { change() }

    @Test
    fun `a member added to the cluster joins a group once it runs it`() {
        formCluster()
        val added = consensus.newMember().apply { start() }
        added.joinGroup(system, initial)
        changeCluster { leader.addMember(system, added.self, TIMEOUT) }
        val reconciler = reconciler()

        assertThat(reconciler.reconcile()).describedAs("waits for it to run the group").isTrue()
        assertThat(leader.members(films)).containsExactlyElementsOf(initial)

        added.startGroup(films, RecordingStateMachine(), initial)

        assertThat(reconciler.reconcile()).isTrue()
        assertThat(leader.members(films)).containsExactlyElementsOf(initial + added.self)
        assertThat(reconciler.reconcile()).describedAs("nothing left to do").isFalse()
    }

    @Test
    fun `a member removed from the cluster leaves every group, before any member joins`() {
        formCluster()
        val added = consensus.newMember().apply { start() }
        added.joinGroup(system, initial)
        added.startGroup(films, RecordingStateMachine(), initial)
        changeCluster {
            leader.addMember(system, added.self, TIMEOUT)
            leader.removeMember(system, logs[2].self.id, TIMEOUT)
        }
        val reconciler = reconciler()

        reconciler.reconcile()

        assertThat(leader.members(films)).containsExactly(logs[0].self, logs[1].self)

        reconciler.reconcile()

        assertThat(leader.members(films)).containsExactly(logs[0].self, logs[1].self, added.self)
        assertThat(logs[2].membership(films).removed).isTrue()
    }

    @Test
    fun `only the system leader changes groups`() {
        formCluster()
        changeCluster { leader.removeMember(system, logs[2].self.id, TIMEOUT) }

        assertThat(reconciler(logs[1]).reconcile()).isFalse()

        assertThat(leader.members(films)).containsExactlyElementsOf(initial)
    }

    @Test
    fun `a group keeps one of the cluster's members`() {
        formCluster()
        val added = consensus.newMember().apply { start() }
        added.joinGroup(system, initial)
        added.startGroup(films, RecordingStateMachine(), initial)
        changeCluster {
            leader.addMember(system, added.self, TIMEOUT)
            leader.removeMember(films, logs[0].self.id, TIMEOUT)
            leader.removeMember(system, logs[1].self.id, TIMEOUT)
            leader.removeMember(system, logs[2].self.id, TIMEOUT)
        }
        val reconciler = reconciler()

        reconciler.reconcile()

        assertThat(leader.members(films).map { it.id })
            .describedAs("gains the cluster's members before losing its own")
            .containsExactly(logs[1].self.id, logs[2].self.id, leader.self.id)
    }

    @Test
    fun `closing waits for a look in progress, and interrupts it quietly`() {
        formCluster()
        val output = ByteArrayOutputStream()
        val reading = CountDownLatch(1)
        val interrupted = CountDownLatch(1)
        val released = CountDownLatch(1)
        val reconciler =
            reconciler(log = Log4jLogProvider(output).getLog(MembershipReconciler::class.java)) {
                reading.countDown()
                // A read of the system graph that fails once interrupted, when the test lets it.
                try {
                    CountDownLatch(1).await()
                } catch (e: InterruptedException) {
                    interrupted.countDown()
                    released.await()
                    throw e
                }
                true
            }
        reconciler.start()
        assertThat(reading.await(1, TimeUnit.MINUTES)).isTrue()

        val closing = thread { reconciler.close() }
        assertThat(interrupted.await(1, TimeUnit.MINUTES)).isTrue()
        // Closing has returned, or waits for the look to end.
        await atMost
            Duration.ofMinutes(1) until
            {
                !closing.isAlive || closing.state == Thread.State.TIMED_WAITING
            }
        val waited = closing.isAlive
        released.countDown()
        closing.join()

        assertThat(waited).describedAs("closing waits for the look in progress").isTrue()
        assertThat(output.toString()).isEmpty()
    }

    private companion object {
        val TIMEOUT: Duration = Duration.ofSeconds(5)
    }
}
