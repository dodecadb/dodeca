package org.dozendb.cluster

import java.io.ByteArrayOutputStream
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.dozendb.consensus.ConsensusLog
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
        member: ConsensusLog = leader,
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
        val look = WaitingLook()
        val reconciler =
            reconciler(log = Log4jLogProvider(output).getLog(MembershipReconciler::class.java)) {
                look.read()
                true
            }
        reconciler.start()

        assertThat(look.closingWaits(reconciler::close))
            .describedAs("closing waits for the look in progress")
            .isTrue()
        assertThat(output.toString()).isEmpty()
    }

    @Test
    fun `closing interrupts a group change in progress quietly`() {
        formCluster()
        changeCluster { leader.removeMember(system, logs[2].self.id, TIMEOUT) }
        val output = ByteArrayOutputStream()
        val changing = CountDownLatch(1)
        val waiting =
            object : ConsensusLog by leader {
                override suspend fun removeMember(
                    database: DatabaseRef,
                    id: MemberId,
                    timeout: Duration,
                ) {
                    changing.countDown()
                    awaitCancellation()
                }
            }
        val reconciler =
            reconciler(waiting, Log4jLogProvider(output).getLog(MembershipReconciler::class.java))
        reconciler.start()
        assertThat(changing.await(1, TimeUnit.MINUTES)).isTrue()

        reconciler.close()

        assertThat(output.toString().lines().filter { " ERROR " in it }).isEmpty()
    }

    private companion object {
        val TIMEOUT: Duration = Duration.ofSeconds(5)
    }
}
