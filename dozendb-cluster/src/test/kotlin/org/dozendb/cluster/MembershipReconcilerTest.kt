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
import org.dozendb.consensus.Member
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
    private val groups = DatabaseGroups()
    private val films = DatabaseRef("films", UUID.randomUUID())
    private val logs = List(3) { consensus.newMember() }
    private val initial = logs.map { it.self }
    private val leader = logs[0]

    /**
     * The reconciler of [member], changing groups on the calling thread, with films placed on the
     * members of the cluster that [hosts].
     */
    private fun reconciler(
        member: ConsensusLog = leader,
        log: Log = NullLog.getInstance(),
        hosts: (MemberId) -> Boolean = { true },
    ) =
        MembershipReconciler(
            member,
            groups,
            { mapOf(films to member.members(system).map { it.id }.filter(hosts).toSet()) },
            log,
            changes = { it.run() },
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
    fun `a member removed from the cluster leaves every group once the one that replaces it votes`() {
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

        assertThat(leader.members(films)).containsExactlyElementsOf(initial + added.self)

        reconciler.reconcile()

        assertThat(leader.members(films)).containsExactly(logs[0].self, logs[1].self, added.self)
        assertThat(logs[2].membership(films).removed).isTrue()
    }

    @Test
    fun `a system leader that hosts no database changes groups that do not run on it`() {
        val free = logs[0]
        val hosting = logs.drop(1).map { it.self }
        for (log in logs) {
            log.start()
            log.startGroup(system, RecordingStateMachine(), initial)
        }
        for (log in logs.drop(1)) log.startGroup(films, RecordingStateMachine(), hosting)
        consensus.electLeader(system, free.self.id)
        consensus.electLeader(films, logs[1].self.id)
        val added = consensus.newMember().apply { start() }
        added.joinGroup(system, initial)
        added.startGroup(films, RecordingStateMachine(), hosting)
        changeCluster { free.addMember(system, added.self, TIMEOUT) }
        val reconciler = reconciler(hosts = { it != free.self.id })

        reconciler.reconcile()

        assertThat(logs[1].members(films)).containsExactlyElementsOf(hosting + added.self)

        changeCluster { free.removeMember(system, logs[2].self.id, TIMEOUT) }
        reconciler.reconcile()

        assertThat(logs[1].members(films)).containsExactly(logs[1].self, added.self)
        assertThat(free.localGroups()).doesNotContain(films.id)
    }

    /** A reconciler of [leader] with films placed on it alone, on a clock that reads [now]. */
    private fun shrinking(now: () -> Long) =
        MembershipReconciler(
            leader,
            groups,
            { mapOf(films to setOf(leader.self.id)) },
            NullLog.getInstance(),
            changes = { it.run() },
            handOverGrace = GRACE,
            nanoTime = now,
        )

    @Test
    fun `the leader of a group leaves it last, once it handed the leadership over`() {
        formCluster()
        val reconciler = shrinking { 0 }

        reconciler.reconcile()

        assertThat(leader.members(films)).containsExactly(logs[0].self, logs[1].self)
        assertThat(reconciler.reconcile()).describedAs("waits for the hand-over").isTrue()
        assertThat(leader.members(films)).containsExactly(logs[0].self, logs[1].self)

        consensus.electLeader(films, logs[0].self.id)
        reconciler.reconcile()

        assertThat(leader.members(films)).containsExactly(logs[0].self)
    }

    @Test
    fun `a leader that does not hand the leadership over leaves after a grace period`() {
        formCluster()
        var now = 0L
        val reconciler = shrinking { now }
        reconciler.reconcile()
        reconciler.reconcile()

        now += GRACE.toNanos() - 1
        reconciler.reconcile()

        assertThat(leader.members(films)).containsExactly(logs[0].self, logs[1].self)

        now += 1
        reconciler.reconcile()

        assertThat(leader.members(films)).containsExactly(logs[0].self)
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
                    members: List<Member>,
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
        val GRACE: Duration = Duration.ofSeconds(30)
    }
}
