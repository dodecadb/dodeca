package org.dozendb.cluster.internal

import java.io.ByteArrayOutputStream
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.dozendb.cluster.WaitingLook
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.fake.InMemoryConsensus
import org.dozendb.consensus.fake.RecordingStateMachine
import org.junit.jupiter.api.Test
import org.neo4j.logging.log4j.Log4jLogProvider

class ServerReconcilerTest {
    private val consensus = InMemoryConsensus()
    private val system = DatabaseRef("system", UUID(0, 1))
    private val leader = consensus.newMember()

    @Test
    fun `closing waits for a look in progress, and interrupts it quietly`() {
        leader.start()
        leader.startGroup(system, RecordingStateMachine(), listOf(leader.self))
        consensus.electLeader(system, leader.self.id)
        val output = ByteArrayOutputStream()
        val look = WaitingLook()
        val reconciler =
            ServerReconciler(
                leader,
                system,
                { error("The look reads nothing") },
                { look.read() },
                leading = { true },
                current = { true },
                report = { null },
                autoEnable = false,
                log = Log4jLogProvider(output).getLog(ServerReconciler::class.java),
            )
        reconciler.start()

        assertThat(look.closingWaits(reconciler::close))
            .describedAs("closing waits for the look in progress")
            .isTrue()
        assertThat(output.toString()).isEmpty()
    }
}
