package org.dozendb.cluster.internal.leadership

import java.io.ByteArrayOutputStream
import java.time.Duration
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.dozendb.cluster.MemberDirectory
import org.dozendb.cluster.WaitingLook
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.fake.InMemoryConsensus
import org.dozendb.consensus.fake.RecordingStateMachine
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.neo4j.logging.NullLog
import org.neo4j.logging.log4j.Log4jLogProvider

class LeaderBalancerTest {
    private val consensus = InMemoryConsensus()
    private val system = DatabaseRef("system", UUID(0, 1))
    private val films = DatabaseRef("films", UUID.randomUUID())
    private val leader = consensus.newMember()
    private val directory =
        MemberDirectory(
            leader,
            system,
            { null },
            { null },
            { emptyList() },
            NullLog.getInstance(),
            Duration.ofHours(1),
        )

    @AfterEach
    fun stopDirectory() {
        directory.stop()
    }

    @Test
    fun `stopping waits for a look in progress, and interrupts it quietly`() {
        leader.start()
        for (database in listOf(system, films)) {
            leader.startGroup(database, RecordingStateMachine(), listOf(leader.self))
            consensus.electLeader(database, leader.self.id)
        }
        directory.refresh()
        val output = ByteArrayOutputStream()
        val look = WaitingLook()
        val balancer =
            LeaderBalancer(
                leader,
                directory,
                created = { listOf(films) },
                handOver = { _, _, _, _ -> false },
                strategy = { BalancingStrategy.NO_BALANCING },
                // The server records are read only for a database with a priority tag.
                priorityTag = { "us" },
                tags = {
                    look.read()
                    emptyMap()
                },
                stabilityDelay = Duration.ZERO,
                transferTimeout = Duration.ofSeconds(5),
                contactLimit = Duration.ofSeconds(5),
                log = Log4jLogProvider(output).getLog(LeaderBalancer::class.java),
                interval = Duration.ofMillis(10),
            )
        balancer.start()

        assertThat(look.closingWaits(balancer::stop))
            .describedAs("stopping waits for the look in progress")
            .isTrue()
        assertThat(output.toString()).isEmpty()
    }
}
