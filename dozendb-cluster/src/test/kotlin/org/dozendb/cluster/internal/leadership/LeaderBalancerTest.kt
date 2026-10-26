package org.dozendb.cluster.internal.leadership

import java.io.ByteArrayOutputStream
import java.time.Duration
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import org.assertj.core.api.Assertions.assertThat
import org.dozendb.cluster.DatabaseStatesCodec
import org.dozendb.cluster.MemberDirectory
import org.dozendb.cluster.MemberMetadata
import org.dozendb.cluster.WaitingLook
import org.dozendb.cluster.internal.topology.DatabaseLocations
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.MemberId
import org.dozendb.consensus.fake.InMemoryConsensus
import org.dozendb.consensus.fake.RecordingStateMachine
import org.dozendb.core.edition.DatabaseState
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.neo4j.logging.InternalLog
import org.neo4j.logging.NullLog
import org.neo4j.logging.log4j.Log4jLogProvider

class LeaderBalancerTest {
    private val consensus = InMemoryConsensus()
    private val system = DatabaseRef("system", UUID(0, 1))
    private val films = DatabaseRef("films", UUID.randomUUID())
    private val members =
        List(3) {
            consensus.newMember(
                metadata =
                    mapOf(
                        MemberMetadata.DATABASES to
                            DatabaseStatesCodec.encode(
                                listOf(
                                    DatabaseState(films.name, films.id, DatabaseState.Status.ONLINE)
                                )
                            )
                    )
            )
        }
    private val leader = members[0]
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

    init {
        members.forEach { it.start() }
        for (database in listOf(system, films)) {
            members.forEach {
                it.startGroup(database, RecordingStateMachine(), members.map { m -> m.self })
            }
            consensus.electLeader(database, leader.self.id)
        }
        directory.refresh()
    }

    @AfterEach
    fun stopDirectory() {
        directory.stop()
    }

    /** The balancer of the leader, where every database has the priority tag `us`. */
    private fun balancer(
        tags: () -> Map<MemberId, Set<String>>,
        log: InternalLog = NullLog.getInstance(),
    ) =
        LeaderBalancer(
            leader,
            directory,
            DatabaseLocations(leader, directory),
            created = { listOf(films) },
            databases = { mapOf(films to emptySet()) },
            handOver = { _, _, _, _ -> false },
            strategy = { BalancingStrategy.NO_BALANCING },
            priorityTag = { "us" },
            tags = tags,
            stabilityDelay = Duration.ZERO,
            transferTimeout = Duration.ofSeconds(5),
            contactLimit = Duration.ofSeconds(5),
            log = log,
            interval = Duration.ofMillis(10),
        )

    @Test
    fun `stopping waits for a look in progress, and interrupts it quietly`() {
        val output = ByteArrayOutputStream()
        val look = WaitingLook()
        val balancer =
            balancer(
                tags = {
                    look.read()
                    emptyMap()
                },
                log = Log4jLogProvider(output).getLog(LeaderBalancer::class.java),
            )
        balancer.start()

        assertThat(look.closingWaits(balancer::stop))
            .describedAs("stopping waits for the look in progress")
            .isTrue()
        assertThat(output.toString()).isEmpty()
    }

    @Test
    fun `a hand-over ranks by the tags the last look read, and reads none itself`() {
        val (_, first, second) = members.map { it.self.id }
        var tags = mapOf(second to setOf("us"))
        val reads = AtomicInteger()
        val balancer =
            balancer(
                tags = {
                    reads.incrementAndGet()
                    tags
                }
            )
        balancer.view()
        tags = mapOf(first to setOf("us"))

        // While the databases stop, system among them.
        assertThat(balancer.rank(films, listOf(first, second))).containsExactly(second, first)
        assertThat(reads).hasValue(1)
    }
}
