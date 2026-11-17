package dev.dodeca.cluster.internal.leadership

import dev.dodeca.cluster.DatabaseGroups
import dev.dodeca.cluster.DatabaseStatesCodec
import dev.dodeca.cluster.MemberDirectory
import dev.dodeca.cluster.MemberMetadata
import dev.dodeca.cluster.WaitingLook
import dev.dodeca.cluster.internal.topology.DatabaseLocations
import dev.dodeca.consensus.DatabaseRef
import dev.dodeca.consensus.MemberId
import dev.dodeca.consensus.fake.InMemoryConsensus
import dev.dodeca.consensus.fake.RecordingStateMachine
import dev.dodeca.core.edition.DatabaseState
import java.io.ByteArrayOutputStream
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.until
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.neo4j.logging.InternalLog
import org.neo4j.logging.NullLog
import org.neo4j.logging.log4j.Log4jLogProvider

class LeaderBalancerTest {
    private val consensus = InMemoryConsensus()
    private val system = DatabaseRef("system", UUID(0, 1))
    private val groups = DatabaseGroups()
    private val films = DatabaseRef("films", UUID.randomUUID())
    private val members = List(3) { consensus.newMember(metadata = reporting()) }
    private val leader = members[0]
    private val directory =
        MemberDirectory(
            leader,
            groups,
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

    /** What a member reports: [films] in [status]. */
    private fun reporting(status: DatabaseState.Status = DatabaseState.Status.ONLINE) =
        mapOf(
            MemberMetadata.DATABASES to
                DatabaseStatesCodec.encode(listOf(DatabaseState(films.name, films.id, status)))
        )

    /** The balancer of the leader, where every database has the priority tag `us` by default. */
    private fun balancer(
        tags: () -> Map<MemberId, Set<String>> = { emptyMap() },
        log: InternalLog = NullLog.getInstance(),
        strategy: BalancingStrategy = BalancingStrategy.NO_BALANCING,
        priorityTag: String? = "us",
        databases: List<DatabaseRef> = listOf(films),
        handOver: (DatabaseRef, MemberId, String, Duration) -> Boolean = { _, _, _, _ -> false },
        /** Counts the looks, each of which ends reading the databases created here. */
        looks: AtomicInteger = AtomicInteger(),
    ) =
        LeaderBalancer(
            leader,
            directory,
            DatabaseLocations(leader, directory),
            created = {
                looks.incrementAndGet()
                databases
            },
            databases = { databases.associateWith { emptySet() } },
            handOver = handOver,
            strategy = { strategy },
            priorityTag = { priorityTag },
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

    @Test
    fun `a member leading more than its share says once why none of the others takes one over`() {
        val (_, starting, down) = members
        starting.metadata = reporting(DatabaseState.Status.STARTING)
        consensus.crash(down.self.id)
        directory.refresh()
        val output = ByteArrayOutputStream()
        val handedOver = CopyOnWriteArrayList<Pair<DatabaseRef, MemberId>>()
        val looks = AtomicInteger()
        val balancer =
            balancer(
                log = Log4jLogProvider(output).getLog(LeaderBalancer::class.java),
                strategy = BalancingStrategy.EQUAL_BALANCING,
                priorityTag = null,
                // The leader leads both, the others none.
                databases = listOf(system, films),
                handOver = { database, to, _, _ ->
                    handedOver += database to to
                    false
                },
                looks = looks,
            )
        balancer.start()
        try {
            val reason =
                "none that leads fewer can take films over: " +
                    listOf(
                            starting.self.id to "reports it STARTING",
                            down.self.id to "does not answer",
                        )
                        .sortedBy { it.first }
                        .joinToString { (member, why) -> "member $member $why" }
            await atMost
                Duration.ofSeconds(10) untilAsserted
                {
                    assertThat(output.toString()).contains("Leaders stay uneven", reason)
                }
            val logged = looks.get()
            await atMost Duration.ofSeconds(10) until { looks.get() >= logged + 20 }
            assertThat(output.toString().lines().filter { "Leaders stay uneven" in it })
                .describedAs("logged once while the reason holds")
                .hasSize(1)

            starting.metadata = reporting()
            directory.refresh()

            await atMost
                Duration.ofSeconds(10) untilAsserted
                {
                    assertThat(handedOver).contains(films to starting.self.id)
                }
        } finally {
            balancer.stop()
        }
    }
}
