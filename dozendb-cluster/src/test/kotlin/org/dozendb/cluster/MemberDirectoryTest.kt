package org.dozendb.cluster

import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.entry
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.until
import org.awaitility.kotlin.untilAsserted
import org.dozendb.consensus.ConsensusUnavailableException
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.fake.InMemoryConsensus
import org.dozendb.consensus.fake.RecordingStateMachine
import org.dozendb.core.edition.DatabaseState
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertTimeoutPreemptively
import org.junit.jupiter.api.Test
import org.neo4j.logging.NullLog

class MemberDirectoryTest {
    private val consensus = InMemoryConsensus()
    private val system = DatabaseRef("system", UUID(0, 1))
    private val logs =
        List(3) { index ->
            consensus.newMember(
                metadata =
                    mapOf(
                        MemberMetadata.BOLT_ADDRESS to "core$index:7687",
                        MemberMetadata.ROUTING_ADDRESS to "core$index:7688",
                    )
            )
        }
    private var selfDatabases = emptyList<DatabaseState>()
    private var selfReadOnly = emptySet<UUID>()
    private val directory =
        MemberDirectory(
            logs[0],
            system,
            { HostPort("self", 7687) },
            { HostPort("self", 7688) },
            { selfDatabases },
            NullLog.getInstance(),
            Duration.ofHours(1),
            { selfReadOnly },
        )

    init {
        logs.forEach { it.start() }
        logs.forEach {
            it.startGroup(system, RecordingStateMachine(), logs.map { log -> log.self })
        }
    }

    /** Directories that tests made, stopped after each. */
    private val others = mutableListOf<MemberDirectory>()

    @AfterEach
    fun stop() {
        directory.stop()
        others.forEach(MemberDirectory::stop)
    }

    @Test
    fun `a refresh with a deadline leaves out a member that answers later, as it was`() {
        directory.refresh()
        logs[2].probeDelay = Duration.ofMinutes(1)

        val answered = directory.refresh(Duration.ofMillis(200))

        assertThat(answered).containsExactly(logs[1].self.id)
        assertThat(directory.answering(logs[2].self.id)).isTrue()
    }

    @Test
    fun `last committed transactions are what members report`() {
        val films = UUID(0, 7)
        logs[1].metadata +=
            MemberMetadata.LAST_COMMITTED_TRANSACTIONS to "$films=42,not=a,=,${UUID(0, 8)}"

        directory.refresh()

        assertThat(directory.lastCommitted(logs[1].self.id)).containsExactly(entry(films, 42L))
        assertThat(directory.lastCommitted(logs[2].self.id)).isEmpty()
    }

    @Test
    fun `learns the addresses members report`() {
        directory.refresh()

        assertThat(directory.boltAddress(logs[1].self.id)).isEqualTo(HostPort("core1", 7687))
        assertThat(directory.boltAddress(logs[2].self.id)).isEqualTo(HostPort("core2", 7687))
        assertThat(directory.boltAddress(logs[0].self.id)).isEqualTo(HostPort("self", 7687))
        assertThat(directory.routingAddress(logs[1].self.id)).isEqualTo(HostPort("core1", 7688))
        assertThat(directory.routingAddress(logs[0].self.id)).isEqualTo(HostPort("self", 7688))
    }

    @Test
    fun `a member that reports no routing address or an invalid one takes no forwarded writes`() {
        logs[1].metadata = mapOf(MemberMetadata.BOLT_ADDRESS to "core1:7687")
        logs[2].metadata =
            mapOf(
                MemberMetadata.BOLT_ADDRESS to "core2:7687",
                MemberMetadata.ROUTING_ADDRESS to "not an address",
            )

        directory.refresh()

        assertThat(directory.routingAddress(logs[1].self.id)).isNull()
        assertThat(directory.routingAddress(logs[2].self.id)).isNull()
    }

    @Test
    fun `keeps the last address of a member that stops answering and ignores invalid ones`() {
        directory.refresh()
        logs[1].close()
        logs[2].metadata = mapOf(MemberMetadata.BOLT_ADDRESS to "not an address")

        directory.refresh()

        assertThat(directory.boltAddress(logs[1].self.id)).isEqualTo(HostPort("core1", 7687))
        assertThat(directory.boltAddress(logs[2].self.id)).isEqualTo(HostPort("core2", 7687))
    }

    @Test
    fun `names the members whose copy of a database failed, this one included`() {
        val films = UUID(0, 7)
        val books = UUID(0, 8)
        fun report(vararg states: DatabaseState) =
            mapOf(
                MemberMetadata.BOLT_ADDRESS to "core:7687",
                MemberMetadata.DATABASES to DatabaseStatesCodec.encode(states.toList()),
            )
        logs[1].metadata =
            report(
                DatabaseState("films", films, DatabaseState.Status.FAILED, "No space left"),
                DatabaseState("books", books, DatabaseState.Status.ONLINE),
            )
        logs[2].metadata = report(DatabaseState("films", films, DatabaseState.Status.ONLINE))
        selfDatabases = listOf(DatabaseState("books", books, DatabaseState.Status.FAILED, "Panic"))

        directory.refresh()

        assertThat(directory.failedCopies(films)).containsExactly(logs[1].self.id)
        assertThat(directory.failedCopies(books)).containsExactly(logs[0].self.id)
        assertThat(directory.failedCopies(UUID(0, 9))).isEmpty()
    }

    @Test
    fun `knows where databases are read-only and which members answer`() {
        val id = UUID.randomUUID()
        logs[1].metadata += MemberMetadata.READ_ONLY_DATABASES to "$id,${UUID(0, 9)}"
        logs[2].metadata += MemberMetadata.READ_ONLY_DATABASES to ""
        selfReadOnly = setOf(id)
        assertThat(directory.refreshed).isFalse()

        directory.refresh()

        assertThat(directory.refreshed).isTrue()
        assertThat(directory.readOnlyDatabases(logs[0].self.id)).containsExactly(id)
        assertThat(directory.readOnlyDatabases(logs[1].self.id)).containsExactly(id, UUID(0, 9))
        assertThat(directory.readOnlyDatabases(logs[2].self.id)).isEmpty()
        assertThat(logs.map { directory.answering(it.self.id) }).containsOnly(true)

        consensus.crash(logs[2].self.id)
        directory.refresh()

        assertThat(directory.answering(logs[2].self.id)).isFalse()
        assertThat(directory.answering(logs[1].self.id)).isTrue()
        // What it reported last stays known.
        assertThat(directory.readOnlyDatabases(logs[1].self.id)).contains(id)
    }

    @Test
    fun `an answer that arrives after a later one is left out`() {
        val run = UUID.randomUUID()
        logs[1].metadata = report(run, 1, "earlier:7687")
        val answers = AtomicInteger()
        val delayed = CountDownLatch(1)
        val held = CompletableDeferred<Unit>()
        logs[1].onAnswered = {
            if (answers.incrementAndGet() == 1) {
                delayed.countDown()
                held.await()
            }
        }
        val earlier = thread { directory.refresh() }
        delayed.await()
        logs[1].metadata = report(run, 2, "later:7687")

        directory.refresh()
        held.complete(Unit)
        earlier.join()

        assertThat(directory.boltAddress(logs[1].self.id)).isEqualTo(HostPort("later", 7687))
    }

    @Test
    fun `a report of a new run of a member counts, whatever its number`() {
        logs[1].metadata = report(UUID.randomUUID(), 5, "before:7687")
        directory.refresh()
        logs[1].metadata = report(UUID.randomUUID(), 1, "restarted:7687")

        directory.refresh()

        assertThat(directory.boltAddress(logs[1].self.id)).isEqualTo(HostPort("restarted", 7687))
    }

    @Test
    fun `a probe that fails after a later one was answered leaves the member answering`() {
        val probes = AtomicInteger()
        val probed = CountDownLatch(1)
        val held = CompletableDeferred<Unit>()
        logs[2].onProbe = {
            if (probes.incrementAndGet() == 1) {
                probed.countDown()
                held.await()
                throw ConsensusUnavailableException("lost on the way")
            }
        }
        val earlier = thread { directory.refresh() }
        probed.await()

        directory.refresh()
        held.complete(Unit)
        earlier.join()

        assertThat(directory.answering(logs[2].self.id)).isTrue()
    }

    @Test
    fun `awaiting a refresh sees what members report once asked`() {
        val films = UUID(0, 7)
        directory.start()
        logs[1].metadata += MemberMetadata.LEADING to "$films=3"

        assertThat(directory.awaitRefresh(Duration.ofSeconds(30))).isTrue()

        assertThat(directory.leading(logs[1].self.id)).containsExactly(entry(films, 3L))
    }

    @Test
    fun `those who await a refresh at the same time share one probe, started after they asked`() {
        val directory = directory(demandDeadline = Duration.ofMinutes(1))
        val probes = AtomicInteger()
        val held = CompletableDeferred<Unit>()
        logs[2].onProbe = {
            probes.incrementAndGet()
            held.await()
        }
        directory.start()
        // The periodic probe on start, then a probe on demand, are under way.
        await atMost Duration.ofSeconds(30) until { probes.get() == 1 }
        val first = thread { directory.awaitRefresh(Duration.ofMinutes(1)) }
        await atMost Duration.ofSeconds(30) until { probes.get() == 2 }
        val refreshed = ConcurrentHashMap.newKeySet<Boolean>()
        val waiting =
            List(3) { thread { refreshed += directory.awaitRefresh(Duration.ofMinutes(1)) } }
        await atMost Duration.ofSeconds(30) until { waiting.all { it.waiting() } }

        held.complete(Unit)
        (waiting + first).forEach { it.join() }

        assertThat(refreshed).containsExactly(true)
        assertThat(probes.get()).isEqualTo(3)
    }

    @Test
    fun `a member that answers nothing holds up no probe on demand`() {
        val films = UUID(0, 7)
        logs[2].onProbe = { CompletableDeferred<Unit>().await() }
        directory.start()
        logs[1].metadata += MemberMetadata.LEADING to "$films=3"

        assertThat(directory.awaitRefresh(Duration.ofSeconds(10))).isTrue()

        assertThat(directory.leading(logs[1].self.id)).containsExactly(entry(films, 3L))
    }

    @Test
    fun `awaiting a refresh gives up after its timeout, keeping what members reported`() {
        directory.start()
        assertThat(directory.awaitRefresh(Duration.ofSeconds(30))).isTrue()
        logs[2].probeDelay = Duration.ofMinutes(1)

        assertThat(directory.awaitRefresh(Duration.ofMillis(200))).isFalse()

        assertThat(directory.boltAddress(logs[2].self.id)).isEqualTo(HostPort("core2", 7687))
    }

    @Test
    fun `a stopped directory keeps no one waiting for a refresh`() {
        val directory = directory(demandDeadline = Duration.ofMinutes(1))
        val probes = AtomicInteger()
        logs[2].onProbe = {
            probes.incrementAndGet()
            CompletableDeferred<Unit>().await()
        }
        directory.start()
        // Those waiting for a probe on demand under way, and those waiting for the next.
        await atMost Duration.ofSeconds(30) until { probes.get() == 1 }
        var first: Boolean? = null
        val under = thread { first = directory.awaitRefresh(Duration.ofMinutes(1)) }
        await atMost Duration.ofSeconds(30) until { probes.get() == 2 }
        assertTimeoutPreemptively(Duration.ofSeconds(30)) {
            var refreshed: Boolean? = null
            val waiting = thread { refreshed = directory.awaitRefresh(Duration.ofMinutes(1)) }
            await atMost Duration.ofSeconds(30) until { waiting.waiting() }

            directory.stop()
            waiting.join()
            under.join()

            assertThat(refreshed).isFalse()
            assertThat(first).isFalse()
            assertThat(directory.awaitRefresh(Duration.ofMinutes(1))).isFalse()
        }
    }

    @Test
    fun `asking for an unknown member triggers a refresh`() {
        directory.start()
        await atMost
            Duration.ofSeconds(5) untilAsserted
            {
                assertThat(directory.boltAddress(logs[1].self.id)).isNotNull()
            }
        logs[2].metadata = mapOf(MemberMetadata.BOLT_ADDRESS to "moved:7688")
        val newcomer =
            consensus
                .newMember(metadata = mapOf(MemberMetadata.BOLT_ADDRESS to "core9:7687"))
                .apply { start() }
        newcomer.joinGroup(system, logs.map { it.self })
        consensus.electLeader(system, logs[0].self.id)
        runBlocking { logs[0].addMember(system, newcomer.self, Duration.ofSeconds(5)) }

        await atMost
            Duration.ofSeconds(5) untilAsserted
            {
                assertThat(directory.boltAddress(newcomer.self.id))
                    .isEqualTo(HostPort("core9", 7687))
            }
        // Refreshing on demand also picks up changes of known members.
        assertThat(directory.boltAddress(logs[2].self.id)).isEqualTo(HostPort("moved", 7688))
    }

    private fun Thread.waiting() = state == Thread.State.TIMED_WAITING

    private fun directory(demandDeadline: Duration) =
        MemberDirectory(
                logs[0],
                system,
                { HostPort("self", 7687) },
                { HostPort("self", 7688) },
                { selfDatabases },
                NullLog.getInstance(),
                Duration.ofHours(1),
                { selfReadOnly },
                demandDeadline = demandDeadline,
            )
            .also { others += it }

    private fun report(run: UUID, number: Long, bolt: String) =
        mapOf(
            MemberMetadata.BOLT_ADDRESS to bolt,
            MemberMetadata.REPORT_NUMBER to "$run/$number",
        )
}
