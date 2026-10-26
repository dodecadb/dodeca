package org.dozendb.cluster.internal.replication

import java.io.ByteArrayOutputStream
import java.io.IOException
import java.lang.reflect.Proxy
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import kotlin.concurrent.thread
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.until
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.LogPosition
import org.dozendb.consensus.Role
import org.dozendb.consensus.Snapshot
import org.dozendb.consensus.fake.InMemoryConsensus
import org.dozendb.consensus.fake.RecordingStateMachine
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.neo4j.internal.id.IdGeneratorFactory
import org.neo4j.kernel.database.DatabaseIdFactory
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.logging.NullLog
import org.neo4j.logging.log4j.Log4jLogProvider

/** A member behind its group's log copies the database while it runs, then swaps the copy in. */
class StagedCatchUpTest {
    private val consensus = InMemoryConsensus()
    private val leader = consensus.newMember()
    private val other = consensus.newMember()
    private val self = consensus.newMember()
    private val all = listOf(leader, other, self)
    private val system = DatabaseRef("system", UUID.randomUUID())
    private val id = DatabaseIdFactory.from("films", UUID.randomUUID())
    private val database = DatabaseRef(id.name(), id.databaseId().uuid())
    private val machines = all.associate { it.self.id to RecordingStateMachine() }
    private val state = machines.getValue(self.self.id)
    private val events = CopyOnWriteArrayList<String>()
    private val scheduler = Executors.newSingleThreadScheduledExecutor()

    /**
     * Stages a copy from the member at the given address; copies of the leader's state by default.
     */
    @Volatile private var stage: (HostPort) -> StagedCopy = ::copyOf

    private val control =
        object : DatabaseControl {
            override fun stop(id: NamedDatabaseId) {
                events += "stop"
                self.detachGroup(database)
            }

            override fun start(id: NamedDatabaseId): Boolean {
                events += "start"
                self.startGroup(database, state, emptyList())
                return true
            }
        }

    private val catchUp =
        CatchUp(
            self,
            { control },
            { source, _, _ -> stage(source) },
            NullLog.getInstance(),
            retryDelay = Duration.ofMillis(10),
        )

    private val replica =
        ReplicatedDatabase(
            id,
            self,
            { error("not used") },
            mustDetach = { false },
            detached = {},
            adopt = {},
            idGeneratorFactory =
                Proxy.newProxyInstance(
                    javaClass.classLoader,
                    arrayOf(IdGeneratorFactory::class.java),
                ) { _, _, _ ->
                    error("not used")
                } as IdGeneratorFactory,
            storeExists = true,
            scheduler,
            NullLog.getInstance(),
            afterApply = {},
            onSnapshotNeeded = { _, _, _ -> },
            notLeaderMessage = { "not the leader" },
        )

    /** Leaves [self] behind the leader's log, which starts after entry 3. */
    @BeforeEach
    fun behindTheLog() =
        runBlocking<Unit> {
            all.forEach { it.start() }
            for (member in all) {
                member.startGroup(system, RecordingStateMachine(), all.map { it.self })
                member.startGroup(database, machines.getValue(member.self.id), all.map { it.self })
            }
            consensus.electLeader(database, leader.self.id)
            self.detachGroup(database)
            repeat(3) { leader.submit(database, "e$it".encodeToByteArray()) }
            consensus.compact(database, 3)
            self.startGroup(database, state, emptyList())
            assertThat(state.snapshotRequests).containsExactly(leader.self to 3L)
        }

    @AfterEach
    fun stop() {
        scheduler.shutdownNow()
    }

    private fun memberAt(address: HostPort) = all.single { it.self.raftAddress == address }

    private fun running(): String =
        if (self.status(database).role == Role.Inactive) "stopped" else "running"

    /** A copy of the state of the member at [address], taken now. */
    private fun copyOf(address: HostPort): StagedCopy {
        val source = memberAt(address)
        val copied =
            RecordingStateMachine().apply { replaceWith(machines.getValue(source.self.id)) }
        val snapshot = checkNotNull(source.currentSnapshot(database))
        events += "copy from ${address.host} while ${running()}"
        return object : StagedCopy {
            override val snapshot = snapshot

            override fun moveIntoPlace() {
                events += "move"
                state.replaceWith(copied)
            }
        }
    }

    @Test
    fun `the database runs while it is copied and stops only to move the copy into place`() =
        runBlocking<Unit> {
            catchUp.run(replica, leader.self, 3)

            assertThat(events)
                .containsExactly(
                    "copy from ${leader.self.raftAddress.host} while running",
                    "stop",
                    "move",
                    "start",
                )
            assertThat(self.latestSnapshot(database)).isEqualTo(Snapshot(LogPosition(3, 1), 3))
            leader.submit(database, "e3".encodeToByteArray())
            assertThat(state.payloads()).containsExactly("e0", "e1", "e2", "e3")
        }

    @Test
    fun `failed copies are retried from every member while the database keeps running`() {
        val failures = mutableMapOf(leader.self.raftAddress to 2, other.self.raftAddress to 1)
        stage = { address ->
            val left = failures.getValue(address)
            if (left > 0) {
                failures[address] = left - 1
                events += "failed from ${address.host} while ${running()}"
                throw IOException("unreachable")
            }
            copyOf(address)
        }

        catchUp.run(replica, leader.self, 3)

        val leaderHost = leader.self.raftAddress.host
        val otherHost = other.self.raftAddress.host
        assertThat(events)
            .containsExactly(
                "failed from $leaderHost while running",
                "failed from $otherHost while running",
                "failed from $leaderHost while running",
                "copy from $otherHost while running",
                "stop",
                "move",
                "start",
            )
        assertThat(self.latestSnapshot(database)).isEqualTo(Snapshot(LogPosition(3, 1), 3))
    }

    @Test
    fun `an interrupted copy leaves the database running as it was`() {
        stage = {
            events += "copying"
            Thread.sleep(Long.MAX_VALUE)
            error("interrupted")
        }
        val catchingUp = thread { catchUp.run(replica, leader.self, 3) }
        await atMost Duration.ofSeconds(10) until { "copying" in events }

        catchingUp.interrupt()
        catchingUp.join()

        assertThat(events).containsExactly("copying")
        assertThat(running()).isEqualTo("running")
        assertThat(self.latestSnapshot(database)).isNull()
    }

    @Test
    fun `a database that soon needs another copy warns that its member does not keep up`() {
        val output = ByteArrayOutputStream()
        val clock = MutableClock(Instant.parse("2026-10-19T10:00:00Z"))
        val warning = "needs another copy"
        val catchUp =
            CatchUp(
                self,
                { control },
                { source, _, _ -> stage(source) },
                Log4jLogProvider(output).getLog(CatchUp::class.java),
                retryDelay = Duration.ofMillis(10),
                clock = clock,
            )

        catchUp.run(replica, leader.self, 3)
        clock.now = clock.now.plus(Duration.ofMinutes(10))
        catchUp.run(replica, leader.self, 3)
        clock.now = clock.now.plus(Duration.ofHours(2))
        catchUp.run(replica, leader.self, 3)

        val log = output.toString()
        assertThat(log.split(warning)).describedAs(log).hasSize(2)
        assertThat(log).contains("$warning 600 s after the previous one")
    }

    private class MutableClock(var now: Instant) : Clock() {
        override fun instant(): Instant = now

        override fun getZone(): ZoneId = ZoneOffset.UTC

        override fun withZone(zone: ZoneId): Clock = this
    }

    @Test
    fun `a copy that cannot be moved into place leaves the database as it was`() {
        stage = { address ->
            val copy = copyOf(address)
            object : StagedCopy by copy {
                override fun moveIntoPlace() = throw IOException("disk full")
            }
        }

        catchUp.run(replica, leader.self, 3)

        assertThat(events)
            .containsExactly(
                "copy from ${leader.self.raftAddress.host} while running",
                "stop",
                "start",
            )
        assertThat(self.latestSnapshot(database)).isNull()
        assertThat(state.payloads()).isEmpty()
        assertThat(state.snapshotRequests).describedAs("asked again").hasSize(2)
    }
}
