package org.dozendb.cluster.internal.leadership

import java.time.Duration
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.until
import org.awaitility.kotlin.untilAsserted
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.PeerInfo
import org.dozendb.consensus.fake.InMemoryConsensus
import org.dozendb.consensus.fake.RecordingStateMachine
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.neo4j.logging.NullLog

class LeaderAnnouncerTest {
    private val consensus = InMemoryConsensus()
    private val system = DatabaseRef("system", UUID(0, 1))
    private val films = UUID(0, 7)
    private val logs = List(3) { consensus.newMember() }
    @Volatile private var leading = emptyMap<UUID, Long>()
    /** How often the announcer asked what this member leads. */
    private val asked = AtomicInteger()
    private val announcer =
        LeaderAnnouncer(
            logs[0],
            { logs[0].members(system) },
            {
                asked.incrementAndGet()
                leading
            },
            Duration.ofSeconds(1),
            NullLog.getInstance(),
        )
    private val heard = logs.map { log ->
        CopyOnWriteArrayList<PeerInfo>().also { heard ->
            log.addAnnouncementListener { heard += it }
        }
    }

    init {
        logs.forEach { it.start() }
        logs.forEach {
            it.startGroup(system, RecordingStateMachine(), logs.map { log -> log.self })
        }
        announcer.start()
    }

    @AfterEach
    fun stop() {
        announcer.stop()
    }

    @Test
    fun `tells the other members when the groups this member leads change, and only then`() {
        leading = mapOf(films to 3L)
        announcer.request()
        await atMost
            Duration.ofSeconds(30) untilAsserted
            {
                assertThat(heard[1]).hasSize(1)
                assertThat(heard[2]).hasSize(1)
            }
        announcer.request()
        awaitAsked(2)
        leading = mapOf(films to 4L)
        announcer.request()
        awaitAsked(3)
        // Asked once more after the change was announced.
        announcer.request()
        awaitAsked(4)

        assertThat(heard[1].map { it.member }).containsExactly(logs[0].self, logs[0].self)
        assertThat(heard[2].map { it.member }).containsExactly(logs[0].self, logs[0].self)
        assertThat(heard[0]).isEmpty()
    }

    @Test
    fun `a member that cannot be reached keeps no other from hearing`() {
        consensus.crash(logs[1].self.id)
        leading = mapOf(films to 3L)

        announcer.request()

        await atMost
            Duration.ofSeconds(30) untilAsserted
            {
                assertThat(heard[2].map { it.member }).containsExactly(logs[0].self)
            }
        assertThat(heard[1]).isEmpty()
    }

    private fun awaitAsked(times: Int) {
        await atMost Duration.ofSeconds(30) until { asked.get() == times }
    }
}
