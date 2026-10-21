package org.dozendb.cluster

import java.time.Duration
import java.util.UUID
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.fake.InMemoryConsensus
import org.dozendb.consensus.fake.RecordingStateMachine
import org.dozendb.core.edition.DatabaseState
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.neo4j.logging.NullLog

class MemberDirectoryTest {
    private val consensus = InMemoryConsensus()
    private val system = DatabaseRef("system", UUID(0, 1))
    private val logs =
        List(3) { index ->
            consensus.newMember(metadata = mapOf(MemberMetadata.BOLT_ADDRESS to "core$index:7687"))
        }
    private var selfDatabases = emptyList<DatabaseState>()
    private val directory =
        MemberDirectory(
            logs[0],
            system,
            { HostPort("self", 7687) },
            { selfDatabases },
            NullLog.getInstance(),
            Duration.ofHours(1),
        )

    init {
        logs.forEach { it.start() }
        logs.forEach {
            it.startGroup(system, RecordingStateMachine(), logs.map { log -> log.self })
        }
    }

    @AfterEach
    fun stop() {
        directory.stop()
    }

    @Test
    fun `learns the addresses members report`() {
        directory.refresh()

        assertThat(directory.boltAddress(logs[1].self.id)).isEqualTo(HostPort("core1", 7687))
        assertThat(directory.boltAddress(logs[2].self.id)).isEqualTo(HostPort("core2", 7687))
        assertThat(directory.boltAddress(logs[0].self.id)).isEqualTo(HostPort("self", 7687))
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
}
