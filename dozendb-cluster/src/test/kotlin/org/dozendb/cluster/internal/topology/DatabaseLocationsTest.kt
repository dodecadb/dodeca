package org.dozendb.cluster.internal.topology

import java.time.Duration
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.dozendb.cluster.DatabaseGroups
import org.dozendb.cluster.DatabaseStatesCodec
import org.dozendb.cluster.MemberDirectory
import org.dozendb.cluster.MemberMetadata
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.MemberId
import org.dozendb.consensus.fake.InMemoryConsensus
import org.dozendb.consensus.fake.InMemoryConsensus.InMemoryConsensusLog
import org.dozendb.consensus.fake.RecordingStateMachine
import org.dozendb.core.edition.DatabaseState
import org.dozendb.core.servers.CopyMode
import org.junit.jupiter.api.Test
import org.neo4j.logging.NullLog

class DatabaseLocationsTest {
    private val consensus = InMemoryConsensus()
    private val system = DatabaseRef("system", UUID(0, 1))
    private val groups = DatabaseGroups()
    private val films = DatabaseRef("films", UUID.randomUUID())

    /** A member that hosts only `system`, and three that host films too, the first leading it. */
    private val free = consensus.newMember()
    private val hosts =
        List(3) {
            consensus.newMember(
                metadata =
                    if (it == 0) mapOf(MemberMetadata.LEADING to "${films.id}=7") else emptyMap()
            )
        }

    /** Members that report a secondary of films: one that runs it, one that copies it in. */
    private val secondary = reporting(DatabaseState.Status.ONLINE)
    private val copying = reporting(DatabaseState.Status.COPYING)
    private val paused = reporting(DatabaseState.Status.ONLINE, paused = true)
    private val placedSecondaries = HashSet<MemberId>()

    private fun reporting(status: DatabaseState.Status, paused: Boolean = false) =
        consensus.newMember(
            metadata =
                mapOf(
                    MemberMetadata.DATABASES to
                        DatabaseStatesCodec.encode(
                            listOf(
                                DatabaseState(
                                    films.name,
                                    films.id,
                                    status,
                                    leaderKnown = false,
                                    mode = CopyMode.SECONDARY,
                                    paused = paused,
                                )
                            )
                        )
                )
        )

    init {
        val all = listOf(free, secondary, copying, paused) + hosts
        all.forEach { it.start() }
        all.forEach { it.startGroup(system, RecordingStateMachine(), all.map { m -> m.self }) }
        hosts.forEach { it.startGroup(films, RecordingStateMachine(), hosts.map { m -> m.self }) }
        consensus.electLeader(system, free.self.id)
        consensus.electLeader(films, hosts[0].self.id)
    }

    private fun locations(member: InMemoryConsensusLog): DatabaseLocations {
        val directory =
            MemberDirectory(
                member,
                groups,
                { null },
                { null },
                { emptyList() },
                NullLog.getInstance(),
                Duration.ofHours(1),
            )
        directory.refresh()
        return DatabaseLocations(member, directory) { placedSecondaries }
    }

    @Test
    fun `a member that does not host a database knows it from what its voters report`() {
        val location = locations(free).of(films)

        assertThat(location).isNotNull
        assertThat(location!!.voters).containsExactlyElementsOf(hosts.map { it.self })
        assertThat(location.leader).isEqualTo(hosts[0].self.id)
        assertThat(location.local).isFalse()
        assertThat(location.settled).isTrue()
    }

    @Test
    fun `a member that votes in the group knows it first hand`() {
        val location = locations(hosts[1]).of(films)!!

        assertThat(location.voters).containsExactlyElementsOf(hosts.map { it.self })
        assertThat(location.leader).isEqualTo(hosts[0].self.id)
        assertThat(location.local).isTrue()
    }

    @Test
    fun `a database's secondaries run it where the system graph places them`() {
        placedSecondaries +=
            listOf(secondary.self.id, copying.self.id, paused.self.id, hosts[1].self.id)

        assertThat(locations(free).secondaries(films)).containsExactly(secondary.self.id)

        placedSecondaries.clear()
        assertThat(locations(free).secondaries(films))
            .describedAs("placed elsewhere now, it drops its copy")
            .isEmpty()
    }

    @Test
    fun `a database whose group no member reports running is nowhere`() {
        assertThat(locations(free).of(DatabaseRef("music", UUID.randomUUID()))).isNull()
    }
}
