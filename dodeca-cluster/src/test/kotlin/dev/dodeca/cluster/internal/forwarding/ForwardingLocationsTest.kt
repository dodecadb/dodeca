package dev.dodeca.cluster.internal.forwarding

import dev.dodeca.cluster.DatabaseGroups
import dev.dodeca.cluster.DatabaseStatesCodec
import dev.dodeca.cluster.MemberDirectory
import dev.dodeca.cluster.MemberMetadata
import dev.dodeca.cluster.internal.topology.DatabaseLocations
import dev.dodeca.consensus.DatabaseRef
import dev.dodeca.consensus.HostPort
import dev.dodeca.consensus.MemberId
import dev.dodeca.consensus.fake.InMemoryConsensus
import dev.dodeca.consensus.fake.RecordingStateMachine
import dev.dodeca.core.edition.DatabaseState
import dev.dodeca.core.remote.RemoteFailure
import dev.dodeca.core.servers.CopyMode
import java.time.Duration
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test
import org.neo4j.boltmessages.AccessMode
import org.neo4j.boltmessages.request.connection.RoutingContext
import org.neo4j.fabric.executor.FabricException
import org.neo4j.fabric.executor.Location
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.database.DatabaseIdFactory
import org.neo4j.kernel.database.DatabaseReferenceImpl
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.logging.NullLog
import org.neo4j.router.transaction.RoutingInfo

/**
 * Where transactions run: write transactions on the leader of their database when they may be
 * routed, and transactions of a database not hosted here on the members that host it.
 */
class ForwardingLocationsTest {
    private val consensus = InMemoryConsensus()
    private val system = DatabaseRef("system", UUID(0, 1))
    private val groups = DatabaseGroups()
    private val films = DatabaseRef("films", UUID(0, 7))
    private val filmsId: NamedDatabaseId = DatabaseIdFactory.from(films.name, films.id)
    private val books: NamedDatabaseId = DatabaseIdFactory.from("books", UUID(0, 8))
    // The second member leads films, and says so when probed.
    private val logs =
        List(3) {
            consensus.newMember(
                metadata =
                    if (it == 1) mapOf(MemberMetadata.LEADING to "${films.id}=1") else emptyMap()
            )
        }
    private val self = logs[0]
    /** A member that hosts only system. */
    private val outsider = consensus.newMember()
    /** A member that runs a secondary of films, where [placedSecondaries] has one. */
    private val secondary =
        consensus.newMember(
            metadata =
                mapOf(
                    MemberMetadata.DATABASES to
                        DatabaseStatesCodec.encode(
                            listOf(
                                DatabaseState(
                                    films.name,
                                    films.id,
                                    DatabaseState.Status.ONLINE,
                                    leaderKnown = false,
                                    mode = CopyMode.SECONDARY,
                                )
                            )
                        )
                )
        )
    private val placedSecondaries = HashSet<MemberId>()
    private val addresses = HashMap<MemberId, HostPort>()
    private val reported = HashSet<MemberId>()

    private fun forwarding(
        member: InMemoryConsensus.InMemoryConsensusLog = self,
        readsOnPrimaries: Boolean = true,
        hosted: (NamedDatabaseId) -> Boolean = { it == filmsId },
    ): ForwardingLocations {
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
        return ForwardingLocations(
            hosted,
            DatabaseLocations(member, directory) { placedSecondaries },
            member,
            { addresses[it] },
            { it in reported },
            { _, _ -> true },
            Duration.ofMillis(100),
            NullLog.getInstance(),
            readsOnPrimaries,
        )
    }

    private val locations by lazy { forwarding() }

    init {
        val all = logs + outsider + secondary
        all.forEach { it.start() }
        all.forEach { it.startGroup(system, RecordingStateMachine(), all.map { log -> log.self }) }
        logs.forEach { it.startGroup(films, RecordingStateMachine(), logs.map { log -> log.self }) }
        for ((i, log) in logs.withIndex()) {
            addresses[log.self.id] = HostPort("core$i", 7688)
            reported += log.self.id
        }
        addresses[secondary.self.id] = HostPort("replica", 7688)
        reported += secondary.self.id
        consensus.electLeader(system, logs[0].self.id)
        consensus.electLeader(films, logs[1].self.id)
    }

    private fun reference(id: NamedDatabaseId) =
        DatabaseReferenceImpl.Internal(NormalizedDatabaseName(id.name()), id, true)

    private fun locationOf(
        id: NamedDatabaseId = filmsId,
        mode: AccessMode = AccessMode.WRITE,
        routing: Boolean = true,
        at: ForwardingLocations = locations,
    ): Location =
        at.locationService(RoutingInfo(reference(id), RoutingContext(routing, emptyMap()), mode))
            .locationOf(reference(id))

    @Test
    fun `a database not hosted here runs on its hosts when it may be routed`() {
        val outside = forwarding(outsider) { false }

        val write = locationOf(at = outside) as Location.Remote.Internal.Direct
        val read =
            locationOf(mode = AccessMode.READ, at = outside) as Location.Remote.Internal.Direct

        assertThat(write.uri.addresses.single().hostname).isEqualTo("core1")
        val follower = logs.filter { it != logs[1] }.minBy { it.self.id }
        assertThat(read.uri.addresses.single().hostname)
            .isEqualTo(addresses.getValue(follower.self.id).host)
    }

    @Test
    fun `reads of a database not hosted here run on a follower or a secondary`() {
        placedSecondaries += secondary.self.id
        val follower = logs.filter { it != logs[1] }.minBy { it.self.id }.self.id
        fun reader(readsOnPrimaries: Boolean): String {
            val outside = forwarding(outsider, readsOnPrimaries) { false }
            val read = locationOf(mode = AccessMode.READ, at = outside)
            return (read as Location.Remote.Internal.Direct).uri.addresses.single().hostname
        }

        assertThat(reader(readsOnPrimaries = true))
            .isEqualTo(addresses.getValue(minOf(follower, secondary.self.id)).host)
        assertThat(reader(readsOnPrimaries = false))
            .describedAs("only secondaries read while one can")
            .isEqualTo("replica")
        placedSecondaries.clear()
        assertThat(reader(readsOnPrimaries = false))
            .describedAs("no secondary to read on")
            .isEqualTo(addresses.getValue(follower).host)
    }

    @Test
    fun `a database not hosted here fails when it may not be routed, saying how to reach it`() {
        val outside = forwarding(outsider) { false }

        for (mode in AccessMode.entries) {
            assertThatThrownBy { locationOf(mode = mode, routing = false, at = outside) }
                .isInstanceOf(RemoteFailure::class.java)
                .hasMessageContaining("Database 'films' is not hosted on this server")
                .hasMessageContaining("neo4j://")
        }
    }

    @Test
    fun `writes that may be routed run on the leader`() {
        val location = locationOf() as Location.Remote.Internal.Direct

        assertThat(location.uri.addresses.single().toString()).isEqualTo("core1:7688")
        assertThat(location.databaseReference.databaseId()).isEqualTo(filmsId)
    }

    @Test
    fun `reads, writes that may not be routed and writes of the leader run here`() {
        assertThat(locationOf(mode = AccessMode.READ)).isInstanceOf(Location.Local::class.java)
        assertThat(locationOf(routing = false)).isInstanceOf(Location.Local::class.java)
        consensus.electLeader(films, self.self.id)
        assertThat(locationOf()).isInstanceOf(Location.Local::class.java)
    }

    @Test
    fun `a transaction keeps the place it chose for a database`() {
        val service =
            locations.locationService(
                RoutingInfo(reference(filmsId), RoutingContext(true, emptyMap()), AccessMode.WRITE)
            )
        val first = service.locationOf(reference(filmsId))
        consensus.electLeader(films, logs[2].self.id)

        assertThat(service.locationOf(reference(filmsId))).isSameAs(first)
        assertThat((locationOf() as Location.Remote.Internal.Direct).uri.addresses.single().port)
            .isEqualTo(7688)
    }

    @Test
    fun `without a leader a write fails once the wait is over`() {
        consensus.crash(logs[1].self.id)

        assertThatThrownBy { locationOf() }
            .isInstanceOf(FabricException::class.java)
            .extracting { (it as FabricException).status() }
            .isEqualTo(Status.Cluster.NotALeader)
    }

    @Test
    fun `a transaction that found no leader does not wait again`() {
        consensus.crash(logs[1].self.id)
        val service =
            locations.locationService(
                RoutingInfo(reference(filmsId), RoutingContext(true, emptyMap()), AccessMode.WRITE)
            )
        val first = catchThrowable { service.locationOf(reference(filmsId)) }
        consensus.electLeader(films, logs[2].self.id)

        assertThat(catchThrowable { service.locationOf(reference(filmsId)) }).isSameAs(first)
    }

    @Test
    fun `a leader that takes no forwarded writes refuses them`() {
        addresses.remove(logs[1].self.id)

        assertThatThrownBy { locationOf() }
            .isInstanceOf(RemoteFailure::class.java)
            .hasMessageContaining("takes no forwarded writes")
        // Until a member first reports, its address may come.
        reported.remove(logs[1].self.id)
        assertThatThrownBy { locationOf() }.isInstanceOf(RemoteFailure::class.java)
    }

    private class Watched(
        override val member: HostPort,
        override val databaseId: NamedDatabaseId,
        override val write: Boolean = true,
    ) : ForwardedTransaction {
        override val database: String = databaseId.name()
        val gone = mutableListOf<String>()

        override fun leaderGone(why: String) {
            gone += why
        }
    }

    @Test
    fun `transactions of a member that stopped leading are given up`() {
        val onLeader = Watched(HostPort("core1", 7688), filmsId)
        val elsewhere = Watched(HostPort("core2", 7688), filmsId)
        val read = Watched(HostPort("core2", 7688), filmsId, write = false)

        locations.watch(listOf(onLeader, elsewhere, read), Duration.ofMinutes(1))

        assertThat(onLeader.gone).isEmpty()
        assertThat(elsewhere.gone.single()).contains("leads database films now")
        assertThat(read.gone).describedAs("reads run on any host").isEmpty()
    }

    @Test
    fun `transactions of a database no member is known to run are given up once that lasts`() {
        val dropped = Watched(HostPort("core1", 7688), books)

        locations.watch(listOf(dropped), Duration.ofMinutes(1))
        assertThat(dropped.gone).isEmpty()
        locations.watch(listOf(dropped), Duration.ZERO)
        assertThat(dropped.gone.single()).contains("no member has led database books")
    }

    @Test
    fun `transactions of a database without a leader for long are given up`() {
        val transaction = Watched(HostPort("core1", 7688), filmsId)
        consensus.crash(logs[1].self.id)

        locations.watch(listOf(transaction), Duration.ofMinutes(1))
        assertThat(transaction.gone).isEmpty()
        locations.watch(listOf(transaction), Duration.ZERO)
        assertThat(transaction.gone.single()).contains("no member has led database films")
    }
}
