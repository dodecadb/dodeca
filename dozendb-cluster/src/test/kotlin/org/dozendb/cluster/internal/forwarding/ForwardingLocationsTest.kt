package org.dozendb.cluster.internal.forwarding

import java.time.Duration
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assertions.catchThrowable
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.MemberId
import org.dozendb.consensus.fake.InMemoryConsensus
import org.dozendb.consensus.fake.RecordingStateMachine
import org.junit.jupiter.api.Test
import org.neo4j.bolt.protocol.common.message.AccessMode
import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext
import org.neo4j.fabric.executor.FabricException
import org.neo4j.fabric.executor.Location
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.database.DatabaseIdFactory
import org.neo4j.kernel.database.DatabaseReferenceImpl
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.logging.NullLog
import org.neo4j.router.transaction.RoutingInfo

/** Where write transactions run: on the leader of their database, when they may be routed. */
class ForwardingLocationsTest {
    private val consensus = InMemoryConsensus()
    private val logs = List(3) { consensus.newMember() }
    private val self = logs[0]
    private val films = DatabaseRef("films", UUID(0, 7))
    private val filmsId: NamedDatabaseId = DatabaseIdFactory.from(films.name, films.id)
    private val books: NamedDatabaseId = DatabaseIdFactory.from("books", UUID(0, 8))
    private val addresses = HashMap<MemberId, HostPort>()
    private val reported = HashSet<MemberId>()
    private val locations =
        ForwardingLocations(
            { id -> films.takeIf { id == filmsId } },
            self,
            { addresses[it] },
            { it in reported },
            Duration.ofMillis(100),
            NullLog.getInstance(),
        )

    init {
        logs.forEach { it.start() }
        logs.forEach { it.startGroup(films, RecordingStateMachine(), logs.map { log -> log.self }) }
        for ((i, log) in logs.withIndex()) {
            addresses[log.self.id] = HostPort("core$i", 7688)
            reported += log.self.id
        }
        consensus.electLeader(films, logs[1].self.id)
    }

    private fun reference(id: NamedDatabaseId) =
        DatabaseReferenceImpl.Internal(NormalizedDatabaseName(id.name()), id, true)

    private fun locationOf(
        id: NamedDatabaseId = filmsId,
        mode: AccessMode = AccessMode.WRITE,
        routing: Boolean = true,
    ): Location =
        locations
            .locationService(RoutingInfo(reference(id), RoutingContext(routing, emptyMap()), mode))
            .locationOf(reference(id))

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
        assertThat(locationOf(books)).isInstanceOf(Location.Local::class.java)
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
        val dropped = Watched(HostPort("core1", 7688), books)

        locations.watch(listOf(onLeader, elsewhere, dropped), Duration.ofMinutes(1))

        assertThat(onLeader.gone).isEmpty()
        assertThat(elsewhere.gone.single()).contains("leads database films now")
        assertThat(dropped.gone.single()).contains("not replicated here")
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
