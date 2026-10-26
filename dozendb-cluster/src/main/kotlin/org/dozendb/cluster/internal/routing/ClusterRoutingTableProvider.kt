package org.dozendb.cluster.internal.routing

import org.dozendb.cluster.ClusterRouting
import org.dozendb.cluster.MemberDirectory
import org.dozendb.cluster.internal.topology.DatabaseLocations
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.HostPort
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.dbms.routing.ClientSideRoutingTableProvider
import org.neo4j.dbms.routing.RoutingResult
import org.neo4j.dbms.routing.RoutingTableTTLProvider
import org.neo4j.dbms.routing.ServerSideRoutingTableProvider
import org.neo4j.kernel.database.DatabaseReferenceImpl
import org.neo4j.values.virtual.MapValue

/**
 * Routing tables for drivers that route (`neo4j://`), from the Raft group of each database where it
 * is known ([DatabaseLocations]): writes to the leader, reads to the followers and optionally the
 * leader, leaving out members whose copy failed, this one too. Every member routes every database,
 * also one it does not host: its table names the members that do. A database whose group no member
 * is known to run, and server-side routing, keep Community's table that names only this member.
 */
internal class ClusterRoutingTableProvider(
    private val locations: DatabaseLocations,
    private val directory: MemberDirectory,
    private val readsOnLeader: Boolean,
    private val ttl: RoutingTableTTLProvider,
    private val local: ClientSideRoutingTableProvider,
    private val serverSide: ServerSideRoutingTableProvider,
) : ClientSideRoutingTableProvider, ServerSideRoutingTableProvider {
    override fun getRoutingResultForClientSideRouting(
        databaseReference: DatabaseReferenceImpl.Internal,
        routingContext: MapValue,
    ): RoutingResult {
        val id = databaseReference.databaseId()
        val database = DatabaseRef(id.name(), id.databaseId().uuid())
        // Also when this member stopped replicating the database after its copy panicked.
        val location =
            locations.of(database)
                ?: return local.getRoutingResultForClientSideRouting(
                    databaseReference,
                    routingContext,
                )
        val endpoints =
            ClusterRouting.endpoints(
                location.leader,
                location.voters,
                directory::boltAddress,
                readsOnLeader,
                directory.failedCopies(database.id),
                location.voters
                    .map { it.id }
                    .filter { database.id in directory.readOnlyDatabases(it) }
                    .toSet(),
            )
        return RoutingResult(
            endpoints.routers.map(::socketAddress),
            endpoints.writers.map(::socketAddress),
            endpoints.readers.map(::socketAddress),
            ttl.nextTTL().toMillis(),
        )
    }

    override fun getServerSideRoutingTable(routingContext: MapValue): RoutingResult =
        serverSide.getServerSideRoutingTable(routingContext)

    private fun socketAddress(address: HostPort) = SocketAddress(address.host, address.port)
}
