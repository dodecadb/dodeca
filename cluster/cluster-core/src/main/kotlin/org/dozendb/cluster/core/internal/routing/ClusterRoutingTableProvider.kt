package org.dozendb.cluster.core.internal.routing

import org.dozendb.cluster.api.ConsensusLog
import org.dozendb.cluster.api.HostPort
import org.dozendb.cluster.core.ClusterRouting
import org.dozendb.cluster.core.MemberDirectory
import org.dozendb.cluster.core.internal.replication.ReplicationService
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.dbms.routing.ClientSideRoutingTableProvider
import org.neo4j.dbms.routing.RoutingResult
import org.neo4j.dbms.routing.RoutingTableTTLProvider
import org.neo4j.dbms.routing.ServerSideRoutingTableProvider
import org.neo4j.kernel.database.DatabaseReferenceImpl
import org.neo4j.values.virtual.MapValue

/**
 * Routing tables for drivers that route (`neo4j://`). A replicated database is routed by its Raft
 * group: writes to the leader, reads to the followers and optionally the leader. Local databases,
 * and server-side routing, keep DozerDB's table that names only this member.
 */
internal class ClusterRoutingTableProvider(
    private val replication: ReplicationService,
    private val consensus: ConsensusLog,
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
        val replica =
            replication.running().firstOrNull {
                it.namedDatabaseId == databaseReference.databaseId()
            }
                ?: return local.getRoutingResultForClientSideRouting(
                    databaseReference,
                    routingContext,
                )
        val endpoints =
            ClusterRouting.endpoints(
                consensus.status(replica.ref),
                consensus.members(replica.ref),
                directory::boltAddress,
                readsOnLeader,
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
