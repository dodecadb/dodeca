package org.dozendb.cluster.internal.routing

import org.dozendb.cluster.ClusterRouting
import org.dozendb.cluster.MemberDirectory
import org.dozendb.cluster.internal.replication.ReplicationService
import org.dozendb.consensus.ConsensusLog
import org.dozendb.consensus.ConsensusUnavailableException
import org.dozendb.consensus.HostPort
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.dbms.routing.ClientSideRoutingTableProvider
import org.neo4j.dbms.routing.RoutingResult
import org.neo4j.dbms.routing.RoutingTableTTLProvider
import org.neo4j.dbms.routing.ServerSideRoutingTableProvider
import org.neo4j.kernel.database.DatabaseReferenceImpl
import org.neo4j.values.virtual.MapValue

/**
 * Routing tables for drivers that route (`neo4j://`). A database whose Raft group runs here is
 * routed by it: writes to the leader, reads to the followers and optionally the leader, leaving out
 * members whose copy failed, this one too. Other databases, such as stopped ones, and server-side
 * routing keep Community's table that names only this member.
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
        // Also when this member stopped replicating the database after its copy panicked.
        val endpoints =
            replication.replica(databaseReference.databaseId())?.let { replica ->
                try {
                    val members = consensus.members(replica.ref)
                    ClusterRouting.endpoints(
                        consensus.status(replica.ref),
                        members,
                        directory::boltAddress,
                        readsOnLeader,
                        directory.failedCopies(replica.ref.id),
                        members
                            .map { it.id }
                            .filter { replica.ref.id in directory.readOnlyDatabases(it) }
                            .toSet(),
                    )
                } catch (_: ConsensusUnavailableException) {
                    // The group does not run here.
                    null
                }
            }
                ?: return local.getRoutingResultForClientSideRouting(
                    databaseReference,
                    routingContext,
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
