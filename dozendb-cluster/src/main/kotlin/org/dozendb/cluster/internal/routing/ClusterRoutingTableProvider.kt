package org.dozendb.cluster.internal.routing

import java.time.Duration
import kotlin.random.Random
import org.dozendb.cluster.ClusterRouting
import org.dozendb.cluster.DatabaseGroups
import org.dozendb.cluster.MemberDirectory
import org.dozendb.cluster.internal.of
import org.dozendb.cluster.internal.topology.DatabaseLocations
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.MemberId
import org.dozendb.core.servers.ServerRules
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.dbms.routing.ClientSideRoutingTableProvider
import org.neo4j.dbms.routing.RoutingResult
import org.neo4j.dbms.routing.RoutingTableTTLProvider
import org.neo4j.dbms.routing.ServerSideRoutingTableProvider
import org.neo4j.kernel.database.DatabaseReferenceImpl
import org.neo4j.values.virtual.MapValue

/**
 * Routing tables for drivers that route (`neo4j://`), from the Raft group of each database where it
 * is known ([DatabaseLocations]): writes to the leader, reads to the followers unless only
 * secondaries are to read ([readsOnPrimaries]), to the secondaries and optionally the leader,
 * leaving out members whose copy failed, this one too. Every member routes every database, also one
 * it does not host: its table names the members that do, and while it knows no leader for it, it
 * asks the members again first, for at most [refreshWait]. A database whose group no member is
 * known to run, and server-side routing, keep Community's table that names only this member.
 *
 * A driver names one of the routing [policies] in its routing context, or gets the default one; the
 * policy chooses readers and routers among the servers that answer, by the [tags] of each, which
 * are read only for a policy with rules. With [shuffle], readers and routers are listed in a new
 * random order each time.
 */
internal class ClusterRoutingTableProvider(
    private val locations: DatabaseLocations,
    private val directory: MemberDirectory,
    private val refreshWait: Duration,
    private val readsOnWriters: () -> Boolean,
    private val ttl: RoutingTableTTLProvider,
    private val local: ClientSideRoutingTableProvider,
    private val serverSide: ServerSideRoutingTableProvider,
    private val readsOnPrimaries: Boolean = true,
    private val groups: DatabaseGroups = DatabaseGroups(),
    private val policies: RoutingPolicies = RoutingPolicies(emptyMap()),
    private val tags: () -> Map<MemberId, Set<String>> = { emptyMap() },
    private val shuffle: Boolean = false,
) : ClientSideRoutingTableProvider, ServerSideRoutingTableProvider {
    override fun getRoutingResultForClientSideRouting(
        databaseReference: DatabaseReferenceImpl.Internal,
        routingContext: MapValue,
    ): RoutingResult {
        val policy = policies.of(routingContext)
        val id = databaseReference.databaseId()
        val database = groups.of(id)
        val location =
            locations.of(database)?.takeIf { it.local || it.leader != null }
                ?: run {
                    // What the members reported last may be from before its leader was elected,
                    // as right after CREATE DATABASE ... WAIT returned.
                    directory.awaitRefresh(refreshWait)
                    locations.of(database)
                }
                // Also when this member stopped replicating the database after its copy panicked.
                ?: return local.getRoutingResultForClientSideRouting(
                    databaseReference,
                    routingContext,
                )
        val endpoints =
            ClusterRouting.endpoints(
                location.leader,
                location.voters,
                directory::boltAddress,
                readsOnWriters(),
                directory.failedCopies(database.id),
                location.voters
                    .map { it.id }
                    .filter { database.id in directory.readOnlyDatabases(it) }
                    .toSet(),
                locations.secondaries(database),
                readsOnPrimaries,
                directory::answering,
                policy,
                tagsOf(policy),
                if (shuffle) Random.Default else null,
            )
        return RoutingResult(
            endpoints.routers.map(::socketAddress),
            endpoints.writers.map(::socketAddress),
            endpoints.readers.map(::socketAddress),
            ttl.nextTTL().toMillis(),
        )
    }

    /** The tags of each server, which only a policy with rules needs. */
    private fun tagsOf(policy: ServerRules): (MemberId) -> Set<String> {
        if (policy == ServerRules.ALL) return { emptySet() }
        val tags = tags()
        return { tags[it].orEmpty() }
    }

    override fun getServerSideRoutingTable(routingContext: MapValue): RoutingResult =
        serverSide.getServerSideRoutingTable(routingContext)

    private fun socketAddress(address: HostPort) = SocketAddress(address.host, address.port)
}
