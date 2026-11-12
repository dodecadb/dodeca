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
import org.dozendb.core.routing.LocalRoutingTables
import org.dozendb.core.routing.RoutingTables
import org.dozendb.core.servers.ServerRules
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.dbms.routing.RoutingResult
import org.neo4j.kernel.database.DatabaseReferenceImpl
import org.neo4j.values.virtual.MapValue

/**
 * Routing tables for drivers that route (`neo4j://`), from the Raft group of each database where it
 * is known ([DatabaseLocations]): writes to the leader, reads to the followers unless only
 * secondaries are to read ([readsOnPrimaries]), to the secondaries and optionally the leader,
 * leaving out members whose copy failed, this one too. Every member routes every database, also one
 * it does not host: its table names the members that do, and while it knows no leader for it, it
 * asks the members again first, for at most [refreshWait]. A database whose group no member is
 * known to run gets the [local] table, which names only this member. Whether a database can be
 * routed at all, the [validator] checks. Each table lasts [ttl].
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
    private val ttl: () -> Duration,
    private val validator: ClusterRoutingValidator,
    private val local: LocalRoutingTables,
    private val readsOnPrimaries: Boolean = true,
    private val groups: DatabaseGroups = DatabaseGroups(),
    private val policies: RoutingPolicies = RoutingPolicies(emptyMap()),
    private val tags: () -> Map<MemberId, Set<String>> = { emptyMap() },
    private val shuffle: Boolean = false,
) : RoutingTables {
    override fun check(reference: DatabaseReferenceImpl.Internal) = validator.check(reference)

    override fun table(
        reference: DatabaseReferenceImpl.Internal,
        routingContext: MapValue,
    ): RoutingResult {
        val policy = policies.of(routingContext)
        val id = reference.databaseId()
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
                ?: return local.table(reference, routingContext)
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
            ttl().toMillis(),
        )
    }

    /** The tags of each server, which only a policy with rules needs. */
    private fun tagsOf(policy: ServerRules): (MemberId) -> Set<String> {
        if (policy == ServerRules.ALL) return { emptySet() }
        val tags = tags()
        return { tags[it].orEmpty() }
    }

    private fun socketAddress(address: HostPort) = SocketAddress(address.host, address.port)
}
