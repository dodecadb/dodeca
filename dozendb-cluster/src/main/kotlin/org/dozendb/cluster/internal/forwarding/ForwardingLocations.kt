package org.dozendb.cluster.internal.forwarding

import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import org.dozendb.consensus.ConsensusLog
import org.dozendb.consensus.ConsensusUnavailableException
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.MemberId
import org.neo4j.bolt.protocol.common.message.AccessMode
import org.neo4j.configuration.helpers.RemoteUri
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.fabric.executor.FabricException
import org.neo4j.fabric.executor.Location
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlParams
import org.neo4j.gqlstatus.GqlStatusInfoCodes
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.database.DatabaseReference
import org.neo4j.kernel.database.DatabaseReferenceImpl
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.logging.InternalLog
import org.neo4j.router.location.LocationService
import org.neo4j.router.transaction.RoutingInfo

/** A transaction that runs on another member, as [ForwardingLocations] watches it. */
internal interface ForwardedTransaction {
    /** The database, by its name on every member. */
    val database: String

    val databaseId: NamedDatabaseId

    /** The routing address of the member the transaction runs on. */
    val member: HostPort

    /**
     * Gives up on the member, which stopped leading the database or cannot be reached: a commit it
     * did not answer yet may or may not have been applied; anything else was not, and the client
     * can retry it on the new leader.
     */
    fun leaderGone(why: String)
}

/**
 * Where Neo4j's query router runs each database's part of a transaction: on the leader of the
 * database for write transactions that may be routed on the server, here otherwise. A transaction
 * keeps the place it chose for a database, or the failure to find one, so it waits only once for a
 * leader, up to [leaderWait].
 *
 * The leaders come from the Raft groups of the databases replicated here ([replica]), and their
 * routing connectors from what members report ([routingAddress], [reported]).
 */
internal class ForwardingLocations(
    private val replica: (NamedDatabaseId) -> DatabaseRef?,
    private val consensus: ConsensusLog,
    private val routingAddress: (MemberId) -> HostPort?,
    private val reported: (MemberId) -> Boolean,
    private val leaderWait: Duration,
    private val log: InternalLog,
) {
    /** The member each database's writes went to last, to log changes. */
    private val targets = ConcurrentHashMap<UUID, HostPort>()

    /** Since when each database has had no leader, while open transactions go to it. */
    private val leaderless = ConcurrentHashMap<UUID, Long>()

    /** The locations of a transaction with [routingInfo]. */
    fun locationService(routingInfo: RoutingInfo): LocationService {
        val forwards =
            routingInfo.accessMode() == AccessMode.WRITE &&
                routingInfo.routingContext().isServerRoutingEnabled
        // The router asks for the session's database at BEGIN and again with the first query.
        val chosen = HashMap<UUID, Result<Location>>()
        return LocationService { reference ->
            val internal = internal(reference)
            chosen
                .getOrPut(internal.id()) {
                    try {
                        Result.success(
                            if (forwards) locationOf(internal) else Location.Local(-1, internal)
                        )
                    } catch (e: RuntimeException) {
                        Result.failure(e)
                    }
                }
                .getOrThrow()
        }
    }

    private fun locationOf(reference: DatabaseReferenceImpl.Internal): Location {
        val local = Location.Local(-1, reference)
        val ref = replica(reference.databaseId()) ?: return local
        val name = reference.alias().name()
        val leader =
            try {
                awaitLeader(ref)
            } catch (_: ConsensusUnavailableException) {
                // The group does not run here: the transaction fails here as it would anyway.
                return local
            } ?: throw FabricException.noLeaderAddress(name)
        if (leader == consensus.self.id) return local
        val address = awaitRoutingAddress(leader) ?: throw takesNoForwardedWrites(name, leader)
        if (targets.put(ref.id, address) != address) {
            log.info(
                "Forwarding writes of database %s to its leader %s at %s",
                name,
                leader,
                address,
            )
        }
        return Location.Remote.Internal.Direct(
            -1,
            reference,
            RemoteUri("bolt", listOf(SocketAddress(address.host, address.port)), null),
        )
    }

    /** The leader of [ref], waiting up to [leaderWait] while it has none. */
    private fun awaitLeader(ref: DatabaseRef): MemberId? {
        val deadline = System.nanoTime() + leaderWait.toNanos()
        while (true) {
            consensus.status(ref).leader?.let {
                return it
            }
            if (System.nanoTime() > deadline) return null
            Thread.sleep(POLL_MILLIS)
        }
    }

    /** The routing address of [leader], waiting up to [leaderWait] for its first report. */
    private fun awaitRoutingAddress(leader: MemberId): HostPort? {
        val deadline = System.nanoTime() + leaderWait.toNanos()
        while (true) {
            routingAddress(leader)?.let {
                return it
            }
            if (reported(leader) || System.nanoTime() > deadline) return null
            Thread.sleep(POLL_MILLIS)
        }
    }

    /**
     * Gives up on those of [transactions] whose member no longer leads their database here, or that
     * no member has led for [leaderlessFor]: a partition may leave them waiting for a member that
     * cannot answer.
     */
    fun watch(transactions: Collection<ForwardedTransaction>, leaderlessFor: Duration) {
        val now = System.nanoTime()
        val stillLeaderless = HashSet<UUID>()
        for (transaction in transactions) {
            val ref = replica(transaction.databaseId)
            if (ref == null) {
                transaction.leaderGone("database ${transaction.database} is not replicated here")
                continue
            }
            val leader =
                try {
                    consensus.status(ref).leader
                } catch (_: ConsensusUnavailableException) {
                    null
                }
            if (leader == null) {
                stillLeaderless += ref.id
                val since = leaderless.computeIfAbsent(ref.id) { now }
                if (now - since > leaderlessFor.toNanos()) {
                    transaction.leaderGone(
                        "no member has led database ${transaction.database} for $leaderlessFor"
                    )
                }
                continue
            }
            val address = routingAddress(leader) ?: continue
            if (address != transaction.member) {
                transaction.leaderGone(
                    "member $leader at $address leads database ${transaction.database} now"
                )
            }
        }
        leaderless.keys.retainAll(stillLeaderless)
    }

    private fun takesNoForwardedWrites(database: String, leader: MemberId) =
        RemoteFailure(
            Status.Cluster.NotALeader,
            "The leader of database '$database', member $leader, takes no forwarded writes: it " +
                "reports no routing address, as without mutual TLS between members or with " +
                "server-side routing disabled there. Connect to the leader instead.",
            ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N00)
                .withParam(GqlParams.StringParam.db, database)
                .build(),
        )

    private companion object {
        const val POLL_MILLIS = 10L

        fun internal(reference: DatabaseReference): DatabaseReferenceImpl.Internal =
            reference as? DatabaseReferenceImpl.Internal
                ?: throw IllegalArgumentException("Unexpected database reference: $reference")
    }
}
