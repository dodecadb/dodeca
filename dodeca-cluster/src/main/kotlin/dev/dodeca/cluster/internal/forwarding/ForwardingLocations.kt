package dev.dodeca.cluster.internal.forwarding

import dev.dodeca.cluster.DatabaseGroups
import dev.dodeca.cluster.internal.of
import dev.dodeca.cluster.internal.topology.DatabaseLocations
import dev.dodeca.consensus.ConsensusLog
import dev.dodeca.consensus.DatabaseRef
import dev.dodeca.consensus.HostPort
import dev.dodeca.consensus.MemberId
import dev.dodeca.core.remote.RemoteFailure
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import org.neo4j.boltmessages.AccessMode
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

    /** Whether it may write, so that it runs on the leader; a read runs on any host. */
    val write: Boolean

    /**
     * Gives up on the member, which stopped leading the database or cannot be reached: a commit it
     * did not answer yet may or may not have been applied; anything else was not, and the client
     * can retry it on the new leader.
     */
    fun leaderGone(why: String)
}

/**
 * Where Neo4j's query router runs each database's part of a transaction: on the leader of the
 * database for write transactions that may be routed on the server, here otherwise. A database not
 * hosted here runs on the members that host it, as in Neo4j Enterprise: writes on its leader, reads
 * on a member that runs it, if the client allows routing on the server; without that, the
 * transaction fails, saying how to reach it. A transaction keeps the place it chose for a database,
 * or the failure to find one, so it waits only once for a leader, up to [leaderWait].
 *
 * The leaders and hosts come from the database's Raft group and secondaries ([locations]), and
 * their routing connectors from what members report ([routingAddress], [reported], [running]).
 *
 * @property hosted whether a database is hosted here, its copy registered on this member.
 * @property readsOnPrimaries whether followers take reads where secondaries can, as routing tables
 *   have it.
 */
internal class ForwardingLocations(
    private val hosted: (NamedDatabaseId) -> Boolean,
    private val locations: DatabaseLocations,
    private val consensus: ConsensusLog,
    private val routingAddress: (MemberId) -> HostPort?,
    private val reported: (MemberId) -> Boolean,
    private val running: (MemberId, UUID) -> Boolean,
    private val leaderWait: Duration,
    private val log: InternalLog,
    private val readsOnPrimaries: Boolean = true,
    private val groups: DatabaseGroups = DatabaseGroups(),
) {
    /** The member each database's writes went to last, to log changes. */
    private val targets = ConcurrentHashMap<UUID, HostPort>()

    /** Since when each database has had no leader, while open transactions go to it. */
    private val leaderless = ConcurrentHashMap<UUID, Long>()

    /** The locations of a transaction with [routingInfo]. */
    fun locationService(routingInfo: RoutingInfo): LocationService {
        val routes = routingInfo.routingContext().isServerRoutingEnabled
        val writes = routingInfo.accessMode() == AccessMode.WRITE
        // The router asks for the session's database at BEGIN and again with the first query.
        val chosen = HashMap<UUID, Result<Location>>()
        return LocationService { reference ->
            val internal = internal(reference)
            chosen
                .getOrPut(internal.id()) {
                    try {
                        Result.success(locationOf(internal, routes, writes))
                    } catch (e: RuntimeException) {
                        Result.failure(e)
                    }
                }
                .getOrThrow()
        }
    }

    private fun locationOf(
        reference: DatabaseReferenceImpl.Internal,
        routes: Boolean,
        writes: Boolean,
    ): Location {
        val local = Location.Local(-1, reference)
        val id = reference.databaseId()
        val name = reference.alias().name()
        val here = hosted(id)
        if (here && !(routes && writes)) return local
        if (!routes) throw notHosted(name)
        val ref = groups.of(id)
        val member =
            if (writes) awaitLeader(ref) ?: throw FabricException.noLeaderAddress(name)
            else host(ref) ?: throw noHost(name)
        if (member == consensus.self.id) return local
        val address = awaitRoutingAddress(member) ?: throw takesNoForwardedWrites(name, member)
        if (writes && targets.put(ref.id, address) != address) {
            log.info(
                "Forwarding writes of database %s to its leader %s at %s",
                name,
                member,
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
            locations.of(ref)?.leader?.let {
                return it
            }
            if (System.nanoTime() > deadline) return null
            Thread.sleep(POLL_MILLIS)
        }
    }

    /**
     * A member that runs [ref] for reads that are not hosted here and takes forwarded transactions,
     * as routing tables list readers: the first by id of the followers, unless only secondaries are
     * to read and one can, and the secondaries; else the leader.
     */
    private fun host(ref: DatabaseRef): MemberId? {
        val location = locations.of(ref)
        val primaries =
            location
                ?.voters
                .orEmpty()
                .map { it.id }
                .filter {
                    running(it, ref.id) && routingAddress(it) != null
                }
        val secondaries = locations.secondaries(ref).filter { routingAddress(it) != null }
        val followers = primaries.filter { it != location?.leader }
        val readers =
            (if (readsOnPrimaries || secondaries.isEmpty()) followers else emptyList()) +
                secondaries
        return readers.minOrNull() ?: primaries.firstOrNull()
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
     * Gives up on those of [transactions] that write and whose member no longer leads their
     * database, or that no member has led for [leaderlessFor]: a partition may leave them waiting
     * for a member that cannot answer.
     */
    fun watch(transactions: Collection<ForwardedTransaction>, leaderlessFor: Duration) {
        val now = System.nanoTime()
        val stillLeaderless = HashSet<UUID>()
        for (transaction in transactions) {
            if (!transaction.write) continue
            val id = transaction.databaseId
            val ref = groups.of(id)
            val leader = locations.of(ref)?.leader
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

    private fun noHost(database: String) =
        RemoteFailure(
            Status.General.DatabaseUnavailable,
            "No member that hosts database '$database' is known to run it and take forwarded " +
                "transactions.",
            ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N09)
                .withParam(GqlParams.StringParam.db, database)
                .build(),
        )

    companion object {
        const val POLL_MILLIS = 10L

        /**
         * Why a transaction of [database], which is not hosted on this member, cannot run here: a
         * client that does not allow routing on the server, as over `bolt://`, must connect to a
         * member that hosts it.
         */
        fun notHosted(database: String) =
            RemoteFailure(
                Status.Cluster.Routing,
                "Database '$database' is not hosted on this server. Connect with neo4j:// to be " +
                    "routed to a server that hosts it, as SHOW DATABASES lists them, or to such " +
                    "a server directly. Server-side routing, with mutual TLS between members, " +
                    "also runs its transactions there.",
                ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N02)
                    .withParam(GqlParams.StringParam.db, database)
                    .withParam(GqlParams.StringParam.cfgSetting, "dbms.routing.enabled")
                    .build(),
            )

        private fun internal(reference: DatabaseReference): DatabaseReferenceImpl.Internal =
            reference as? DatabaseReferenceImpl.Internal
                ?: throw IllegalArgumentException("Unexpected database reference: $reference")
    }
}
