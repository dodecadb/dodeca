package dev.dodeca.cluster.internal.topology

import dev.dodeca.cluster.DatabaseGroups
import dev.dodeca.cluster.MemberDirectory
import dev.dodeca.cluster.MemberMetadata
import dev.dodeca.cluster.internal.InitialServerOptions
import dev.dodeca.cluster.internal.Neo4jSystemGraph
import dev.dodeca.cluster.internal.SystemSecondaryRecords
import dev.dodeca.cluster.internal.of
import dev.dodeca.consensus.ConsensusLog
import dev.dodeca.consensus.ConsensusUnavailableException
import dev.dodeca.consensus.DatabaseRef
import dev.dodeca.consensus.HostPort
import dev.dodeca.consensus.Member
import dev.dodeca.consensus.MemberId
import dev.dodeca.core.SystemGraphReads
import dev.dodeca.core.edition.DatabaseState
import dev.dodeca.core.servers.CopyMode
import dev.dodeca.core.servers.DroppedServer
import dev.dodeca.core.servers.Placements
import dev.dodeca.core.servers.ServerOptions
import dev.dodeca.core.servers.ServerRecords
import java.time.Duration
import java.util.Optional
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.dbms.database.DatabaseDetails
import org.neo4j.dbms.database.ServerDetails
import org.neo4j.dbms.database.TopologyInfoService
import org.neo4j.dbms.identity.ServerId
import org.neo4j.dbms.systemgraph.InstanceModeConstraint
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.InstanceStatus
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Transaction
import org.neo4j.kernel.database.NamedDatabaseId

/**
 * What SHOW DATABASES and SHOW SERVERS show of a cluster, as Neo4j Enterprise shows them: for each
 * database a row for each server it is placed on, as a primary or a secondary, each member of its
 * Raft group and each member that runs a secondary of it, also where it does not run here (see
 * [DatabaseRows], [DatabaseLocations]), and a server for each member of the cluster. Other members'
 * rows are what they report when this member asks them, as it does for each command, waiting at
 * most [probeDeadline]: a member that has not answered by then shows as unknown, or as an
 * unavailable server.
 *
 * @property report what a member reports about itself, this member included; null while unknown.
 * @property system the system database, which each command reads in a transaction of its own, read
 *   again if a DROP deletes what it reads meanwhile (see [SystemGraphReads]).
 */
internal class ClusterTopologyInfoService(
    private val community: TopologyInfoService,
    private val consensus: ConsensusLog,
    private val groups: DatabaseGroups,
    private val directory: MemberDirectory,
    private val locations: DatabaseLocations,
    private val report: (MemberId) -> Map<String, String>?,
    private val system: () -> GraphDatabaseService,
    private val probeDeadline: Duration = Duration.ofSeconds(1),
    private val voters: () -> List<Member> = { consensus.members(groups.system) },
) : TopologyInfoService {
    /** The group of `system` (see [DatabaseGroups]). */
    private val systemDatabase: DatabaseRef
        get() = groups.system

    /**
     * A server for each server of the cluster, a member of `system`, or a system secondary or lost
     * server that the system graph records: Enabled with its record, Free without one, except that
     * every member counts as enabled, with the options it reports, until the servers the cluster
     * started with are recorded (see `ServerReconciler`). It hosts the databases it reports
     * running, and is to host `system` and the databases placed on it that are requested online. A
     * deallocating server is Deallocated once no database is placed on it and it votes in no
     * database's group. A dropped server is Dropped while the cluster still has it, and after that
     * while it answers where it was.
     */
    override fun servers(transaction: Transaction): Set<ServerDetails> {
        val answering = answering()
        val (servers, left) = SystemGraphReads.read(system()) { tx -> servers(tx, answering) }
        return servers.apply { addAll(stillAnswering(left)) }
    }

    /** The servers of the cluster, and the dropped servers that left it, as [transaction] reads. */
    private fun servers(
        transaction: Transaction,
        answering: Set<MemberId>,
    ): Pair<MutableSet<ServerDetails>, List<DroppedServer>> {
        val records = ServerRecords.records(transaction)
        val dropped = ServerRecords.dropped(transaction)
        val initialRecorded = ServerRecords.initialServersEnabled(transaction)
        val online = requestedOnline(transaction)
        val members = clusterServers(transaction)
        val servers =
            members.mapTo(LinkedHashSet()) { member ->
                val record = records[member.uuid]
                val enabled = record != null || !initialRecorded
                val reported = report(member)
                val available = member in answering
                val options =
                    record?.options
                        ?: reported?.takeIf { enabled }?.let(InitialServerOptions::reported)
                        ?: ServerOptions()
                ServerDetails(
                    ServerId(member.uuid),
                    record?.name ?: dropped[member.uuid]?.name ?: member.uuid.toString(),
                    Optional.ofNullable(directory.boltAddress(member)?.let(::socketAddress)),
                    Optional.ofNullable(reported?.get(MemberMetadata.HTTP_ADDRESS)?.let(::parsed)),
                    Optional.ofNullable(reported?.get(MemberMetadata.HTTPS_ADDRESS)?.let(::parsed)),
                    LinkedHashSet(options.tags),
                    when {
                        member.uuid in dropped -> ServerDetails.State.DROPPED
                        !enabled -> ServerDetails.State.FREE
                        record?.status == InstanceStatus.CORDONED -> ServerDetails.State.CORDONED
                        record?.status == InstanceStatus.DEALLOCATING ->
                            if (deallocated(transaction, member)) ServerDetails.State.DEALLOCATED
                            else ServerDetails.State.DEALLOCATING
                        else -> ServerDetails.State.ENABLED
                    },
                    if (available) ServerDetails.RunningState.AVAILABLE
                    else ServerDetails.RunningState.UNAVAILABLE,
                    if (available) running(member) else emptySet(),
                    if (enabled) requested(transaction, member, online) else emptySet(),
                    LinkedHashSet(options.allowedDatabases),
                    LinkedHashSet(options.deniedDatabases),
                    options.modeConstraint,
                    Optional.ofNullable(reported?.get(MemberMetadata.NEO4J_VERSION)),
                )
            }
        val left = dropped.values.filter { it.address != null && MemberId(it.id) !in members }
        return servers to left
    }

    /** Whether the deallocating [member] is placed on no database, and votes in no group. */
    private fun deallocated(transaction: Transaction, member: MemberId): Boolean =
        Placements.placedOn(transaction, member.uuid).isEmpty() &&
            locations.votedIn(transaction, member).isEmpty()

    /** A Dropped server for each of the servers that left the cluster and still answer. */
    private fun stillAnswering(left: List<DroppedServer>): List<ServerDetails> = runBlocking {
        left
            .map { server ->
                async {
                    val address =
                        server.address?.let { runCatching { HostPort.parse(it) }.getOrNull() }
                    val peer =
                        try {
                            address?.let {
                                withTimeoutOrNull(probeDeadline.toMillis()) { consensus.probe(it) }
                            }
                        } catch (_: ConsensusUnavailableException) {
                            null
                        }
                    peer
                        ?.takeIf { it.member.id.uuid == server.id }
                        ?.let { dropped(server, it.metadata) }
                }
            }
            .awaitAll()
            .filterNotNull()
    }

    /** The row of the dropped [server], which reports [metadata]. */
    private fun dropped(server: DroppedServer, metadata: Map<String, String>): ServerDetails =
        ServerDetails(
            ServerId(server.id),
            server.name,
            Optional.ofNullable(metadata[MemberMetadata.BOLT_ADDRESS]?.let(::parsed)),
            Optional.ofNullable(metadata[MemberMetadata.HTTP_ADDRESS]?.let(::parsed)),
            Optional.ofNullable(metadata[MemberMetadata.HTTPS_ADDRESS]?.let(::parsed)),
            emptySet(),
            ServerDetails.State.DROPPED,
            ServerDetails.RunningState.AVAILABLE,
            emptySet(),
            emptySet(),
            emptySet(),
            emptySet(),
            InstanceModeConstraint.NONE,
            Optional.ofNullable(metadata[MemberMetadata.NEO4J_VERSION]),
        )

    /** The members that answer this member now, after asking them all for at most a deadline. */
    private fun answering(): Set<MemberId> =
        try {
            directory.refresh(probeDeadline) + consensus.self.id
        } catch (_: ConsensusUnavailableException) {
            setOf(consensus.self.id)
        }

    /** The names of the databases [member] reports running. */
    private fun running(member: MemberId): Set<String> =
        directory
            .databaseStates(member)
            .orEmpty()
            .filter { it.status == DatabaseState.Status.ONLINE }
            .mapTo(HashSet()) { it.name }

    /**
     * The databases [member] is to host: `system`, and those of [online] placed on it, the
     * databases requested online.
     */
    private fun requested(
        transaction: Transaction,
        member: MemberId,
        online: Set<String>,
    ): Set<String> =
        (Placements.placedOn(transaction, member.uuid) + SYSTEM).filterTo(HashSet()) {
            it in online
        }

    /** The names of the databases the system graph requests online, `system` among them. */
    private fun requestedOnline(transaction: Transaction): Set<String> =
        transaction.findNodes(TopologyGraphDbmsModel.DATABASE_LABEL).use { nodes ->
            nodes
                .asSequence()
                .filter {
                    it.getProperty(TopologyGraphDbmsModel.DATABASE_STATUS_PROPERTY, null) ==
                        TopologyGraphDbmsModel.DatabaseStatus.ONLINE.statusName()
                }
                .mapNotNull {
                    it.getProperty(TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY, null) as? String
                }
                .toHashSet()
        }

    private fun parsed(address: String): SocketAddress? = runCatching {
        socketAddress(HostPort.parse(address))
    }
        .getOrNull()

    private fun socketAddress(address: HostPort) = SocketAddress(address.host, address.port)

    override fun databases(
        transaction: Transaction,
        databaseIds: Set<NamedDatabaseId>,
        requestedExtras: TopologyInfoService.RequestedExtras,
    ): Set<DatabaseDetails> {
        val answering = answering()
        return SystemGraphReads.read(system()) { tx ->
            databases(tx, databaseIds, requestedExtras, answering)
        }
    }

    private fun databases(
        transaction: Transaction,
        databaseIds: Set<NamedDatabaseId>,
        requestedExtras: TopologyInfoService.RequestedExtras,
        answering: Set<MemberId>,
    ): Set<DatabaseDetails> {
        val hosts = hosting(transaction)
        val servers = { clusterServers(transaction) }
        return community.databases(transaction, databaseIds, requestedExtras).flatMapTo(
            HashSet()
        ) { own ->
            val id = own.namedDatabaseId()
            val database = groups.of(id)
            val location = locations.of(database)
            val placed = placed(transaction, id.name())
            DatabaseRows.rows(
                own,
                members(location, placed.keys, hosts, servers, database, answering).map {
                    copy(it, it in answering, database, placed[it])
                },
                location?.leader,
                requestedExtras.txInfo(),
                placed.keys,
                Placements.database(transaction, id.name())
                    ?.let(Neo4jSystemGraph::recreationStatus),
            )
        }
    }

    /**
     * The servers that the database [name] is placed on, with the mode of each copy; none for
     * `system` or while unplaced.
     */
    private fun placed(transaction: Transaction, name: String): Map<MemberId, CopyMode> =
        Placements.database(transaction, name)
            ?.let(Placements::copies)
            ?.mapKeys { MemberId(it.key) }
            .orEmpty()

    /**
     * The voting members of a database's group at [location], the servers it is [placed] on, and
     * the [answering] members that report running a secondary of [database], as one it moves off
     * does until it drops it; while none of them is known, the [servers] that [hosts].
     */
    private fun members(
        location: DatabaseLocations.Location?,
        placed: Set<MemberId>,
        hosts: (MemberId) -> Boolean,
        servers: () -> List<MemberId>,
        database: DatabaseRef,
        answering: Set<MemberId>,
    ): List<MemberId> {
        val voters = location?.voters?.map { it.id }
        val secondaries = answering.filter { member ->
            directory.databaseStates(member).orEmpty().any {
                it.id == database.id && it.mode == CopyMode.SECONDARY
            }
        }
        if (voters == null && placed.isEmpty() && secondaries.isEmpty()) {
            return servers().filter(hosts)
        }
        return (voters.orEmpty() + placed + secondaries.sortedBy { it.uuid }).distinct()
    }

    /**
     * The servers of the cluster, the voters of `system`, and the system secondaries and the lost
     * servers, which `system` was re-formed without, that [transaction] reads; this member alone
     * among the voters while they are not known, as while consensus does not run.
     */
    private fun clusterServers(transaction: Transaction): List<MemberId> {
        val voters =
            try {
                voters().map { it.id }
            } catch (_: ConsensusUnavailableException) {
                listOf(consensus.self.id)
            }
        return (voters +
                SystemSecondaryRecords.all(transaction).map { it.id } +
                ServerRecords.lost(transaction).sorted().map(::MemberId))
            .distinct()
    }

    /**
     * Whether a member hosts the databases other than `system`, as the system graph that
     * [transaction] reads has it: an enabled server, and every member while the initial servers are
     * not recorded.
     */
    private fun hosting(transaction: Transaction): (MemberId) -> Boolean {
        if (!ServerRecords.initialServersEnabled(transaction)) return { true }
        val records = ServerRecords.records(transaction)
        return { it.uuid in records }
    }

    /** What [member] has of [database], hosted in the mode it reports, else as [placed] there. */
    private fun copy(
        member: MemberId,
        answering: Boolean,
        database: DatabaseRef,
        placed: CopyMode?,
    ): Copy {
        val state = directory.databaseStates(member)?.firstOrNull { it.id == database.id }
        return Copy(
            member,
            answering,
            directory.boltAddress(member)?.let(::socketAddress),
            state,
            database.id in directory.readOnlyDatabases(member),
            directory.lastCommitted(member)[database.id],
            state?.mode ?: placed ?: CopyMode.PRIMARY,
        )
    }

    private companion object {
        const val SYSTEM = "system"
    }
}
