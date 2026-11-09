package org.dozendb.cluster.internal.topology

import java.time.Duration
import java.util.Optional
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.dozendb.cluster.MemberDirectory
import org.dozendb.cluster.MemberMetadata
import org.dozendb.cluster.internal.InitialServerOptions
import org.dozendb.consensus.ConsensusLog
import org.dozendb.consensus.ConsensusUnavailableException
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.MemberId
import org.dozendb.core.edition.DatabaseState
import org.dozendb.core.servers.CopyMode
import org.dozendb.core.servers.DroppedServer
import org.dozendb.core.servers.Placements
import org.dozendb.core.servers.ServerOptions
import org.dozendb.core.servers.ServerRecords
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.dbms.database.DatabaseDetails
import org.neo4j.dbms.database.ServerDetails
import org.neo4j.dbms.database.TopologyInfoService
import org.neo4j.dbms.identity.ServerId
import org.neo4j.dbms.systemgraph.InstanceModeConstraint
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.InstanceStatus
import org.neo4j.graphdb.Transaction
import org.neo4j.kernel.database.NamedDatabaseId

/**
 * What SHOW DATABASES and SHOW SERVERS show of a cluster, as Neo4j Enterprise shows them: for each
 * database a row for each server it is placed on and each member of its Raft group, also where it
 * does not run here (see [DatabaseRows], [DatabaseLocations]), and a server for each member of the
 * cluster. Other members' rows are what they report when this member asks them, as it does for each
 * command, waiting at most [probeDeadline]: a member that has not answered by then shows as
 * unknown, or as an unavailable server.
 *
 * @property report what a member reports about itself, this member included; null while unknown.
 */
internal class ClusterTopologyInfoService(
    private val community: TopologyInfoService,
    private val consensus: ConsensusLog,
    private val systemDatabase: DatabaseRef,
    private val directory: MemberDirectory,
    private val locations: DatabaseLocations,
    private val report: (MemberId) -> Map<String, String>?,
    private val probeDeadline: Duration = Duration.ofSeconds(1),
) : TopologyInfoService {
    /**
     * A server for each member of the cluster: Enabled with its record, Free without one, except
     * that every member counts as enabled, with the options it reports, until the servers the
     * cluster started with are recorded (see `ServerReconciler`). It hosts the databases it reports
     * running, and is to host `system` and the databases placed on it that are requested online. A
     * deallocating server is Deallocated once no database is placed on it and it votes in no
     * database's group. A dropped server is Dropped while the cluster still has it, and after that
     * while it answers where it was.
     */
    override fun servers(transaction: Transaction): Set<ServerDetails> {
        val answering = answering()
        val records = ServerRecords.records(transaction)
        val dropped = ServerRecords.dropped(transaction)
        val initialRecorded = ServerRecords.initialServersEnabled(transaction)
        val online = requestedOnline(transaction)
        val members = systemMembers()
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
        return servers.apply { addAll(stillAnswering(left)) }
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
        val hosts = hosting(transaction)
        return community.databases(transaction, databaseIds, requestedExtras).flatMapTo(
            HashSet()
        ) { own ->
            val id = own.namedDatabaseId()
            val database = DatabaseRef(id.name(), id.databaseId().uuid())
            val location = locations.of(database)
            val placed = placed(transaction, id.name())
            DatabaseRows.rows(
                own,
                members(location, placed, hosts).map { copy(it, it in answering, database) },
                location?.leader,
                requestedExtras.txInfo(),
                placed,
            )
        }
    }

    /**
     * The servers that host a primary of the database [name]; none for `system` or while unplaced.
     */
    private fun placed(transaction: Transaction, name: String): Set<MemberId> =
        Placements.database(transaction, name)
            ?.let { Placements.hosts(it, CopyMode.PRIMARY) }
            ?.mapTo(LinkedHashSet(), ::MemberId)
            .orEmpty()

    /**
     * The voting members of a database's group at [location] and the servers it is [placed] on;
     * while neither is known, the members of the cluster that [hosts]; this member alone while
     * consensus does not run.
     */
    private fun members(
        location: DatabaseLocations.Location?,
        placed: Set<MemberId>,
        hosts: (MemberId) -> Boolean,
    ): List<MemberId> {
        val voters = location?.voters?.map { it.id }
        if (voters == null && placed.isEmpty()) return systemMembers().filter(hosts)
        return (voters.orEmpty() + placed).distinct()
    }

    /** The members of the cluster; this member alone while consensus does not run. */
    private fun systemMembers(): List<MemberId> =
        try {
            consensus.members(systemDatabase).map { it.id }
        } catch (_: ConsensusUnavailableException) {
            listOf(consensus.self.id)
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

    private fun copy(member: MemberId, answering: Boolean, database: DatabaseRef): Copy =
        Copy(
            member,
            answering,
            directory.boltAddress(member)?.let(::socketAddress),
            directory.databaseStates(member)?.firstOrNull { it.id == database.id },
            database.id in directory.readOnlyDatabases(member),
            directory.lastCommitted(member)[database.id],
        )

    private companion object {
        const val SYSTEM = "system"
    }
}
