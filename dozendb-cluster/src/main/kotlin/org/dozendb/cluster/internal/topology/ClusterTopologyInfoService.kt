package org.dozendb.cluster.internal.topology

import java.time.Duration
import java.util.Optional
import org.dozendb.cluster.MemberDirectory
import org.dozendb.cluster.MemberMetadata
import org.dozendb.consensus.ConsensusLog
import org.dozendb.consensus.ConsensusUnavailableException
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.MemberId
import org.dozendb.core.edition.DatabaseState
import org.dozendb.core.servers.ServerOptions
import org.dozendb.core.servers.ServerRecords
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.dbms.database.DatabaseDetails
import org.neo4j.dbms.database.ServerDetails
import org.neo4j.dbms.database.TopologyInfoService
import org.neo4j.dbms.identity.ServerId
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel
import org.neo4j.graphdb.Transaction
import org.neo4j.kernel.database.NamedDatabaseId

/**
 * What SHOW DATABASES and SHOW SERVERS show of a cluster, as Neo4j Enterprise shows them: for each
 * database a row for each member of its Raft group (see [DatabaseRows]), and a server for each
 * member of the cluster. Other members' rows are what they report when this member asks them, as it
 * does for each command, waiting at most [probeDeadline]: a member that has not answered by then
 * shows as unknown, or as an unavailable server.
 *
 * @property report what a member reports about itself, this member included; null while unknown.
 */
internal class ClusterTopologyInfoService(
    private val community: TopologyInfoService,
    private val consensus: ConsensusLog,
    private val systemDatabase: DatabaseRef,
    private val directory: MemberDirectory,
    private val report: (MemberId) -> Map<String, String>?,
    private val probeDeadline: Duration = Duration.ofSeconds(1),
) : TopologyInfoService {
    /**
     * A server for each member of the cluster: Enabled with its record, Free without one, except
     * that every member counts as enabled, with the options it reports, until the servers the
     * cluster started with are recorded (see `ServerReconciler`). It hosts the databases it reports
     * running, and is to host every database requested online while enabled.
     */
    override fun servers(transaction: Transaction): Set<ServerDetails> {
        val answering = answering()
        val records = ServerRecords.records(transaction)
        val initialRecorded = ServerRecords.initialServersEnabled(transaction)
        val requested = requestedOnline(transaction)
        return members(systemDatabase).mapTo(LinkedHashSet()) { member ->
            val record = records[member.uuid]
            val enabled = record != null || !initialRecorded
            val reported = report(member)
            val available = member in answering
            val options =
                record?.options
                    ?: reported
                        ?.get(MemberMetadata.INITIAL_SERVER_TAGS)
                        ?.takeIf { enabled }
                        ?.let { ServerOptions(MemberMetadata.splitList(it)) }
                    ?: ServerOptions()
            ServerDetails(
                ServerId(member.uuid),
                record?.name ?: member.uuid.toString(),
                Optional.ofNullable(directory.boltAddress(member)?.let(::socketAddress)),
                Optional.ofNullable(reported?.get(MemberMetadata.HTTP_ADDRESS)?.let(::parsed)),
                Optional.ofNullable(reported?.get(MemberMetadata.HTTPS_ADDRESS)?.let(::parsed)),
                LinkedHashSet(options.tags),
                if (enabled) ServerDetails.State.ENABLED else ServerDetails.State.FREE,
                if (available) ServerDetails.RunningState.AVAILABLE
                else ServerDetails.RunningState.UNAVAILABLE,
                if (available) running(member) else emptySet(),
                if (enabled) requested else emptySet(),
                LinkedHashSet(options.allowedDatabases),
                LinkedHashSet(options.deniedDatabases),
                options.modeConstraint,
                Optional.ofNullable(reported?.get(MemberMetadata.NEO4J_VERSION)),
            )
        }
    }

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
        return community.databases(transaction, databaseIds, requestedExtras).flatMapTo(
            HashSet()
        ) { own ->
            val id = own.namedDatabaseId()
            val database = DatabaseRef(id.name(), id.databaseId().uuid())
            DatabaseRows.rows(
                own,
                members(database).map { copy(it, it in answering, database) },
                consensus.status(database).leader,
                requestedExtras.txInfo(),
            )
        }
    }

    /**
     * The members of [database]'s group; of the cluster while the group does not run here; this
     * member alone while consensus does not run.
     */
    private fun members(database: DatabaseRef): List<MemberId> =
        listOf(database, systemDatabase).firstNotNullOfOrNull { group ->
            try {
                consensus.members(group).map { it.id }
            } catch (_: ConsensusUnavailableException) {
                null
            }
        } ?: listOf(consensus.self.id)

    private fun copy(member: MemberId, answering: Boolean, database: DatabaseRef): Copy =
        Copy(
            member,
            answering,
            directory.boltAddress(member)?.let(::socketAddress),
            directory.databaseStates(member)?.firstOrNull { it.id == database.id },
            database.id in directory.readOnlyDatabases(member),
            directory.lastCommitted(member)[database.id],
        )
}
