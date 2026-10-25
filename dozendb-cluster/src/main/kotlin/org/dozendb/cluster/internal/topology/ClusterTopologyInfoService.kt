package org.dozendb.cluster.internal.topology

import java.time.Duration
import org.dozendb.cluster.MemberDirectory
import org.dozendb.consensus.ConsensusLog
import org.dozendb.consensus.ConsensusUnavailableException
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.MemberId
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.dbms.database.DatabaseDetails
import org.neo4j.dbms.database.ServerDetails
import org.neo4j.dbms.database.TopologyInfoService
import org.neo4j.graphdb.Transaction
import org.neo4j.kernel.database.NamedDatabaseId

/**
 * What SHOW DATABASES shows of a cluster, as Neo4j Enterprise shows it: a row for each member of a
 * database's Raft group (see [DatabaseRows]). Other members' rows are what they report when this
 * member asks them, as it does for each SHOW DATABASES, waiting at most [probeDeadline]: a member
 * that has not answered by then shows as unknown. Servers are [community]'s.
 */
internal class ClusterTopologyInfoService(
    private val community: TopologyInfoService,
    private val consensus: ConsensusLog,
    private val systemDatabase: DatabaseRef,
    private val directory: MemberDirectory,
    private val probeDeadline: Duration = Duration.ofSeconds(1),
) : TopologyInfoService {
    override fun servers(transaction: Transaction): Set<ServerDetails> =
        community.servers(transaction)

    override fun databases(
        transaction: Transaction,
        databaseIds: Set<NamedDatabaseId>,
        requestedExtras: TopologyInfoService.RequestedExtras,
    ): Set<DatabaseDetails> {
        val answering =
            try {
                directory.refresh(probeDeadline) + consensus.self.id
            } catch (_: ConsensusUnavailableException) {
                setOf(consensus.self.id)
            }
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
            directory.boltAddress(member)?.let { SocketAddress(it.host, it.port) },
            directory.databaseStates(member)?.firstOrNull { it.id == database.id },
            database.id in directory.readOnlyDatabases(member),
            directory.lastCommitted(member)[database.id],
        )
}
