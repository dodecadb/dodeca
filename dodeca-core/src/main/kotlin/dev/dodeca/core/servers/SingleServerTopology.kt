package dev.dodeca.core.servers

import dev.dodeca.core.edition.DodecaDatabaseStateService
import java.util.Optional
import org.neo4j.configuration.Config
import org.neo4j.dbms.database.DatabaseDetails
import org.neo4j.dbms.database.ServerDetails
import org.neo4j.dbms.database.TopologyInfoService
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Transaction
import org.neo4j.kernel.database.NamedDatabaseId

/**
 * What SHOW SERVERS shows of a server without clustering: Community's row for it, with the name and
 * options of its record, or with those of its `initial.server.*` settings while it has none. SHOW
 * DATABASES shows Community's rows, with the status and the numbers of primaries and secondaries
 * requested in the database's record, where Community always shows `online` and no numbers, and
 * where a database in quarantine has the role `unknown` and is no writer, as in Neo4j Enterprise.
 */
class SingleServerTopology(
    private val community: TopologyInfoService,
    private val config: Config,
) : TopologyInfoService by community {
    override fun servers(transaction: Transaction): Set<ServerDetails> {
        val records = ServerRecords.records(transaction)
        return community.servers(transaction).mapTo(LinkedHashSet()) { server ->
            val record = records[server.serverId().uuid()]
            val options = record?.options ?: ServerOptions.initial(config)
            ServerDetails(
                server.serverId(),
                record?.name ?: server.name(),
                server.boltAddress(),
                server.httpAddress(),
                server.httpsAddress(),
                LinkedHashSet(options.tags),
                server.state(),
                server.runningState(),
                server.hostedDatabases(),
                server.desiredDatabases(),
                LinkedHashSet(options.allowedDatabases),
                LinkedHashSet(options.deniedDatabases),
                options.modeConstraint,
                server.neo4jVersion(),
            )
        }
    }

    override fun databases(
        transaction: Transaction?,
        databaseIds: Set<NamedDatabaseId>,
        extras: TopologyInfoService.RequestedExtras,
    ): Set<DatabaseDetails> =
        community.databases(transaction, databaseIds, extras).mapTo(LinkedHashSet()) { row ->
            val record =
                transaction?.findNode(
                    TopologyGraphDbmsModel.DATABASE_LABEL,
                    TopologyGraphDbmsModel.DATABASE_UUID_PROPERTY,
                    row.namedDatabaseId().databaseId().uuid().toString(),
                )
            val quarantined =
                row.actualStatus() == DodecaDatabaseStateService.Quarantined.DESCRIPTION
            DatabaseDetails(
                row.serverId(),
                row.databaseAccess(),
                row.boltAddress(),
                if (quarantined) Optional.of(UNKNOWN_ROLE) else row.role(),
                row.writer() && !quarantined,
                row.actualStatus(),
                row.statusMessage(),
                row.lastCommittedTxId(),
                row.txCommitLag(),
                row.shardCommitLag(),
                row.namedDatabaseId(),
                record?.getProperty(TopologyGraphDbmsModel.DATABASE_STATUS_PROPERTY, null)
                    as String? ?: row.requestedStatus(),
                row.type(),
                row.options(),
                row.storeId(),
                row.externalStoreId(),
                record.count(TopologyGraphDbmsModel.DATABASE_PRIMARIES_PROPERTY),
                row.actualPrimariesCount(),
                record.count(TopologyGraphDbmsModel.DATABASE_SECONDARIES_PROPERTY),
                row.actualSecondariesCount(),
                row.creationTime(),
                row.lastStartTime(),
                row.lastStopTime(),
                row.cypherVersion(),
            )
        }

    private fun Node?.count(property: String): Int? =
        (this?.getProperty(property, null) as Number?)?.toInt()

    private companion object {
        const val UNKNOWN_ROLE = "unknown"
    }
}
