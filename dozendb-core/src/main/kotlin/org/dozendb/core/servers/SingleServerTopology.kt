package org.dozendb.core.servers

import java.util.Optional
import org.dozendb.core.edition.DozenDbDatabaseStateService
import org.neo4j.configuration.Config
import org.neo4j.dbms.database.DatabaseDetails
import org.neo4j.dbms.database.ServerDetails
import org.neo4j.dbms.database.TopologyInfoService
import org.neo4j.graphdb.Transaction
import org.neo4j.kernel.database.NamedDatabaseId

/**
 * What SHOW SERVERS shows of a server without clustering: Community's row for it, with the name and
 * options of its record, or with those of its `initial.server.*` settings while it has none. SHOW
 * DATABASES shows Community's rows, except that a database in quarantine has the role `unknown` and
 * is no writer, as in Neo4j Enterprise.
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
        transaction: Transaction,
        databaseIds: Set<NamedDatabaseId>,
        extras: TopologyInfoService.RequestedExtras,
    ): Set<DatabaseDetails> =
        community.databases(transaction, databaseIds, extras).mapTo(LinkedHashSet()) { row ->
            if (row.status() != DozenDbDatabaseStateService.Quarantined.DESCRIPTION)
                return@mapTo row
            DatabaseDetails(
                row.serverId(),
                row.databaseAccess(),
                row.boltAddress(),
                Optional.of(UNKNOWN_ROLE),
                false,
                row.status(),
                row.statusMessage(),
                row.lastCommittedTxId(),
                row.txCommitLag(),
                row.namedDatabaseId(),
                row.type(),
                row.options(),
                row.storeId(),
                row.externalStoreId(),
                row.actualPrimariesCount(),
                row.actualSecondariesCount(),
            )
        }

    private companion object {
        const val UNKNOWN_ROLE = "unknown"
    }
}
