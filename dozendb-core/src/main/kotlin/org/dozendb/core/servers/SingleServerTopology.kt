package org.dozendb.core.servers

import org.neo4j.configuration.Config
import org.neo4j.dbms.database.ServerDetails
import org.neo4j.dbms.database.TopologyInfoService
import org.neo4j.graphdb.Transaction

/**
 * What SHOW SERVERS shows of a server without clustering: Community's row for it, with the name and
 * options of its record, or with those of its `initial.server.*` settings while it has none.
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
}
