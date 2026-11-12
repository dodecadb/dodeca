package org.dozendb.cluster.internal.forwarding

import java.time.Duration
import org.dozendb.cluster.internal.ClusterSettings
import org.dozendb.core.security.ForwardedLogin
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI
import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI
import org.neo4j.bolt.dbapi.BoltTransaction
import org.neo4j.boltmessages.AccessMode
import org.neo4j.boltmessages.request.connection.RoutingContext
import org.neo4j.configuration.Config
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.impl.query.QueryExecutionConfiguration
import org.neo4j.memory.MemoryTracker

/**
 * Neo4j's Bolt databases, which the HTTP APIs use too, whose transactions say through their routing
 * context whether this member may forward their writes to a leader: those of clients that allow
 * routing on the server (`neo4j://` drivers and the HTTP APIs) may, and those of `bolt://` clients
 * too while `dbms.dozendb.routing.forward_bolt_writes` is on in [config]. Transactions that another
 * member forwarded here never are again.
 */
internal class RoutingBoltDatabases(
    private val delegate: BoltGraphDatabaseManagementServiceSPI,
    private val config: Config,
) : BoltGraphDatabaseManagementServiceSPI {
    override fun database(
        databaseName: String,
        memoryTracker: MemoryTracker,
    ): BoltGraphDatabaseServiceSPI {
        val database = delegate.database(databaseName, memoryTracker)
        return object : BoltGraphDatabaseServiceSPI by database {
            override fun beginTransaction(
                type: KernelTransaction.Type,
                loginContext: LoginContext,
                clientInfo: ClientConnectionInfo,
                bookmarks: List<String>,
                txTimeout: Duration?,
                accessMode: AccessMode,
                txMetadata: Map<String, Any>?,
                routingContext: RoutingContext,
                queryExecutionConfiguration: QueryExecutionConfiguration,
            ): BoltTransaction =
                database.beginTransaction(
                    type,
                    loginContext,
                    clientInfo,
                    bookmarks,
                    txTimeout,
                    accessMode,
                    txMetadata,
                    routed(loginContext, routingContext),
                    queryExecutionConfiguration,
                )
        }
    }

    private fun routed(login: LoginContext, context: RoutingContext): RoutingContext =
        when {
            login is ForwardedLogin ->
                if (context.isServerRoutingEnabled) RoutingContext(false, context.parameters())
                else context
            !context.isServerRoutingEnabled && config.get(ClusterSettings.forwardBoltWrites) ->
                RoutingContext(true, context.parameters())
            else -> context
        }
}
