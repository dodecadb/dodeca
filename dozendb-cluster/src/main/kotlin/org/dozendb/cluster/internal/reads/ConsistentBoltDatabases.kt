package org.dozendb.cluster.internal.reads

import java.time.Duration
import org.dozendb.cluster.internal.ReadConsistency
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI
import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI
import org.neo4j.bolt.dbapi.BoltTransaction
import org.neo4j.bolt.protocol.common.message.AccessMode
import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.impl.query.QueryExecutionConfiguration
import org.neo4j.memory.MemoryTracker

/**
 * Neo4j's Bolt databases, whose transactions ask for a read consistency they know, through the
 * `dozendb.consistency` metadata key: one that asks for another fails as it begins, before Neo4j's
 * query router decides where it runs. [LinearizableLocalTransactions] waits for linearizable reads
 * there.
 */
internal class ConsistentBoltDatabases(
    private val delegate: BoltGraphDatabaseManagementServiceSPI,
    private val defaultConsistency: ReadConsistency,
) : BoltGraphDatabaseManagementServiceSPI {
    override fun database(
        databaseName: String,
        memoryTracker: MemoryTracker,
    ): BoltGraphDatabaseServiceSPI =
        ConsistentDatabase(delegate.database(databaseName, memoryTracker))

    private inner class ConsistentDatabase(private val database: BoltGraphDatabaseServiceSPI) :
        BoltGraphDatabaseServiceSPI by database {
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
        ): BoltTransaction {
            ReadConsistency.of(txMetadata, defaultConsistency)
            return database.beginTransaction(
                type,
                loginContext,
                clientInfo,
                bookmarks,
                txTimeout,
                accessMode,
                txMetadata,
                routingContext,
                queryExecutionConfiguration,
            )
        }
    }
}

/** A read could not be made as consistent as requested. */
internal class ReadConsistencyException(
    private val status: Status,
    message: String,
    cause: Throwable? = null,
) : RuntimeException(message, cause), Status.HasStatus {
    override fun status(): Status = status
}
