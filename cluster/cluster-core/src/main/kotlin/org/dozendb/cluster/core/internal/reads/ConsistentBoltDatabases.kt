package org.dozendb.cluster.core.internal.reads

import java.time.Duration
import org.dozendb.cluster.core.internal.ReadConsistency
import org.dozendb.cluster.core.internal.replication.ReplicatedDatabase
import org.dozendb.cluster.core.internal.replication.ReplicationService
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI
import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI
import org.neo4j.bolt.dbapi.BoltTransaction
import org.neo4j.bolt.protocol.common.message.AccessMode
import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.database.DatabaseReferenceImpl
import org.neo4j.kernel.impl.query.QueryExecutionConfiguration
import org.neo4j.memory.MemoryTracker

/**
 * DozerDB's Bolt databases with per-transaction read consistency. A transaction on a replicated
 * database that asks for linearizable reads, through the `dozendb.consistency` metadata key or the
 * server default, begins only once this member has applied everything the cluster had committed
 * when it asked. Bookmarks are handled by DozerDB's databases as before.
 */
internal class ConsistentBoltDatabases(
    private val delegate: BoltGraphDatabaseManagementServiceSPI,
    private val replication: ReplicationService,
    private val defaultConsistency: ReadConsistency,
) : BoltGraphDatabaseManagementServiceSPI {
    override fun database(
        databaseName: String,
        memoryTracker: MemoryTracker,
    ): BoltGraphDatabaseServiceSPI {
        val database = delegate.database(databaseName, memoryTracker)
        val id =
            (database.databaseReference as? DatabaseReferenceImpl.Internal)?.databaseId()
                ?: return database
        if (!replication.replicates(id)) return database
        return ConsistentDatabase(database) {
            replication.running().firstOrNull { it.namedDatabaseId == id }
        }
    }

    /** The consistency a transaction asks for; throws a client error for an unknown value. */
    fun consistencyOf(metadata: Map<String, Any?>?): ReadConsistency {
        val requested = metadata?.get(ReadConsistency.METADATA_KEY) ?: return defaultConsistency
        return ReadConsistency.parse(requested)
            ?: throw ReadConsistencyException(
                Status.Request.Invalid,
                "Unknown ${ReadConsistency.METADATA_KEY} '$requested'; use one of " +
                    ReadConsistency.entries.joinToString { it.name.lowercase() },
            )
    }

    private inner class ConsistentDatabase(
        private val database: BoltGraphDatabaseServiceSPI,
        private val replica: () -> ReplicatedDatabase?,
    ) : BoltGraphDatabaseServiceSPI by database {
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
            if (consistencyOf(txMetadata) == ReadConsistency.LINEARIZABLE) {
                val running =
                    replica()
                        ?: throw ReadConsistencyException(
                            Status.Cluster.ReplicationFailure,
                            "Database ${database.databaseReference.alias().name()} is not replicating on this member",
                        )
                running.awaitLinearizable()
            }
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
