package dev.dodeca.core.monitoring.sources

import dev.dodeca.core.SystemGraphReads
import dev.dodeca.core.monitoring.MetricRegistry
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.dbms.database.DatabaseOperationCounts
import org.neo4j.dbms.database.StandaloneDatabaseContext
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel
import org.neo4j.kernel.database.NamedDatabaseId

/**
 * The DBMS's database operations and states, under `db.operation.count.` and `db.state.count.` as
 * Neo4j Enterprise names them.
 */
internal object DatabaseOperationMetrics {
    fun register(
        metrics: MetricRegistry.MetricGroup,
        counts: DatabaseOperationCounts,
        databases: DatabaseContextProvider<StandaloneDatabaseContext>,
    ) {
        fun counter(name: String, description: String, read: DatabaseOperationCounts.() -> Long) =
            metrics.counter("db.operation.count.$name", description) { counts.read() }
        counter("create", "Databases created.") { createCount() }
        counter("start", "Databases started.") { startCount() }
        counter("stop", "Databases stopped.") { stopCount() }
        counter("drop", "Databases dropped.") { dropCount() }
        counter("failed", "Database operations that failed.") { failedCount() }
        counter("recovered", "Database operations that failed before and then succeeded.") {
            recoveredCount()
        }
        metrics.gauge("db.state.count.hosted", "Databases started on this server.") {
            databases.registeredDatabases().values.count {
                !it.isFailed && it.database().isStarted
            }
        }
        metrics.gauge("db.state.count.failed", "Databases failed on this server.") {
            databases.registeredDatabases().values.count { it.isFailed }
        }
        metrics.gauge(
            "db.state.count.desired_started",
            "Databases that should be started on this server.",
        ) {
            desiredStarted(databases)
        }
    }

    /** The databases the system graph wants online, `system` included; null before it starts. */
    private fun desiredStarted(databases: DatabaseContextProvider<*>): Int? {
        val system =
            databases.getDatabaseContext(NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID).orElse(null)
                ?: return null
        if (!system.database().isStarted) return null
        return SystemGraphReads.read(system.databaseFacade()) { tx ->
            tx.findNodes(TopologyGraphDbmsModel.DATABASE_LABEL).use { nodes ->
                nodes.asSequence().count {
                    it.getProperty(TopologyGraphDbmsModel.DATABASE_STATUS_PROPERTY, null) ==
                        TopologyGraphDbmsModel.DatabaseStatus.ONLINE.statusName()
                }
            }
        }
    }
}
