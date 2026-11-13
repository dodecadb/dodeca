package dev.dodeca.core.monitoring.sources

import com.codahale.metrics.Counter
import dev.dodeca.core.monitoring.MetricRegistry
import dev.dodeca.core.monitoring.SummaryHistogram
import java.util.concurrent.ConcurrentHashMap
import org.neo4j.gqlstatus.ErrorGqlStatusObject
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.query.ExecutingQuery
import org.neo4j.kernel.impl.query.QueryExecutionMonitor

/**
 * The queries each database ran, under `db.query.execution.` as Neo4j Enterprise names them, and
 * under `db.query.execution.slotted.` for those of the slotted runtime; a listener for the global
 * monitors. Queries that never reached a database, such as those that do not parse, count nowhere.
 */
internal class QueryMetrics : QueryExecutionMonitor {
    private class Counters(metrics: MetricRegistry.MetricGroup, name: String, runtime: String) {
        val success: Counter? = metrics.counter("$name.success", "Successful $runtime queries.")
        val failure: Counter? = metrics.counter("$name.failure", "Failed $runtime queries.")
        val latency: SummaryHistogram? =
            metrics.histogram("$name.latency.millis", "Milliseconds $runtime queries took.")
    }

    private class Database(metrics: MetricRegistry.MetricGroup) {
        val all = Counters(metrics, "db.query.execution", "")
        val slotted = Counters(metrics, "db.query.execution.slotted", "slotted runtime")

        fun byRuntime(query: ExecutingQuery): Counters? =
            if (query.cypherRuntime().equals("slotted", ignoreCase = true)) slotted else null
    }

    private val databases = ConcurrentHashMap<String, Database>()

    /** Counts [database]'s queries in [metrics]; the returned action stops it. */
    fun register(database: String, metrics: MetricRegistry.MetricGroup): () -> Unit {
        val counters = Database(metrics)
        databases[database] = counters
        return { databases.remove(database, counters) }
    }

    private fun database(query: ExecutingQuery): Database? =
        query.databaseId().orElse(null)?.let { databases[it.name()] }

    override fun startProcessing(query: ExecutingQuery) {}

    override fun startExecution(query: ExecutingQuery) {}

    override fun endSuccess(query: ExecutingQuery) {
        val database = database(query) ?: return
        val millis = query.elapsedMillis()
        for (counters in listOfNotNull(database.all, database.byRuntime(query))) {
            counters.success?.inc()
            counters.latency?.update(millis)
        }
    }

    override fun endFailure(query: ExecutingQuery, failure: Throwable) = failed(query)

    override fun endFailure(
        query: ExecutingQuery,
        reason: String?,
        status: Status?,
        errorGqlStatusObject: ErrorGqlStatusObject?,
    ) = failed(query)

    private fun failed(query: ExecutingQuery) {
        val database = database(query) ?: return
        for (counters in listOfNotNull(database.all, database.byRuntime(query))) {
            counters.failure?.inc()
        }
    }
}
