package dev.dodeca.core.monitoring.sources

import dev.dodeca.core.monitoring.MetricRegistry
import org.neo4j.kernel.impl.query.QueryRoutingMonitor

/**
 * Where Neo4j's query router sent queries, under `routing.query.count.` as Neo4j Enterprise names
 * them; a listener for the global monitors.
 */
internal class RoutingMetrics(metrics: MetricRegistry.MetricGroup) : QueryRoutingMonitor {
    private val local = metrics.counter("routing.query.count.local", "Queries run on this server.")
    private val internal =
        metrics.counter(
            "routing.query.count.remote_internal",
            "Queries routed to another member of the cluster.",
        )
    private val external =
        metrics.counter(
            "routing.query.count.remote_external",
            "Queries routed to a server outside the cluster.",
        )

    override fun queryRoutedLocal() {
        local?.inc()
    }

    override fun queryRoutedRemoteInternal() {
        internal?.inc()
    }

    override fun queryRoutedRemoteExternal() {
        external?.inc()
    }
}
