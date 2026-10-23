package org.dozendb.core.monitoring

import org.neo4j.kernel.database.AbstractDatabase

/**
 * Metrics from outside DozenDB core, such as clustering's. Implementations in the global
 * dependencies when the DBMS starts are asked for theirs.
 */
interface MetricsContributor {
    /** Registers DBMS-wide metrics in [metrics]. */
    fun dbms(metrics: MetricRegistry.MetricGroup) {}

    /**
     * Registers the metrics of [database], which has just started, in [metrics]. They go when it
     * stops.
     */
    fun database(database: AbstractDatabase, metrics: MetricRegistry.MetricGroup) {}
}
