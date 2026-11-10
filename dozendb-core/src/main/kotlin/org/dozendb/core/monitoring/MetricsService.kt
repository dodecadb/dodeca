package org.dozendb.core.monitoring

import com.codahale.metrics.jmx.JmxReporter
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import org.dozendb.core.monitoring.sources.BoltMetrics
import org.dozendb.core.monitoring.sources.DatabaseMetrics
import org.dozendb.core.monitoring.sources.DatabaseOperationMetrics
import org.dozendb.core.monitoring.sources.JvmMetrics
import org.dozendb.core.monitoring.sources.MemoryPoolMetrics
import org.dozendb.core.monitoring.sources.PageCacheMetrics
import org.dozendb.core.monitoring.sources.QueryMetrics
import org.dozendb.core.monitoring.sources.RoutingMetrics
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.dbms.database.DatabaseOperationCounts
import org.neo4j.dbms.database.StandaloneDatabaseContext
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.event.DatabaseEventContext
import org.neo4j.graphdb.event.DatabaseEventListener
import org.neo4j.graphdb.factory.module.GlobalModule
import org.neo4j.kernel.api.database.DatabaseSizeService
import org.neo4j.kernel.database.AbstractDatabase
import org.neo4j.kernel.lifecycle.LifecycleAdapter
import org.neo4j.kernel.monitoring.DefaultDatabaseEvent
import org.neo4j.scheduler.Group
import org.neo4j.scheduler.JobHandle
import org.neo4j.scheduler.JobMonitoringParams

/**
 * The metrics of this DBMS, reported through the exporters the settings enable: CSV files,
 * Prometheus and JMX. A database's metrics are registered when it starts and removed when it stops,
 * is dropped or panics, as Neo4j Enterprise documents: "Database metrics are only available during
 * the lifetime of the database".
 *
 * Added to the global life before the databases start, so it sees every database start.
 */
class MetricsService
private constructor(
    private val globalModule: GlobalModule,
    private val databases: DatabaseContextProvider<StandaloneDatabaseContext>,
) : LifecycleAdapter(), DatabaseEventListener {
    private val config = globalModule.globalConfig
    private val log = globalModule.logService.internalLogProvider.getLog(javaClass)

    val registry =
        MetricRegistry(config.get(MetricsSettings.prefix), config.get(MetricsSettings.filter), log)

    /** The Prometheus endpoint, when enabled. */
    val prometheus: PrometheusEndpoint? =
        if (config.get(MetricsSettings.prometheusEnabled)) {
            PrometheusEndpoint(config.get(MetricsSettings.prometheusEndpoint), registry, log)
        } else {
            null
        }

    private val jmx: JmxReporter? =
        if (config.get(MetricsSettings.jmxEnabled)) {
            JmxReporter.forRegistry(registry.dropwizard).inDomain(JMX_DOMAIN).build()
        } else {
            null
        }

    private val csv: CsvReporter? =
        if (config.get(MetricsSettings.csvEnabled)) {
            CsvReporter(
                registry,
                config.get(MetricsSettings.directory),
                config.get(MetricsSettings.csvRotationSize),
                config.get(MetricsSettings.csvRotationKeep),
                config.get(MetricsSettings.csvRotationCompression),
                globalModule.globalClock,
                log,
            )
        } else {
            null
        }

    private var csvJob: JobHandle<*>? = null

    private val queries = QueryMetrics()
    private var dbms: MetricRegistry.MetricGroup? = null
    private val listeners = mutableListOf<Any>()
    private val databaseMetrics = ConcurrentHashMap<String, DatabaseMetricGroup>()

    /** A database's metrics, and what undoes the changes registering them made to it. */
    private class DatabaseMetricGroup(
        val metrics: MetricRegistry.MetricGroup,
        val undo: () -> Unit,
    ) {
        fun close() {
            metrics.close()
            undo()
        }
    }

    override fun init() {
        globalModule.databaseEventListeners.registerDatabaseEventListener(this)
    }

    override fun start() {
        val dbms = registry.dbms().also { dbms = it }
        JvmMetrics.register(dbms)
        JvmMetrics.pauses(dbms)?.let(::listen)
        PageCacheMetrics.register(
            dbms,
            globalModule.tracers.pageCacheTracer,
            globalModule.pageCache,
        )
        MemoryPoolMetrics.register(dbms, globalModule.memoryPools)
        listen(BoltMetrics.register(dbms))
        listen(RoutingMetrics(dbms))
        listen(queries)
        DatabaseOperationMetrics.register(
            dbms,
            globalModule.globalDependencies.resolveDependency(DatabaseOperationCounts::class.java),
            databases,
        )
        for (contributor in contributors()) contributor.dbms(dbms)
        jmx?.start()
        prometheus?.start()
        csvJob = csv?.let { csv ->
            schedule(
                Group.METRICS_CSV_WRITE,
                "Writes the metrics to CSV files",
                MetricsSettings.csvInterval,
                csv::report,
            )
        }
    }

    /** Runs [report] every [interval] on Neo4j's thread group [group] for it. */
    private fun schedule(
        group: Group,
        description: String,
        interval: Setting<Duration>,
        report: () -> Unit,
    ): JobHandle<*> {
        val millis = config.get(interval).toMillis()
        return globalModule.jobScheduler.scheduleRecurring(
            group,
            JobMonitoringParams.systemJob(description),
            report,
            millis,
            millis,
            TimeUnit.MILLISECONDS,
        )
    }

    override fun stop() {
        csvJob?.cancel()
        csvJob = null
        prometheus?.stop()
        jmx?.stop()
        listeners.forEach(globalModule.globalMonitors::removeMonitorListener)
        listeners.clear()
        databaseMetrics.values.forEach(DatabaseMetricGroup::close)
        databaseMetrics.clear()
        dbms?.close()
        dbms = null
    }

    override fun shutdown() {
        globalModule.databaseEventListeners.unregisterDatabaseEventListener(this)
    }

    /** Adds [listener] to the global monitors, which every database's monitors report to. */
    private fun listen(listener: Any) {
        globalModule.globalMonitors.addMonitorListener(listener)
        listeners += listener
    }

    private fun contributors(): List<MetricsContributor> =
        globalModule.globalDependencies
            .resolveTypeDependencies(MetricsContributor::class.java)
            .toList()

    override fun databaseStart(eventContext: DatabaseEventContext) {
        val name = eventContext.databaseName
        val context =
            when (eventContext) {
                is DefaultDatabaseEvent -> databases.getDatabaseContext(eventContext.databaseId)
                else -> databases.getDatabaseContext(name)
            }.orElse(null) ?: return
        databaseMetrics.remove(name)?.close()
        val metrics = registry.database(name)
        try {
            databaseMetrics[name] = register(context.database(), metrics)
        } catch (e: RuntimeException) {
            metrics.close()
            log.warn("The metrics of database '$name' are not reported.", e)
        }
    }

    private fun register(
        database: AbstractDatabase,
        metrics: MetricRegistry.MetricGroup,
    ): DatabaseMetricGroup {
        val sizes =
            globalModule.globalDependencies.resolveDependency(DatabaseSizeService::class.java)
        val name = database.namedDatabaseId.name()
        val undoDatabase = DatabaseMetrics.register(metrics, database, sizes)
        val undoQueries = queries.register(name, metrics)
        MemoryPoolMetrics.register(metrics, globalModule.memoryPools, name)
        for (contributor in contributors()) contributor.database(database, metrics)
        return DatabaseMetricGroup(metrics) {
            undoQueries()
            undoDatabase()
        }
    }

    override fun databaseShutdown(eventContext: DatabaseEventContext) = remove(eventContext)

    override fun databasePanic(eventContext: DatabaseEventContext) = remove(eventContext)

    override fun databaseCreate(eventContext: DatabaseEventContext) {}

    override fun databaseDrop(eventContext: DatabaseEventContext) = remove(eventContext)

    private fun remove(eventContext: DatabaseEventContext) {
        databaseMetrics.remove(eventContext.databaseName)?.close()
    }

    companion object {
        /** The JMX domain of the metrics' MBeans, Neo4j Enterprise's. */
        const val JMX_DOMAIN = "neo4j.metrics"

        /**
         * The metrics service for [globalModule], whose databases [databases] provides; null when
         * metrics or every exporter are disabled.
         */
        fun create(
            globalModule: GlobalModule,
            databases: DatabaseContextProvider<StandaloneDatabaseContext>,
        ): MetricsService? {
            val config = globalModule.globalConfig
            val exported =
                config.get(MetricsSettings.csvEnabled) ||
                    config.get(MetricsSettings.jmxEnabled) ||
                    config.get(MetricsSettings.prometheusEnabled)
            return if (config.get(MetricsSettings.enabled) && exported) {
                MetricsService(globalModule, databases)
            } else {
                null
            }
        }
    }
}
