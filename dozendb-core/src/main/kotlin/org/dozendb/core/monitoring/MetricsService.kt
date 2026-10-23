package org.dozendb.core.monitoring

import com.codahale.metrics.jmx.JmxReporter
import java.util.concurrent.ConcurrentHashMap
import org.dozendb.core.monitoring.sources.JvmMetrics
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.graphdb.event.DatabaseEventContext
import org.neo4j.graphdb.event.DatabaseEventListener
import org.neo4j.graphdb.factory.module.GlobalModule
import org.neo4j.kernel.database.AbstractDatabase
import org.neo4j.kernel.lifecycle.LifecycleAdapter
import org.neo4j.kernel.monitoring.DefaultDatabaseEvent

/**
 * The metrics of this DBMS, reported through the exporters the settings enable: Prometheus and JMX.
 * A database's metrics are registered when it starts and removed when it stops, is dropped or
 * panics, as Neo4j Enterprise documents: "Database metrics are only available during the lifetime
 * of the database".
 *
 * Added to the global life before the databases start, so it sees every database start.
 */
class MetricsService
private constructor(
    private val globalModule: GlobalModule,
    private val databases: DatabaseContextProvider<*>,
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

    private var dbms: MetricRegistry.MetricGroup? = null
    private val listeners = mutableListOf<Any>()
    private val databaseMetrics = ConcurrentHashMap<String, MetricRegistry.MetricGroup>()

    override fun init() {
        globalModule.databaseEventListeners.registerDatabaseEventListener(this)
    }

    override fun start() {
        val dbms = registry.dbms().also { dbms = it }
        JvmMetrics.register(dbms)
        JvmMetrics.pauses(dbms)?.let(::listen)
        for (contributor in contributors()) contributor.dbms(dbms)
        jmx?.start()
        prometheus?.start()
    }

    override fun stop() {
        prometheus?.stop()
        jmx?.stop()
        listeners.forEach(globalModule.globalMonitors::removeMonitorListener)
        listeners.clear()
        databaseMetrics.values.forEach(MetricRegistry.MetricGroup::close)
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
        val metrics = registry.database(name)
        databaseMetrics.put(name, metrics)?.close()
        register(context.database(), metrics)
    }

    private fun register(database: AbstractDatabase, metrics: MetricRegistry.MetricGroup) {
        for (contributor in contributors()) contributor.database(database, metrics)
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
            databases: DatabaseContextProvider<*>,
        ): MetricsService? {
            val config = globalModule.globalConfig
            val exported =
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
