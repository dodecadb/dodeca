package org.dozendb.core.logging

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.GraphDatabaseSettings.LogQueryLevel
import org.neo4j.configuration.SettingChangeListener
import org.neo4j.gqlstatus.ErrorGqlStatusObject
import org.neo4j.graphdb.config.Setting
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.query.ExecutingQuery
import org.neo4j.kernel.impl.query.QueryExecutionMonitor
import org.neo4j.kernel.impl.query.TransactionExecutionMonitor
import org.neo4j.kernel.lifecycle.LifecycleAdapter
import org.neo4j.logging.InternalLog
import org.neo4j.monitoring.Monitors

/**
 * Neo4j's query log, which Community declares the settings and the `query.log` file of but never
 * writes: queries and transactions, as `db.logs.query.*` asks, to the `QueryLogger`.
 *
 * Its listeners are on the global monitors, which every database's monitors report to, only while
 * their level is not OFF, so a log that is off costs nothing. The other settings are read for each
 * line, so changing them applies at once.
 */
class QueryLog(
    private val config: Config,
    private val monitors: Monitors,
    private val log: InternalLog,
) : LifecycleAdapter() {
    private val queries = Queries()
    private val transactions = Transactions()
    private val listening = mutableSetOf<Any>()
    private val settingListeners =
        mutableListOf<Pair<Setting<LogQueryLevel>, SettingChangeListener<LogQueryLevel>>>()

    override fun start() {
        follow(GraphDatabaseSettings.log_queries, queries)
        follow(GraphDatabaseSettings.log_queries_transactions_level, transactions)
    }

    override fun stop() {
        for ((setting, listener) in settingListeners) config.removeListener(setting, listener)
        settingListeners.clear()
        synchronized(this) { listening.toList().forEach(::unlisten) }
    }

    /** Listens with [listener] while [setting] is not OFF. */
    private fun follow(setting: Setting<LogQueryLevel>, listener: Any) {
        val change = SettingChangeListener<LogQueryLevel> { _, level -> update(level, listener) }
        config.addListener(setting, change)
        settingListeners += setting to change
        update(config.get(setting), listener)
    }

    @Synchronized
    private fun update(level: LogQueryLevel, listener: Any) {
        if (level == LogQueryLevel.OFF) {
            if (listener in listening) unlisten(listener)
        } else if (listening.add(listener)) {
            monitors.addMonitorListener(listener)
        }
    }

    private fun unlisten(listener: Any) {
        monitors.removeMonitorListener(listener)
        listening.remove(listener)
    }

    private fun options(level: LogQueryLevel) =
        QueryLogOptions(
            verbose = level == LogQueryLevel.VERBOSE,
            parameters = config.get(GraphDatabaseSettings.log_queries_parameter_logging_enabled),
            maxParameterLength = config.get(GraphDatabaseSettings.query_log_max_parameter_length),
            obfuscateLiterals = config.get(GraphDatabaseSettings.log_queries_obfuscate_literals),
            obfuscateErrors = config.get(GraphDatabaseSettings.log_queries_obfuscate_errors),
            plan = config.get(GraphDatabaseSettings.log_queries_query_plan),
            annotationFormat = config.get(GraphDatabaseSettings.log_queries_annotation_data_format),
        )

    private inner class Queries : QueryExecutionMonitor {
        private fun level() = config.get(GraphDatabaseSettings.log_queries)

        override fun startProcessing(query: ExecutingQuery) {
            val level = level()
            if (
                level != LogQueryLevel.VERBOSE ||
                    !config.get(GraphDatabaseSettings.log_queries_early_raw_logging_enabled)
            ) {
                return
            }
            val snapshot = query.snapshot()
            // The documented source format, which Neo4j's security log writes too.
            @Suppress("DEPRECATION")
            val raw =
                QueryLogLine.RawQuery(
                    query.internalQueryId(),
                    snapshot.clientConnection().asConnectionDetails(),
                    query.databaseId().map { it.name() }.orElse(QueryLogLine.NO_DATABASE),
                    query.executingUsername(),
                    query.authenticatedUsername(),
                    query.rawQueryText(),
                    query.rawQueryParameters(),
                )
            log.info(QueryLogLine.rawStarted(raw, options(level)))
        }

        override fun startExecution(query: ExecutingQuery) {
            val level = level()
            if (level != LogQueryLevel.VERBOSE) return
            log.info(QueryLogLine.started(query.snapshot(), options(level)))
        }

        override fun endSuccess(query: ExecutingQuery) = end(query, null)

        override fun endFailure(query: ExecutingQuery, failure: Throwable) =
            end(query, QueryFailure.of(failure))

        override fun endFailure(
            query: ExecutingQuery,
            reason: String?,
            status: Status?,
            errorGqlStatusObject: ErrorGqlStatusObject?,
        ) = end(query, QueryFailure(reason, status, errorGqlStatusObject))

        private fun end(query: ExecutingQuery, failure: QueryFailure?) {
            val level = level()
            when (level) {
                LogQueryLevel.OFF -> return
                LogQueryLevel.INFO ->
                    if (
                        query.elapsedNanos() <
                            config.get(GraphDatabaseSettings.log_queries_threshold).toNanos()
                    ) {
                        return
                    }
                LogQueryLevel.VERBOSE -> {}
            }
            val line = QueryLogLine.ended(query.snapshot(), options(level), failure)
            if (failure == null) log.info(line) else log.error(line)
        }
    }

    private inner class Transactions : TransactionExecutionMonitor {
        private fun level() = config.get(GraphDatabaseSettings.log_queries_transactions_level)

        override fun start(tx: KernelTransaction) {
            if (level() == LogQueryLevel.VERBOSE) log.info(TransactionLogLine.started(tx))
        }

        override fun commit(tx: KernelTransaction) = end(tx, TransactionLogLine.Event.COMMIT, null)

        override fun rollback(tx: KernelTransaction, failure: Throwable?) =
            end(tx, TransactionLogLine.Event.ROLLBACK, failure)

        private fun end(
            tx: KernelTransaction,
            event: TransactionLogLine.Event,
            failure: Throwable?,
        ) {
            val elapsedNanos = System.nanoTime() - tx.startTimeNanos()
            when (level()) {
                LogQueryLevel.OFF -> return
                LogQueryLevel.INFO ->
                    if (
                        elapsedNanos <
                            config
                                .get(GraphDatabaseSettings.log_queries_transaction_threshold)
                                .toNanos()
                    ) {
                        return
                    }
                LogQueryLevel.VERBOSE -> {}
            }
            val obfuscate = config.get(GraphDatabaseSettings.log_queries_obfuscate_errors)
            val line =
                TransactionLogLine.ended(
                    tx,
                    event,
                    elapsedNanos / 1_000_000,
                    failure?.let { QueryFailure.of(it).text(obfuscate) },
                )
            log.info(line)
        }
    }
}
