package dev.dodeca.core.monitoring.sources

import dev.dodeca.core.monitoring.MetricRegistry
import dev.dodeca.core.pagecache.PageCacheWarmup
import java.util.concurrent.TimeUnit
import org.neo4j.internal.schema.IndexType
import org.neo4j.io.pagecache.context.CursorContext
import org.neo4j.kernel.api.database.DatabaseSizeService
import org.neo4j.kernel.database.AbstractDatabase
import org.neo4j.kernel.impl.index.DatabaseIndexStats
import org.neo4j.kernel.impl.monitoring.TransactionSizeMonitor
import org.neo4j.kernel.impl.query.CacheMetrics
import org.neo4j.kernel.impl.query.QueryCacheStatistics
import org.neo4j.kernel.impl.store.stats.StoreEntityCounters
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer
import org.neo4j.storageengine.api.TransactionIdStore

/**
 * A database's metrics, as Neo4j Enterprise names them: transactions, checkpoints, the transaction
 * log, entity counts, store sizes, indexes and Cypher's caches. Read from the database's own
 * counters when reported, except the transaction sizes, which Neo4j reports to one callback.
 */
internal object DatabaseMetrics {
    /** Store sizes walk the database's files, so a scrape reads them at most this often. */
    private val STORE_SIZE_CACHE_NANOS = TimeUnit.SECONDS.toNanos(10)

    /** Registers [database]'s metrics; the returned action undoes what they changed in it. */
    fun register(
        metrics: MetricRegistry.MetricGroup,
        database: AbstractDatabase,
        sizes: DatabaseSizeService,
    ): () -> Unit {
        val dependencies = database.dependencyResolver
        val transactions = dependencies.resolveDependency(DatabaseTransactionStats::class.java)
        transactions(
            metrics,
            transactions,
            dependencies.resolveDependency(TransactionIdStore::class.java),
        )
        val tracer = dependencies.resolveDependency(DatabaseTracer::class.java)
        checkpoints(metrics, tracer)
        transactionLog(metrics, tracer)
        counts(metrics, dependencies.resolveDependency(StoreEntityCounters::class.java))
        storeSizes(metrics, database, sizes)
        indexes(metrics, dependencies.resolveDependency(DatabaseIndexStats::class.java))
        cypher(metrics, dependencies.resolveDependency(QueryCacheStatistics::class.java))
        dependencies.resolveTypeDependencies(PageCacheWarmup::class.java).firstOrNull()?.let {
            warmup(metrics, it)
        }

        val heap = metrics.histogram("transaction.tx_size_heap", "Heap bytes of each transaction.")
        val native =
            metrics.histogram("transaction.tx_size_native", "Native bytes of each transaction.")
        if (heap == null && native == null) return {}
        transactions.setTransactionSizeCallback(
            object : TransactionSizeMonitor {
                override fun addHeapTransactionSize(transactionSizeHeap: Long) {
                    heap?.update(transactionSizeHeap)
                }

                override fun addNativeTransactionSize(transactionSizeNative: Long) {
                    native?.update(transactionSizeNative)
                }
            }
        )
        return { transactions.setTransactionSizeCallback(null) }
    }

    private fun transactions(
        metrics: MetricRegistry.MetricGroup,
        stats: DatabaseTransactionStats,
        ids: TransactionIdStore,
    ) {
        fun counter(name: String, description: String, read: DatabaseTransactionStats.() -> Long) =
            metrics.counter("transaction.$name", description) { stats.read() }
        fun gauge(name: String, description: String, read: DatabaseTransactionStats.() -> Long) =
            metrics.gauge("transaction.$name", description) { stats.read() }
        counter("started", "Transactions started.") { numberOfStartedTransactions }
        counter("peak_concurrent", "The most transactions active at once.") {
            peakConcurrentNumberOfTransactions
        }
        gauge("active", "Transactions active now.") { numberOfActiveTransactions }
        gauge("active_read", "Read transactions active now.") { numberOfActiveReadTransactions }
        gauge("active_write", "Write transactions active now.") { numberOfActiveWriteTransactions }
        counter("committed", "Transactions committed.") { numberOfCommittedTransactions }
        counter("committed_read", "Read transactions committed.") {
            numberOfCommittedReadTransactions
        }
        counter("committed_write", "Write transactions committed.") {
            numberOfCommittedWriteTransactions
        }
        counter("rollbacks", "Transactions rolled back.") { numberOfRolledBackTransactions }
        counter("rollbacks_read", "Read transactions rolled back.") {
            numberOfRolledBackReadTransactions
        }
        counter("rollbacks_write", "Write transactions rolled back.") {
            numberOfRolledBackWriteTransactions
        }
        counter("terminated", "Transactions terminated.") { numberOfTerminatedTransactions }
        counter("terminated_read", "Read transactions terminated.") {
            numberOfTerminatedReadTransactions
        }
        counter("terminated_write", "Write transactions terminated.") {
            numberOfTerminatedWriteTransactions
        }
        metrics.counter("transaction.last_committed_tx_id", "The last committed transaction id.") {
            ids.lastCommittedTransactionId
        }
        metrics.counter("transaction.last_closed_tx_id", "The last closed transaction id.") {
            ids.highestGapFreeClosedTransactionId
        }
    }

    private fun warmup(metrics: MetricRegistry.MetricGroup, warmup: PageCacheWarmup) {
        metrics.counter(
            "page_cache.warmup.pages_loaded",
            "Pages the page cache warmup, or preload, loaded since the database started.",
        ) {
            warmup.pagesLoaded
        }
        metrics.gauge(
            "page_cache.warmup.duration",
            "Milliseconds the page cache warmup, or preload, took, once it ended.",
        ) {
            warmup.durationMillis
        }
    }

    private fun checkpoints(metrics: MetricRegistry.MetricGroup, tracer: DatabaseTracer) {
        metrics.counter("check_point.events", "Checkpoints.") { tracer.numberOfCheckPoints() }
        metrics.counter("check_point.total_time", "Milliseconds spent checkpointing.") {
            tracer.checkPointAccumulatedTotalTimeMillis()
        }
        metrics.gauge("check_point.duration", "Milliseconds the last checkpoint took.") {
            tracer.lastCheckpointTimeMillis()
        }
        metrics.counter("check_point.flushed_bytes", "Bytes flushed by checkpoints.") {
            tracer.flushedBytes()
        }
        metrics.gauge(
            "check_point.limit_millis",
            "Milliseconds the IO limiter paused the last checkpoint.",
        ) {
            tracer.lastCheckpointIOLimitedMillis()
        }
        metrics.gauge(
            "check_point.limit_times",
            "Times the IO limiter paused the last checkpoint.",
        ) {
            tracer.lastCheckpointIOLimitedTimes()
        }
        metrics.gauge("check_point.pages_flushed", "Pages the last checkpoint flushed.") {
            tracer.lastCheckpointPagesFlushed()
        }
        metrics.gauge("check_point.io_performed", "IO operations of the last checkpoint.") {
            tracer.lastCheckpointIOs()
        }
        metrics.gauge("check_point.io_limit", "The IO limit of the last checkpoint.") {
            tracer.lastCheckpointIOLimit()
        }
    }

    private fun transactionLog(metrics: MetricRegistry.MetricGroup, tracer: DatabaseTracer) {
        metrics.counter("log.rotation_events", "Transaction log rotations.") {
            tracer.numberOfLogRotations()
        }
        metrics.counter(
            "log.rotation_total_time",
            "Milliseconds spent rotating the transaction log.",
        ) {
            tracer.logRotationAccumulatedTotalTimeMillis()
        }
        metrics.gauge("log.rotation_duration", "Milliseconds the last log rotation took.") {
            tracer.lastLogRotationTimeMillis()
        }
        metrics.counter("log.appended_bytes", "Bytes appended to the transaction log.") {
            tracer.appendedBytes()
        }
        metrics.counter("log.flushes", "Transaction log flushes.") { tracer.numberOfFlushes() }
        metrics.gauge("log.append_batch_size", "Transactions in the last append batch.") {
            tracer.lastTransactionLogAppendBatch()
        }
    }

    private fun counts(metrics: MetricRegistry.MetricGroup, counters: StoreEntityCounters) {
        metrics.gauge("neo4j.count.relationship", "Relationships in the database.") {
            counters.allRelationshipsCountStore(CursorContext.NULL_CONTEXT)
        }
        metrics.gauge("neo4j.count.node", "Nodes in the database.") {
            counters.allNodesCountStore(CursorContext.NULL_CONTEXT)
        }
        metrics.gauge("neo4j.count.relationship_types", "Relationship type ids in use.") {
            counters.relationshipTypes(CursorContext.NULL_CONTEXT)
        }
        metrics.gauge("ids_in_use.relationship_type", "Relationship type ids in use.") {
            counters.relationshipTypes(CursorContext.NULL_CONTEXT)
        }
        metrics.gauge("ids_in_use.property", "Property ids in use.") {
            counters.properties(CursorContext.NULL_CONTEXT)
        }
        metrics.gauge("ids_in_use.relationship", "Relationship ids in use.") {
            counters.relationships(CursorContext.NULL_CONTEXT)
        }
        metrics.gauge("ids_in_use.node", "Node ids in use.") {
            counters.nodes(CursorContext.NULL_CONTEXT)
        }
    }

    private fun storeSizes(
        metrics: MetricRegistry.MetricGroup,
        database: AbstractDatabase,
        sizes: DatabaseSizeService,
    ) {
        val id = database.namedDatabaseId
        val total = Cached { sizes.getDatabaseTotalSize(id) }
        val data = Cached { sizes.getDatabaseDataSize(id) }
        // Enterprise renamed store.size.total in 2025.01.
        metrics.gauge("store.size.full", "Bytes of the database and its transaction logs.") {
            total.get()
        }
        metrics.gauge("store.size.database", "Bytes of the database without transaction logs.") {
            data.get()
        }
        metrics.gauge(
            "store.size.available_reserved",
            "Bytes the database's files have reserved but not used yet.",
        ) {
            sizes.getDatabaseAvailableReservedSize(id)
        }
    }

    private fun indexes(metrics: MetricRegistry.MetricGroup, stats: DatabaseIndexStats) {
        for (type in IndexType.values()) {
            val name = type.name.lowercase()
            metrics.counter("index.$name.queried", "Times $name indexes were queried.") {
                stats.getQueryCount(type)
            }
            metrics.counter("index.$name.populated", "Completed $name index populations.") {
                stats.getPopulationCount(type)
            }
        }
    }

    private fun cypher(metrics: MetricRegistry.MetricGroup, stats: QueryCacheStatistics) {
        metrics.counter(
            "cypher.replan_events",
            "Queries planned again after their plan went stale.",
        ) {
            stats.numberOfReplans()
        }
        metrics.counter(
            "cypher.replan_wait_time",
            "Seconds between stale plans and their replanning.",
        ) {
            stats.replanWaitTime()
        }
        val entries =
            mapOf<String, QueryCacheStatistics.() -> Long>(
                "preparser" to { preParserCacheEntries() },
                "ast" to { astCacheEntries() },
                "logical_plan" to { logicalPlanCacheEntries() },
                "execution_plan" to { executionPlanCacheEntries() },
                "executable_query" to { executableQueryCacheEntries() },
            )
        for ((kind, read) in entries) {
            val name = "cypher.cache.$kind"
            fun counter(stat: String, description: String, value: (CacheMetrics) -> Long) =
                metrics.counter("$name.$stat", description) {
                    stats.metricsPerCacheKind()[kind]?.let(value) ?: 0
                }
            counter("hits", "Hits in the $kind cache.") { it.hits }
            counter("misses", "Misses in the $kind cache.") { it.misses }
            // Enterprise's meaning: plans recompiled with expressions as code, which Community
            // never does.
            counter("compiled", "Plans in the $kind cache compiled with expressions as code.") {
                it.compiledWithExpressionCodeGen
            }
            counter("discards", "Entries discarded from the $kind cache.") { it.discards }
            counter("stale_entries", "Stale entries found in the $kind cache.") { it.staleEntries }
            counter("cache_flushes", "Times the $kind cache was flushed.") { it.cacheFlushes }
            metrics.gauge("$name.entries", "Entries in the $kind cache.") { stats.read() }
        }
    }

    /** A value read at most every [STORE_SIZE_CACHE_NANOS]. */
    private class Cached(private val read: () -> Long) {
        private var value: Long? = null
        private var readAt: Long = 0

        @Synchronized
        fun get(): Long {
            val now = System.nanoTime()
            val value = this.value
            if (value != null && now - readAt < STORE_SIZE_CACHE_NANOS) return value
            return read().also {
                this.value = it
                readAt = now
            }
        }
    }
}
