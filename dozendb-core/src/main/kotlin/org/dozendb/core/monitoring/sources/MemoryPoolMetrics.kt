package org.dozendb.core.monitoring.sources

import org.dozendb.core.monitoring.MetricRegistry
import org.neo4j.memory.MemoryGroup
import org.neo4j.memory.MemoryPool
import org.neo4j.memory.MemoryPools
import org.neo4j.memory.ScopedMemoryPool

/**
 * Neo4j's memory pools, under `pool.<pool>.` for the DBMS and `pool.<pool>.<database>.` for a
 * database, as Neo4j Enterprise names them. A pool is named by its memory group, lower case, with
 * spaces as `_`: `transaction`, `bolt`, `page_cache`.
 */
internal object MemoryPoolMetrics {
    /** The DBMS's pools that exist now; each is read by its group, so a replaced pool is too. */
    fun register(metrics: MetricRegistry.MetricGroup, pools: MemoryPools) {
        for (group in pools.pools.map { it.group() }.distinct()) {
            stats(metrics, "pool.${name(group)}") {
                pools.pools.firstOrNull { it.group() == group }
            }
        }
    }

    /** The pools of [database] that exist now. */
    fun register(metrics: MetricRegistry.MetricGroup, pools: MemoryPools, database: String) {
        fun pool(group: MemoryGroup): ScopedMemoryPool? =
            pools.pools
                .filter { it.group() == group }
                .flatMap { it.databasePools }
                .firstOrNull { it.databaseName() == database }
        val groups =
            pools.pools.filter { pool -> pool.databasePools.any { it.databaseName() == database } }
        for (group in groups.map { it.group() }.distinct()) {
            stats(metrics, "pool.${name(group)}.$database") { pool(group) }
        }
    }

    private fun stats(metrics: MetricRegistry.MetricGroup, name: String, pool: () -> MemoryPool?) {
        metrics.gauge("$name.used_heap", "Heap bytes used or reserved.") { pool()?.usedHeap() }
        metrics.gauge("$name.used_native", "Native bytes used or reserved.") {
            pool()?.usedNative()
        }
        metrics.gauge("$name.total_used", "Heap and native bytes used or reserved.") {
            pool()?.totalUsed()
        }
        metrics.gauge("$name.total_size", "Bytes the pool may hold; 0 is unlimited.") {
            pool()?.totalSize()
        }
        metrics.gauge("$name.free", "Bytes the pool has left.") { pool()?.free() }
    }

    private fun name(group: MemoryGroup): String = group.getName().lowercase().replace(' ', '_')
}
