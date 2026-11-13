package dev.dodeca.core.monitoring.sources

import dev.dodeca.core.monitoring.MetricRegistry
import org.neo4j.io.pagecache.PageCache
import org.neo4j.io.pagecache.monitoring.PageCacheCounters

/** The page cache's metrics, under `page_cache.` as Neo4j Enterprise names them. */
internal object PageCacheMetrics {
    fun register(
        metrics: MetricRegistry.MetricGroup,
        counters: PageCacheCounters,
        cache: PageCache,
    ) {
        fun counter(name: String, description: String, read: PageCacheCounters.() -> Long) =
            metrics.counter("page_cache.$name", description) { counters.read() }

        counter("eviction_exceptions", "Exceptions while evicting pages.") { evictionExceptions() }
        counter("flushes", "Page flushes.") { flushes() }
        counter("merges", "Page merges.") { merges() }
        counter("unpins", "Page unpins.") { unpins() }
        counter("pins", "Page pins.") { pins() }
        counter("evictions", "Page evictions.") { evictions() }
        counter("evictions.cooperative", "Evictions by threads that needed a free page.") {
            cooperativeEvictions()
        }
        counter("eviction.flushes", "Pages flushed by evictions.") { evictionFlushes() }
        counter("eviction.cooperative.flushes", "Pages flushed by cooperative evictions.") {
            cooperativeEvictionFlushes()
        }
        counter("page_faults", "Page faults, failed or not.") { faults() }
        counter("page_fault_failures", "Failed page faults.") { failedFaults() }
        counter("page_cancelled_faults", "Page faults that found the page loaded already.") {
            noFaults()
        }
        counter("page_vectored_faults", "Vectored page faults, failed or not.") {
            vectoredFaults()
        }
        counter("page_vectored_faults_failures", "Failed vectored page faults.") {
            failedVectoredFaults()
        }
        counter("page_no_pin_page_faults", "Pages loaded by vectored faults, without pins.") {
            noPinFaults()
        }
        counter("hits", "Page hits.") { hits() }
        counter("bytes_read", "Bytes read into the page cache.") { bytesRead() }
        counter("bytes_written", "Bytes written from the page cache.") { bytesWritten() }
        counter("iops", "IO operations of the page cache.") { iopqPerformed() }
        counter("throttled.times", "Times the flush IO limiter throttled.") { ioLimitedTimes() }
        counter("throttled.millis", "Milliseconds the flush IO limiter throttled.") {
            ioLimitedMillis()
        }
        counter("pages_copied", "Page copies.") { copiedPages() }
        metrics.gauge("page_cache.hit_ratio", "Hits among page lookups, from 0 to 1.") {
            counters.hitRatio()
        }
        metrics.gauge("page_cache.usage_ratio", "Pages in use among all pages, from 0 to 1.") {
            val pages = cache.maxCachedPages()
            if (pages > 0) (pages - cache.freePages()).toDouble() / pages else Double.NaN
        }
    }
}
