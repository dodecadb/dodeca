package org.dozendb.core.monitoring.sources

import com.codahale.metrics.Counter
import com.sun.management.UnixOperatingSystemMXBean
import java.lang.management.BufferPoolMXBean
import java.lang.management.ManagementFactory
import org.dozendb.core.monitoring.MetricRegistry
import org.neo4j.monitoring.VmPauseMonitor

/**
 * The JVM's metrics, under `vm.` as Neo4j Enterprise names them, read from its management beans.
 * Garbage collectors, memory pools and buffer pools are named by their beans, lower case, with
 * every run of other characters as `_`: `G1 Old Gen` is `g1_old_gen`.
 */
internal object JvmMetrics {
    fun register(metrics: MetricRegistry.MetricGroup) {
        val os = ManagementFactory.getOperatingSystemMXBean()
        if (os is UnixOperatingSystemMXBean) {
            metrics.gauge("vm.file.descriptors.count", "Open file descriptors.") {
                os.openFileDescriptorCount
            }
            metrics.gauge("vm.file.descriptors.maximum", "The open file descriptors allowed.") {
                os.maxFileDescriptorCount
            }
        }
        for (gc in ManagementFactory.getGarbageCollectorMXBeans()) {
            val name = beanName(gc.name)
            metrics.counter("vm.gc.time.$name", "Milliseconds spent in ${gc.name} collections.") {
                gc.collectionTime
            }
            metrics.counter("vm.gc.count.$name", "${gc.name} collections.") { gc.collectionCount }
        }
        val memory = ManagementFactory.getMemoryMXBean()
        metrics.gauge("vm.heap.committed", "Bytes of heap the JVM has committed.") {
            memory.heapMemoryUsage.committed
        }
        metrics.gauge("vm.heap.used", "Bytes of heap in use.") { memory.heapMemoryUsage.used }
        metrics.gauge("vm.heap.max", "The most bytes of heap the JVM can use.") {
            memory.heapMemoryUsage.max
        }
        for (pool in ManagementFactory.getPlatformMXBeans(BufferPoolMXBean::class.java)) {
            val name = "vm.memory.buffer.${beanName(pool.name)}"
            metrics.gauge("$name.count", "Buffers in the ${pool.name} buffer pool.") { pool.count }
            metrics.gauge("$name.used", "Bytes the ${pool.name} buffer pool uses.") {
                pool.memoryUsed
            }
            metrics.gauge("$name.capacity", "Bytes of buffers in the ${pool.name} buffer pool.") {
                pool.totalCapacity
            }
        }
        for (pool in ManagementFactory.getMemoryPoolMXBeans()) {
            metrics.gauge("vm.memory.pool.${beanName(pool.name)}", "Bytes ${pool.name} uses.") {
                pool.usage?.used
            }
        }
        val threads = ManagementFactory.getThreadMXBean()
        metrics.gauge("vm.threads", "Live threads, daemon or not.") { threads.threadCount }
    }

    /**
     * Counts the time of the VM pauses Neo4j's pause monitor detects; a listener for the global
     * monitors, or null when the filter leaves the metric out.
     */
    fun pauses(metrics: MetricRegistry.MetricGroup): VmPauseMonitor.Monitor? {
        val time =
            metrics.counter("vm.pause_time", "Milliseconds of detected VM pauses.") ?: return null
        return PauseTime(time)
    }

    private class PauseTime(private val time: Counter) : VmPauseMonitor.Monitor.Adapter() {
        override fun pauseDetected(info: VmPauseMonitor.VmPauseInfo) = time.inc(info.pauseTime)
    }

    private fun beanName(name: String): String =
        name.lowercase().replace(Regex("[^a-z0-9]+"), "_").trim('_')
}
