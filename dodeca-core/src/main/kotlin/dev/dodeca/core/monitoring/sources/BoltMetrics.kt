package dev.dodeca.core.monitoring.sources

import com.codahale.metrics.Counter
import dev.dodeca.core.monitoring.MetricRegistry
import dev.dodeca.core.monitoring.SummaryHistogram
import org.neo4j.bolt.protocol.common.connection.BoltConnectionMetricsMonitor
import org.neo4j.bolt.protocol.common.connection.BoltDriverMetricsMonitor
import org.neo4j.kernel.api.exceptions.Status

/**
 * Bolt's metrics, under `bolt.` and `bolt_driver.api.` as Neo4j Enterprise names them, counted from
 * the events Bolt reports to the global monitors.
 *
 * Bolt reports a connection's activation and its return to idle in pairs (in a finally block), and
 * opens and closes only for negotiated connections, so the running and idle gauges derived from
 * them do not drift.
 */
internal class BoltMetrics private constructor(metrics: MetricRegistry.MetricGroup) :
    BoltConnectionMetricsMonitor, BoltDriverMetricsMonitor {
    // Always counted: the running and idle gauges derive from them.
    private val opened = Counter()
    private val closed = Counter()
    private val activated = Counter()
    private val waiting = Counter()
    private val received = metrics.counter("bolt.messages_received", "Bolt messages received.")
    private val started =
        metrics.counter("bolt.messages_started", "Bolt messages whose processing started.")
    private val done =
        metrics.counter("bolt.messages_done", "Bolt messages processed, successfully or not.")
    private val failed = metrics.counter("bolt.messages_failed", "Bolt messages that failed.")
    private val queueTime =
        metrics.counter(
            "bolt.accumulated_queue_time",
            "Milliseconds Bolt messages waited for a worker thread.",
        )
    private val processingTime =
        metrics.counter(
            "bolt.accumulated_processing_time",
            "Milliseconds worker threads processed Bolt messages.",
        )
    private val boundTime: SummaryHistogram? =
        metrics.histogram(
            "bolt.worker_thread_bound_time",
            "Milliseconds a worker thread was bound to a connection.",
        )
    private val responseSuccess =
        metrics.counter(
            "bolt.response_success",
            "Successful Bolt responses, with internal.server.bolt.response_metrics.",
        )
    private val responseIgnored =
        metrics.counter(
            "bolt.response_ignored",
            "Ignored Bolt responses, with internal.server.bolt.response_metrics.",
        )
    private val responseFailed =
        metrics.counter(
            "bolt.response_failed",
            "Failed Bolt responses, with internal.server.bolt.response_metrics.",
        )
    private val managedCalls =
        metrics.counter(
            "bolt_driver.api.managed_transaction_function_calls",
            "Managed transaction function calls, with server.bolt.telemetry.enabled.",
        )
    private val unmanagedCalls =
        metrics.counter(
            "bolt_driver.api.unmanaged_transaction_calls",
            "Unmanaged transaction calls, with server.bolt.telemetry.enabled.",
        )
    private val implicitCalls =
        metrics.counter(
            "bolt_driver.api.implicit_transaction_calls",
            "Implicit transaction calls, with server.bolt.telemetry.enabled.",
        )
    private val executeCalls =
        metrics.counter(
            "bolt_driver.api.execute_calls",
            "Driver-level execute calls, with server.bolt.telemetry.enabled.",
        )

    init {
        metrics.counter("bolt.connections_opened", "Bolt connections opened.") { opened.count }
        metrics.counter("bolt.connections_closed", "Bolt connections closed.") { closed.count }
        metrics.gauge("bolt.connections_running", "Bolt connections a worker thread serves.") {
            running()
        }
        metrics.gauge("bolt.connections_idle", "Open Bolt connections no worker thread serves.") {
            // Read in this order, a connection that opens or activates meanwhile is not counted
            // twice.
            val running = running()
            (opened.count - closed.count - running).coerceAtLeast(0)
        }
    }

    private fun running(): Long = (activated.count - waiting.count).coerceAtLeast(0)

    override fun connectionOpened() = opened.inc()

    override fun connectionActivated() = activated.inc()

    override fun connectionWaiting() = waiting.inc()

    override fun messageReceived() {
        received?.inc()
    }

    override fun messageProcessingStarted(queueTime: Long) {
        started?.inc()
        this.queueTime?.inc(queueTime)
    }

    override fun messageProcessingCompleted(processingTime: Long) {
        done?.inc()
        this.processingTime?.inc(processingTime)
    }

    // Bolt reports a failed message as completed too.
    override fun messageProcessingFailed() {
        failed?.inc()
    }

    override fun responseSuccess() {
        responseSuccess?.inc()
    }

    override fun responseIgnored() {
        responseIgnored?.inc()
    }

    override fun responseFailed(status: Status) {
        responseFailed?.inc()
    }

    override fun workerThreadReleased(boundMillis: Long) {
        boundTime?.update(boundMillis)
    }

    override fun connectionClosed() = closed.inc()

    override fun managedTransactionFunctionsInterfaceCalled() {
        managedCalls?.inc()
    }

    override fun unmanagedTransactionInterfaceCalled() {
        unmanagedCalls?.inc()
    }

    override fun implicitTransactionInterfaceCalled() {
        implicitCalls?.inc()
    }

    override fun executeInterfaceCalled() {
        executeCalls?.inc()
    }

    companion object {
        /** Bolt's metrics in [metrics]; a listener for the global monitors. */
        fun register(metrics: MetricRegistry.MetricGroup): BoltMetrics = BoltMetrics(metrics)
    }
}
