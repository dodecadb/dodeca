package org.dozendb.core.monitoring

import java.io.IOException
import java.net.InetSocketAddress
import java.net.Socket
import java.time.Clock
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.logging.InternalLog

/**
 * Sends the metrics of [registry] to the Graphite server at [server], as Neo4j Enterprise does, in
 * Graphite's plaintext protocol over TCP: a line `<name> <value> <t>` for each counter and gauge
 * (none while a gauge has none), and for each field of a histogram as `<name>.<field>`, the fields
 * of Dropwizard's Graphite reporter; `t` in seconds since the epoch. Each [report] connects anew,
 * resolving the host name again, which the JVM's address cache may answer. A server that cannot be
 * reached is logged once, until the reports succeed again.
 */
internal class GraphiteReporter(
    private val registry: MetricRegistry,
    private val server: SocketAddress,
    private val clock: Clock,
    private val log: InternalLog,
    /** How long connecting and each write may take. */
    private val timeoutMillis: Int = 10_000,
) {
    private var failing = false

    /** Sends the value of each metric now. Called by one thread at a time. */
    @Synchronized
    fun report() {
        val seconds = clock.millis() / 1000
        val text = StringBuilder()
        for (metric in registry.metrics()) {
            try {
                text.append(lines(metric, seconds))
            } catch (e: RuntimeException) {
                // A source that went away while it was read, such as a stopping database.
                log.debug("Metric ${metric.name} could not be read: $e")
            }
        }
        try {
            Socket().use { socket ->
                socket.connect(InetSocketAddress(server.hostname, server.port), timeoutMillis)
                socket.soTimeout = timeoutMillis
                val out = socket.getOutputStream()
                out.write(text.toString().toByteArray())
                out.flush()
            }
            if (failing) log.info("Sending metrics to Graphite at $server again.")
            failing = false
        } catch (e: IOException) {
            if (!failing) log.warn("Cannot send metrics to Graphite at $server: $e")
            failing = true
        }
    }

    /** The lines of [metric] at [seconds]. */
    private fun lines(metric: Metric, seconds: Long): String {
        val name = sanitise(metric.name)
        return when (metric.type) {
            MetricType.COUNTER -> line(name, metric.count(), seconds)
            MetricType.GAUGE -> metric.gauge()?.let { line(name, it, seconds) }.orEmpty()
            MetricType.HISTOGRAM -> {
                val histogram = metric.histogram()
                val snapshot = histogram.snapshot
                listOf(
                        "count" to histogram.count,
                        "max" to snapshot.max,
                        "mean" to snapshot.mean,
                        "min" to snapshot.min,
                        "stddev" to snapshot.stdDev,
                        "p50" to snapshot.median,
                        "p75" to snapshot.get75thPercentile(),
                        "p95" to snapshot.get95thPercentile(),
                        "p98" to snapshot.get98thPercentile(),
                        "p99" to snapshot.get99thPercentile(),
                        "p999" to snapshot.get999thPercentile(),
                    )
                    .joinToString("") { (field, value) -> line("$name.$field", value, seconds) }
            }
        }
    }

    /** A line, none for a value Graphite cannot take: not a number, or infinite. */
    private fun line(name: String, value: Number, seconds: Long): String =
        if (value is Double && !value.isFinite() || value is Float && !value.isFinite()) {
            ""
        } else {
            "$name $value $seconds\n"
        }

    /** [name] without whitespace, which separates a line's parts. */
    private fun sanitise(name: String) = name.trim().replace(WHITESPACE, "-")

    private companion object {
        val WHITESPACE = Regex("\\s+")
    }
}
