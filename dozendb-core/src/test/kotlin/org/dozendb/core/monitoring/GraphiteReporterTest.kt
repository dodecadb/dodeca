package org.dozendb.core.monitoring

import java.io.ByteArrayOutputStream
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.util.concurrent.TimeUnit
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.neo4j.configuration.helpers.GlobbingPattern
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.logging.log4j.Log4jLogProvider
import org.neo4j.time.FakeClock

/** The metrics Neo4j Enterprise sends to Graphite. */
class GraphiteReporterTest {
    private val output = ByteArrayOutputStream()
    private val log = Log4jLogProvider(output).getLog(GraphiteReporter::class.java)
    private val registry = MetricRegistry("neo4j", listOf(GlobbingPattern("*")), log)
    private val clock = FakeClock(1_700_000_000_123, TimeUnit.MILLISECONDS)

    /** A Graphite server on [port] of the loopback address, 0 for any. */
    private class Graphite(port: Int = 0) : AutoCloseable {
        val socket =
            ServerSocket().apply {
                reuseAddress = true
                soTimeout = 30_000
                bind(InetSocketAddress(InetAddress.getLoopbackAddress(), port))
            }

        val address = SocketAddress(socket.inetAddress.hostAddress, socket.localPort)

        /** The lines of the next connection, which a report made. */
        fun received(): List<String> =
            socket.accept().use { it.getInputStream().bufferedReader().readLines() }

        override fun close() = socket.close()
    }

    @Test
    fun `counters and gauges go by their names, histograms by field, with the time in seconds`() {
        val dbms = registry.dbms()
        checkNotNull(dbms.counter("bolt.connections_opened", "Opened.")).inc(3)
        dbms.gauge("vm.heap.used", "Heap.") { 1024 }
        dbms.gauge("page_cache.usage_ratio", "Ratio.") { 0.25 }
        dbms.gauge("vm.heap.max", "None now.") { null }
        dbms.gauge("page_cache.hit_ratio", "Not a number.") { Double.NaN }
        val latency =
            checkNotNull(
                registry.database("films").histogram("db.query.execution.latency.millis", "L.")
            )
        latency.update(10)

        Graphite().use { graphite ->
            GraphiteReporter(registry, graphite.address, clock, log).report()

            val latencyName = "neo4j.database.films.db.query.execution.latency.millis"
            assertThat(graphite.received())
                .containsExactlyInAnyOrder(
                    "neo4j.dbms.bolt.connections_opened 3 1700000000",
                    "neo4j.dbms.vm.heap.used 1024 1700000000",
                    "neo4j.dbms.page_cache.usage_ratio 0.25 1700000000",
                    "$latencyName.count 1 1700000000",
                    "$latencyName.max 10 1700000000",
                    "$latencyName.mean 10.0 1700000000",
                    "$latencyName.min 10 1700000000",
                    "$latencyName.stddev 0.0 1700000000",
                    "$latencyName.p50 10.0 1700000000",
                    "$latencyName.p75 10.0 1700000000",
                    "$latencyName.p95 10.0 1700000000",
                    "$latencyName.p98 10.0 1700000000",
                    "$latencyName.p99 10.0 1700000000",
                    "$latencyName.p999 10.0 1700000000",
                )
        }
    }

    @Test
    fun `a server that cannot be reached is logged once, until the reports succeed again`() {
        registry.dbms().gauge("vm.threads", "Threads.") { 12 }
        // A port nothing listens on, until the server below starts.
        val port = Graphite().use { it.socket.localPort }
        val server = SocketAddress(InetAddress.getLoopbackAddress().hostAddress, port)
        val reporter = GraphiteReporter(registry, server, clock, log)

        reporter.report()
        reporter.report()
        Graphite(port).use { graphite ->
            reporter.report()

            assertThat(graphite.received()).containsExactly("neo4j.dbms.vm.threads 12 1700000000")
        }

        val logged = output.toString()
        assertThat(Regex("Cannot send metrics to Graphite at ").findAll(logged).count())
            .isEqualTo(1)
        assertThat(logged).contains("Sending metrics to Graphite at $server again.")
    }

    @Test
    fun `a metric that cannot be read is left out of the report`() {
        val dbms = registry.dbms()
        dbms.gauge("vm.heap.used", "Heap.") { throw IllegalStateException("database stopped") }
        dbms.gauge("vm.threads", "Threads.") { 12 }

        Graphite().use { graphite ->
            GraphiteReporter(registry, graphite.address, clock, log).report()

            assertThat(graphite.received()).containsExactly("neo4j.dbms.vm.threads 12 1700000000")
        }
    }
}
