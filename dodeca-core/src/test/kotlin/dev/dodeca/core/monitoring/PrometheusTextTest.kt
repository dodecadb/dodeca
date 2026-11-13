package dev.dodeca.core.monitoring

import java.io.ByteArrayOutputStream
import java.io.StringWriter
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.neo4j.configuration.helpers.GlobbingPattern
import org.neo4j.logging.NullLog
import org.neo4j.logging.log4j.Log4jLogProvider

class PrometheusTextTest {
    private val output = ByteArrayOutputStream()
    private val log = Log4jLogProvider(output).getLog(javaClass)
    private val registry =
        MetricRegistry("neo4j", GlobbingPattern.create("*"), NullLog.getInstance())

    private val writer = PrometheusText(log)

    private fun text(): String =
        StringWriter().also { writer.write(registry.metrics(), it) }.toString()

    @Test
    fun `writes every type with Enterprise's Prometheus names`() {
        val dbms = registry.dbms()
        dbms.counter("bolt.connections_opened", "Bolt connections opened.")!!.inc(3)
        dbms.counter("page_cache.hits", "Page hits.") { 42 }
        dbms.counter("failing", "Cannot be read now.") { error("the database stopped") }
        dbms.gauge("vm.heap.used", "Bytes of heap in use.") { 1024L }
        dbms.gauge("page_cache.hit_ratio", "Hit ratio.") { 0.25 }
        dbms.gauge("page_cache.usage_ratio", "No pages yet.") { Double.NaN }
        dbms.gauge("vm.absent", "No value now.") { null }
        dbms.gauge("help.escaping", "Back\\slash and\nnewline.") { 1 }
        val archive = registry.database("film.archive-2")
        archive.counter("transaction.committed", "Committed transactions.") { 7 }
        val latency = archive.histogram("db.query.execution.latency.millis", "Query latency.")!!
        listOf(1L, 3L, 5L).forEach(latency::update)
        registry
            .database("films")
            .histogram("transaction.tx_size_heap", "Heap bytes per transaction.")
        registry.database("my-db").gauge("store.size.total", "Bytes of the database.") { 100 }
        registry.database("my.db").gauge("store.size.total", "Bytes of the database.") { 200 }

        val expected = javaClass.getResource("metrics.prom")!!.readText()
        assertThat(text()).isEqualTo(expected)
        assertThat(output.toString())
            .contains(
                "Metric neo4j.database.my.db.store.size.total is not reported to Prometheus: " +
                    "another metric is reported as neo4j_database_my_db_store_size_total."
            )

        // Once per name.
        output.reset()
        text()
        assertThat(output.toString()).isEmpty()
    }

    @Test
    fun `a histogram's samples clash with other metrics' names`() {
        registry.dbms().histogram("tx", "A histogram.")!!.update(1)
        registry.dbms().gauge("tx_count", "A gauge named like its count.") { 2 }

        assertThat(text()).contains("neo4j_dbms_tx_count 1").doesNotContain("neo4j_dbms_tx_count 2")
    }

    @Test
    fun `a name starting with a digit starts with an underscore`() {
        val registry = MetricRegistry("4j", GlobbingPattern.create("*"), NullLog.getInstance())
        registry.dbms().gauge("up", "Up.") { 1 }

        assertThat(
                StringWriter().also { PrometheusText(log).write(registry.metrics(), it) }.toString()
            )
            .endsWith("_4j_dbms_up 1\n")
    }
}
