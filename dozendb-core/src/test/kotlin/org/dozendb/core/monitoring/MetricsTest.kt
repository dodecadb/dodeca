package org.dozendb.core.monitoring

import java.io.ByteArrayInputStream
import java.lang.management.ManagementFactory
import java.nio.file.Path
import java.time.Duration
import java.util.zip.GZIPInputStream
import javax.management.ObjectName
import kotlin.io.path.exists
import kotlin.io.path.readText
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.dozendb.core.StandaloneServer
import org.dozendb.core.StandaloneServer.Companion.SYSTEM
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.helpers.GlobbingPattern
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.graphdb.config.Setting

class MetricsTest {
    @TempDir lateinit var root: Path

    private fun server(vararg settings: Pair<Setting<*>, Any>) =
        StandaloneServer(
            root,
            mapOf(
                MetricsSettings.prometheusEnabled to true,
                MetricsSettings.prometheusEndpoint to SocketAddress("localhost", 0),
            ) + settings,
        )

    @Test
    fun `serves the metrics to Prometheus and JMX`() {
        server().use { server ->
            server.start()

            val response = server.scrape("/metrics")
            assertThat(response.statusCode()).isEqualTo(200)
            assertThat(response.headers().firstValue("Content-Type"))
                .hasValue(PrometheusText.CONTENT_TYPE)
            val body = response.body().decodeToString()
            assertThat(body)
                .contains(
                    "# HELP neo4j_dbms_vm_heap_used neo4j.dbms.vm.heap.used: Bytes of heap in use.\n" +
                        "# TYPE neo4j_dbms_vm_heap_used gauge\n" +
                        "neo4j_dbms_vm_heap_used "
                )
                .contains("# TYPE neo4j_dbms_vm_threads gauge")
                .contains("# TYPE neo4j_dbms_vm_pause_time_total counter")
                .contains("neo4j_dbms_vm_memory_pool_g1_eden_space ")
                // Not in the default filter.
                .doesNotContain("neo4j_dbms_vm_heap_max")

            val gzipped = server.scrape("/", gzip = true)
            assertThat(gzipped.headers().firstValue("Content-Encoding")).hasValue("gzip")
            val unzipped =
                GZIPInputStream(ByteArrayInputStream(gzipped.body()))
                    .readAllBytes()
                    .decodeToString()
            assertThat(unzipped).contains("# TYPE neo4j_dbms_vm_heap_used gauge")

            assertThat(server.scrape("/other").statusCode()).isEqualTo(404)

            val beans =
                server.rows("CALL dbms.queryJmx('neo4j.metrics:*') YIELD name, attributes", SYSTEM)
            assertThat(beans.map { it["name"] })
                .contains("neo4j.metrics:name=neo4j.dbms.vm.heap.used,type=gauges")
                .contains("neo4j.metrics:name=neo4j.dbms.vm.pause_time,type=counters")
        }
        // Stopping removes the beans.
        assertThat(
                ManagementFactory.getPlatformMBeanServer()
                    .queryNames(ObjectName("neo4j.metrics:*"), null)
            )
            .isEmpty()
    }

    @Test
    fun `the filter, prefix and exporters follow the settings`() {
        server(
                MetricsSettings.prefix to "dozen",
                MetricsSettings.filter to listOf(GlobbingPattern("*vm.heap.max")),
                MetricsSettings.jmxEnabled to false,
            )
            .use { server ->
                server.start()

                val body = server.scrape("/metrics").body().decodeToString()
                assertThat(body.lines().filter { it.isNotEmpty() && !it.startsWith("#") })
                    .singleElement()
                    .asString()
                    .startsWith("dozen_dbms_vm_heap_max ")
                assertThat(server.rows("CALL dbms.queryJmx('neo4j.metrics:*') YIELD name", SYSTEM))
                    .isEmpty()
            }
    }

    @Test
    fun `nothing is reported with metrics or every exporter disabled`() {
        server(MetricsSettings.enabled to false).use { server ->
            server.start()
            assertThat(server.metrics()).isNull()
        }
        server(
                MetricsSettings.prometheusEnabled to false,
                MetricsSettings.jmxEnabled to false,
                MetricsSettings.csvEnabled to false,
            )
            .use { server ->
                server.start()
                assertThat(server.metrics()).isNull()
                assertThat(server.home.resolve("metrics")).doesNotExist()
            }
    }

    @Test
    fun `CSV files hold a line per interval, a database's only while it runs`() {
        server(MetricsSettings.csvInterval to Duration.ofMillis(100)).use { server ->
            server.start()
            server.administer("CREATE DATABASE films WAIT")
            val metrics = server.home.resolve("metrics")
            val dbms = metrics.resolve("neo4j.dbms.vm.heap.used.csv")
            val films = metrics.resolve("neo4j.database.films.check_point.events.csv")

            await atMost
                Duration.ofSeconds(30) untilAsserted
                {
                    assertThat(lines(dbms)).hasSizeGreaterThan(3).first().isEqualTo("t,value")
                    assertThat(lines(films)).hasSizeGreaterThan(3).first().isEqualTo("t,count")
                }

            server.administer("STOP DATABASE films WAIT")
            // A report under way may still write one: count once the next has started.
            awaitLines(dbms, lines(dbms).size + 2)
            val written = lines(films).size
            awaitLines(dbms, lines(dbms).size + 2)
            assertThat(lines(films)).hasSize(written)
        }
    }

    private fun lines(file: Path): List<String> =
        if (file.exists()) file.readText().lines().filter { it.isNotEmpty() } else emptyList()

    private fun awaitLines(file: Path, count: Int) {
        await atMost
            Duration.ofSeconds(30) untilAsserted
            {
                assertThat(lines(file)).hasSizeGreaterThanOrEqualTo(count)
            }
    }

    @Test
    fun `Enterprise's CSV settings have Enterprise's defaults`() {
        val config = Config.defaults(GraphDatabaseSettings.neo4j_home, root)

        assertThat(config.get(MetricsSettings.csvEnabled)).isTrue()
        assertThat(config.get(MetricsSettings.csvInterval)).isEqualTo(Duration.ofSeconds(30))
        assertThat(config.get(MetricsSettings.csvRotationSize)).isEqualTo(10L * 1024 * 1024)
        assertThat(config.get(MetricsSettings.csvRotationKeep)).isEqualTo(7)
        assertThat(config.get(MetricsSettings.csvRotationCompression))
            .isEqualTo(CsvCompression.NONE)
        assertThat(config.get(MetricsSettings.directory)).isEqualTo(root.resolve("metrics"))
        assertThatThrownBy {
                Config.newBuilder()
                    .setRaw(mapOf("server.metrics.csv.rotation.keep_number" to "0"))
                    .build()
            }
            .hasMessageContaining("server.metrics.csv.rotation.keep_number")
    }

    @Test
    fun `Enterprise's Graphite settings are refused, not ignored`() {
        val setting = "server.metrics.graphite.enabled"
        assertThatThrownBy { Config.newBuilder().setRaw(mapOf(setting to "true")).build() }
            .hasMessageContaining("Unrecognized setting")
            .hasMessageContaining(setting)
    }
}
