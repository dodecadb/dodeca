package org.dozendb.core.monitoring

import java.io.ByteArrayInputStream
import java.lang.management.ManagementFactory
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse.BodyHandlers
import java.nio.file.Path
import java.util.zip.GZIPInputStream
import javax.management.ObjectName
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.dozendb.core.StandaloneServer
import org.dozendb.core.StandaloneServer.Companion.SYSTEM
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.Config
import org.neo4j.configuration.helpers.GlobbingPattern
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.graphdb.config.Setting

class MetricsTest {
    @TempDir lateinit var root: Path

    private val http = HttpClient.newHttpClient()

    private fun server(vararg settings: Pair<Setting<*>, Any>) =
        StandaloneServer(
            root,
            mapOf(
                MetricsSettings.prometheusEnabled to true,
                MetricsSettings.prometheusEndpoint to SocketAddress("localhost", 0),
            ) + settings,
        )

    private fun StandaloneServer.metrics(): MetricsService? =
        database(SYSTEM)
            .dependencyResolver
            .resolveTypeDependencies(MetricsService::class.java)
            .firstOrNull()

    private fun StandaloneServer.get(path: String, gzip: Boolean = false) =
        http.send(
            HttpRequest.newBuilder(
                    URI("http://localhost:${metrics()!!.prometheus!!.boundAddress!!.port}$path")
                )
                .apply { if (gzip) header("Accept-Encoding", "gzip") }
                .build(),
            BodyHandlers.ofByteArray(),
        )

    @Test
    fun `serves the metrics to Prometheus and JMX`() {
        server().use { server ->
            server.start()

            val response = server.get("/metrics")
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

            val gzipped = server.get("/", gzip = true)
            assertThat(gzipped.headers().firstValue("Content-Encoding")).hasValue("gzip")
            val unzipped =
                GZIPInputStream(ByteArrayInputStream(gzipped.body()))
                    .readAllBytes()
                    .decodeToString()
            assertThat(unzipped).contains("# TYPE neo4j_dbms_vm_heap_used gauge")

            assertThat(server.get("/other").statusCode()).isEqualTo(404)

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

                val body = server.get("/metrics").body().decodeToString()
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
        server(MetricsSettings.prometheusEnabled to false, MetricsSettings.jmxEnabled to false)
            .use { server ->
                server.start()
                assertThat(server.metrics()).isNull()
            }
    }

    @Test
    fun `Enterprise's CSV and Graphite settings are refused, not ignored`() {
        for (setting in listOf("server.metrics.csv.enabled", "server.metrics.graphite.enabled")) {
            assertThatThrownBy { Config.newBuilder().setRaw(mapOf(setting to "true")).build() }
                .hasMessageContaining("Unrecognized setting")
                .hasMessageContaining(setting)
        }
    }
}
