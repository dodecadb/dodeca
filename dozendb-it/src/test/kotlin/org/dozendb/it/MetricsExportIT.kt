package org.dozendb.it

import java.net.URI
import java.net.URLEncoder
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse.BodyHandlers
import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.dozendb.consensus.testing.TestPorts
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.lifecycle.Startables
import org.testcontainers.utility.DockerImageName

/**
 * Enterprise's other metrics exporters and checkpoint settings in Neo4j's image: CSV files where
 * Enterprise writes them, Graphite's own carbon and graphite-web taking what DozenDB sends, and the
 * CONTINUOUS policy with a low I/O limit under a write load.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MetricsExportIT {
    private val network = Network.newNetwork()
    private val graphite =
        GenericContainer(DockerImageName.parse(System.getProperty("graphite.image")))
            .withNetwork(network)
            .withNetworkAliases("graphite")
            .withExposedPorts(80)
            .waitingFor(Wait.forHttp("/").forPort(80).withStartupTimeout(Duration.ofMinutes(2)))

    // Few enough metrics for carbon, which creates at most 50 a minute.
    private val server =
        DozenDbContainer(DozenDbContainer.dozenDbImage, "server")
            .inNetwork(network)
            .boltOnHostPort(TestPorts.free())
            .prometheusOnHostPort(
                TestPorts.free(),
                "*vm.heap.used,*check_point.events,*transaction.last_committed_tx_id," +
                    "*db.query.execution.latency.millis",
            )
            .withEnv("NEO4J_server_metrics_csv_interval", "1s")
            .withEnv("NEO4J_server_metrics_graphite_enabled", "true")
            .withEnv("NEO4J_server_metrics_graphite_server", "graphite:2003")
            .withEnv("NEO4J_server_metrics_graphite_interval", "1s")
            .withEnv("NEO4J_db_checkpoint", "CONTINUOUS")
            .withEnv("NEO4J_db_checkpoint_iops_limit", "10")

    private val http = HttpClient.newHttpClient()

    @BeforeAll
    fun start() {
        Startables.deepStart(graphite, server).join()
    }

    @AfterAll
    fun stop() {
        server.saveLogs(DozenDbContainer.logsDirectory.resolve(javaClass.simpleName))
        server.stop()
        graphite.stop()
        network.close()
    }

    private fun eventually(assertion: () -> Unit) {
        await.ignoreExceptions() withPollInterval
            Duration.ofSeconds(1) atMost
            Duration.ofMinutes(3) untilAsserted
            {
                assertion()
            }
    }

    private fun csv(name: String): List<String> =
        server
            .copyFileFromContainer("/var/lib/neo4j/metrics/$name.csv") {
                it.readAllBytes().decodeToString()
            }
            .lines()
            .filter { it.isNotEmpty() }

    @Test
    fun `writes CSV files in the metrics directory of the home, by default`() {
        eventually {
            assertThat(csv("neo4j.dbms.vm.heap.used"))
                .hasSizeGreaterThan(3)
                .first()
                .isEqualTo("t,value")
            assertThat(csv("neo4j.database.neo4j.check_point.events"))
                .hasSizeGreaterThan(3)
                .first()
                .isEqualTo("t,count")
            assertThat(csv("neo4j.database.neo4j.db.query.execution.latency.millis").first())
                .isEqualTo("t,count,max,mean,min,stddev,p50,p75,p95,p98,p99,p999")
        }
    }

    @Test
    fun `Graphite renders what DozenDB sends`() {
        server.rows("UNWIND range(1, 10) AS i RETURN count(*)")

        eventually {
            assertThat(values("neo4j.dbms.vm.heap.used")).anyMatch { it > 0 }
            assertThat(values("neo4j.database.neo4j.db.query.execution.latency.millis.count"))
                .anyMatch { it > 0 }
        }
    }

    /** The values graphite-web renders for [target] over the last ten minutes. */
    private fun values(target: String): List<Double> {
        val url =
            "http://${graphite.host}:${graphite.getMappedPort(80)}/render?format=json" +
                "&from=-10min&target=${URLEncoder.encode(target, Charsets.UTF_8)}"
        val response = http.send(HttpRequest.newBuilder(URI(url)).build(), BodyHandlers.ofString())
        check(response.statusCode() == 200) { "graphite-web: HTTP ${response.statusCode()}" }
        // [{"target": ..., "datapoints": [[value or null, time], ...]}]
        return Regex("""\[([-+0-9.eE]+), \d+]""")
            .findAll(response.body())
            .map { it.groupValues[1].toDouble() }
            .toList()
    }

    @Test
    fun `the continuous policy checkpoints a write load as it commits, at the I-O limit`() {
        val start = committed()

        // Within the limit: 10 I/Os a second cannot keep up with a load that dirties more.
        repeat(200) { batch ->
            server.rows(
                "UNWIND range(1, 50) AS i CREATE (:Film {batch: \$batch, i: i})",
                parameters = mapOf("batch" to batch),
            )
        }

        eventually { assertThat(checkpoints().last().transaction).isEqualTo(committed()) }
        val checkpoints = checkpoints().filter { it.transaction > start }
        assertThat(checkpoints).allMatch { "IO limit: 10," in it.line }
        assertThat(checkpoints).anyMatch { Regex("paused in total [1-9]").containsMatchIn(it.line) }
        assertThat(server.debugLog()).doesNotContain("Could not load check point policy")
    }

    private fun committed(): Long =
        server
            .samples()
            .getValue("neo4j_database_neo4j_transaction_last_committed_tx_id_total")
            .toLong()

    private class Checkpoint(val line: String) {
        val transaction = Regex("@ txId: (\\d+)").find(line)!!.groupValues[1].toLong()
    }

    /** The continuous checkpoints of `neo4j` that completed, in order. */
    private fun checkpoints(): List<Checkpoint> =
        server
            .debugLog()
            .lines()
            .filter {
                "[neo4j/" in it &&
                    "Scheduled checkpoint for continuous checkpointing" in it &&
                    "checkpoint completed" in it
            }
            .map(::Checkpoint)
}
