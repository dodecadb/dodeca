package dev.dodeca.core.monitoring

import dev.dodeca.core.DodecaSettings
import dev.dodeca.core.StandaloneServer
import dev.dodeca.core.StandaloneServer.Companion.SYSTEM
import java.nio.file.Path
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.GraphDatabaseSettings.LogQueryLevel
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.configuration.helpers.GlobbingPattern
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.graphdb.config.Setting

/**
 * What the query log and metrics cost: Bolt queries per second, from one client and from eight at
 * once, and small write transactions per second, with the query log off, at INFO and at VERBOSE,
 * and with metrics off, as configured by default (JMX, Neo4j's default filter) and all of them over
 * JMX and Prometheus; and how long a scrape of every metric takes with a hundred databases. Not
 * part of the suites; run it with `./mvnw -pl dodeca-core test -Dtest=MonitoringBenchmark` and read
 * what it prints.
 */
class MonitoringBenchmark {
    // Kept when a test fails, for the servers' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path

    @Test
    fun `measure what the query log and metrics cost`() {
        val report = StringBuilder()
        report.appendLine(
            "%-8s %-8s %14s %14s %14s %14s"
                .format("metrics", "log", "reads/s, 1", "p99 ms, 1", "reads/s, 8", "writes/s, 1")
        )
        for ((name, settings) in METRICS) {
            StandaloneServer(root.resolve(name), settings + (BoltConnector.enabled to true)).use {
                server ->
                server.start()
                assertThat(server.metrics() != null).describedAs(name).isEqualTo(name != "off")
                GraphDatabase.driver("bolt://${server.boltAddress()}").use { driver ->
                    driver.verifyConnectivity()
                    val best = LEVELS.associateWith { DoubleArray(3) }
                    val latencies = LEVELS.associateWith { mutableListOf<Long>() }
                    // In turns, so that each level is measured as warm as the others; the first
                    // round only warms up for the latencies.
                    repeat(ROUNDS) { round ->
                        for (level in LEVELS) {
                            server.set(GraphDatabaseSettings.log_queries, level)
                            val single = LongArray(READS)
                            val rates =
                                doubleArrayOf(
                                    reads(driver, 1, single),
                                    reads(driver, 8),
                                    writes(driver),
                                )
                            if (round > 0) latencies.getValue(level) += single.toList()
                            val kept = best.getValue(level)
                            rates.forEachIndexed { i, rate -> kept[i] = maxOf(kept[i], rate) }
                        }
                    }
                    for (level in LEVELS) {
                        val (one, eight, writes) = best.getValue(level).toList()
                        val sorted = latencies.getValue(level).sorted()
                        val p99 = sorted[sorted.size * 99 / 100] / 1e6
                        report.appendLine(
                            "%-8s %-8s %14.0f %14.2f %14.0f %14.0f"
                                .format(name, level, one, p99, eight, writes)
                        )
                    }
                }
            }
        }
        println(report)
    }

    @Test
    fun `measure what scraping costs with many databases`() {
        val all =
            METRICS.getValue("all") +
                mapOf(
                    GraphDatabaseSettings.log_queries to LogQueryLevel.OFF,
                    DodecaSettings.maxDatabases to DATABASES + 2,
                )
        StandaloneServer(root, all).use { server ->
            server.start()
            for (i in 1..DATABASES) server.administer("CREATE DATABASE db$i WAIT")
            val metrics = checkNotNull(server.metrics())
            repeat(SCRAPES) { metrics.scrape() }
            val times =
                (1..SCRAPES).map {
                    val start = System.nanoTime()
                    val response = metrics.scrape()
                    assertThat(response.statusCode()).isEqualTo(200)
                    (System.nanoTime() - start) / 1e6
                }
            val text = metrics.scrape().body().decodeToString()
            val series = text.lines().count { it.isNotEmpty() && !it.startsWith("#") }
            val gzipped = metrics.scrape(gzip = true).body().size
            println(
                "%d databases: %d series, %d kB (%d kB gzipped), median scrape %.1f ms, slowest %.1f ms"
                    .format(
                        DATABASES + 2,
                        series,
                        text.length / 1024,
                        gzipped / 1024,
                        times.sorted()[SCRAPES / 2],
                        times.max(),
                    )
            )
        }
    }

    private fun <T : Any> StandaloneServer.set(setting: Setting<T>, value: T) =
        database(SYSTEM)
            .dependencyResolver
            .resolveDependency(Config::class.java)
            .setDynamic(setting, value, "benchmark")

    /**
     * Auto-commit reads per second, from [clients] sessions at once; with one, each read's
     * nanoseconds go to [latencies].
     */
    private fun reads(driver: Driver, clients: Int, latencies: LongArray? = null): Double {
        val pool = Executors.newFixedThreadPool(clients)
        try {
            val start = System.nanoTime()
            val done =
                (1..clients).map {
                    pool.submit {
                        driver.session().use { session ->
                            repeat(READS) { i ->
                                val start = System.nanoTime()
                                session.run("RETURN \$i AS i", mapOf("i" to i)).consume()
                                latencies?.set(i, System.nanoTime() - start)
                            }
                        }
                    }
                }
            done.forEach { it.get() }
            return clients * READS / ((System.nanoTime() - start) / 1e9)
        } finally {
            pool.shutdown()
            pool.awaitTermination(1, TimeUnit.MINUTES)
        }
    }

    /** Small auto-commit write transactions per second, from one session. */
    private fun writes(driver: Driver): Double {
        driver.session().use { session ->
            val start = System.nanoTime()
            repeat(WRITES) { i ->
                session.run("CREATE (:Item {i: \$i})", mapOf("i" to i)).consume()
            }
            return WRITES / ((System.nanoTime() - start) / 1e9)
        }
    }

    private companion object {
        const val READS = 5_000
        const val WRITES = 1_000
        const val ROUNDS = 5
        const val DATABASES = 100
        const val SCRAPES = 20

        val LEVELS = listOf(LogQueryLevel.OFF, LogQueryLevel.INFO, LogQueryLevel.VERBOSE)

        val METRICS: Map<String, Map<Setting<*>, Any>> =
            mapOf(
                "off" to mapOf(MetricsSettings.enabled to false),
                "default" to emptyMap(),
                "all" to
                    mapOf(
                        MetricsSettings.prometheusEnabled to true,
                        MetricsSettings.prometheusEndpoint to SocketAddress("localhost", 0),
                        MetricsSettings.filter to listOf(GlobbingPattern("*")),
                    ),
            )
    }
}
