package dev.dodeca.core.monitoring

import dev.dodeca.core.StandaloneServer
import dev.dodeca.core.StandaloneServer.Companion.SYSTEM
import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.concurrent.thread
import kotlin.io.path.readText
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.configuration.helpers.GlobbingPattern
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.driver.GraphDatabase
import org.neo4j.wal.checkpoint.CheckPointer
import org.neo4j.wal.checkpoint.SimpleTriggerInfo

/** The metrics of each source move with what they measure. */
class MetricSourcesTest {
    @TempDir lateinit var root: Path

    /** A server reporting every metric, with Bolt on. */
    private fun server() =
        StandaloneServer(
            root,
            mapOf(
                MetricsSettings.prometheusEnabled to true,
                MetricsSettings.prometheusEndpoint to SocketAddress("localhost", 0),
                MetricsSettings.filter to listOf(GlobbingPattern("*")),
                BoltConnector.enabled to true,
            ),
        )

    private fun eventually(assertion: () -> Unit) {
        await.ignoreExceptions() atMost Duration.ofSeconds(30) untilAsserted { assertion() }
    }

    @Test
    fun `DBMS metrics count Bolt connections and messages, the page cache and databases`() {
        server().use { server ->
            server.start()
            fun boltConnections() =
                server.rows("CALL dbms.listConnections() YIELD connector", SYSTEM).count {
                    it["connector"] == "bolt"
                }
            fun connections(running: Double, idle: Double) {
                val samples = server.samples()
                assertThat(samples["neo4j_dbms_bolt_connections_running"]).isEqualTo(running)
                assertThat(samples["neo4j_dbms_bolt_connections_idle"]).isEqualTo(idle)
                assertThat(
                        samples["neo4j_dbms_bolt_connections_opened_total"]!! -
                            samples["neo4j_dbms_bolt_connections_closed_total"]!!
                    )
                    .isEqualTo(boltConnections().toDouble())
            }
            GraphDatabase.driver("bolt://${server.boltAddress()}").use { driver ->
                // A worker thread stays bound to a connection while its transaction is open.
                val first = driver.session().beginTransaction()
                val second = driver.session().beginTransaction()
                first.run("RETURN 1").consume()
                second.run("CREATE (:Movie)").consume()
                eventually { connections(running = 2.0, idle = 0.0) }

                first.commit()
                second.commit()
                eventually { connections(running = 0.0, idle = 2.0) }

                val samples = server.samples()
                assertThat(samples["neo4j_dbms_bolt_messages_received_total"]).isGreaterThan(4.0)
                assertThat(samples["neo4j_dbms_bolt_messages_started_total"]).isGreaterThan(4.0)
                assertThat(samples["neo4j_dbms_bolt_messages_done_total"]).isGreaterThan(4.0)
                assertThat(samples["neo4j_dbms_bolt_messages_failed_total"]).isEqualTo(0.0)
                assertThat(samples["neo4j_dbms_bolt_worker_thread_bound_time_count"])
                    .isGreaterThanOrEqualTo(2.0)
                assertThat(samples["neo4j_dbms_routing_query_count_local_total"])
                    .isGreaterThanOrEqualTo(2.0)
                assertThat(samples)
                    .containsKeys(
                        "neo4j_dbms_pool_bolt_total_used",
                        "neo4j_dbms_pool_transaction_used_heap",
                        "neo4j_dbms_pool_other_free",
                    )
            }
            eventually { connections(running = 0.0, idle = 0.0) }

            val samples = server.samples()
            assertThat(samples["neo4j_dbms_page_cache_hits_total"]).isGreaterThan(0.0)
            assertThat(samples["neo4j_dbms_page_cache_page_faults_total"]).isGreaterThan(0.0)
            assertThat(samples["neo4j_dbms_page_cache_hit_ratio"]).isBetween(0.0, 1.0)
            assertThat(samples["neo4j_dbms_page_cache_usage_ratio"]).isStrictlyBetween(0.0, 1.0)
            assertThat(samples["neo4j_dbms_db_operation_count_start_total"]).isEqualTo(2.0)
            assertThat(samples["neo4j_dbms_db_operation_count_failed_total"]).isEqualTo(0.0)
            assertThat(samples["neo4j_dbms_db_state_count_hosted"]).isEqualTo(2.0)
            assertThat(samples["neo4j_dbms_db_state_count_failed"]).isEqualTo(0.0)
            assertThat(samples["neo4j_dbms_db_state_count_desired_started"]).isEqualTo(2.0)
            assertThat(samples.keys.filter { it.startsWith("neo4j_dbms_vm_gc_count_") })
                .isNotEmpty()
        }
    }

    @Test
    fun `database metrics follow the database's counters`() {
        server().use { server ->
            server.start()
            val database = server.database()
            val before = server.samples()

            database.executeTransactionally("CREATE (:Movie), (:Movie), (:Person)")
            database.executeTransactionally("CREATE RANGE INDEX FOR (m:Movie) ON (m.title)")
            database.executeTransactionally("CALL db.awaitIndexes()")
            repeat(2) { database.executeTransactionally("MATCH (m:Movie) RETURN count(m)") }
            runCatching { database.executeTransactionally("MATCH (m:Movie) RETURN x") }
            database.dependencyResolver
                .resolveDependency(CheckPointer::class.java)
                .forceCheckPoint(SimpleTriggerInfo("test"))

            val after = server.samples()
            fun moved(name: String): Double =
                after["neo4j_database_neo4j_$name"]!! -
                    (before["neo4j_database_neo4j_$name"] ?: 0.0)
            // Each new label and property key commits in a transaction of its own too.
            val writes = moved("transaction_committed_write_total")
            assertThat(writes).isGreaterThanOrEqualTo(2.0)
            assertThat(moved("transaction_last_committed_tx_id_total")).isEqualTo(writes)
            // Every committed transaction reports its size, reads too.
            assertThat(moved("transaction_tx_size_heap_count")).isGreaterThanOrEqualTo(writes)
            assertThat(after["neo4j_database_neo4j_transaction_active"]).isEqualTo(0.0)
            assertThat(moved("check_point_events_total")).isEqualTo(1.0)
            assertThat(after["neo4j_database_neo4j_check_point_pages_flushed"]).isGreaterThan(0.0)
            assertThat(moved("log_appended_bytes_total")).isGreaterThan(0.0)
            assertThat(after["neo4j_database_neo4j_neo4j_count_node"]).isEqualTo(3.0)
            assertThat(after["neo4j_database_neo4j_ids_in_use_node"]).isEqualTo(3.0)
            assertThat(after["neo4j_database_neo4j_store_size_full"]).isGreaterThan(0.0)
            assertThat(moved("index_range_populated_total")).isEqualTo(1.0)
            assertThat(moved("cypher_cache_executable_query_hits_total"))
                .isGreaterThanOrEqualTo(1.0)
            assertThat(after["neo4j_database_neo4j_cypher_cache_executable_query_entries"])
                .isGreaterThan(0.0)
            assertThat(moved("db_query_execution_success_total")).isGreaterThanOrEqualTo(5.0)
            assertThat(moved("db_query_execution_slotted_success_total")).isGreaterThan(0.0)
            assertThat(moved("db_query_execution_failure_total")).isEqualTo(1.0)
            assertThat(moved("db_query_execution_latency_millis_count"))
                .isEqualTo(moved("db_query_execution_success_total"))
            assertThat(after)
                .containsKeys(
                    "neo4j_database_neo4j_pool_transaction_neo4j_used_heap",
                    "neo4j_database_system_transaction_committed_total",
                    "neo4j_database_neo4j_page_cache_warmup_pages_loaded_total",
                    "neo4j_database_neo4j_page_cache_warmup_duration",
                )
        }
    }

    @Test
    fun `a database's metrics come and go with it, while Prometheus scrapes`() {
        server().use { server ->
            server.start()
            val scraping = AtomicBoolean(true)
            val failure = AtomicReference<Throwable>()
            val scraper = thread {
                try {
                    while (scraping.get()) {
                        assertThat(server.scrape().statusCode()).isEqualTo(200)
                    }
                } catch (e: Throwable) {
                    failure.set(e)
                }
            }
            val committed = "neo4j_database_films_transaction_committed_total"
            try {
                server.administer("CREATE DATABASE films WAIT")
                assertThat(server.samples()).containsKey(committed)
                server.administer("STOP DATABASE films WAIT")
                assertThat(server.samples()).doesNotContainKey(committed)
                server.administer("START DATABASE films WAIT")
                assertThat(server.samples()).containsKey(committed)
                assertThat(server.samples()["neo4j_dbms_db_state_count_hosted"]).isEqualTo(3.0)
                server.administer("DROP DATABASE films WAIT")
                assertThat(server.samples().keys.filter { "_films_" in it }).isEmpty()
                assertThat(server.metrics()!!.registry.dropwizard.names.filter { ".films." in it })
                    .isEmpty()
            } finally {
                scraping.set(false)
                scraper.join()
            }
            assertThat(failure.get()).isNull()
            assertThat(server.home.resolve("logs/debug.log").readText())
                .doesNotContain("Error while handling database event")
                .doesNotContain("is registered already")
        }
    }
}
