package org.dozendb.it

import java.nio.file.Files
import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.untilNotNull
import org.awaitility.kotlin.withPollInterval
import org.dozendb.consensus.testing.TestPorts
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestMethodOrder
import org.testcontainers.containers.Network
import org.testcontainers.lifecycle.Startables

/**
 * Every metric of a standalone server and of a three-member cluster, scraped from the Prometheus
 * endpoint and checked with Prometheus's own `promtool`; each database has one leader, also once
 * its leader is killed.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class MetricsIT {
    private val network = Network.newNetwork()
    private val aliases = listOf("core1", "core2", "core3")
    // Fixed host ports: Docker may publish a restarted container on other ports.
    private val members = aliases.map { alias ->
        DozenDbContainer(DozenDbContainer.dozenDbImage, alias)
            .inNetwork(network)
            .clusterMember(aliases.map { "$it:7700" })
            .boltOnHostPort(TestPorts.free())
            .prometheusOnHostPort(TestPorts.free())
    }
    private val standalone =
        DozenDbContainer(DozenDbContainer.dozenDbImage, "standalone")
            .prometheusOnHostPort(TestPorts.free())

    @BeforeAll
    fun start() {
        Startables.deepStart(members + standalone).join()
    }

    @AfterAll
    fun stop() {
        (members + standalone).forEach {
            it.saveLogs(DozenDbContainer.logsDirectory.resolve(javaClass.simpleName))
            it.stop()
        }
        network.close()
    }

    private fun eventually(assertion: () -> Unit) {
        await.ignoreExceptions() withPollInterval
            Duration.ofMillis(500) atMost
            Duration.ofSeconds(60) untilAsserted
            {
                assertion()
            }
    }

    /**
     * Asserts that promtool reads the metrics of [server] and finds no problem but those of
     * Enterprise's names. What it found is saved with the logs.
     */
    private fun assertPasses(server: DozenDbContainer, metrics: String) {
        val check = Promtool.check(metrics)
        val directory =
            Files.createDirectories(DozenDbContainer.logsDirectory.resolve(javaClass.simpleName))
        Files.write(directory.resolve("promtool-${server.alias}.txt"), check.problems)
        assertThat(check.exitCode).describedAs(check.problems.joinToString("\n")).isIn(0, 3)
        assertThat(check.problems).allSatisfy { problem ->
            assertThat(ENTERPRISE_NAMES.any { it.matches(problem) })
                .describedAs("a problem other than Enterprise's names: $problem")
                .isTrue()
        }
    }

    private fun DozenDbContainer.isLeader(database: String): Double? =
        samples()["neo4j_database_${database}_cluster_raft_is_leader"]

    @Test
    @Order(1)
    fun `a standalone server reports Enterprise's metrics in a format Prometheus reads`() {
        standalone.rows("CREATE (:Movie {title: 'Alien'})")

        val metrics = standalone.scrape()

        assertPasses(standalone, metrics)
        val samples = Promtool.samples(metrics)
        assertThat(samples["neo4j_dbms_vm_heap_used"]).isPositive()
        assertThat(samples["neo4j_database_neo4j_transaction_committed_total"]).isPositive()
        assertThat(samples).containsKey("neo4j_dbms_page_cache_hit_ratio")
        assertThat(samples.keys.filter { "cluster" in it }).isEmpty()
    }

    @Test
    @Order(2)
    fun `each member reports its Raft progress, and one member leads each database`() {
        eventually {
            for (database in DATABASES) {
                assertThat(members.sumOf { it.isLeader(database)!! })
                    .describedAs("leaders of $database")
                    .isEqualTo(1.0)
            }
        }
        for (member in members) {
            val metrics = member.scrape()
            assertPasses(member, metrics)
            assertThat(Promtool.samples(metrics)["neo4j_dbms_dozendb_cluster_members"])
                .isEqualTo(3.0)
        }
    }

    @Test
    @Order(3)
    fun `another member leads once the leader is killed, and the leader rejoins`() {
        val leader =
            await withPollInterval
                Duration.ofMillis(500) atMost
                Duration.ofSeconds(60) untilNotNull
                {
                    members.singleOrNull { it.isLeader("neo4j") == 1.0 }
                }
        val others = members - leader

        leader.kill()

        eventually {
            for (database in DATABASES) {
                assertThat(others.sumOf { it.isLeader(database)!! })
                    .describedAs("leaders of $database")
                    .isEqualTo(1.0)
            }
        }
        leader.restart()
        eventually {
            for (database in DATABASES) {
                assertThat(members.sumOf { it.isLeader(database)!! })
                    .describedAs("leaders of $database")
                    .isEqualTo(1.0)
                assertThat(
                        leader
                            .samples()[
                                "neo4j_database_${database}_cluster_raft_last_leader_message"]
                    )
                    .isBetween(0.0, 5_000.0)
            }
        }
    }

    companion object {
        private val DATABASES = listOf("system", "neo4j")

        /**
         * What promtool finds wrong with names Neo4j Enterprise gave its metrics, which DozenDB
         * keeps: gauges whose names end like a counter's (`store.size.total`) or a summary's
         * (`vm.file.descriptors.count`, `vm.memory.buffer.<pool>.count`).
         */
        private val ENTERPRISE_NAMES =
            listOf(
                Regex(
                    """neo4j_dbms_vm_(file_descriptors|memory_buffer_\w+)_count non-histogram and """ +
                        """non-summary metrics should not have "_count" suffix"""
                )
            )
    }
}
