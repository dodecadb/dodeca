package org.dozendb.core.monitoring

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.neo4j.configuration.Config
import org.neo4j.logging.NullLog

class MetricRegistryTest {
    private val defaultFilter = Config.defaults().get(MetricsSettings.filter)

    private fun registry(filter: String? = null) =
        MetricRegistry(
            "neo4j",
            filter?.let {
                Config.newBuilder()
                    .setRaw(mapOf(MetricsSettings.filter.name() to it))
                    .build()
                    .get(MetricsSettings.filter)
            } ?: defaultFilter,
            NullLog.getInstance(),
        )

    @Test
    fun `Enterprise's default filter reports what its documentation lists, matched on full names`() {
        val registry = registry()
        val dbms = registry.dbms()
        val database = registry.database("films")

        assertThat(dbms.enabled("vm.heap.used")).isTrue()
        assertThat(dbms.enabled("vm.heap.max")).isFalse()
        assertThat(dbms.enabled("page_cache.hit_ratio")).isTrue()
        assertThat(dbms.enabled("page_cache.evictions")).isFalse()
        assertThat(dbms.enabled("bolt.connections_opened")).isTrue()
        assertThat(dbms.enabled("bolt.messages_done")).isFalse()
        assertThat(database.enabled("transaction.committed_read")).isTrue()
        assertThat(database.enabled("transaction.started")).isFalse()
        assertThat(database.enabled("count.node")).isFalse()
        assertThat(database.enabled("db.query.execution.latency.millis")).isTrue()
        assertThat(database.enabled("cluster.raft.is_leader")).isTrue()
        assertThat(database.enabled("pool.transaction.films.total_used")).isTrue()

        assertThat(dbms.counter("bolt.messages_done", "Left out.")).isNull()
        dbms.gauge("vm.heap.max", "Left out.") { 1 }
        assertThat(registry.metrics()).isEmpty()
    }

    @Test
    fun `patterns use star and question mark, other characters literally`() {
        val dbms = registry("neo4j.dbms.vm.?eap.*,*page_cache.hits").dbms()

        assertThat(dbms.enabled("vm.heap.used")).isTrue()
        assertThat(dbms.enabled("vm.heap")).isFalse()
        assertThat(dbms.enabled("page_cache.hits")).isTrue()
        assertThat(dbms.enabled("page_cache_hits")).isFalse()
        assertThat(registry("neo4j.vm.heap.used").dbms().enabled("vm.heap.used")).isFalse()
    }

    @Test
    fun `a group removes its metrics, and only its own, when closed`() {
        val registry = registry("*")
        val films = registry.database("films")
        val archive = registry.database("films.archive")
        films.gauge("store.size.total", "Bytes.") { 1 }
        archive.gauge("store.size.total", "Bytes.") { 2 }

        films.close()
        films.gauge("count.node", "After closing.") { 3 }

        assertThat(registry.metrics().map { it.name })
            .containsExactly("neo4j.database.films.archive.store.size.total")
        assertThat(registry.dropwizard.names)
            .containsExactly("neo4j.database.films.archive.store.size.total")
    }

    @Test
    fun `a name registered twice keeps the first metric`() {
        val registry = registry("*")
        registry.dbms().gauge("vm.threads", "First.") { 1 }

        assertThat(registry.dbms().counter("vm.threads", "Second.")).isNull()
        assertThat(registry.metric("neo4j.dbms.vm.threads")!!.description).isEqualTo("First.")
    }
}
