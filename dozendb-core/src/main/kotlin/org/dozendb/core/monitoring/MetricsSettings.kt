package org.dozendb.core.monitoring

import org.neo4j.configuration.Description
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.SettingImpl.newBuilder
import org.neo4j.configuration.SettingValueParsers.BOOL
import org.neo4j.configuration.SettingValueParsers.GLOBBING_PATTERN
import org.neo4j.configuration.SettingValueParsers.SOCKET_ADDRESS
import org.neo4j.configuration.SettingValueParsers.STRING
import org.neo4j.configuration.SettingValueParsers.listOf
import org.neo4j.configuration.SettingsDeclaration
import org.neo4j.configuration.helpers.GlobbingPattern
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.graphdb.config.Setting

/**
 * Neo4j Enterprise's metrics settings, with Enterprise's names and defaults, for the exporters
 * DozenDB implements: Prometheus and JMX. The CSV and Graphite settings stay undeclared, so that
 * Neo4j's strict validation refuses a configuration that names them instead of DozenDB ignoring
 * them. Registered in `META-INF/services`.
 */
class MetricsSettings : SettingsDeclaration {
    companion object {
        /** Enterprise's default filter, verbatim from its settings reference. */
        const val DEFAULT_FILTER =
            "*bolt.connections*,*bolt.messages_received*,*bolt.messages_started*," +
                "*dbms.pool.bolt.free,*dbms.pool.bolt.total_size,*dbms.pool.bolt.total_used," +
                "*dbms.pool.bolt.used_heap,*cluster.raft.is_leader," +
                "*cluster.raft.last_leader_message,*cluster.raft.replication_attempt," +
                "*cluster.raft.replication_fail,*cluster.raft.last_applied," +
                "*cluster.raft.last_appended,*cluster.raft.append_index," +
                "*cluster.raft.commit_index,*cluster.raft.applied_index," +
                "*cluster.internal.discovery.memberset.left," +
                "*cluster.internal.discovery.crdt.gossip_id_data.size," +
                "*cluster.internal.discovery.crdt.server_data.size," +
                "*cluster.internal.discovery.crdt.database_data.size," +
                "*cluster.internal.discovery.crdt.leader_data.size," +
                "*cluster.internal.discovery.crdt.total_merge_operations," +
                "*cluster.internal.discovery.crdt.total_update_operations," +
                "*cluster.internal.discovery.gossip.incoming_queue_size," +
                "*cluster.internal.discovery.gossip.total_received_data," +
                "*cluster.internal.discovery.gossip.total_sent_data," +
                "*cluster.internal.discovery.gossip.uncontactable_members_exist,*check_point.*," +
                "*cypher.replan_events,*cypher.cache*,*ids_in_use*," +
                "*pool.transaction.*.total_used,*pool.transaction.*.used_heap," +
                "*pool.transaction.*.used_native,*store.size*,*transaction.active_read," +
                "*transaction.active_write,*transaction.committed*," +
                "*transaction.last_committed_tx_id,*transaction.peak_concurrent," +
                "*transaction.rollbacks*,*page_cache.hit*,*page_cache.page_faults," +
                "*page_cache.usage_ratio,*vm.file.descriptors.count,*vm.gc.time.*," +
                "*vm.heap.used,*vm.memory.buffer.direct.used,*vm.memory.pool.g1_eden_space," +
                "*vm.memory.pool.g1_old_gen,*vm.pause_time,*vm.thread*,*db.query.execution*," +
                "*protocol*"

        @JvmField
        @field:Description("Enables metrics. `false` turns off every metric and exporter.")
        val enabled: Setting<Boolean> = newBuilder("server.metrics.enabled", BOOL, true).build()

        @JvmField
        @field:Description("The prefix of every metric name.")
        val prefix: Setting<String> = newBuilder("server.metrics.prefix", STRING, "neo4j").build()

        @JvmField
        @field:Description(
            "The metrics reported: a comma-separated list of globbing patterns, with `*` and " +
                "`?`, matched against the full metric name, prefix included. For example " +
                "`*check_point*,neo4j.dbms.page_cache.evictions` reports the checkpoint metrics " +
                "and page cache evictions."
        )
        val filter: Setting<List<GlobbingPattern>> =
            newBuilder(
                    "server.metrics.filter",
                    listOf(GLOBBING_PATTERN),
                    GlobbingPattern.create(*DEFAULT_FILTER.split(',').toTypedArray()),
                )
                .build()

        @JvmField
        @field:Description("Publishes the metrics as JMX MBeans, in the domain `neo4j.metrics`.")
        val jmxEnabled: Setting<Boolean> =
            newBuilder("server.metrics.jmx.enabled", BOOL, true).build()

        @JvmField
        @field:Description(
            "Serves the metrics in Prometheus' text format over HTTP, without TLS or " +
                "authentication, at server.metrics.prometheus.endpoint."
        )
        val prometheusEnabled: Setting<Boolean> =
            newBuilder("server.metrics.prometheus.enabled", BOOL, false).build()

        @JvmField
        @field:Description(
            "The address the Prometheus endpoint binds to. A port without a host uses " +
                "server.default_listen_address."
        )
        val prometheusEndpoint: Setting<SocketAddress> =
            newBuilder(
                    "server.metrics.prometheus.endpoint",
                    SOCKET_ADDRESS,
                    SocketAddress("localhost", 2004),
                )
                .setDependency(GraphDatabaseSettings.default_listen_address)
                .build()
    }
}
