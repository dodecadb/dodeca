package org.dozendb.core.monitoring

import java.nio.file.Path
import java.time.Duration
import org.neo4j.configuration.Description
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.SettingConstraints.min
import org.neo4j.configuration.SettingConstraints.range
import org.neo4j.configuration.SettingImpl.newBuilder
import org.neo4j.configuration.SettingValueParsers.BOOL
import org.neo4j.configuration.SettingValueParsers.BYTES
import org.neo4j.configuration.SettingValueParsers.DURATION
import org.neo4j.configuration.SettingValueParsers.GLOBBING_PATTERN
import org.neo4j.configuration.SettingValueParsers.INT
import org.neo4j.configuration.SettingValueParsers.PATH
import org.neo4j.configuration.SettingValueParsers.SOCKET_ADDRESS
import org.neo4j.configuration.SettingValueParsers.STRING
import org.neo4j.configuration.SettingValueParsers.listOf
import org.neo4j.configuration.SettingValueParsers.ofEnum
import org.neo4j.configuration.SettingsDeclaration
import org.neo4j.configuration.helpers.GlobbingPattern
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.graphdb.config.Setting
import org.neo4j.io.ByteUnit

/**
 * Neo4j Enterprise's metrics settings, with Enterprise's names and defaults, for its exporters: CSV
 * files, Graphite, Prometheus and JMX. Registered in `META-INF/services`.
 */
class MetricsSettings : SettingsDeclaration {
    companion object {
        /**
         * Enterprise's default filter, verbatim from its settings reference of 2026.10, which
         * changed it in 2025.03 and 2025.06.
         */
        const val DEFAULT_FILTER =
            "*bolt.connections*,*bolt.messages_received*,*bolt.messages_started*," +
                "*dbms.pool.bolt.free,*dbms.pool.bolt.total_size,*dbms.pool.bolt.total_used," +
                "*dbms.pool.bolt.used_heap,*cluster.raft.is_leader," +
                "*cluster.raft.last_leader_message,*cluster.raft.replication_attempt," +
                "*cluster.raft.replication_fail,*cluster.raft.last_applied," +
                "*cluster.raft.last_appended,*cluster.raft.append_index," +
                "*cluster.raft.commit_index,*cluster.raft.applied_index,*check_point.*," +
                "*cypher.replan_events,*cypher.cache*,*ids_in_use*,*.neo4j.count.*," +
                "*pool.transaction.*.total_used,*pool.transaction.*.used_heap," +
                "*pool.transaction.*.used_native,*store.size*,*transaction.active_read," +
                "*transaction.active_write,*transaction.committed*," +
                "*transaction.last_committed_tx_id,*transaction.peak_concurrent," +
                "*transaction.rollbacks*,*page_cache.hit*,*page_cache.page_faults," +
                "*page_cache.usage_ratio,*vm.file.descriptors.count,*vm.gc.time.*,*vm.heap.used," +
                "*vm.memory.buffer.direct.used,*vm.memory.pool.g1_eden_space," +
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
        @field:Description(
            "Writes the metrics to CSV files in server.directories.metrics: a file per metric, " +
                "named after it, with a line every server.metrics.csv.interval."
        )
        val csvEnabled: Setting<Boolean> =
            newBuilder("server.metrics.csv.enabled", BOOL, true).build()

        @JvmField
        @field:Description("How often a line is written to each CSV file.")
        val csvInterval: Setting<Duration> =
            newBuilder("server.metrics.csv.interval", DURATION, Duration.ofSeconds(30))
                .addConstraint(min(Duration.ofMillis(1)))
                .build()

        @JvmField
        @field:Description(
            "The size at which a CSV file is rotated: renamed `<name>.csv.1`, after shifting the " +
                "older ones up, and started anew. 0 never rotates."
        )
        val csvRotationSize: Setting<Long> =
            newBuilder("server.metrics.csv.rotation.size", BYTES, ByteUnit.mebiBytes(10))
                .addConstraint(range(0L, Long.MAX_VALUE))
                .build()

        @JvmField
        @field:Description("How many rotated files each CSV file keeps.")
        val csvRotationKeep: Setting<Int> =
            newBuilder("server.metrics.csv.rotation.keep_number", INT, 7)
                .addConstraint(min(1))
                .build()

        @JvmField
        @field:Description(
            "Compresses rotated CSV files: `NONE`, `GZ` (`<name>.csv.1.gz`) or `ZIP` " +
                "(`<name>.csv.1.zip`), Enterprise's default since 2025.01."
        )
        val csvRotationCompression: Setting<CsvCompression> =
            newBuilder(
                    "server.metrics.csv.rotation.compression",
                    ofEnum(CsvCompression::class.java),
                    CsvCompression.ZIP,
                )
                .build()

        @JvmField
        @field:Description(
            "The directory of the CSV metrics files. A relative path is relative to the server's " +
                "home directory."
        )
        val directory: Setting<Path> =
            newBuilder("server.directories.metrics", PATH, Path.of("metrics"))
                .setDependency(GraphDatabaseSettings.neo4j_home)
                .immutable()
                .build()

        @JvmField
        @field:Description(
            "Sends the metrics to Graphite at server.metrics.graphite.server every " +
                "server.metrics.graphite.interval, in Graphite's plaintext protocol over TCP."
        )
        val graphiteEnabled: Setting<Boolean> =
            newBuilder("server.metrics.graphite.enabled", BOOL, false).build()

        @JvmField
        @field:Description(
            "The address of the Graphite server. A port without a host uses " +
                "server.default_listen_address. The JVM caches the address a host name " +
                "resolves to, as `networkaddress.cache.ttl` says."
        )
        val graphiteServer: Setting<SocketAddress> =
            newBuilder("server.metrics.graphite.server", SOCKET_ADDRESS, SocketAddress(2003))
                .setDependency(GraphDatabaseSettings.default_listen_address)
                .build()

        @JvmField
        @field:Description("How often the metrics are sent to Graphite.")
        val graphiteInterval: Setting<Duration> =
            newBuilder("server.metrics.graphite.interval", DURATION, Duration.ofSeconds(30))
                .addConstraint(min(Duration.ofMillis(1)))
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

/** How rotated CSV metrics files are compressed. */
enum class CsvCompression {
    NONE,
    ZIP,
    GZ,
}
