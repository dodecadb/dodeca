package org.dozendb.cluster.core.internal

import java.nio.file.Path
import java.time.Duration
import org.neo4j.configuration.Description
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.SettingConstraints.NO_ALL_INTERFACES_ADDRESS
import org.neo4j.configuration.SettingConstraints.min
import org.neo4j.configuration.SettingImpl.newBuilder
import org.neo4j.configuration.SettingValueParsers.BOOL
import org.neo4j.configuration.SettingValueParsers.DURATION
import org.neo4j.configuration.SettingValueParsers.PATH
import org.neo4j.configuration.SettingValueParsers.SOCKET_ADDRESS
import org.neo4j.configuration.SettingValueParsers.listOf
import org.neo4j.configuration.SettingValueParsers.ofEnum
import org.neo4j.configuration.SettingsDeclaration
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.graphdb.config.Setting

/**
 * DozenDB settings. Declared even when clustering is disabled, so that a configuration that names
 * them passes Neo4j's strict validation. Registered in `META-INF/services`.
 *
 * Neo4j only accepts setting names in a fixed set of namespaces. Following Neo4j's convention,
 * settings that must agree across the cluster live under `dbms.dozendb.`, settings specific to one
 * server under `server.dozendb.`.
 */
class DozenDbSettings : SettingsDeclaration {
    companion object {
        const val DEFAULT_RAFT_PORT = 7700

        @JvmField
        @field:Description(
            "Enables DozenDB clustering. When false, the plugin behaves exactly like DozerDB."
        )
        val clusterEnabled: Setting<Boolean> =
            newBuilder("dbms.dozendb.cluster.enabled", BOOL, false).immutable().build()

        @JvmField
        @field:Description(
            "Address the Raft server binds to. A port without a host uses " +
                "server.default_listen_address."
        )
        val listenAddress: Setting<SocketAddress> =
            newBuilder(
                    "server.dozendb.cluster.listen_address",
                    SOCKET_ADDRESS,
                    SocketAddress(DEFAULT_RAFT_PORT),
                )
                .setDependency(GraphDatabaseSettings.default_listen_address)
                .immutable()
                .build()

        @JvmField
        @field:Description(
            "Address other members use to reach this member's Raft server. A port without a " +
                "host uses server.default_advertised_address."
        )
        val advertisedAddress: Setting<SocketAddress> =
            newBuilder(
                    "server.dozendb.cluster.advertised_address",
                    SOCKET_ADDRESS,
                    SocketAddress(DEFAULT_RAFT_PORT),
                )
                .addConstraint(NO_ALL_INTERFACES_ADDRESS)
                .setDependency(GraphDatabaseSettings.default_advertised_address)
                .immutable()
                .build()

        @JvmField
        @field:Description(
            "Advertised Raft addresses of all members that form the cluster for the first time, " +
                "including this one. Only read by a member that has no Raft state yet."
        )
        val initialMembers: Setting<List<SocketAddress>> =
            newBuilder("dbms.dozendb.cluster.initial_members", listOf(SOCKET_ADDRESS), emptyList())
                .immutable()
                .build()

        @JvmField
        @field:Description(
            "Directory of the Raft logs, one subdirectory per database. Relative paths are " +
                "resolved against server.directories.data."
        )
        val raftDirectory: Setting<Path> =
            newBuilder("server.dozendb.cluster.raft_directory", PATH, Path.of("raft"))
                .setDependency(GraphDatabaseSettings.data_directory)
                .immutable()
                .build()

        @JvmField
        @field:Description(
            "Whether commits to replicated databases force only the Raft log to disk. Neo4j's " +
                "transaction log is then forced before each checkpoint instead of at every " +
                "commit; after a crash, the Raft log restores the transactions it lost. When " +
                "false, every commit forces both logs."
        )
        val singleFsync: Setting<Boolean> =
            newBuilder("server.dozendb.cluster.single_fsync", BOOL, true).immutable().build()

        @JvmField
        @field:Description(
            "A follower that hears nothing from the leader for a random time between this " +
                "and dbms.dozendb.cluster.election_timeout_max starts an election."
        )
        val electionTimeoutMin: Setting<Duration> =
            newBuilder("dbms.dozendb.cluster.election_timeout_min", DURATION, Duration.ofSeconds(1))
                .addConstraint(min(Duration.ofMillis(100)))
                .immutable()
                .build()

        @JvmField
        @field:Description(
            "Upper bound of the election timeout. Must be greater than " +
                "dbms.dozendb.cluster.election_timeout_min."
        )
        val electionTimeoutMax: Setting<Duration> =
            newBuilder("dbms.dozendb.cluster.election_timeout_max", DURATION, Duration.ofSeconds(2))
                .addConstraint(min(Duration.ofMillis(200)))
                .immutable()
                .build()

        @JvmField
        @field:Description(
            "Whether routing tables list the leader as a reader too. Without it the leader takes " +
                "only writes, unless it is the only member."
        )
        val readsOnLeader: Setting<Boolean> =
            newBuilder("dbms.dozendb.routing.reads_on_leader", BOOL, true).immutable().build()

        @JvmField
        @field:Description(
            "Read consistency of transactions that do not set the dozendb.consistency metadata " +
                "key. CAUSAL reads wait for the transaction's bookmarks; LINEARIZABLE reads also " +
                "see every write acknowledged before they started, at the cost of a round trip to " +
                "the leader."
        )
        val defaultReadConsistency: Setting<ReadConsistency> =
            newBuilder(
                    "dbms.dozendb.reads.default_consistency",
                    ofEnum(ReadConsistency::class.java),
                    ReadConsistency.CAUSAL,
                )
                .immutable()
                .build()
    }
}
