package org.dozendb.cluster.internal

import java.nio.file.Path
import java.time.Duration
import org.dozendb.cluster.FormatLevels
import org.dozendb.cluster.internal.leadership.BalancingStrategy
import org.dozendb.consensus.ratis.RatisConsensusConfig
import org.dozendb.core.servers.UpstreamRules
import org.dozendb.core.servers.UpstreamSelection
import org.neo4j.configuration.Description
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.Internal
import org.neo4j.configuration.SettingConstraint
import org.neo4j.configuration.SettingConstraints.NO_ALL_INTERFACES_ADDRESS
import org.neo4j.configuration.SettingConstraints.min
import org.neo4j.configuration.SettingConstraints.range
import org.neo4j.configuration.SettingImpl.newBuilder
import org.neo4j.configuration.SettingValueParsers.BOOL
import org.neo4j.configuration.SettingValueParsers.BYTES
import org.neo4j.configuration.SettingValueParsers.DURATION
import org.neo4j.configuration.SettingValueParsers.INT
import org.neo4j.configuration.SettingValueParsers.PATH
import org.neo4j.configuration.SettingValueParsers.SOCKET_ADDRESS
import org.neo4j.configuration.SettingValueParsers.STRING
import org.neo4j.configuration.SettingValueParsers.listOf
import org.neo4j.configuration.SettingValueParsers.ofEnum
import org.neo4j.configuration.SettingsDeclaration
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.graphdb.config.Configuration
import org.neo4j.graphdb.config.Setting

/**
 * DozenDB's clustering settings, which `DozenDbSettings.clusterEnabled` enables. Declared even when
 * clustering is disabled, so that a configuration that names them passes Neo4j's strict validation.
 * Registered in `META-INF/services`.
 *
 * Neo4j only accepts setting names in a fixed set of namespaces. Following Neo4j's convention,
 * settings that must agree across the cluster live under `dbms.dozendb.`, settings specific to one
 * server under `server.dozendb.`, and undocumented settings for tests under `internal.`. Settings
 * that Neo4j Enterprise documents for the same purpose keep Enterprise's names.
 */
class ClusterSettings : SettingsDeclaration {
    companion object {
        const val DEFAULT_RAFT_PORT = 7700

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
            "Whether routing tables list the primaries that are not the writer as readers, or " +
                "only the secondaries. Without secondaries that can read a database, its " +
                "primaries are readers whatever this says. Server-side routing reads where " +
                "routing tables do."
        )
        val readsOnPrimaries: Setting<Boolean> =
            newBuilder("dbms.routing.reads_on_primaries_enabled", BOOL, true).immutable().build()

        @JvmField
        @field:Description(
            "Address other members use to reach this server's routing connector " +
                "(server.routing.listen_address), to forward the writes of databases this " +
                "member leads. A port without a host uses server.default_advertised_address."
        )
        val routingAdvertisedAddress: Setting<SocketAddress> =
            newBuilder(
                    "server.routing.advertised_address",
                    SOCKET_ADDRESS,
                    SocketAddress(GraphDatabaseSettings.DEFAULT_ROUTING_CONNECTOR_PORT),
                )
                .addConstraint(NO_ALL_INTERFACES_ADDRESS)
                .setDependency(GraphDatabaseSettings.default_advertised_address)
                .immutable()
                .build()

        @JvmField
        @field:Description(
            "Whether a member that does not lead a database forwards write transactions of " +
                "bolt:// connections to the database's leader, as it does for neo4j:// " +
                "connections and the HTTP APIs. Drivers open transactions in WRITE mode unless " +
                "asked for READ, so most bolt:// transactions on a follower then run on the " +
                "leader, reads included."
        )
        val forwardBoltWrites: Setting<Boolean> =
            newBuilder("dbms.dozendb.routing.forward_bolt_writes", BOOL, false).dynamic().build()

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

        @JvmField
        @field:Description(
            "How members move the leadership of databases on their own. EQUAL_BALANCING moves " +
                "leaders so that every member leads as many databases as the others, give or " +
                "take one; NO_BALANCING leaves leaders where elections put them. Either way, a " +
                "member hands over the leadership of a database that is read-only on it, and " +
                "hands its leaderships to the members leading the fewest databases when it stops."
        )
        val balancingStrategy: Setting<BalancingStrategy> =
            newBuilder(
                    "dbms.cluster.raft.leader_transfer.balancing_strategy",
                    ofEnum(BalancingStrategy::class.java),
                    BalancingStrategy.EQUAL_BALANCING,
                )
                .dynamic()
                .build()

        @JvmField
        @field:Description(
            "The server tag of the members that should lead every database without a priority " +
                "tag of its own (db.cluster.raft.leader_transfer.priority_tag.<database>). A " +
                "member that leads such a database without the tag hands the leadership to one " +
                "with it, when one can take it over; balancing leaves these databases where they " +
                "are. Set it alike on every member. Named like Neo4j Enterprise's setting."
        )
        val leaderPriorityTag: Setting<String> =
            newBuilder(DatabasePriorityTag.PREFIX, STRING, null).build()

        @JvmField
        @field:Description(
            "Whether a server that joins the cluster is enabled at once, so that databases are " +
                "placed on it, instead of staying Free until ENABLE SERVER. Taken at the " +
                "cluster's first start; dbms.cluster.setAutomaticallyEnableFreeServers changes " +
                "it later. Named like Neo4j Enterprise's setting."
        )
        val autoEnableFreeServers: Setting<Boolean> =
            newBuilder("initial.dbms.automatically_enable_free_servers", BOOL, false).build()

        @JvmField
        @field:Description(
            "The number of primaries a new database has when CREATE DATABASE gives no TOPOLOGY, " +
                "and the default database of a new cluster at most. Taken at the cluster's first " +
                "start; dbms.setDefaultAllocationNumbers changes it later. Raise it for " +
                "fault tolerance: with one primary, a database has a single copy. Named like Neo4j " +
                "Enterprise's setting."
        )
        val defaultPrimaries: Setting<Int> =
            newBuilder("initial.dbms.default_primaries_count", INT, 1)
                .addConstraint(range(1, 11))
                .build()

        @JvmField
        @field:Description(
            "The number of secondaries a new database has when CREATE DATABASE gives no number of " +
                "secondaries, and the default database of a new cluster at most. Taken at the " +
                "cluster's first start; dbms.setDefaultAllocationNumbers changes it later. Named " +
                "like Neo4j Enterprise's setting."
        )
        val defaultSecondaries: Setting<Int> =
            newBuilder("initial.dbms.default_secondaries_count", INT, 0)
                .addConstraint(range(0, 20))
                .build()

        @JvmField
        @field:Description(
            "How often a secondary asks its upstream for the transactions it lacks. While an " +
                "answer brings as many as one pull carries, it asks again at once; a read that " +
                "waits for a transaction the secondary lacks asks at once too. Named like Neo4j " +
                "Enterprise's setting."
        )
        val pullInterval: Setting<Duration> =
            newBuilder("db.cluster.catchup.pull_interval", DURATION, Duration.ofSeconds(1))
                .addConstraint(min(Duration.ofMillis(1)))
                .build()

        @JvmField
        @field:Description(
            "The strategies a secondary uses to choose the server it pulls from, asked in order: " +
                "the first that finds one wins, chosen again for every pull. Empty means " +
                "typically-connect-to-random-secondary; a random primary is the last resort. " +
                "connect-to-random-primary-server, typically-connect-to-random-secondary (a " +
                "secondary, one pull in ten a primary), connect-randomly-to-server-tags (a " +
                "secondary with a tag of server.cluster.catchup.connect_randomly_to_server_tags), " +
                "connect-randomly-within-server-tags (a secondary with a tag of this server's), " +
                "leader-only, user-defined (server.cluster.catchup.user_defined_upstream_strategy), " +
                "and the deprecated connect-randomly-to-server-group and " +
                "connect-randomly-within-server-group. Named like Neo4j Enterprise's setting."
        )
        val upstreamStrategy: Setting<List<String>> =
            newBuilder("server.cluster.catchup.upstream_strategy", listOf(STRING), emptyList())
                .addConstraint(
                    object : SettingConstraint<List<String>>() {
                        override fun validate(value: List<String>, config: Configuration) {
                            val unknown = UpstreamSelection.unknown(value)
                            require(unknown.isEmpty()) {
                                "unknown upstream strategies $unknown; the strategies are " +
                                    UpstreamSelection.NAMES.joinToString()
                            }
                        }

                        override fun getDescription() =
                            "Each one of ${UpstreamSelection.NAMES.joinToString()}."
                    }
                )
                .build()

        @JvmField
        @field:Description(
            "The rules of the user-defined upstream strategy: rules separated by ';', asked in " +
                "order, the first that passes a server wins. A rule chains filters with '->', " +
                "from every server that runs the database: tags(a, b) passes those with any of " +
                "the tags, min(n) passes the servers if there are at least n, all() every " +
                "server, and halt(), which only ends the last rule, none, and no all() follows " +
                "the rules. Named like Neo4j Enterprise's setting."
        )
        val userDefinedUpstreamStrategy: Setting<String> =
            newBuilder("server.cluster.catchup.user_defined_upstream_strategy", STRING, null)
                .addConstraint(
                    object : SettingConstraint<String>() {
                        override fun validate(value: String?, config: Configuration) {
                            if (value != null) UpstreamRules.parse(value)
                        }

                        override fun getDescription() = "Rules that parse."
                    }
                )
                .build()

        @JvmField
        @field:Description(
            "The server tags of the secondaries that the connect-randomly-to-server-tags upstream " +
                "strategy connects to. Named like Neo4j Enterprise's setting."
        )
        val connectRandomlyToServerTags: Setting<List<String>> =
            newBuilder(
                    "server.cluster.catchup.connect_randomly_to_server_tags",
                    listOf(STRING),
                    emptyList(),
                )
                .dynamic()
                .build()

        @JvmField
        @field:Description(
            "Deprecated, as server groups became server tags: the server tags of the " +
                "secondaries that the connect-randomly-to-server-group upstream strategy " +
                "connects to. Named like Neo4j Enterprise's setting."
        )
        val connectRandomlyToServerGroup: Setting<List<String>> =
            newBuilder(
                    "server.cluster.catchup.connect_randomly_to_server_group",
                    listOf(STRING),
                    emptyList(),
                )
                .dynamic()
                .build()

        @JvmField
        @field:Internal
        @field:Description(
            "For tests: how long leaders must have stayed put on every member before balancing " +
                "moves one."
        )
        val balancingDelay: Setting<Duration> =
            newBuilder(
                    "internal.dbms.dozendb.cluster.leadership_balancing_delay",
                    DURATION,
                    Duration.ofSeconds(10),
                )
                .immutable()
                .build()

        @JvmField
        @field:Internal
        @field:Description(
            "For tests: the size at which a Raft log segment is closed, raised to twice " +
                "internal.dbms.dozendb.cluster.raft_max_entry_size if smaller. Logs are discarded " +
                "a whole closed segment at a time, so smaller segments discard them sooner."
        )
        val raftLogSegmentSize: Setting<Long> =
            newBuilder(
                    "internal.dbms.dozendb.cluster.raft_log_segment_size",
                    BYTES,
                    RatisConsensusConfig.DEFAULT_LOG_SEGMENT_SIZE,
                )
                .addConstraint(min(1L))
                .immutable()
                .build()

        @JvmField
        @field:Internal
        @field:Description(
            "For tests: the newest format level this member supports, above that of this " +
                "DozenDB version to act as a newer version. The formats of all levels are the " +
                "same so far."
        )
        val newestFormatLevel: Setting<Int> =
            newBuilder(
                    "internal.dbms.dozendb.cluster.newest_format_level",
                    INT,
                    FormatLevels.SUPPORTED.newest,
                )
                .addConstraint(min(FormatLevels.SUPPORTED.newest))
                .immutable()
                .build()

        @JvmField
        @field:Internal
        @field:Description(
            "For tests: how often a member probes the others for what they report, such as the " +
                "databases they lead, besides probing them on demand."
        )
        val memberRefreshInterval: Setting<Duration> =
            newBuilder(
                    "internal.dbms.dozendb.cluster.member_refresh_interval",
                    DURATION,
                    Duration.ofSeconds(10),
                )
                .immutable()
                .build()

        @JvmField
        @field:Internal
        @field:Description(
            "For tests: the largest Raft entry, and so the largest transaction, a database " +
                "accepts. Every member must use the same value."
        )
        val raftMaxEntrySize: Setting<Long> =
            newBuilder(
                    "internal.dbms.dozendb.cluster.raft_max_entry_size",
                    BYTES,
                    RatisConsensusConfig.DEFAULT_MAX_ENTRY_SIZE.toLong(),
                )
                .addConstraint(
                    range(
                        RatisConsensusConfig.MIN_MAX_ENTRY_SIZE.toLong(),
                        RatisConsensusConfig.MAX_MAX_ENTRY_SIZE.toLong(),
                    )
                )
                .immutable()
                .build()
    }
}
