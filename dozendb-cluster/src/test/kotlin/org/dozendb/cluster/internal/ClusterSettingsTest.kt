package org.dozendb.cluster.internal

import java.nio.file.Path
import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.dozendb.consensus.ratis.RatisConsensusConfig
import org.dozendb.core.DozenDbSettings
import org.junit.jupiter.api.Test
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.helpers.SocketAddress

class ClusterSettingsTest {
    private fun config(vararg settings: Pair<String, String>): Config =
        Config.newBuilder()
            .set(GraphDatabaseSettings.neo4j_home, Path.of("/neo4j"))
            .setRaw(settings.toMap())
            .build()

    @Test
    fun `defaults keep clustering off`() {
        val config = config()

        assertThat(config.get(DozenDbSettings.clusterEnabled)).isFalse()
        assertThat(config.get(ClusterSettings.initialMembers)).isEmpty()
        assertThat(config.get(ClusterSettings.raftDirectory)).isEqualTo(Path.of("/neo4j/data/raft"))
        assertThat(config.get(ClusterSettings.electionTimeoutMin)).isEqualTo(Duration.ofSeconds(1))
        assertThat(config.get(ClusterSettings.electionTimeoutMax)).isEqualTo(Duration.ofSeconds(2))
        assertThat(config.get(ClusterSettings.routingAdvertisedAddress))
            .isEqualTo(SocketAddress("localhost", 7688))
        assertThat(config.get(ClusterSettings.forwardBoltWrites)).isFalse()
    }

    @Test
    fun `addresses without a host use the server defaults`() {
        val config =
            config(
                "server.default_listen_address" to "0.0.0.0",
                "server.default_advertised_address" to "core1",
            )

        assertThat(config.get(ClusterSettings.listenAddress))
            .isEqualTo(SocketAddress("0.0.0.0", 7700))
        assertThat(config.get(ClusterSettings.advertisedAddress))
            .isEqualTo(SocketAddress("core1", 7700))
        assertThat(config.get(ClusterSettings.routingAdvertisedAddress))
            .isEqualTo(SocketAddress("core1", 7688))
    }

    @Test
    fun `refuses the settings Enterprise removed in 2025_01, naming those that replaced them`() {
        mapOf(
                "db.cluster.raft.leader_transfer.priority_group" to
                    "db.cluster.raft.leader_transfer.priority_tag",
                "db.cluster.raft.leader_transfer.priority_group.films" to
                    "db.cluster.raft.leader_transfer.priority_tag.films",
                "server.cluster.catchup.connect_randomly_to_server_group" to
                    "server.cluster.catchup.connect_randomly_to_server_tags",
            )
            .forEach { (removed, replacement) ->
                assertThatThrownBy { config(removed to "eu") }
                    .rootCause()
                    .hasMessageStartingWith(
                        "Setting '$removed' was removed in Neo4j 2025.01; use '$replacement' " +
                            "instead."
                    )
            }
    }

    @Test
    fun `parses the initial members`() {
        val config =
            config("dbms.dozendb.cluster.initial_members" to "core1:7700,core2:7701,[::1]:7702")

        assertThat(config.get(ClusterSettings.initialMembers))
            .containsExactly(
                SocketAddress("core1", 7700),
                SocketAddress("core2", 7701),
                SocketAddress("::1", 7702),
            )
    }

    @Test
    fun `Raft log sizes default to the consensus defaults and parse units`() {
        assertThat(config().get(ClusterSettings.raftLogSegmentSize))
            .isEqualTo(RatisConsensusConfig.DEFAULT_LOG_SEGMENT_SIZE)
        assertThat(config().get(ClusterSettings.raftMaxEntrySize))
            .isEqualTo(RatisConsensusConfig.DEFAULT_MAX_ENTRY_SIZE.toLong())

        val config =
            config(
                "internal.dbms.dozendb.cluster.raft_log_segment_size" to "512k",
                "internal.dbms.dozendb.cluster.raft_max_entry_size" to "1m",
            )

        assertThat(config.get(ClusterSettings.raftLogSegmentSize)).isEqualTo(512L shl 10)
        assertThat(config.get(ClusterSettings.raftMaxEntrySize)).isEqualTo(1L shl 20)
        assertThatThrownBy { config("internal.dbms.dozendb.cluster.raft_max_entry_size" to "32k") }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `a database has its own priority tag, or else the one for every database`() {
        val tags =
            DatabasePriorityTag.lookup(
                config(
                    "db.cluster.raft.leader_transfer.priority_tag" to "eu",
                    // Database names ignore case.
                    "db.cluster.raft.leader_transfer.priority_tag.Sales" to "us",
                )
            )

        assertThat(tags("sales")).isEqualTo("us")
        assertThat(tags("neo4j")).isEqualTo("eu")
        assertThat(tags("system")).isEqualTo("eu")
        assertThat(DatabasePriorityTag.lookup(config())("neo4j")).isNull()
    }

    @Test
    fun `refuses two priority tags for one database`() {
        val twice =
            config(
                "db.cluster.raft.leader_transfer.priority_tag.sales" to "us",
                "db.cluster.raft.leader_transfer.priority_tag.SALES" to "eu",
            )

        assertThatThrownBy { DatabasePriorityTag.lookup(twice) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("database sales")
    }

    @Test
    fun `a database whose name has a dot gets no priority tag of its own`() {
        // Neo4j reads the name of a group up to the first dot, and drops the setting.
        val tags =
            DatabasePriorityTag.lookup(
                config(
                    "db.cluster.raft.leader_transfer.priority_tag" to "eu",
                    "db.cluster.raft.leader_transfer.priority_tag.a.b" to "us",
                )
            )

        assertThat(tags("a.b")).isEqualTo("eu")
        assertThat(tags("a")).isEqualTo("eu")
    }

    @Test
    fun `rejects election timeouts that are too short`() {
        assertThatThrownBy { config("dbms.dozendb.cluster.election_timeout_min" to "10ms") }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `takes Enterprise's upstream strategies and rules, and refuses others at start`() {
        val config =
            config(
                "server.cluster.catchup.upstream_strategy" to
                    "connect-randomly-to-server-tags,user-defined",
                "server.cluster.catchup.user_defined_upstream_strategy" to "tags(east); halt()",
                "server.cluster.catchup.connect_randomly_to_server_tags" to "east,west",
            )

        assertThat(config.get(ClusterSettings.upstreamStrategy))
            .containsExactly("connect-randomly-to-server-tags", "user-defined")
        assertThat(config.get(ClusterSettings.connectRandomlyToServerTags))
            .containsExactly("east", "west")
        assertThat(config().get(ClusterSettings.upstreamStrategy)).isEmpty()
        assertThat(config().get(ClusterSettings.pullInterval)).isEqualTo(Duration.ofSeconds(1))
        assertThatThrownBy { config("server.cluster.catchup.upstream_strategy" to "closest") }
            .hasMessageContaining("unknown upstream strategies [closest]")
        assertThatThrownBy {
                config(
                    "server.cluster.catchup.upstream_strategy" to
                        "leader-only,connect-randomly-within-server-group"
                )
            }
            .hasMessageContaining(
                "the upstream strategy connect-randomly-within-server-group was removed in Neo4j " +
                    "2025.01; use connect-randomly-within-server-tags instead"
            )
        assertThatThrownBy {
                config(
                    "server.cluster.catchup.upstream_strategy" to "connect-randomly-to-server-group"
                )
            }
            .hasMessageContaining(
                "the upstream strategy connect-randomly-to-server-group was removed in Neo4j " +
                    "2025.01; use connect-randomly-to-server-tags instead"
            )
        assertThatThrownBy {
                config("server.cluster.catchup.user_defined_upstream_strategy" to "tags(a), all()")
            }
            .hasMessageContaining("a comma separates rules")
    }
}
