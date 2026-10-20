package org.dozendb.cluster.core.internal

import java.nio.file.Path
import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.dozendb.consensus.ratis.RatisConsensusConfig
import org.junit.jupiter.api.Test
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.helpers.SocketAddress

class DozenDbSettingsTest {
    private fun config(vararg settings: Pair<String, String>): Config =
        Config.newBuilder()
            .set(GraphDatabaseSettings.neo4j_home, Path.of("/neo4j"))
            .setRaw(settings.toMap())
            .build()

    @Test
    fun `defaults keep clustering off`() {
        val config = config()

        assertThat(config.get(DozenDbSettings.clusterEnabled)).isFalse()
        assertThat(config.get(DozenDbSettings.initialMembers)).isEmpty()
        assertThat(config.get(DozenDbSettings.raftDirectory)).isEqualTo(Path.of("/neo4j/data/raft"))
        assertThat(config.get(DozenDbSettings.electionTimeoutMin)).isEqualTo(Duration.ofSeconds(1))
        assertThat(config.get(DozenDbSettings.electionTimeoutMax)).isEqualTo(Duration.ofSeconds(2))
    }

    @Test
    fun `addresses without a host use the server defaults`() {
        val config =
            config(
                "server.default_listen_address" to "0.0.0.0",
                "server.default_advertised_address" to "core1",
            )

        assertThat(config.get(DozenDbSettings.listenAddress))
            .isEqualTo(SocketAddress("0.0.0.0", 7700))
        assertThat(config.get(DozenDbSettings.advertisedAddress))
            .isEqualTo(SocketAddress("core1", 7700))
    }

    @Test
    fun `parses the initial members`() {
        val config =
            config("dbms.dozendb.cluster.initial_members" to "core1:7700,core2:7701,[::1]:7702")

        assertThat(config.get(DozenDbSettings.initialMembers))
            .containsExactly(
                SocketAddress("core1", 7700),
                SocketAddress("core2", 7701),
                SocketAddress("::1", 7702),
            )
    }

    @Test
    fun `Raft log sizes default to the consensus defaults and parse units`() {
        assertThat(config().get(DozenDbSettings.raftLogSegmentSize))
            .isEqualTo(RatisConsensusConfig.DEFAULT_LOG_SEGMENT_SIZE)
        assertThat(config().get(DozenDbSettings.raftMaxEntrySize))
            .isEqualTo(RatisConsensusConfig.DEFAULT_MAX_ENTRY_SIZE.toLong())

        val config =
            config(
                "internal.dbms.dozendb.cluster.raft_log_segment_size" to "512k",
                "internal.dbms.dozendb.cluster.raft_max_entry_size" to "1m",
            )

        assertThat(config.get(DozenDbSettings.raftLogSegmentSize)).isEqualTo(512L shl 10)
        assertThat(config.get(DozenDbSettings.raftMaxEntrySize)).isEqualTo(1L shl 20)
        assertThatThrownBy { config("internal.dbms.dozendb.cluster.raft_max_entry_size" to "32k") }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `rejects election timeouts that are too short`() {
        assertThatThrownBy { config("dbms.dozendb.cluster.election_timeout_min" to "10ms") }
            .isInstanceOf(IllegalArgumentException::class.java)
    }
}
