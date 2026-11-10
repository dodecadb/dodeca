package org.dozendb.cluster.internal

import org.assertj.core.api.Assertions.assertThat
import org.dozendb.cluster.internal.ClusterProtocols.Row
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.MemberConnection
import org.junit.jupiter.api.Test

class ClusterProtocolsTest {
    @Test
    fun `a row per connection and protocol, inbound first, with versions and TLS`() {
        val rows =
            ClusterProtocols.rows(
                listOf(
                    MemberConnection(false, HostPort("core2", 6000), setOf("raft"), true),
                    MemberConnection(
                        true,
                        HostPort("10.0.0.3", 51234),
                        setOf("dozendb.pull", "bulk"),
                        true,
                    ),
                    MemberConnection(
                        true,
                        HostPort("10.0.0.2", 50001),
                        setOf("probe", "raft"),
                        false,
                    ),
                    MemberConnection(false, HostPort("core3", 6000), setOf("dozendb.later"), false),
                )
            )

        assertThat(rows)
            .containsExactly(
                Row("inbound", "10.0.0.2:50001", "raft", 1, ""),
                Row("inbound", "10.0.0.2:50001", "probe", 2, ""),
                Row("inbound", "10.0.0.3:51234", "bulk", 1, "TLS"),
                Row("inbound", "10.0.0.3:51234", "dozendb.pull", 1, "TLS"),
                Row("outbound", "core2:6000", "raft", 1, "TLS"),
                // A transfer of a later version that this one does not know.
                Row("outbound", "core3:6000", "dozendb.later", null, ""),
            )
    }

    @Test
    fun `knows the version of every bulk transfer between members`() {
        assertThat(ClusterProtocols.VERSIONS)
            .containsEntry("dozendb.storeCopy", 2)
            .containsKeys(
                "dozendb.pull",
                "dozendb.readBarrier",
                "dozendb.statusCheck",
                "dozendb.entryTerm",
                "dozendb.seedChecksums",
            )
    }
}
