package org.dozendb.consensus.ratis

import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.Member
import org.dozendb.consensus.MemberId
import org.dozendb.consensus.PeerInfo
import org.junit.jupiter.api.Test

class ProbeCodecTest {
    private fun member(host: String) = Member(MemberId(UUID.randomUUID()), HostPort(host, 7700))

    @Test
    fun `round-trips a response`() {
        val members = listOf(member("core1"), member("::1"))
        val info =
            PeerInfo(
                members[0],
                mapOf("neo4j.version" to "5.26.27", "dozendb.version" to "0.1.0"),
                mapOf(UUID.randomUUID() to members, UUID.randomUUID() to emptyList()),
            )

        assertThat(ProbeCodec.decodeResponse(ProbeCodec.encodeResponse(info))).isEqualTo(info)
    }

    @Test
    fun `round-trips a request`() {
        val sender = member("core2")

        assertThat(ProbeCodec.decodeRequest(ProbeCodec.encodeRequest(sender))).isEqualTo(sender)
    }

    @Test
    fun `rejects unknown format versions and trailing bytes`() {
        val request = ProbeCodec.encodeRequest(member("core1"))

        assertThatThrownBy { ProbeCodec.decodeRequest(request.copyOf().also { it[0] = 2 }) }
            .isInstanceOf(ProbeCodec.UnsupportedFormatException::class.java)
        assertThatThrownBy { ProbeCodec.decodeRequest(request + byteArrayOf(0)) }
            .hasMessageContaining("trailing bytes")
    }
}
