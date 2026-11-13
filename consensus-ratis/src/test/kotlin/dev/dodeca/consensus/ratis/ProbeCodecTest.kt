package dev.dodeca.consensus.ratis

import dev.dodeca.consensus.HostPort
import dev.dodeca.consensus.Member
import dev.dodeca.consensus.MemberId
import dev.dodeca.consensus.PeerInfo
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class ProbeCodecTest {
    private fun member(host: String) = Member(MemberId(UUID.randomUUID()), HostPort(host, 7700))

    @Test
    fun `round-trips a response`() {
        val members = listOf(member("core1"), member("::1"))
        val info =
            PeerInfo(
                members[0],
                mapOf("neo4j.version" to "5.26.27", "dodeca.version" to "0.1.0"),
                mapOf(UUID.randomUUID() to members, UUID.randomUUID() to emptyList()),
            )

        assertThat(ProbeCodec.decodeResponse(ProbeCodec.encodeResponse(info))).isEqualTo(info)
    }

    @Test
    fun `round-trips values longer than 64 KB, in any script`() {
        val info =
            PeerInfo(
                member("core1"),
                mapOf("dodeca.databases" to "films\tönline\n".repeat(10_000)),
                emptyMap(),
            )

        assertThat(ProbeCodec.decodeResponse(ProbeCodec.encodeResponse(info))).isEqualTo(info)
        assertThat(ProbeCodec.decodeAnnouncement(ProbeCodec.encodeAnnouncement(info)))
            .isEqualTo(info)
    }

    @Test
    fun `rejects a text longer than the message`() {
        val response =
            ProbeCodec.encodeResponse(PeerInfo(member("core1"), mapOf("k" to "v"), emptyMap()))

        assertThatThrownBy { ProbeCodec.decodeResponse(response.copyOf(response.size - 5)) }
            .isInstanceOf(java.io.IOException::class.java)
    }

    @Test
    fun `round-trips a request`() {
        val sender = member("core2")

        assertThat(ProbeCodec.decodeRequest(ProbeCodec.encodeRequest(sender))).isEqualTo(sender)
    }

    @Test
    fun `rejects unknown format versions and trailing bytes`() {
        val request = ProbeCodec.encodeRequest(member("core1"))

        val unknown = (ProbeCodec.FORMAT_VERSION + 1).toByte()
        assertThatThrownBy { ProbeCodec.decodeRequest(request.copyOf().also { it[0] = unknown }) }
            .isInstanceOf(ProbeCodec.UnsupportedFormatException::class.java)
        assertThatThrownBy { ProbeCodec.decodeRequest(request + byteArrayOf(0)) }
            .hasMessageContaining("trailing bytes")
    }
}
