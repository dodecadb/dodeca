package org.dozendb.cluster.api

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class HostPortTest {
    @Test
    fun `parses host names and IPv4 addresses`() {
        assertThat(HostPort.parse("core1:7700")).isEqualTo(HostPort("core1", 7700))
        assertThat(HostPort.parse(" 10.0.0.1:1 ")).isEqualTo(HostPort("10.0.0.1", 1))
    }

    @Test
    fun `parses and prints bracketed IPv6 addresses`() {
        val address = HostPort.parse("[::1]:7700")
        assertThat(address).isEqualTo(HostPort("::1", 7700))
        assertThat(address.toString()).isEqualTo("[::1]:7700")
    }

    @Test
    fun `rejects malformed addresses`() {
        listOf("core1", "core1:", ":7700", "core1:x", "core1:0", "core1:65536", "::1:7700")
            .forEach {
                assertThatThrownBy { HostPort.parse(it) }
                    .describedAs(it)
                    .isInstanceOf(IllegalArgumentException::class.java)
            }
    }
}
