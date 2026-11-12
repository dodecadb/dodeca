package org.dozendb.core

import java.io.ByteArrayOutputStream
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.neo4j.configuration.Config
import org.neo4j.logging.log4j.Log4jLogProvider

/**
 * Settings that Neo4j Enterprise removed in 2025.01: refused as Enterprise refuses settings it does
 * not know, under strict validation, but naming the setting that replaced them.
 */
class RemovedSettingsTest {
    @Test
    fun `server groups are refused, naming the initial server tags`() {
        assertThatThrownBy { Config.newBuilder().setRaw(mapOf("server.groups" to "eu,us")).build() }
            .rootCause()
            .hasMessage(
                "Setting 'server.groups' was removed in Neo4j 2025.01; use 'initial.server.tags' " +
                    "instead. Cleanup the config or disable " +
                    "'server.config.strict_validation.enabled' to continue."
            )
    }

    @Test
    fun `without strict validation, server groups are ignored with a warning`() {
        val config =
            Config.newBuilder()
                .setRaw(
                    mapOf(
                        "server.groups" to "eu,us",
                        "server.config.strict_validation.enabled" to "false",
                    )
                )
                .build()
        val output = ByteArrayOutputStream()
        config.setLogger(Log4jLogProvider(output).getLog(javaClass))

        assertThat(config.get(DozenDbSettings.initialServerTags)).isEmpty()
        assertThat(output.toString())
            .contains("WARN")
            .contains(
                "Setting 'server.groups' was removed in Neo4j 2025.01; use " +
                    "'initial.server.tags' instead. It is ignored."
            )
    }
}
