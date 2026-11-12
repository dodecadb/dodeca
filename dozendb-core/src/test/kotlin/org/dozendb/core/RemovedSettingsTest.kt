package org.dozendb.core

import java.io.ByteArrayOutputStream
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.neo4j.configuration.Config
import org.neo4j.logging.log4j.Log4jLogProvider
import org.neo4j.server.startup.validation.Neo4jConfigValidator

/**
 * Settings that Neo4j Enterprise removed in 2025.01: refused as Enterprise refuses settings it does
 * not know, under strict validation, but naming the setting that replaced them.
 */
class RemovedSettingsTest {
    @Test
    fun `server groups are refused, naming the initial server tags`() {
        assertThatThrownBy { Config.newBuilder().setRaw(mapOf("server.groups" to "eu,us")).build() }
            .hasMessage(REFUSAL)
    }

    @Test
    fun `the refusal is what Neo4j's validation at startup shows`() {
        assertThat(issues(mapOf("server.groups" to "eu,us"))).containsExactly("Error: $REFUSAL")
    }

    @Test
    fun `without strict validation, Neo4j's validation at startup shows the warning`() {
        assertThat(
                issues(
                    mapOf(
                        "server.groups" to "eu,us",
                        "server.config.strict_validation.enabled" to "false",
                    )
                )
            )
            .containsExactly(
                "Warning: Setting 'server.groups' was removed in Neo4j 2025.01; use " +
                    "'initial.server.tags' instead. It is ignored."
            )
    }

    /** The issues that Neo4j's validation at startup prints for [settings]. */
    private fun issues(settings: Map<String, String>): List<String> =
        Neo4jConfigValidator({ Config.newBuilder().setRaw(settings).build() }, null)
            .validate()
            .map {
                it.message
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

    private companion object {
        const val REFUSAL =
            "Unrecognized setting. No declared setting with name: server.groups (removed in Neo4j " +
                "2025.01; use initial.server.tags instead). Cleanup the config or disable " +
                "'server.config.strict_validation.enabled' to continue."
    }
}
