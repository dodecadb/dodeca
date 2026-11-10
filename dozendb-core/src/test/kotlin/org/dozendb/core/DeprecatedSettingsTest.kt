package org.dozendb.core

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.neo4j.configuration.Config

/** Neo4j Enterprise's deprecated setting names, which stand for the settings that replace them. */
class DeprecatedSettingsTest {
    @Test
    fun `server groups are the initial server tags`() {
        val config = Config.newBuilder().setRaw(mapOf("server.groups" to "eu,us")).build()

        assertThat(config.get(DozenDbSettings.initialServerTags)).containsExactly("eu", "us")
    }
}
