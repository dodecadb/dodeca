package org.dozendb.core.remote

import java.io.ByteArrayOutputStream
import java.util.ResourceBundle
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.neo4j.logging.Level
import org.neo4j.logging.log4j.Log4jLogProvider

/** What bolt-connection logs reaches the debug log. */
class BoltClientsTest {
    @Test
    fun `logs messages without parameters, as the JDK's own overloads pass them`() {
        val output = ByteArrayOutputStream()
        val logger = DebugLogLogging(Log4jLogProvider(output), Level.DEBUG).getLog("channel")
        // System.Logger.log(Level, String), which bolt-connection's channel loggers inherit, passes
        // a null array of parameters on: as when a connection closes.
        System.Logger::class
            .java
            .getMethod(
                "log",
                System.Logger.Level::class.java,
                ResourceBundle::class.java,
                String::class.java,
                Array<Any>::class.java,
            )
            .invoke(logger, System.Logger.Level.INFO, null, "Channel is inactive", null)

        assertThat(output.toString()).contains("Channel is inactive")
    }
}
