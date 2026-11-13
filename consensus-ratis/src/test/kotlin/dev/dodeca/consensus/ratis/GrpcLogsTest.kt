package dev.dodeca.consensus.ratis

import java.util.logging.Handler
import java.util.logging.Level
import java.util.logging.LogRecord
import java.util.logging.Logger
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class GrpcLogsTest {
    @Test
    fun `gRPC's channels warn under the logger and with the message that are left out`() {
        val listener =
            checkNotNull(
                javaClass.classLoader.getResourceAsStream(
                    GrpcLogs.CHANNEL_LOGGER.replace('.', '/') + "\$NameResolverListener.class"
                )
            ) {
                "gRPC's channel class moved"
            }
        val text = listener.use { it.readBytes().toString(Charsets.ISO_8859_1) }

        assertThat(text).contains("[{0}] " + GrpcLogs.UNRESOLVED)
    }

    @Test
    fun `leaves out a channel's warnings that it failed to resolve a host name, and no others`() {
        GrpcLogs.install()
        val published = mutableListOf<LogRecord>()
        val handler =
            object : Handler() {
                override fun publish(record: LogRecord) {
                    published += record
                }

                override fun flush() {}

                override fun close() {}
            }
        val logger = Logger.getLogger(GrpcLogs.CHANNEL_LOGGER)
        logger.addHandler(handler)
        try {
            logger.log(
                Level.WARNING,
                "[{0}] Failed to resolve name. status={1}",
                arrayOf("Channel<4>: (core3:7700)", "UNAVAILABLE"),
            )
            logger.log(Level.WARNING, "[{0}] Something else went wrong", "Channel<4>")
        } finally {
            logger.removeHandler(handler)
        }

        assertThat(published.map { it.message }).containsExactly("[{0}] Something else went wrong")
    }
}
