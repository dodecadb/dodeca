package org.dozendb.core.edition

import java.nio.file.Path
import java.util.concurrent.LinkedBlockingQueue
import org.assertj.core.api.Assertions.assertThat
import org.dozendb.core.DozenDbSettings
import org.dozendb.core.StandaloneServer
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.neo4j.graphdb.config.Setting
import org.neo4j.monitoring.DatabaseHealth

/** `server.panic.shutdown_on_panic`: a database's panic ends the server, as in Neo4j Enterprise. */
class ShutdownOnPanicTest {
    @TempDir lateinit var root: Path
    private val exits = LinkedBlockingQueue<Int>()

    private fun panicking(settings: Map<Setting<*>, Any>) {
        StandaloneServer(root, settings, dependencies = listOf(ServerExit { exits += it })).use {
            server ->
            server.start()

            server
                .database()
                .dependencyResolver
                .resolveDependency(DatabaseHealth::class.java)
                .panic(IllegalStateException("the disk failed"))
        }
    }

    @Test
    fun `a panic ends the server with server panic shutdown_on_panic`() {
        panicking(mapOf(DozenDbSettings.shutdownOnPanic to true))

        assertThat(exits).containsExactly(1)
    }

    @Test
    fun `a panic fails only the database by default`() {
        panicking(emptyMap())

        // Neo4j tells listeners of a panic before panic returns.
        assertThat(exits).isEmpty()
    }
}
