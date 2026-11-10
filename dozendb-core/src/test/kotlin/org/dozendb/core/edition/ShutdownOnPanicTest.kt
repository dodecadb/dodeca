package org.dozendb.core.edition

import java.nio.file.Path
import java.util.UUID
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

    /** Panics the default database; returns its id. */
    private fun panicking(settings: Map<Setting<*>, Any>): UUID =
        StandaloneServer(root, settings, dependencies = listOf(ServerExit { exits += it })).use {
            server ->
            server.start()
            val database = server.database()
            database.dependencyResolver
                .resolveDependency(DatabaseHealth::class.java)
                .panic(IllegalStateException("the disk failed"))
            database.databaseId().databaseId().uuid()
        }

    @Test
    fun `a panic ends the server with server panic shutdown_on_panic, its copy in quarantine`() {
        val id = panicking(mapOf(DozenDbSettings.shutdownOnPanic to true))

        assertThat(exits).containsExactly(1)
        // So that the restart that a process manager makes keeps it stopped.
        assertThat(Quarantines(root.resolve("server/data").resolve(Quarantines.FILE_NAME)).of(id))
            .isNotNull
    }

    @Test
    fun `a panic fails only the database by default`() {
        panicking(emptyMap())

        // Neo4j tells listeners of a panic before panic returns.
        assertThat(exits).isEmpty()
    }
}
