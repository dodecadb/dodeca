package org.dozendb.core.edition

import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.dozendb.core.StandaloneServer
import org.dozendb.core.StandaloneServer.Companion.SYSTEM
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.neo4j.kernel.database.Database

/**
 * The numbers each checkpoint reports, in its log line and the `check_point.*` metrics: pages,
 * I/Os, the I/O limit and its pauses. Neo4j's page cache tracer hands every database the same flush
 * event to count them in, so that databases checkpointing at once counted, and reset, each other's.
 */
class CheckpointTracingTest {
    @TempDir lateinit var root: Path

    @Test
    fun `each database counts the flushes of its checkpoints apart`() {
        StandaloneServer(root).use { server ->
            server.start()
            server.administer("CREATE DATABASE films WAIT")

            val flushes =
                listOf(StandaloneServer.DEFAULT_DATABASE, SYSTEM, "films").map { name ->
                    val database =
                        server
                            .database(name)
                            .dependencyResolver
                            .resolveDependency(Database::class.java)
                    database.tracers.databaseTracer.beginCheckPoint().beginDatabaseFlush()
                }

            assertThat(flushes.toSet()).hasSize(3)
        }
    }
}
