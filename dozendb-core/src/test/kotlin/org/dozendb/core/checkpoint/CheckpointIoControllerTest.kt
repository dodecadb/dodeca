package org.dozendb.core.checkpoint

import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread
import kotlin.io.path.readText
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.dozendb.core.StandaloneServer
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.GraphDatabaseSettings.CheckpointPolicy
import org.neo4j.io.pagecache.tracing.FileFlushEvent
import org.neo4j.time.Clocks
import org.neo4j.time.FakeClock

/** Neo4j Enterprise's `db.checkpoint.iops.limit`, which Neo4j Community ignores. */
class CheckpointIoControllerTest {
    @TempDir lateinit var root: Path

    /** A flush event that records the pauses of its flushing. */
    private class Flush : FileFlushEvent by FileFlushEvent.NULL {
        val pauses = mutableListOf<Long>()

        override fun throttle(recentlyCompletedIOs: Long, millis: Long) {
            pauses += millis
        }
    }

    private val clock = FakeClock()
    private val parks = mutableListOf<Long>()

    /** A controller with [limit] on the fake clock, which waiting moves on. */
    private fun controller(
        limit: Int,
        config: Config = config(limit),
        background: () -> Boolean = { true },
    ) =
        CheckpointIoController(config, clock, background) { nanos ->
            parks += nanos
            clock.forward(nanos, TimeUnit.NANOSECONDS)
        }

    private fun config(limit: Int) =
        Config.defaults(GraphDatabaseSettings.check_point_iops_limit, limit)

    @Test
    fun `a tenth of a second's I-Os go at once, and the next waits for its turn`() {
        val controller = controller(600)
        val flush = Flush()

        repeat(60) { controller.maybeLimitIO(1, flush) }
        assertThat(parks).isEmpty()

        controller.maybeLimitIO(1, flush)
        // 61 I/Os at 600 a second, less the tenth of a second that went at once.
        assertThat(parks)
            .containsExactly(
                61 * (TimeUnit.SECONDS.toNanos(1) / 600) - CheckpointIoController.BURST_NANOS
            )
        assertThat(flush.pauses).hasSize(1)
    }

    @Test
    fun `flushing goes at the limit`() {
        val controller = controller(600)
        val flush = Flush()
        val start = clock.nanos()

        repeat(120) { controller.maybeLimitIO(10, flush) }

        // 1200 I/Os: the first 60 at once, the rest at 600 a second.
        assertThat(Duration.ofNanos(clock.nanos() - start))
            .isBetween(Duration.ofMillis(1899), Duration.ofMillis(1900))
        // Each pause in whole milliseconds, rounded down.
        assertThat(flush.pauses.sum()).isBetween(1800L, 1900L)
    }

    @Test
    fun `I-Os spread over time do not wait`() {
        val controller = controller(600)

        repeat(1000) {
            controller.maybeLimitIO(1, Flush())
            clock.forward(2, TimeUnit.MILLISECONDS)
        }

        assertThat(parks).isEmpty()
    }

    @Test
    fun `-1 and 0 mean no limit`() {
        for (none in listOf(-1, 0)) {
            val controller = controller(none)

            repeat(10_000) { controller.maybeLimitIO(100, Flush()) }

            assertThat(parks).isEmpty()
            assertThat(controller.isEnabled).isFalse()
            assertThat(controller.configuredLimit()).isEqualTo(-1)
        }
    }

    @Test
    fun `the limit follows the dynamic setting`() {
        val config = config(600)
        val controller = controller(600, config)
        assertThat(controller.isEnabled).isTrue()
        assertThat(controller.configuredLimit()).isEqualTo(600)

        config.setDynamic(GraphDatabaseSettings.check_point_iops_limit, 10, "test")
        assertThat(controller.configuredLimit()).isEqualTo(10)
        controller.maybeLimitIO(1, Flush())
        controller.maybeLimitIO(1, Flush())
        assertThat(parks).containsExactly(TimeUnit.MILLISECONDS.toNanos(100))

        config.setDynamic(GraphDatabaseSettings.check_point_iops_limit, -1, "test")
        repeat(1000) { controller.maybeLimitIO(100, Flush()) }
        assertThat(parks).hasSize(1)
        assertThat(controller.configuredLimit()).isEqualTo(-1)
    }

    @Test
    fun `a limit removed while a flush asks keeps that flush at the limit it read`() {
        val config = config(10)
        // The setting changes to none just as the flush asks whether it is limited.
        val controller =
            controller(10, config) {
                config.setDynamic(GraphDatabaseSettings.check_point_iops_limit, 0, "test")
                true
            }

        // Two I/Os at 10 a second, one beyond the burst; then none.
        repeat(2) { controller.maybeLimitIO(2, Flush()) }

        assertThat(parks).containsExactly(TimeUnit.MILLISECONDS.toNanos(100))
        assertThat(controller.configuredLimit()).isEqualTo(-1)
    }

    @Test
    fun `only the background checkpoints' threads are limited`() {
        var background = false
        val controller = controller(10, background = { background })

        repeat(100) { controller.maybeLimitIO(10, Flush()) }
        assertThat(parks).isEmpty()
        assertThat(controller.isEnabled).isFalse()
        assertThat(controller.configuredLimit()).isEqualTo(-1)

        background = true
        assertThat(controller.isEnabled).isTrue()
        assertThat(controller.configuredLimit()).isEqualTo(10)
        repeat(2) { controller.maybeLimitIO(1, Flush()) }
        assertThat(parks).hasSize(1)
    }

    @Test
    fun `a lift lifts the limit until it is closed`() {
        val controller = controller(10)

        val lift = controller.lift()
        val other = controller.lift()
        repeat(1000) { controller.maybeLimitIO(100, Flush()) }
        assertThat(parks).isEmpty()
        assertThat(controller.isEnabled).isFalse()
        assertThat(controller.configuredLimit()).isEqualTo(-1)

        lift.close()
        lift.close()
        assertThat(controller.isEnabled).describedAs("the other lift is open").isFalse()
        other.close()
        assertThat(controller.isEnabled).isTrue()
        assertThat(controller.configuredLimit()).isEqualTo(10)
        repeat(3) { controller.maybeLimitIO(1, Flush()) }
        assertThat(parks).hasSize(2)
    }

    @Test
    fun `a flush that waits goes on when the limit is lifted`() {
        val controller = CheckpointIoController(config(1), Clocks.nanoClock())
        val waiting = CountDownLatch(1)
        val flush =
            object : FileFlushEvent by FileFlushEvent.NULL {
                @Volatile var paused = false

                override fun throttle(recentlyCompletedIOs: Long, millis: Long) {
                    paused = true
                }
            }

        // 100 I/Os at 1 a second: 100 seconds.
        val flushing = thread {
            waiting.countDown()
            controller.maybeLimitIO(100, flush)
        }
        waiting.await()
        await atMost
            Duration.ofSeconds(30) untilAsserted
            {
                assertThat(flushing.state).isEqualTo(Thread.State.TIMED_WAITING)
            }
        controller.lift()

        flushing.join(30_000)
        assertThat(flushing.isAlive).isFalse()
        assertThat(flush.paused).isTrue()
    }

    @Test
    fun `a flush that waits goes on when interrupted, still interrupted`() {
        val controller = CheckpointIoController(config(1), Clocks.nanoClock())
        val interrupted = AtomicBoolean()

        val flushing = thread {
            controller.maybeLimitIO(100, FileFlushEvent.NULL)
            interrupted.set(Thread.currentThread().isInterrupted)
        }
        await atMost
            Duration.ofSeconds(30) untilAsserted
            {
                assertThat(flushing.state).isEqualTo(Thread.State.TIMED_WAITING)
            }
        flushing.interrupt()

        flushing.join(30_000)
        assertThat(flushing.isAlive).isFalse()
        assertThat(interrupted).isTrue()
    }

    @Test
    fun `background checkpoints keep to the limit, which changes while the server runs`() {
        StandaloneServer(
                root,
                mapOf(
                    GraphDatabaseSettings.check_point_policy to CheckpointPolicy.CONTINUOUS,
                    GraphDatabaseSettings.check_point_iops_limit to 10,
                ),
            )
            .use { server ->
                server.start()
                server.rows("CREATE (:Film)")
                await atMost
                    Duration.ofSeconds(60) untilAsserted
                    {
                        assertThat(checkpoints(server, "Scheduled checkpoint")).anyMatch {
                            LIMITED.containsMatchIn(it)
                        }
                    }

                server.administer("CALL dbms.setConfigValue('db.checkpoint.iops.limit', '20')")
                server.rows("CREATE (:Film)")
                await atMost
                    Duration.ofSeconds(60) untilAsserted
                    {
                        assertThat(checkpoints(server, "Scheduled checkpoint")).anyMatch {
                            "IO limit: 20," in it
                        }
                    }
            }
    }

    @Test
    fun `checkpoints someone waits for are not limited`() {
        val server =
            StandaloneServer(root, mapOf(GraphDatabaseSettings.check_point_iops_limit to 10))
        server.use {
            server.start()
            server.rows("UNWIND range(1, 1000) AS i CREATE (:Film {id: i})")

            server.rows("CALL db.checkpoint()")

            assertThat(checkpoints(server, "Call to db.checkpoint() procedure"))
                .singleElement()
                .matches { UNLIMITED.containsMatchIn(it) }
            server.rows("UNWIND range(1, 1000) AS i CREATE (:Film {id: i})")
        }
        for (trigger in listOf("Database init completed", "Database shutdown")) {
            assertThat(checkpoints(server, trigger))
                .describedAs("$trigger of neo4j and system")
                .hasSize(2)
                .allMatch { UNLIMITED.containsMatchIn(it) }
        }
    }

    /** The lines of [server]'s debug log about checkpoints [trigger] completed. */
    private fun checkpoints(server: StandaloneServer, trigger: String) =
        server.home.resolve("logs/debug.log").readText().lines().filter {
            "triggered by \"$trigger" in it && "checkpoint completed" in it
        }

    private companion object {
        val LIMITED = Regex("IO limit: 10, paused in total [1-9]")
        val UNLIMITED = Regex("IO limit: unlimited, paused in total 0 times")
    }
}
