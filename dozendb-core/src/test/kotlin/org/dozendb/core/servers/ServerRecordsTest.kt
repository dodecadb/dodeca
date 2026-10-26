package org.dozendb.core.servers

import java.nio.file.Path
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.until
import org.dozendb.core.StandaloneServer
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir

class ServerRecordsTest {
    // Kept when a test fails, for the server's logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path

    @Test
    fun `changes of the records happen one after the other, also the first ones`() {
        StandaloneServer(root).use { server ->
            server.start()
            val system = server.database(StandaloneServer.SYSTEM)
            val id = UUID.randomUUID()
            val first = system.beginTx()
            ServerRecords.lock(first)
            val seen = CompletableFuture<Set<UUID>>()
            val second = thread {
                system.beginTx().use { tx ->
                    ServerRecords.lock(tx)
                    seen.complete(ServerRecords.records(tx).keys)
                }
            }

            await atMost
                Duration.ofSeconds(30) until
                {
                    // Waiting for the lock, or not, if it does not lock the other out.
                    seen.isDone ||
                        second.state in setOf(Thread.State.WAITING, Thread.State.TIMED_WAITING)
                }
            ServerRecords.write(first, ServerRecord(id))
            first.commit()
            first.close()

            assertThat(seen.get(30, TimeUnit.SECONDS))
                .describedAs("seen after the lock")
                .contains(id)
        }
    }
}
