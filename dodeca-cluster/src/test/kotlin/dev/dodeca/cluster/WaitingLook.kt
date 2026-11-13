package dev.dodeca.cluster

import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.until

/**
 * A look of a background loop that reads the system graph and waits, as a read of a stopping system
 * graph does, until closing interrupts it; it fails then, once [closingWaits] lets it.
 */
class WaitingLook {
    private val reading = CountDownLatch(1)
    private val interrupted = CountDownLatch(1)
    private val released = CountDownLatch(1)

    fun read() {
        reading.countDown()
        try {
            CountDownLatch(1).await()
        } catch (e: InterruptedException) {
            interrupted.countDown()
            released.await()
            throw e
        }
    }

    /**
     * Waits for the look to read, closes with [close] on another thread and returns whether closing
     * waited for the look to end.
     */
    fun closingWaits(close: () -> Unit): Boolean {
        assertThat(reading.await(1, TimeUnit.MINUTES)).isTrue()
        val closing = thread { close() }
        assertThat(interrupted.await(1, TimeUnit.MINUTES)).isTrue()
        // Closing has returned, or waits for the look to end.
        await atMost
            Duration.ofMinutes(1) until
            {
                !closing.isAlive || closing.state == Thread.State.TIMED_WAITING
            }
        val waited = closing.isAlive
        released.countDown()
        closing.join()
        return waited
    }
}
