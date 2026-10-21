package org.dozendb.consensus.ratis

import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.nio.file.Path
import java.security.DigestInputStream
import java.security.DigestOutputStream
import java.security.MessageDigest
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.random.Random
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.dozendb.consensus.BulkSource
import org.dozendb.consensus.ConsensusUnavailableException
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class BulkTransferTest {
    @TempDir lateinit var root: Path
    private lateinit var cluster: RatisTestCluster
    private lateinit var source: RatisTestCluster.Node
    private lateinit var receiver: RatisTestCluster.Node

    @BeforeEach
    fun startMembers() {
        cluster = RatisTestCluster(root, 2)
        cluster.nodes.forEach { it.start() }
        source = cluster.nodes[0]
        receiver = cluster.nodes[1]
    }

    @AfterEach
    fun stopMembers() {
        cluster.close()
    }

    private fun <T> fetch(
        name: String,
        request: ByteArray = ByteArray(0),
        consume: (InputStream) -> T,
    ): T = receiver.consensus.fetchBulk(source.consensus.self.raftAddress, name, request, consume)

    /** Writes [size] pseudo-random bytes derived from the request, in uneven writes. */
    private class RandomSource(private val size: Long) : BulkSource {
        val digest: MessageDigest = MessageDigest.getInstance("SHA-256")

        override fun serve(request: ByteArray, out: OutputStream) {
            val random = Random(request.contentHashCode())
            val sink = DigestOutputStream(out, digest)
            var remaining = size
            while (remaining > 0) {
                val bytes = random.nextBytes(minOf(remaining, random.nextLong(1, 300_000)).toInt())
                sink.write(bytes)
                remaining -= bytes.size
            }
        }
    }

    @Test
    fun `streams a large payload intact`() {
        val size = 40L shl 20
        val random = RandomSource(size)
        source.consensus.registerBulkSource("random", random)

        val (received, digest) =
            fetch("random", "seed".encodeToByteArray()) { stream ->
                val digest = MessageDigest.getInstance("SHA-256")
                val count =
                    DigestInputStream(stream, digest).use {
                        it.transferTo(OutputStream.nullOutputStream())
                    }
                count to digest.digest()
            }

        assertThat(received).isEqualTo(size)
        assertThat(digest).isEqualTo(random.digest.digest())
    }

    @Test
    fun `an empty payload completes`() {
        source.consensus.registerBulkSource("empty") { _, _ -> }

        assertThat(fetch("empty") { it.readAllBytes() }).isEmpty()
    }

    @Test
    fun `the request reaches the source`() {
        source.consensus.registerBulkSource("echo") { request, out -> out.write(request) }

        assertThat(
                fetch("echo", "hello".encodeToByteArray()) { it.readAllBytes().decodeToString() }
            )
            .isEqualTo("hello")
    }

    @Test
    fun `an unknown source fails cleanly`() {
        assertThatThrownBy { fetch("missing") { it.readAllBytes() } }
            .isInstanceOf(ConsensusUnavailableException::class.java)
            .hasMessageContaining("NOT_FOUND")
    }

    @Test
    fun `a source failing mid-stream fails the transfer`() {
        source.consensus.registerBulkSource("broken") { _, out ->
            out.write(ByteArray(3 shl 20))
            throw IOException("disk on fire")
        }

        assertThatThrownBy { fetch("broken") { it.readAllBytes() } }
            .isInstanceOf(ConsensusUnavailableException::class.java)
            .hasMessageContaining("disk on fire")
    }

    @Test
    fun `a source refusing a request tells why`() {
        source.consensus.registerBulkSource("refusing") { _, _ ->
            throw IllegalStateException("there is no database films here")
        }

        assertThatThrownBy { fetch("refusing") { it.readAllBytes() } }
            .isInstanceOf(ConsensusUnavailableException::class.java)
            .hasMessageEndingWith("failed: FAILED_PRECONDITION: there is no database films here")
    }

    @Test
    fun `an unreachable member fails cleanly`() {
        val address = source.consensus.self.raftAddress
        source.stop()

        assertThatThrownBy {
                receiver.consensus.fetchBulk(address, "random", ByteArray(0)) { it.readAllBytes() }
            }
            .isInstanceOf(ConsensusUnavailableException::class.java)
            // One line that ends with why, without a stack trace.
            .hasMessageNotContaining("\n")
            .hasMessageContaining("Connection refused")
    }

    /** Never ends; counts what it wrote and signals when the transfer stopped it. */
    private class EndlessSource : BulkSource {
        val written = AtomicLong()
        val stopped = CountDownLatch(1)

        override fun serve(request: ByteArray, out: OutputStream) {
            val chunk = ByteArray(64 shl 10)
            try {
                while (true) {
                    out.write(chunk)
                    written.addAndGet(chunk.size.toLong())
                }
            } finally {
                stopped.countDown()
            }
        }
    }

    @Test
    fun `a slow receiver holds the source back`() {
        val endless = EndlessSource()
        source.consensus.registerBulkSource("endless", endless)

        fetch("endless") { stream ->
            stream.read()
            Thread.sleep(2_000)
            // Only the transport windows and one chunk are in flight, not the whole stream.
            assertThat(endless.written.get()).isLessThan(16L shl 20)
        }
    }

    @Test
    fun `a receiver that stops early ends the source`() {
        val endless = EndlessSource()
        source.consensus.registerBulkSource("endless", endless)

        fetch("endless") { stream -> stream.readNBytes(5 shl 20) }

        assertThat(endless.stopped.await(10, TimeUnit.SECONDS)).isTrue()
    }
}
