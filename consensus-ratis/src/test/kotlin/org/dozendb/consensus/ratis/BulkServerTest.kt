package org.dozendb.consensus.ratis

import java.io.IOException
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.security.MessageDigest
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.random.Random
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.dozendb.consensus.BulkSource
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.testing.TestAuthority
import org.dozendb.consensus.testing.TestCertificate
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test

class BulkServerTest {
    private val authority = TestAuthority()
    private val servers = mutableListOf<BulkServer>()

    @AfterEach
    fun stop() {
        servers.forEach { it.close() }
    }

    private fun tls(
        certificate: TestCertificate = authority.issue(),
        requireClientCertificates: Boolean = true,
    ) =
        TlsConfig(
            privateKey = certificate.privateKey,
            certificateChain = listOf(certificate.certificate),
            trustManagers = TestAuthority.trusting(authority).trustManagers.toList(),
            requireClientCertificates = requireClientCertificates,
            verifyHostname = false,
        )

    private fun start(tls: TlsConfig?, sources: Map<String, BulkSource>): BulkServer =
        BulkServer.start(
                InetSocketAddress("127.0.0.1", 0),
                tls?.let(::ReloadableTls),
                "test-bulk",
                sources::get,
            )
            .also {
                servers += it
            }

    private fun BulkServer.fetch(tls: TlsConfig?, name: String = GREETING): String =
        BulkServer.fetch(
            HostPort("127.0.0.1", port),
            tls?.let(::ReloadableTls),
            name,
            "you".toByteArray(),
        ) {
            it.readAllBytes().decodeToString()
        }

    private val greeting = BulkSource { request, out ->
        out.write("hello ${request.decodeToString()}".toByteArray())
    }

    @Test
    fun `serves sources in plaintext on the port the system chose`() {
        val server = start(null, mapOf(GREETING to greeting))

        assertThat(server.port).isPositive
        assertThat(server.fetch(null)).isEqualTo("hello you")
    }

    @Test
    fun `streams more than fits in memory chunk by chunk`() {
        val chunk = Random(1).nextBytes(1 shl 20)
        val chunks = 64
        val server =
            start(
                null,
                mapOf(GREETING to BulkSource { _, out -> repeat(chunks) { out.write(chunk) } }),
            )
        val expected = MessageDigest.getInstance("SHA-256")
        repeat(chunks) { expected.update(chunk) }

        val received =
            BulkServer.fetch(HostPort("127.0.0.1", server.port), null, GREETING, ByteArray(0)) {
                val digest = MessageDigest.getInstance("SHA-256")
                val buffer = ByteArray(8192)
                var total = 0L
                while (true) {
                    val read = it.read(buffer)
                    if (read < 0) break
                    digest.update(buffer, 0, read)
                    total += read
                }
                total to digest.digest()
            }

        assertThat(received.first).isEqualTo(chunks.toLong() * chunk.size)
        assertThat(received.second).isEqualTo(expected.digest())
    }

    @Test
    fun `serves clients with a trusted certificate over TLS`() {
        val server = start(tls(), mapOf(GREETING to greeting))

        assertThat(server.fetch(tls())).isEqualTo("hello you")
    }

    @Test
    fun `refuses clients without a trusted certificate when it requires them`() {
        val server = start(tls(), mapOf(GREETING to greeting))
        val foreign = TestAuthority("Foreign CA")
        val untrusted =
            tls().let {
                val certificate = foreign.issue()
                TlsConfig(
                    privateKey = certificate.privateKey,
                    certificateChain = listOf(certificate.certificate),
                    trustManagers = it.trustManagers,
                    requireClientCertificates = true,
                    verifyHostname = false,
                )
            }

        assertThatThrownBy { server.fetch(untrusted) }
            .isInstanceOf(IOException::class.java)
            .hasMessageContaining("UNAVAILABLE")
        assertThatThrownBy { server.fetch(null) }
            .isInstanceOf(IOException::class.java)
            .hasMessageContaining("UNAVAILABLE")
    }

    @Test
    fun `serves clients with any certificate over TLS when it does not require trusted ones`() {
        val server = start(tls(requireClientCertificates = false), mapOf(GREETING to greeting))
        val foreign = TestAuthority("Foreign CA").issue()
        val client =
            TlsConfig(
                privateKey = foreign.privateKey,
                certificateChain = listOf(foreign.certificate),
                trustManagers = TestAuthority.trusting(authority).trustManagers.toList(),
                requireClientCertificates = true,
                verifyHostname = false,
            )

        assertThat(server.fetch(client)).isEqualTo("hello you")
    }

    @Test
    fun `tells the client when it serves no source of that name`() {
        val server = start(null, mapOf(GREETING to greeting))

        assertThatThrownBy { server.fetch(null, "other") }
            .isInstanceOf(IOException::class.java)
            .hasMessageContaining("NOT_FOUND: no bulk source other")
    }

    @Test
    fun `fails to start on a port in use`() {
        ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress()).use { taken ->
            assertThatThrownBy {
                    BulkServer.start(
                        InetSocketAddress("127.0.0.1", taken.localPort),
                        null,
                        "test-bulk",
                    ) {
                        null
                    }
                }
                .isInstanceOf(IOException::class.java)
        }
    }

    @Test
    fun `cancels the transfers under way when it closes`() {
        val sending = CountDownLatch(1)
        val sourceFailure = CompletableFuture<Throwable?>()
        val server =
            start(
                null,
                mapOf(
                    GREETING to
                        BulkSource { _, out ->
                            try {
                                val chunk = ByteArray(1 shl 20)
                                while (true) {
                                    out.write(chunk)
                                    sending.countDown()
                                }
                            } catch (e: Throwable) {
                                sourceFailure.complete(e)
                                throw e
                            }
                        }
                ),
            )
        val client = CompletableFuture.supplyAsync {
            runCatching {
                BulkServer.fetch(HostPort("127.0.0.1", server.port), null, GREETING, ByteArray(0)) {
                    it.read()
                    sending.await()
                    it.readAllBytes()
                }
            }
        }
        assertThat(sending.await(30, TimeUnit.SECONDS)).isTrue

        server.close()

        assertThat(client.get(30, TimeUnit.SECONDS).exceptionOrNull())
            .isInstanceOf(IOException::class.java)
        assertThat(sourceFailure.get(30, TimeUnit.SECONDS)).isNotNull
    }

    @Test
    fun `a port it cannot listen on is reported as the JDK words it, on every transport`() {
        ServerSocket(0).use { taken ->
            assertThatThrownBy {
                    BulkServer.start(InetSocketAddress(taken.localPort), null, "test") { null }
                }
                .isInstanceOf(IOException::class.java)
                .hasMessage("Address already in use")
        }
        // As Netty's native transport, which gRPC uses on Linux, words it.
        val native =
            IOException(
                "Failed to bind to address /127.0.0.1:6362",
                IOException("bind(..) failed with error(-98): Address already in use"),
            )
        assertThat(BulkServer.whyNotListening(native)).isEqualTo("Address already in use")
    }

    private companion object {
        const val GREETING = "greeting"
    }
}
