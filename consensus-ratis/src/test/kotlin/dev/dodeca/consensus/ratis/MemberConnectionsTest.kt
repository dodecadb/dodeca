package dev.dodeca.consensus.ratis

import dev.dodeca.consensus.BulkSource
import dev.dodeca.consensus.MemberConnection
import dev.dodeca.consensus.testing.TestAuthority
import java.nio.file.Path
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

/** The connections between members that each one reports open, for `dbms.cluster.protocols`. */
class MemberConnectionsTest {
    @TempDir lateinit var root: Path
    private var cluster: RatisTestCluster? = null

    @AfterEach
    fun stop() {
        cluster?.close()
    }

    private fun RatisTestCluster.Node.connections(): List<MemberConnection> =
        consensus.connections()

    @Test
    fun `a leader replicates over a connection to each follower, which sees it inbound`() {
        val cluster = RatisTestCluster(root, 3).also { this.cluster = it }
        cluster.form()
        val leader = cluster.awaitLeader()
        val followers = cluster.nodes - leader

        await atMost
            RatisTestCluster.AWAIT untilAsserted
            {
                assertThat(leader.connections().filter { !it.inbound })
                    .containsExactlyInAnyOrderElementsOf(
                        followers.map {
                            MemberConnection(false, it.consensus.self.raftAddress, RAFT, false)
                        }
                    )
                for (follower in followers) {
                    assertThat(follower.connections()).describedAs("on $follower").anySatisfy {
                        assertThat(it.inbound).isTrue()
                        assertThat(it.protocols).contains("raft")
                        assertThat(it.remoteAddress.host).isEqualTo("127.0.0.1")
                    }
                }
            }
    }

    @Test
    fun `a bulk transfer is seen at both ends while it lasts, with its name, over TLS`() {
        val authority = TestAuthority()
        val cluster =
            RatisTestCluster(root, 2) { config ->
                    val certificate = authority.issue()
                    config.copy(
                        tls =
                            ReloadableTls(
                                TlsConfig(
                                    privateKey = certificate.privateKey,
                                    certificateChain = listOf(certificate.certificate),
                                    trustManagers =
                                        TestAuthority.trusting(authority).trustManagers.toList(),
                                    requireClientCertificates = true,
                                    verifyHostname = false,
                                )
                            )
                    )
                }
                .also { this.cluster = it }
        cluster.nodes.forEach { it.start() }
        val (source, receiver) = cluster.nodes
        val serving = CountDownLatch(1)
        val release = CountDownLatch(1)
        source.consensus.registerBulkSource(
            "dodeca.test",
            BulkSource { _, out ->
                serving.countDown()
                release.await(30, TimeUnit.SECONDS)
                out.write(1)
            },
        )
        val fetching = thread {
            receiver.consensus.fetchBulk(
                source.consensus.self.raftAddress,
                "dodeca.test",
                ByteArray(0),
            ) {
                it.readAllBytes()
            }
        }
        try {
            assertThat(serving.await(30, TimeUnit.SECONDS)).isTrue()

            assertThat(receiver.connections())
                .contains(
                    MemberConnection(
                        false,
                        source.consensus.self.raftAddress,
                        setOf("bulk", "dodeca.test"),
                        true,
                    )
                )
            assertThat(source.connections()).anySatisfy {
                assertThat(it.inbound).isTrue()
                assertThat(it.protocols).isEqualTo(setOf("bulk", "dodeca.test"))
                assertThat(it.tls).isTrue()
            }
        } finally {
            release.countDown()
            fetching.join()
        }
        assertThat(receiver.connections().filter { !it.inbound }).isEmpty()
        await atMost
            RatisTestCluster.AWAIT untilAsserted
            {
                assertThat(source.connections().filter { "bulk" in it.protocols }).isEmpty()
            }
    }

    @Test
    fun `a probe's connection goes once the probe returned`() {
        val cluster = RatisTestCluster(root, 2).also { this.cluster = it }
        cluster.nodes.forEach { it.start() }
        val (asking, asked) = cluster.nodes

        runBlocking { asking.consensus.probe(asked.consensus.self.raftAddress) }

        assertThat(asking.connections()).isEmpty()
        await atMost
            RatisTestCluster.AWAIT untilAsserted
            {
                assertThat(asked.connections()).isEmpty()
            }
    }

    private companion object {
        val RAFT = setOf("raft")
    }
}
