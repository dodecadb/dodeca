package org.dozendb.consensus.ratis

import java.nio.file.Path
import java.time.Duration
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.ThrowableAssert.ThrowingCallable
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.dozendb.consensus.CommitResult
import org.dozendb.consensus.ConsensusUnavailableException
import org.dozendb.consensus.MemberId
import org.dozendb.consensus.testing.TestAuthority
import org.dozendb.consensus.testing.TestCertificate
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class TlsTest {
    @TempDir lateinit var root: Path
    private val authority = TestAuthority()
    private val foreign = TestAuthority("Foreign CA")
    private val clusters = mutableListOf<RatisTestCluster>()

    @AfterEach
    fun stop() {
        clusters.forEach { it.close() }
    }

    private fun tls(
        certificate: TestCertificate = authority.issue(),
        trusted: List<TestAuthority> = listOf(certificate.authority),
        requireClientCertificates: Boolean = true,
        verifyHostname: Boolean = false,
    ) =
        TlsConfig(
            privateKey = certificate.privateKey,
            certificateChain = listOf(certificate.certificate),
            trustManagers = TestAuthority.trusting(*trusted.toTypedArray()).trustManagers.toList(),
            requireClientCertificates = requireClientCertificates,
            verifyHostname = verifyHostname,
        )

    /** A running cluster whose members use [tls], in order; null for plaintext. */
    private fun cluster(vararg tls: TlsConfig?): RatisTestCluster {
        val byMember = mutableMapOf<MemberId, TlsConfig?>()
        val cluster =
            RatisTestCluster(root, tls.size) {
                it.copy(tls = byMember[it.memberId]?.let(::ReloadableTls))
            }
        cluster.nodes.zip(tls).forEach { (node, config) -> byMember[node.id] = config }
        clusters += cluster
        return cluster
    }

    private fun RatisTestCluster.Node.probe(other: RatisTestCluster.Node): MemberId = runBlocking {
        consensus.probe(other.consensus.self.raftAddress).member.id
    }

    private fun RatisTestCluster.Node.announce(other: RatisTestCluster.Node) = runBlocking {
        consensus.announce(other.consensus.self.raftAddress)
    }

    private fun RatisTestCluster.Node.fetch(other: RatisTestCluster.Node): String =
        consensus.fetchBulk(other.consensus.self.raftAddress, GREETING, ByteArray(0)) {
            it.readAllBytes().decodeToString()
        }

    private fun RatisTestCluster.startAll() {
        nodes.forEach {
            it.start()
            it.consensus.registerBulkSource(GREETING) { _, out -> out.write("hello".toByteArray()) }
        }
    }

    private fun assertUnreachable(call: ThrowingCallable) {
        assertThatThrownBy(call)
            .isInstanceOf(ConsensusUnavailableException::class.java)
            // With the cause that tells why, not its stack trace.
            .message()
            .doesNotContain("\n")
            .contains("UNAVAILABLE")
    }

    @Test
    fun `members replicate, change membership, answer probes and stream data over mutual TLS`() {
        val cluster =
            cluster(
                tls(verifyHostname = true),
                tls(verifyHostname = true),
                tls(verifyHostname = true),
            )
        cluster.form()
        cluster.nodes.forEach {
            it.consensus.registerBulkSource(GREETING) { _, out -> out.write("hello".toByteArray()) }
        }
        val leader = cluster.awaitLeader()
        val follower = cluster.nodes.first { it != leader }

        val result = runBlocking {
            leader.consensus.submit(cluster.database, "entry".encodeToByteArray())
        }

        assertThat(result).isInstanceOf(CommitResult.Committed::class.java)
        await atMost
            RatisTestCluster.AWAIT untilAsserted
            {
                cluster.nodes.forEach { assertThat(it.machine.payloads()).containsExactly("entry") }
            }
        assertThat(follower.probe(leader)).isEqualTo(leader.id)
        assertThat(follower.fetch(leader)).isEqualTo("hello")
        runBlocking { leader.consensus.removeMember(cluster.database, follower.id, CHANGE_TIMEOUT) }
        assertThat(leader.consensus.members(cluster.database).map { it.id })
            .doesNotContain(follower.id)
    }

    @Test
    fun `members refuse a member whose certificate an untrusted authority issued`() {
        val cluster = cluster(tls(), tls(foreign.issue(), trusted = listOf(authority, foreign)))
        cluster.startAll()
        val (member, stranger) = cluster.nodes

        // The member does not trust the stranger as a server, nor as a client.
        assertUnreachable { member.probe(stranger) }
        assertUnreachable { member.fetch(stranger) }
        assertUnreachable { stranger.probe(member) }
        assertUnreachable { stranger.announce(member) }
        assertUnreachable { stranger.fetch(member) }
    }

    @Test
    fun `members with TLS and members without cannot reach each other`() {
        val cluster = cluster(tls(), null)
        cluster.startAll()
        val (encrypted, plain) = cluster.nodes

        assertUnreachable { encrypted.probe(plain) }
        assertUnreachable { plain.probe(encrypted) }
        assertUnreachable { plain.fetch(encrypted) }
    }

    @Test
    fun `a certificate must name the address dialled only with hostname verification`() {
        fun elsewhere() = authority.issue(names = listOf("elsewhere.example"))
        val cluster =
            cluster(
                tls(elsewhere(), verifyHostname = false),
                tls(elsewhere(), verifyHostname = false),
                tls(elsewhere(), verifyHostname = true),
            )
        cluster.startAll()
        val (member, other, verifying) = cluster.nodes

        assertThat(other.probe(member)).isEqualTo(member.id)
        assertThat(other.fetch(member)).isEqualTo("hello")
        // As a server, the verifying member does not check its clients' names.
        assertThat(member.probe(verifying)).isEqualTo(verifying.id)
        assertUnreachable { verifying.probe(member) }
        assertUnreachable { verifying.fetch(member) }
    }

    @Test
    fun `members that do not require client certificates accept members they do not trust`() {
        val cluster =
            cluster(
                tls(requireClientCertificates = false),
                tls(foreign.issue(), trusted = listOf(authority, foreign)),
            )
        cluster.startAll()
        val (member, stranger) = cluster.nodes

        assertThat(stranger.probe(member)).isEqualTo(member.id)
        // The member still checks the servers it connects to.
        assertUnreachable { member.probe(stranger) }
    }

    private companion object {
        const val GREETING = "greeting"
        val CHANGE_TIMEOUT: Duration = Duration.ofSeconds(20)
    }
}
