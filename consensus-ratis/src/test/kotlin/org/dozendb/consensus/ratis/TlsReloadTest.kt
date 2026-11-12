package org.dozendb.consensus.ratis

import java.nio.file.Path
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
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

/**
 * Certificates and trusted authorities that change while members run ([ReloadableTls.reload]):
 * connections opened afterwards use the new ones, as Neo4j Enterprise's `dbms.security.reloadTLS`.
 */
class TlsReloadTest {
    @TempDir lateinit var root: Path
    private val old = TestAuthority("Old CA")
    private val new = TestAuthority("New CA")
    private val clusters = mutableListOf<RatisTestCluster>()

    @AfterEach
    fun stop() {
        clusters.forEach { it.close() }
    }

    private fun tls(certificate: TestCertificate, vararg trusted: TestAuthority) =
        TlsConfig(
            privateKey = certificate.privateKey,
            certificateChain = listOf(certificate.certificate),
            trustManagers = TestAuthority.trusting(*trusted).trustManagers.toList(),
            requireClientCertificates = true,
            verifyHostname = false,
        )

    /** Members with [tls], in order, each of which [byMember] gives on every start. */
    private fun cluster(
        vararg tls: TlsConfig,
        byMember: MutableMap<MemberId, ReloadableTls> = mutableMapOf(),
    ): Pair<RatisTestCluster, MutableMap<MemberId, ReloadableTls>> {
        val cluster = RatisTestCluster(root, tls.size) { it.copy(tls = byMember[it.memberId]) }
        cluster.nodes.zip(tls).forEach { (node, config) ->
            byMember[node.id] = ReloadableTls(config)
        }
        clusters += cluster
        return cluster to byMember
    }

    private fun RatisTestCluster.Node.probe(other: RatisTestCluster.Node): MemberId = runBlocking {
        consensus.probe(other.consensus.self.raftAddress).member.id
    }

    private fun RatisTestCluster.startAll() = nodes.forEach { it.start() }

    private fun assertUnreachable(call: () -> Any) {
        assertThatThrownBy { call() }.isInstanceOf(ConsensusUnavailableException::class.java)
    }

    @Test
    fun `a member presents its reloaded certificate on connections opened afterwards`() {
        val (cluster, tls) = cluster(tls(old.issue(), old, new), tls(new.issue(), new))
        cluster.startAll()
        val (member, observer) = cluster.nodes
        // The observer trusts only the new authority, which has not issued the member's yet.
        assertUnreachable { observer.probe(member) }

        tls.getValue(member.id).reload(tls(new.issue(), old, new))

        assertThat(observer.probe(member)).isEqualTo(member.id)
        assertThat(member.probe(observer)).isEqualTo(observer.id)
    }

    @Test
    fun `a member trusts the reloaded authorities on connections opened afterwards`() {
        val (cluster, tls) = cluster(tls(old.issue(), old), tls(new.issue(), old, new))
        cluster.startAll()
        val (member, stranger) = cluster.nodes
        // The member trusts only the old authority, as a server and as a client.
        assertUnreachable { stranger.probe(member) }
        assertUnreachable { member.probe(stranger) }

        tls.getValue(member.id).reload(tls(old.issue(), old, new))

        assertThat(stranger.probe(member)).isEqualTo(member.id)
        assertThat(member.probe(stranger)).isEqualTo(stranger.id)
    }

    @Test
    fun `members move to a new authority one reload at a time and replicate throughout`() {
        val (cluster, tls) =
            cluster(tls(old.issue(), old), tls(old.issue(), old), tls(old.issue(), old))
        cluster.form()
        val members = cluster.nodes.map { it.id }
        fun commit(entry: String) {
            val leader = cluster.awaitLeader()
            val result = runBlocking {
                leader.consensus.submit(cluster.database, entry.encodeToByteArray())
            }
            assertThat(result).describedAs(entry).isInstanceOf(CommitResult.Committed::class.java)
        }

        // Trust both, then present the new certificates, then trust only the new authority.
        members.forEach { tls.getValue(it).reload(tls(old.issue(), old, new)) }
        commit("trusting both")
        members.forEach { tls.getValue(it).reload(tls(new.issue(), old, new)) }
        commit("new certificates")
        members.forEach { tls.getValue(it).reload(tls(new.issue(), new)) }
        commit("new authority only")
        // Every member reaches every other on new connections, with new certificates only.
        cluster.nodes.forEach { from ->
            cluster.nodes
                .filter { it != from }
                .forEach { to ->
                    assertThat(from.probe(to)).isEqualTo(to.id)
                }
        }
        // A member that restarts connects anew, with its certificate from the new authority.
        val restarted = cluster.nodes.first { it != cluster.awaitLeader() }
        restarted.stop()
        tls[restarted.id] = ReloadableTls(tls(new.issue(), new))
        restarted.start()
        restarted.consensus.startGroup(cluster.database, restarted.machine, emptyList())
        commit("after a restart")

        await atMost
            RatisTestCluster.AWAIT untilAsserted
            {
                cluster.nodes.forEach {
                    assertThat(it.machine.payloads())
                        .containsExactly(
                            "trusting both",
                            "new certificates",
                            "new authority only",
                            "after a restart",
                        )
                }
            }
    }

    @Test
    fun `protocols, cipher suites, client certificates and hostname checks wait for a restart`() {
        val tls = ReloadableTls(tls(old.issue(), old))
        val certificate = new.issue()
        val changed =
            TlsConfig(
                privateKey = certificate.privateKey,
                certificateChain = listOf(certificate.certificate),
                trustManagers = TestAuthority.trusting(new).trustManagers.toList(),
                protocols = listOf("TLSv1.3"),
                cipherSuites = listOf("TLS_AES_256_GCM_SHA384"),
                requireClientCertificates = false,
                verifyHostname = true,
            )

        val waiting = tls.reload(changed)

        assertThat(waiting)
            .containsExactly(
                "protocols",
                "cipher suites",
                "whether clients need certificates",
                "hostname verification",
            )
        assertThat(tls.current).isSameAs(changed)
        assertThat(tls.reload(tls(old.issue(), old))).isEmpty()
    }
}
