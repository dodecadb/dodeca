package org.dozendb.cluster.internal

import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.UUID
import kotlin.concurrent.thread
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.dozendb.consensus.testing.TestAuthority
import org.dozendb.core.DozenDbSettings
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir

/** Members that talk to each other over TLS, or in plaintext. */
class TlsMembersTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    /** Runs [query] on the system leader among the first three members. */
    private fun ClusterHarness.administer(query: String) {
        awaitLeader(SYSTEM) { members.take(3) }.database(SYSTEM).executeTransactionally(query)
    }

    /**
     * Runs [block] while [member] starts in the background. It waits for initial members it cannot
     * reach until it is interrupted afterwards.
     */
    private fun <T> starting(member: ClusterHarness.Member, block: () -> T): T {
        val start = thread(name = "start-$member") { runCatching { member.start() } }
        try {
            return block()
        } finally {
            start.interrupt()
            start.join(ClusterHarness.START_TIMEOUT.toMillis())
            check(!start.isAlive) { "$member did not stop starting" }
        }
    }

    /** Asserts that adding [candidate] fails, once it listens, with a message containing [why]. */
    private fun ClusterHarness.refusesToAdd(candidate: ClusterHarness.Member, why: String) {
        val address = "127.0.0.1:${candidate.raftPort}"
        await withPollInterval
            Duration.ofMillis(200) atMost
            ClusterHarness.AWAIT untilAsserted
            {
                assertThatThrownBy { administer("CALL dozendb.cluster.addMember('$address')") }
                    .hasMessageContaining("No DozenDB member answers at $address")
                    .hasMessageContaining(why)
            }
    }

    /** Writes on the leader and waits until every running member has the write. */
    private fun ClusterHarness.replicates() {
        val title = mapOf("title" to UUID.randomUUID().toString())
        awaitLeader(ClusterHarness.DEFAULT_DATABASE)
            .database()
            .executeTransactionally("CREATE (:Film {title: ${'$'}title})", title)
        await withPollInterval
            Duration.ofMillis(200) atMost
            ClusterHarness.AWAIT untilAsserted
            {
                for (member in running()) {
                    assertThat(
                            member.database().executeTransactionally(
                                "MATCH (f:Film {title: ${'$'}title}) RETURN count(f) AS films",
                                title,
                            ) {
                                it.next()["films"]
                            }
                        )
                        .describedAs("$member")
                        .isEqualTo(1L)
                }
            }
    }

    @Test
    fun `members replicate in plaintext when it is chosen`() {
        val cluster = ClusterHarness(root, 3, authority = null).also { harness = it }
        cluster.start()

        cluster.replicates()
    }

    @Test
    fun `members move to another authority's certificates, restarting one at a time`() {
        val old = TestAuthority("Old CA")
        val new = TestAuthority("New CA")
        val cluster = ClusterHarness(root, 3, authority = old).also { harness = it }
        cluster.start()
        cluster.replicates()

        // Restarts each member in turn with a certificate of [issuer], trusting [trusted].
        fun restartEach(issuer: TestAuthority, trusted: List<TestAuthority>) {
            for (member in cluster.members) {
                member.stop()
                issuer.issue().writeTo(member.certificates, trusted)
                member.start()
                cluster.replicates()
            }
        }

        // As the operator guide says: trust the new authority, then take certificates from it,
        // then stop trusting the old one.
        restartEach(old, trusted = listOf(old, new))
        restartEach(new, trusted = listOf(old, new))
        restartEach(new, trusted = listOf(new))

        // The members no longer accept a certificate of the old authority.
        val candidate = cluster.newMember()
        starting(candidate) { cluster.refusesToAdd(candidate, "certification path") }
    }

    @Test
    fun `members move to another authority's certificates by reloading them, without restarts`() {
        val old = TestAuthority("Old CA")
        val new = TestAuthority("New CA")
        val cluster =
            ClusterHarness(
                    root,
                    3,
                    settings = mapOf(DozenDbSettings.tlsReloadEnabled to true),
                    authority = old,
                )
                .also { harness = it }
        cluster.start()
        cluster.replicates()

        // Has each member in turn reload a certificate of [issuer], trusting [trusted].
        fun reloadEach(issuer: TestAuthority, trusted: List<TestAuthority>) {
            for (member in cluster.members) {
                issuer.issue().writeTo(member.certificates, trusted)
                member.database(SYSTEM).executeTransactionally("CALL dbms.security.reloadTLS()")
                cluster.replicates()
            }
        }

        // As for restarts: trust the new authority, then take certificates from it, then stop
        // trusting the old one.
        reloadEach(old, trusted = listOf(old, new))
        reloadEach(new, trusted = listOf(old, new))
        reloadEach(new, trusted = listOf(new))

        // A member that connects anew, trusting the new authority only, finds the others present
        // its certificates and accept its own.
        val restarted = cluster.members.last()
        restarted.stop()
        restarted.start()
        cluster.replicates()
        // The members no longer accept a certificate of the old authority.
        val candidate = cluster.newMember()
        starting(candidate) { cluster.refusesToAdd(candidate, "certification path") }
    }

    @Test
    fun `members switch between TLS and plaintext when all restart together`() {
        val cluster = ClusterHarness(root, 3).also { harness = it }
        cluster.start()
        cluster.replicates()
        val raftLog = root.resolve("raft.log")

        for (tls in listOf(false, true)) {
            val started = Files.readString(raftLog).length
            cluster.members.forEach { it.stop() }
            cluster.members.forEach {
                it.settings = mapOf(ClusterHarness.CLUSTER_TLS.enabled to tls)
            }
            cluster.start()

            cluster.replicates()
            val transport = if (tls) "(TLS, " else "(plaintext)"
            assertThat(Files.readString(raftLog).substring(started).split(transport))
                .describedAs("members starting with $transport")
                .hasSize(4)
        }
    }

    @Test
    fun `members do not add a member whose certificate an untrusted authority issued`() {
        val cluster = ClusterHarness(root, 3).also { harness = it }
        val candidate = cluster.newMember()
        val foreign = TestAuthority("Foreign CA")
        // It trusts the cluster, but the cluster does not trust it.
        foreign
            .issue()
            .writeTo(
                candidate.home.resolve(ClusterHarness.CERTIFICATES),
                trusted = listOf(foreign, checkNotNull(cluster.authority)),
            )
        cluster.start(cluster.members.take(3))

        starting(candidate) { cluster.refusesToAdd(candidate, "certification path") }
        cluster.replicates()
    }

    @Test
    fun `members do not add a member whose certificate they find revoked`() {
        val authority = TestAuthority()
        val cluster = ClusterHarness(root, 3, authority = authority).also { harness = it }
        val candidate = cluster.newMember()
        val revoked = authority.issue()
        revoked.writeTo(candidate.home.resolve(ClusterHarness.CERTIFICATES))
        for (member in cluster.members.take(3)) {
            authority
                .issue()
                .writeTo(
                    member.home.resolve(ClusterHarness.CERTIFICATES),
                    revocations = listOf(authority.revocationList(revoked)),
                )
        }
        cluster.start(cluster.members.take(3))

        starting(candidate) { cluster.refusesToAdd(candidate, "revoked") }
    }

    private companion object {
        const val SYSTEM = "system"
    }
}
