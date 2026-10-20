package org.dozendb.cluster.internal

import java.nio.file.Path
import java.time.Duration
import kotlin.concurrent.thread
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.dozendb.consensus.testing.TestAuthority
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

    private fun ClusterHarness.replicates() {
        awaitLeader(ClusterHarness.DEFAULT_DATABASE)
            .database()
            .executeTransactionally("CREATE (:Film {title: 'Alien'})")
        await withPollInterval
            Duration.ofMillis(200) atMost
            ClusterHarness.AWAIT untilAsserted
            {
                for (member in running()) {
                    assertThat(
                            member.database().executeTransactionally(
                                "MATCH (f:Film) RETURN count(f) AS films",
                                emptyMap(),
                            ) {
                                it.next()["films"]
                            }
                        )
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
