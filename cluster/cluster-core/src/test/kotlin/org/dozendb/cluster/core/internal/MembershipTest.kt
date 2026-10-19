package org.dozendb.cluster.core.internal

import java.nio.file.Path
import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.dozendb.cluster.core.MemberState
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir

/** Members added to and removed from a running cluster. */
class MembershipTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    private fun cluster(): ClusterHarness =
        ClusterHarness(root, 3).also {
            harness = it
            it.start()
        }

    private fun eventually(assertion: () -> Unit) {
        await withPollInterval
            Duration.ofMillis(200) atMost
            ClusterHarness.AWAIT untilAsserted
            {
                assertion()
            }
    }

    /** The members that vote in `system`, as [observer] knows them. */
    private fun ClusterHarness.voters(
        observer: ClusterHarness.Member
    ): List<ClusterHarness.Member> {
        val ids = observer.cluster.members().map { it.member.id }
        return members.filter { it.running && it.cluster.self.id in ids }
    }

    /** Runs [query] on the system leader and returns its rows. */
    private fun ClusterHarness.administer(
        query: String,
        among: () -> List<ClusterHarness.Member> = { members.take(3) },
    ): List<Map<String, Any?>> =
        awaitLeader(SYSTEM, among).database(SYSTEM).executeTransactionally(query, emptyMap()) {
            it.stream().toList()
        }

    @Test
    fun `a member started after the cluster formed votes in system once it is added`() {
        val cluster = cluster()
        val joining = cluster.newMember().apply { start() }
        assertThat(joining.cluster.state).isEqualTo(MemberState.JOINING)
        assertThat(cluster.voters(cluster.members[0])).doesNotContain(joining)

        val added =
            cluster.administer("CALL dozendb.cluster.addMember('127.0.0.1:${joining.raftPort}')")

        assertThat(added)
            .containsExactly(
                mapOf(
                    "memberId" to joining.cluster.self.id.toString(),
                    "raftAddress" to "127.0.0.1:${joining.raftPort}",
                )
            )
        eventually {
            assertThat(joining.cluster.state).isEqualTo(MemberState.MEMBER)
            for (member in cluster.members) {
                assertThat(cluster.voters(member)).describedAs("voters seen by $member").hasSize(4)
            }
        }
        cluster.administer(
            "CREATE USER ann SET PASSWORD 'a-long-password' CHANGE NOT REQUIRED",
            among = { cluster.members },
        )
        eventually {
            assertThat(
                    joining.database(SYSTEM).executeTransactionally(
                        "SHOW USERS YIELD user",
                        emptyMap(),
                    ) {
                        it.stream().map { row -> row["user"] }.toList()
                    }
                )
                .contains("ann")
        }
    }

    private companion object {
        const val SYSTEM = "system"
    }
}
