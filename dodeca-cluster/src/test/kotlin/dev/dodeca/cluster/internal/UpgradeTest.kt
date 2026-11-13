package dev.dodeca.cluster.internal

import java.nio.file.Path
import java.time.Duration
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.graphdb.config.Setting

/**
 * Rolling upgrades of Dodeca. A member that supports format level 2 acts as a newer version; the
 * formats of both levels are the same so far.
 */
class UpgradeTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    private fun eventually(assertion: () -> Unit) {
        await withPollInterval
            Duration.ofMillis(200) atMost
            ClusterHarness.AWAIT untilAsserted
            {
                assertion()
            }
    }

    /** Runs [query] on `system` of [member], or of the system leader, and returns its rows. */
    private fun ClusterHarness.call(
        query: String,
        member: ClusterHarness.Member = awaitLeader(SYSTEM),
    ): List<Map<String, Any?>> =
        member.database(SYSTEM).executeTransactionally(query, emptyMap()) { result ->
            result.asSequence().map { it.toMap() }.toList()
        }

    /** Writes on the leader and waits until every running member has the write. */
    private fun ClusterHarness.replicates() {
        val title = mapOf("title" to UUID.randomUUID().toString())
        awaitLeader()
            .database()
            .executeTransactionally("CREATE (:Film {title: ${'$'}title})", title)
        eventually {
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

    /** Restarts [member] as the newer version, or as the previous one, while writes go on. */
    private fun ClusterHarness.restart(member: ClusterHarness.Member, newer: Boolean) {
        member.stop()
        member.settings = if (newer) NEWER else emptyMap()
        member.start()
        replicates()
    }

    private fun finishUpgrade(cluster: ClusterHarness): Pair<Any?, Any?> =
        cluster.call("CALL dodeca.cluster.finishUpgrade()").single().let {
            it["previousFormatLevel"] to it["formatLevel"]
        }

    @Test
    fun `a rolling upgrade finishes once every member runs the newer version`() {
        val cluster = ClusterHarness(root, 3).also { harness = it }
        cluster.start()
        cluster.replicates()
        val (first, second, third) = cluster.members

        // Nothing newer runs yet.
        assertThat(finishUpgrade(cluster)).isEqualTo(1L to 1L)
        cluster.restart(first, newer = true)
        assertThatThrownBy { finishUpgrade(cluster) }
            .hasMessageContaining("Upgrade every member to a Dodeca version that supports format")
            .hasMessageContaining("supports format levels 1")
        // Until the upgrade is finished, a member can return to the previous version.
        cluster.restart(first, newer = false)
        for (member in cluster.members) cluster.restart(member, newer = true)
        eventually {
            assertThat(cluster.call("CALL dodeca.cluster.members()").map { it["formatLevels"] })
                .containsOnly("1-2")
        }

        assertThat(finishUpgrade(cluster)).isEqualTo(1L to 2L)

        eventually {
            for (member in cluster.members) {
                assertThat(cluster.call("CALL dodeca.cluster.formatLevel()", member).single())
                    .describedAs("$member")
                    .isEqualTo(mapOf("formatLevel" to 2L, "supportedFormatLevels" to "1-2"))
            }
        }
        cluster.replicates()
        assertThat(finishUpgrade(cluster)).isEqualTo(2L to 2L)
        // Afterwards no member returns to the previous version, nor joins with it.
        third.stop()
        third.settings = emptyMap()
        assertThatThrownBy { third.start() }
            .hasStackTraceContaining("The cluster uses format level 2, as this member last saw it")
        val candidate = cluster.newMember()
        candidate.start()
        // The candidate, which is not a member yet, does not know the leader.
        val leader = cluster.awaitLeader(SYSTEM) { listOf(first, second) }
        assertThatThrownBy {
                cluster.call(
                    "CALL dodeca.cluster.addMember('127.0.0.1:${candidate.raftPort}')",
                    leader,
                )
            }
            .hasMessageContaining("supports format levels 1, but this cluster uses format level 2")
        listOf(first, second).forEach { assertThat(it.running).isTrue() }
    }

    private companion object {
        const val SYSTEM = "system"
        val NEWER: Map<Setting<*>, Any> = mapOf(ClusterSettings.newestFormatLevel to 2)
    }
}
