package dev.dodeca.cluster.internal

import java.nio.file.Path
import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.until
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.kernel.api.exceptions.Status

/** `dbms.setDefaultDatabase` in a cluster: every member follows, and the old default may go. */
class ClusterDefaultDatabaseTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    /** Runs [cypher] on the leader of `system` among the first members, not a joiner. */
    private fun ClusterHarness.administer(cypher: String): List<Map<String, Any?>> =
        awaitLeader("system") { members.take(3) }.query(cypher)

    private fun ClusterHarness.Member.query(cypher: String): List<Map<String, Any?>> =
        database("system").executeTransactionally(cypher, emptyMap()) { result ->
            result.asSequence().map { it.toMap() }.toList()
        }

    /** The default database [this] names, in each of its rows: one per server hosting it. */
    private fun ClusterHarness.Member.defaultDatabase(): Any? =
        query("SHOW DEFAULT DATABASE YIELD name").map { it["name"] }.distinct().single()

    @Test
    fun `every member takes the new default, also after restarts, and members join after the old one went`() {
        val cluster = ClusterHarness(root, 3).also { harness = it }
        cluster.start()
        cluster.administer("CREATE DATABASE films TOPOLOGY 3 PRIMARIES WAIT")
        cluster.awaitLeader("films").database("films").executeTransactionally("CREATE (:Film)")
        cluster.administer("STOP DATABASE neo4j WAIT")

        val follower =
            cluster.members.take(3).first {
                it != cluster.awaitLeader("system") { cluster.members.take(3) }
            }
        val refused = catchThrowable { follower.query("CALL dbms.setDefaultDatabase('films')") }
        assertThat(
                generateSequence(refused) { it.cause }
                    .filterIsInstance<Status.HasStatus>()
                    .map { it.status() }
                    .firstOrNull()
            )
            .describedAs("$refused")
            .isEqualTo(Status.Cluster.NotALeader)
        assertThat(cluster.administer("CALL dbms.setDefaultDatabase('films')"))
            .containsExactly(mapOf("result" to "Default database set to films"))
        await atMost
            AWAIT untilAsserted
            {
                for (member in cluster.members) {
                    assertThat(member.defaultDatabase()).describedAs("$member").isEqualTo("films")
                }
            }
        val dropped = cluster.administer("DROP DATABASE neo4j WAIT")
        assertThat(dropped.map { it["success"] }).describedAs("$dropped").containsOnly(true)

        cluster.members.forEach { it.stop() }
        cluster.start()

        for (member in cluster.members) {
            assertThat(member.defaultDatabase()).describedAs("$member").isEqualTo("films")
            assertThat(member.query("SHOW DATABASES YIELD name").map { it["name"] })
                .describedAs("$member")
                .doesNotContain("neo4j")
        }
        val joiner = cluster.newMember().also { it.start() }
        // Refused while the joiner still starts; the last refusal tells why it never was added.
        var refusal: Throwable? = null
        val added = runCatching {
            await atMost
                AWAIT until
                {
                    runCatching {
                        cluster.administer(
                            "CALL dodeca.cluster.addMember('127.0.0.1:${joiner.raftPort}')"
                        )
                    }
                        .onFailure { refusal = it }
                        .isSuccess
                }
        }
        assertThat(added.isSuccess).describedAs("added; last refusal: $refusal").isTrue
        await atMost
            AWAIT untilAsserted
            {
                assertThat(joiner.defaultDatabase()).isEqualTo("films")
            }
    }

    private companion object {
        val AWAIT: Duration = Duration.ofSeconds(60)
    }
}
