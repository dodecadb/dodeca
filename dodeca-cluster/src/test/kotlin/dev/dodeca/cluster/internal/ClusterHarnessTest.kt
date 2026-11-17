package dev.dodeca.cluster.internal

import dev.dodeca.cluster.internal.replication.ReplicatedDatabase
import dev.dodeca.cluster.internal.replication.ReplicationService
import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.CompletableFuture
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir

/** What tests rely on of [ClusterHarness]. */
class ClusterHarnessTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    private fun eventually(assertion: () -> Unit) {
        await withPollInterval
            Duration.ofMillis(100) atMost
            ClusterHarness.AWAIT untilAsserted
            {
                assertion()
            }
    }

    private fun ClusterHarness.Member.replica(): ReplicatedDatabase {
        val database = database()
        return checkNotNull(
            database.dependencyResolver
                .resolveDependency(ReplicationService::class.java)
                .replica(database.databaseId())
        )
    }

    /** Transactions on this member's default database that wait for a lock on a `:Lock` node. */
    private fun ClusterHarness.Member.blockedOnLock(): Long =
        database("system").executeTransactionally(
            "SHOW TRANSACTIONS YIELD database, currentQuery, status " +
                "WHERE database = 'neo4j' AND currentQuery STARTS WITH 'MATCH (l:Lock)' " +
                "AND status STARTS WITH 'Blocked' RETURN count(*) AS c",
            emptyMap(),
        ) {
            it.next()["c"] as Long
        }

    private fun ClusterHarness.Member.films(): Long =
        database().executeTransactionally("MATCH (f:Film) RETURN count(f) AS c", emptyMap()) {
            it.next()["c"] as Long
        }

    @Test
    fun `a write whose leader stops leading before it commits runs on the next leader, once`() {
        val cluster = ClusterHarness(root, 3).also { harness = it }
        cluster.start()
        cluster.awaitWrite("MERGE (:Lock)")
        val leader = cluster.awaitLeader()
        val next = cluster.members.first { it != leader }

        // The write waits for a lock on the leader until the leader no longer leads, as when a
        // leader steps down while a write that has just reached it commits.
        val writing =
            leader.database().beginTx().use { lock ->
                lock.execute("MATCH (l:Lock) SET l.holder = 'test'").resultAsString()
                val writing = CompletableFuture.runAsync {
                    cluster.awaitWrite("MATCH (l:Lock) SET l.holder = 'writer' MERGE (:Film)")
                }
                eventually { assertThat(leader.blockedOnLock()).isEqualTo(1) }
                assertThat(
                        leader
                            .replica()
                            .transferLeadership(next.cluster.self.id, "a test", TRANSFER)
                    )
                    .isTrue()
                writing
            }
        writing.join()

        for (member in cluster.members) eventually { assertThat(member.films()).isEqualTo(1) }
    }

    private companion object {
        val TRANSFER: Duration = Duration.ofSeconds(5)
    }
}
