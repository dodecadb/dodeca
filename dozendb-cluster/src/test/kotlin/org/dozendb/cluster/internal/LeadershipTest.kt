package org.dozendb.cluster.internal

import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.dozendb.cluster.internal.ClusterHarness.Companion.DEFAULT_DATABASE
import org.dozendb.cluster.internal.replication.ReplicatedDatabase
import org.dozendb.cluster.internal.replication.ReplicationService
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.kernel.api.exceptions.Status

/** Moving the leadership of a database on purpose. */
class LeadershipTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    private fun cluster(
        size: Int = 3,
        fileSystem: ((ClusterHarness.Member) -> FileSystemAbstraction)? = null,
    ): ClusterHarness {
        println("Cluster harness in $root")
        return ClusterHarness(root, size, fileSystem).also {
            harness = it
            it.start()
        }
    }

    private fun ClusterHarness.Member.replica(name: String = DEFAULT_DATABASE): ReplicatedDatabase {
        val database = database(name)
        return checkNotNull(
            database.dependencyResolver
                .resolveDependency(ReplicationService::class.java)
                .replica(database.databaseId())
        )
    }

    private fun ClusterHarness.Member.count(cypher: String): Long =
        database().executeTransactionally(cypher, emptyMap()) { it.next().values.single() as Long }

    private fun statusOf(error: Throwable?): Status? =
        generateSequence(error) { it.cause }
            .filterIsInstance<Status.HasStatus>()
            .firstOrNull()
            ?.status()

    @Test
    fun `a transfer lets the commits in flight finish and refuses later ones for a retry`() {
        val cluster = cluster()
        val leader = cluster.awaitLeader()
        val target = cluster.running().first { it != leader }
        val acknowledged = ConcurrentLinkedQueue<Long>()
        val failures = ConcurrentLinkedQueue<Throwable>()
        val ids = AtomicLong()
        val done = AtomicBoolean()
        val writers = Executors.newFixedThreadPool(WRITERS)
        repeat(WRITERS) {
            writers.execute {
                while (!done.get()) {
                    val id = ids.incrementAndGet()
                    try {
                        leader.database().executeTransactionally("CREATE (:Written {id: $id})")
                        acknowledged += id
                    } catch (e: Exception) {
                        failures += e
                    }
                }
            }
        }
        await atMost
            Duration.ofSeconds(30) untilAsserted
            {
                assertThat(acknowledged).hasSizeGreaterThan(200)
            }

        val transferred =
            leader
                .replica()
                .transferLeadership(target.cluster.self.id, "a test", Duration.ofSeconds(5))
        done.set(true)
        writers.shutdown()
        assertThat(writers.awaitTermination(30, TimeUnit.SECONDS)).isTrue()

        assertThat(transferred).isTrue()
        assertThat(cluster.awaitLeader()).isEqualTo(target)
        // Drivers retry both; neither committed.
        assertThat(failures.map(::statusOf).distinct())
            .isSubsetOf(Status.Cluster.NotALeader, Status.Transaction.LeaseExpired)
        await atMost
            Duration.ofSeconds(30) untilAsserted
            {
                cluster.running().forEach {
                    assertThat(it.count("MATCH (n:Written) RETURN count(n)"))
                        .describedAs("$it")
                        .isEqualTo(acknowledged.size.toLong())
                }
            }
    }

    @Test
    fun `a transfer waits for the commit in flight, which succeeds`() {
        val fileSystems = ConcurrentHashMap<Int, SlowLogFileSystem>()
        val cluster = cluster { fileSystems.getOrPut(it.index) { SlowLogFileSystem() } }
        val leader = cluster.awaitLeader()
        val target = cluster.running().first { it != leader }
        val leaderFiles = fileSystems.getValue(leader.index)
        // A new label commits its token first, in a transaction of its own.
        leader.database().executeTransactionally("CREATE (n:InFlight) DELETE n")
        val hold = SlowLogFileSystem.Hold(DEFAULT_DATABASE)
        leaderFiles.holding = hold
        val write: CompletableFuture<Void>
        val transfer: CompletableFuture<Boolean>
        try {
            write = CompletableFuture.runAsync {
                leader.database().executeTransactionally("CREATE (:InFlight)")
            }
            // Committed: the followers applied it, while the leader has yet to, and to answer.
            assertThat(hold.held.await(30, TimeUnit.SECONDS)).isTrue()
            await atMost
                Duration.ofSeconds(30) untilAsserted
                {
                    assertThat(target.count("MATCH (n:InFlight) RETURN count(n)")).isEqualTo(1)
                }
            val replica = leader.replica()

            transfer = CompletableFuture.supplyAsync {
                replica.transferLeadership(target.cluster.self.id, "a test", Duration.ofSeconds(5))
            }
            await atMost
                Duration.ofSeconds(30) untilAsserted
                {
                    assertThat(transfer.isDone || replica.handingOver).isTrue()
                }
        } finally {
            // Also if the test fails before, so that the leader can stop.
            leaderFiles.holding = null
            hold.release()
        }

        assertThatCode { write.get(30, TimeUnit.SECONDS) }.doesNotThrowAnyException()
        assertThat(transfer.get(30, TimeUnit.SECONDS)).isTrue()
        assertThat(cluster.awaitLeader()).isEqualTo(target)
    }

    private companion object {
        const val WRITERS = 4
    }
}
