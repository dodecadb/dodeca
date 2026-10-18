package org.dozendb.cluster.core.internal

import java.nio.file.Path
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.storageengine.api.TransactionIdStore

class ReplicationTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    private fun cluster(size: Int = 3): ClusterHarness {
        println("Cluster harness in $root")
        return ClusterHarness(root, size).also {
            harness = it
            it.start()
        }
    }

    private fun ClusterHarness.Member.write(cypher: String) {
        database().executeTransactionally(cypher)
    }

    private fun ClusterHarness.Member.count(cypher: String): Long =
        database().executeTransactionally(cypher, emptyMap()) { it.next().values.single() as Long }

    private fun ClusterHarness.Member.lastTransaction() =
        database()
            .dependencyResolver
            .resolveDependency(TransactionIdStore::class.java)
            .lastCommittedTransaction

    /** Everything in the graph, with record IDs, in a stable order. */
    private fun ClusterHarness.Member.contents(): List<String> =
        database().executeTransactionally(
            "MATCH (n) OPTIONAL MATCH (n)-[r]->(m) " +
                "RETURN id(n) AS n, labels(n) AS labels, properties(n) AS props, " +
                "id(r) AS r, type(r) AS type, id(m) AS m, properties(r) AS relProps ORDER BY n, r",
            emptyMap(),
        ) { result ->
            result
                .stream()
                .map { row ->
                    row.mapValues { (_, value) ->
                            if (value is Map<*, *>) value.toSortedMap(compareBy { it.toString() })
                            else value
                        }
                        .toSortedMap()
                        .toString()
                }
                .toList()
        }

    private fun ClusterHarness.awaitIdentical(members: List<ClusterHarness.Member> = running()) {
        await atMost
            ClusterHarness.AWAIT untilAsserted
            {
                val reference = members.first()
                val expected = reference.contents()
                val transaction = reference.lastTransaction()
                members.drop(1).forEach {
                    assertThat(it.contents()).describedAs("contents of $it").isEqualTo(expected)
                    val actual = it.lastTransaction()
                    assertThat(actual.id())
                        .describedAs("transaction id of $it")
                        .isEqualTo(transaction.id())
                    assertThat(actual.consensusIndex())
                        .describedAs("consensus index of $it")
                        .isEqualTo(transaction.consensusIndex())
                }
            }
    }

    private fun statusOf(error: Throwable?): Status? =
        generateSequence(error) { it.cause }
            .filterIsInstance<Status.HasStatus>()
            .firstOrNull()
            ?.status()

    @Test
    fun `writes on the leader are applied on every member`() {
        val cluster = cluster()
        val leader = cluster.awaitLeader()

        repeat(10) { batch ->
            leader.write(
                "UNWIND range(1, 10) AS i CREATE (:Item {batch: $batch, i: i})-[:NEXT {w: i}]->(:Tail)"
            )
        }
        leader.write("MATCH (n:Tail) WHERE id(n) % 3 = 0 DETACH DELETE n")
        leader.write("MATCH (n:Item {i: 1}) SET n:First, n.flag = true REMOVE n.batch")

        cluster.awaitIdentical()
        assertThat(cluster.members.first().count("MATCH (n:Item) RETURN count(n)")).isEqualTo(100)
    }

    @Test
    fun `followers reject writes with a retriable not-a-leader error`() {
        val cluster = cluster()
        val leader = cluster.awaitLeader()
        val follower = cluster.running().first { it != leader }

        val error = catchThrowable { follower.write("CREATE (:Rejected)") }

        assertThat(statusOf(error)).isEqualTo(Status.Cluster.NotALeader)
        assertThat(leader.count("MATCH (n:Rejected) RETURN count(n)")).isZero()
        follower.database().executeTransactionally("MATCH (n) RETURN count(n)")
    }

    @Test
    fun `schema changes and existence constraints replicate and survive a restart`() {
        val cluster = cluster()
        val leader = cluster.awaitLeader()

        leader.write("CREATE CONSTRAINT person_name FOR (p:Person) REQUIRE p.name IS NOT NULL")
        leader.write("CREATE CONSTRAINT person_id FOR (p:Person) REQUIRE p.id IS UNIQUE")
        leader.write("CREATE INDEX person_age FOR (p:Person) ON (p.age)")
        leader.write("UNWIND range(1, 50) AS i CREATE (:Person {id: i, name: 'p' + i, age: i % 7})")

        cluster.awaitIdentical()
        val follower = cluster.running().first { it != leader }
        follower.stop()
        follower.start()
        cluster.awaitIdentical()
        cluster.running().forEach {
            val names =
                it.database().executeTransactionally("SHOW CONSTRAINTS YIELD name", emptyMap()) {
                    result ->
                    result.stream().map { row -> row["name"] }.toList()
                }
            assertThat(names).describedAs("constraints of $it").contains("person_name", "person_id")
        }
        val writer = cluster.awaitLeader()
        val duplicate = catchThrowable { writer.write("CREATE (:Person {id: 1, name: 'again'})") }
        assertThat(statusOf(duplicate)).isEqualTo(Status.Schema.ConstraintValidationFailed)
        val nameless = catchThrowable { writer.write("CREATE (:Person {id: 99})") }
        assertThat(statusOf(nameless)).isEqualTo(Status.Schema.ConstraintValidationFailed)
        writer.write("CREATE (:Person {id: 100, name: 'p100'})")
        cluster.awaitIdentical()
    }

    @Test
    fun `concurrent writers on the leader all commit in one order`() {
        val cluster = cluster()
        val leader = cluster.awaitLeader()
        val writers = Executors.newFixedThreadPool(8)

        try {
            val futures =
                (1..200).map { n ->
                    writers.submit { leader.write("CREATE (:Concurrent {n: $n})") }
                }
            futures.forEach { it.get(1, TimeUnit.MINUTES) }
        } finally {
            writers.shutdownNow()
        }

        cluster.awaitIdentical()
        assertThat(leader.count("MATCH (n:Concurrent) RETURN count(n)")).isEqualTo(200)
    }

    @Test
    fun `after the leader stops a new leader takes writes and the old one catches up`() {
        val cluster = cluster()
        val first = cluster.awaitLeader()
        first.write("UNWIND range(1, 20) AS i CREATE (:Before {i: i})")
        cluster.awaitIdentical()

        first.stop()
        val second = cluster.awaitLeader()
        assertThat(second).isNotEqualTo(first)
        second.write("UNWIND range(1, 20) AS i CREATE (:After {i: i})")
        second.write("MATCH (n:Before) WHERE n.i <= 10 DELETE n")
        second.write("UNWIND range(1, 20) AS i CREATE (:Reused {i: i})")
        cluster.awaitIdentical()

        first.start()
        cluster.awaitIdentical()
        assertThat(first.count("MATCH (n:After) RETURN count(n)")).isEqualTo(20)
        assertThat(first.count("MATCH (n:Before) RETURN count(n)")).isEqualTo(10)
    }

    @Test
    fun `a transaction too large to replicate fails cleanly`() {
        val cluster = cluster()
        val leader = cluster.awaitLeader()

        val error = catchThrowable {
            leader.write(
                "UNWIND range(1, 40000) AS i CREATE (:Big {i: i, text: reduce(s = '', x IN range(1, 50) | s + 'xxxxxxxx')})"
            )
        }

        assertThat(statusOf(error)).isEqualTo(Status.Transaction.TransactionCommitFailed)
        assertThat(error).hasStackTraceContaining("too large to replicate")
        leader.write("CREATE (:Small)")
        cluster.awaitIdentical()
        assertThat(leader.count("MATCH (n:Big) RETURN count(n)")).isZero()
    }
}
