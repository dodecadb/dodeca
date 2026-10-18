package org.dozendb.cluster.core.internal

import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.kernel.api.exceptions.Status

/**
 * Property existence constraints with clustering enabled. With clustering disabled they fail the
 * database, a defect of the public DozerDB source that `DisabledParityIT` documents.
 */
class ClusteredConstraintsTest {
    // Kept when a test fails, for the member's logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    private fun statusOf(error: Throwable?): Status? =
        generateSequence(error) { it.cause }
            .filterIsInstance<Status.HasStatus>()
            .firstOrNull()
            ?.status()

    @Test
    fun `existence constraints are enforced and survive a restart`() {
        val cluster = ClusterHarness(root, 1).also { harness = it }
        cluster.start()
        val member = cluster.members.single()
        fun write(cypher: String) = cluster.awaitLeader().database().executeTransactionally(cypher)
        fun people(): Long =
            member.database().executeTransactionally(
                "MATCH (p:Person) RETURN count(p) AS c",
                emptyMap(),
            ) {
                it.next()["c"] as Long
            }

        write("CREATE CONSTRAINT person_name FOR (p:Person) REQUIRE p.name IS NOT NULL")
        write("CREATE CONSTRAINT knows_since FOR ()-[k:KNOWS]-() REQUIRE k.since IS NOT NULL")
        write("CREATE (:Person {name: 'ada'})-[:KNOWS {since: 1}]->(:Person {name: 'bob'})")

        assertThat(statusOf(catchThrowable { write("CREATE (:Person {age: 3})") }))
            .isEqualTo(Status.Schema.ConstraintValidationFailed)
        assertThat(
                statusOf(catchThrowable { write("MATCH (p:Person {name: 'ada'}) REMOVE p.name") })
            )
            .isEqualTo(Status.Schema.ConstraintValidationFailed)
        assertThat(
                statusOf(
                    catchThrowable {
                        write("MATCH (a {name: 'ada'}), (b {name: 'bob'}) CREATE (b)-[:KNOWS]->(a)")
                    }
                )
            )
            .isEqualTo(Status.Schema.ConstraintValidationFailed)
        assertThat(statusOf(catchThrowable { write("MATCH ()-[k:KNOWS]->() REMOVE k.since") }))
            .isEqualTo(Status.Schema.ConstraintValidationFailed)
        write("MATCH ()-[k:KNOWS]->() SET k.since = 2, k.extra = true")
        write("CREATE (:Unconstrained)-[:OTHER]->(:Unconstrained)")

        member.stop()
        member.start()

        assertThat(people()).isEqualTo(2)
        assertThat(statusOf(catchThrowable { write("CREATE (:Person)") }))
            .isEqualTo(Status.Schema.ConstraintValidationFailed)
        write("CREATE (:Person {name: 'cy'})")
        assertThat(people()).isEqualTo(3)
    }
}
