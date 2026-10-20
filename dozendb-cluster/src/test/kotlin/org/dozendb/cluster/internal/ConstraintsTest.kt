package org.dozendb.cluster.internal

import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.kernel.api.exceptions.Status

/** Property existence constraints, with clustering enabled and disabled. */
class ConstraintsTest {
    // Kept when a test fails, for the logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var running: AutoCloseable? = null

    @AfterEach
    fun stop() {
        running?.close()
    }

    @Test
    fun `existence constraints are enforced and survive a restart, with clustering`() {
        val cluster = ClusterHarness(root, 1).also { running = it }
        cluster.start()
        val member = cluster.members.single()

        assertEnforced(
            write = { cluster.awaitLeader().database().executeTransactionally(it) },
            read = { member.database().executeTransactionally(it, emptyMap()) { r -> r.next() } },
            restart = {
                member.stop()
                member.start()
            },
        )
    }

    @Test
    fun `existence constraints are enforced and survive a restart, without clustering`() {
        val server = StandaloneServer(root).also { running = it }
        server.start()

        assertEnforced(
            write = { server.database().executeTransactionally(it) },
            read = { server.rows(it).single() },
            restart = server::restart,
        )
    }

    @Test
    fun `creating existence and key constraints checks existing data, with clustering`() {
        val cluster = ClusterHarness(root, 1).also { running = it }
        cluster.start()

        assertExistingDataChecked { cluster.awaitLeader().database().executeTransactionally(it) }
    }

    @Test
    fun `creating existence and key constraints checks existing data, without clustering`() {
        val server = StandaloneServer(root).also { running = it }
        server.start()

        assertExistingDataChecked { server.database().executeTransactionally(it) }
    }

    private fun assertExistingDataChecked(write: (String) -> Unit) {
        val constraints =
            listOf(
                "CREATE CONSTRAINT person_name FOR (p:Person) REQUIRE p.name IS NOT NULL",
                "CREATE CONSTRAINT person_key FOR (p:Person) REQUIRE p.name IS NODE KEY",
                "CREATE CONSTRAINT knows_since FOR ()-[k:KNOWS]-() REQUIRE k.since IS NOT NULL",
                "CREATE CONSTRAINT knows_key FOR ()-[k:KNOWS]-() REQUIRE k.since IS RELATIONSHIP KEY",
            )
        write("CREATE (:Person {age: 3})-[:KNOWS]->(:Person {name: 'bob'})")

        for (constraint in constraints) {
            assertThat(statusOf(catchThrowable { write(constraint) }))
                .describedAs(constraint)
                .isEqualTo(Status.Schema.ConstraintCreationFailed)
        }
        write("MATCH (p:Person) WHERE p.name IS NULL SET p.name = 'ada'")
        write("MATCH ()-[k:KNOWS]->() SET k.since = 1")
        constraints.forEach(write)
    }

    private fun statusOf(error: Throwable?): Status? =
        generateSequence(error) { it.cause }
            .filterIsInstance<Status.HasStatus>()
            .firstOrNull()
            ?.status()

    private fun assertEnforced(
        write: (String) -> Unit,
        read: (String) -> Map<String, Any?>,
        restart: () -> Unit,
    ) {
        fun people(): Long = read("MATCH (p:Person) RETURN count(p) AS c")["c"] as Long
        fun rejected(cypher: String): Status? = statusOf(catchThrowable { write(cypher) })

        write("CREATE CONSTRAINT person_name FOR (p:Person) REQUIRE p.name IS NOT NULL")
        write("CREATE CONSTRAINT knows_since FOR ()-[k:KNOWS]-() REQUIRE k.since IS NOT NULL")
        write("CREATE (:Person {name: 'ada'})-[:KNOWS {since: 1}]->(:Person {name: 'bob'})")

        assertThat(rejected("CREATE (:Person {age: 3})"))
            .isEqualTo(Status.Schema.ConstraintValidationFailed)
        assertThat(rejected("MATCH (p:Person {name: 'ada'}) REMOVE p.name"))
            .isEqualTo(Status.Schema.ConstraintValidationFailed)
        assertThat(rejected("MATCH (a {name: 'ada'}), (b {name: 'bob'}) CREATE (b)-[:KNOWS]->(a)"))
            .isEqualTo(Status.Schema.ConstraintValidationFailed)
        assertThat(rejected("MATCH ()-[k:KNOWS]->() REMOVE k.since"))
            .isEqualTo(Status.Schema.ConstraintValidationFailed)
        write("MATCH ()-[k:KNOWS]->() SET k.since = 2, k.extra = true")
        write("CREATE (:Unconstrained)-[:OTHER]->(:Unconstrained)")

        restart()

        assertThat(people()).isEqualTo(2)
        assertThat(rejected("CREATE (:Person)")).isEqualTo(Status.Schema.ConstraintValidationFailed)
        write("CREATE (:Person {name: 'cy'})")
        assertThat(people()).isEqualTo(3)
    }
}
