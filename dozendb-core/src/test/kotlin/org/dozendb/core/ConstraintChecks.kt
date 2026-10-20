package org.dozendb.core

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.neo4j.kernel.api.exceptions.Status

/** Checks of DozenDB's constraints, shared by the tests with and without clustering. */
object ConstraintChecks {
    /** Creating existence and key constraints fails while existing data violates them. */
    fun assertExistingDataChecked(write: (String) -> Unit) {
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

    /**
     * Property type constraints reject writes that would violate them, before and after [restart]:
     * created or changed properties, and labels added to nodes whose properties then violate one.
     */
    fun assertTypesEnforced(write: (String) -> Unit, restart: () -> Unit) {
        fun rejected(cypher: String): Throwable? = catchThrowable { write(cypher) }
        fun assertRejected(cypher: String) {
            assertThat(statusOf(rejected(cypher)))
                .describedAs(cypher)
                .isEqualTo(Status.Schema.ConstraintValidationFailed)
        }

        write("CREATE CONSTRAINT person_age FOR (p:Person) REQUIRE p.age IS :: INTEGER")
        write("CREATE CONSTRAINT knows_since FOR ()-[k:KNOWS]-() REQUIRE k.since IS :: DATE")
        write("CREATE (:Person {age: 3})-[:KNOWS {since: date('2020-01-01')}]->(:Person)")
        write("CREATE (:Robot {age: 'new'})")

        assertThat(rejected("CREATE (:Person {age: 'three'})"))
            .hasMessageContaining("INTEGER")
            .hasMessageContaining("Person")
        assertRejected("MATCH (p:Person {age: 3}) SET p.age = 3.5")
        assertRejected("MATCH (r:Robot) SET r:Person")
        assertRejected("MATCH ()-[k:KNOWS]->() SET k.since = 2020")
        assertRejected(
            "MATCH (a:Person {age: 3}), (r:Robot) CREATE (a)-[:KNOWS {since: 'today'}]->(r)"
        )
        write("MATCH (p:Person {age: 3}) SET p.age = 4")
        write("MATCH (p:Person) WHERE p.age IS NULL SET p.name = 'no age'")

        restart()

        assertRejected("CREATE (:Person {age: true})")
        write("CREATE (:Person {age: 5})")
        assertRejected("MATCH (p:Person {age: 5}) SET p.age = '5'")
    }

    /** The status of [error] or of its first cause that has one. */
    fun statusOf(error: Throwable?): Status? =
        generateSequence(error) { it.cause }
            .filterIsInstance<Status.HasStatus>()
            .firstOrNull()
            ?.status()

    /** Existence constraints reject writes that would violate them, before and after [restart]. */
    fun assertEnforced(
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
