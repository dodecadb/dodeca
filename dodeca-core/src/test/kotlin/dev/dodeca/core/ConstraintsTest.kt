package dev.dodeca.core

import dev.dodeca.core.ConstraintChecks.assertEnforced
import dev.dodeca.core.ConstraintChecks.assertExistingDataChecked
import dev.dodeca.core.ConstraintChecks.assertTypesEnforced
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.graphdb.config.Setting
import org.neo4j.internal.schema.EndpointType
import org.neo4j.internal.schema.SchemaDescriptors
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.impl.coreapi.InternalTransaction

/** Constraints without clustering. */
class ConstraintsTest {
    // Kept when a test fails, for the logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var running: StandaloneServer? = null

    private fun start(settings: Map<Setting<*>, Any> = emptyMap()): StandaloneServer =
        StandaloneServer(root, settings).also {
            running = it
            it.start()
        }

    @AfterEach
    fun stop() {
        running?.close()
    }

    @Test
    fun `existence constraints are enforced and survive a restart`() {
        val server = start()

        assertEnforced(
            write = { server.database().executeTransactionally(it) },
            read = { server.rows(it).single() },
            restart = server::restart,
        )
    }

    @Test
    fun `property type constraints are enforced and survive a restart`() {
        val server = start()

        assertTypesEnforced(
            write = { server.database().executeTransactionally(it) },
            restart = server::restart,
        )
    }

    /** Creates, through Neo4j's kernel API, the constraint [create] makes, and commits it. */
    private fun StandaloneServer.constraint(create: (KernelTransaction) -> Unit) {
        database().beginTx().use { tx ->
            create((tx as InternalTransaction).kernelTransaction())
            tx.commit()
        }
    }

    private fun KernelTransaction.label(name: String): Int =
        tokenWrite().labelGetOrCreateForName(name)

    private fun KernelTransaction.type(name: String): Int =
        tokenWrite().relationshipTypeGetOrCreateForName(name)

    /** That every person also be named, as a graph type's `(:Person => :Named)` requires. */
    private fun StandaloneServer.personNamed() = constraint {
        it.schemaWrite()
            .nodeLabelExistenceConstraintCreate(
                SchemaDescriptors.forNodeLabelExistence(it.label("Person")),
                "person_named",
                it.label("Named"),
            )
    }

    /** That a KNOWS start at a person, as `(:Person)-[:KNOWS =>]->()` requires. */
    private fun StandaloneServer.knowsFromPerson() = constraint {
        it.schemaWrite()
            .relationshipEndpointLabelConstraintCreate(
                SchemaDescriptors.forRelationshipEndpointLabel(it.type("KNOWS")),
                "knows_person",
                it.label("Person"),
                EndpointType.START,
            )
    }

    /** That a KNOWS end at a city, as `()-[:KNOWS =>]->(:City)` requires. */
    private fun StandaloneServer.knowsToCity() = constraint {
        it.schemaWrite()
            .relationshipEndpointLabelConstraintCreate(
                SchemaDescriptors.forRelationshipEndpointLabel(it.type("KNOWS")),
                "knows_city",
                it.label("City"),
                EndpointType.END,
            )
    }

    private fun StandaloneServer.write(cypher: String) = database().executeTransactionally(cypher)

    private fun StandaloneServer.count(cypher: String): Long =
        rows(cypher).single().values.single() as Long

    @Test
    fun `label existence constraints are enforced and survive a restart`() {
        val server = start()
        server.write("CREATE (:Person:Named {name: 'Ann'})")
        server.personNamed()

        assertThatThrownBy { server.write("CREATE (:Person {name: 'Bob'})") }
            .hasMessageContaining("with label Person is required to have label Named")
        assertThatThrownBy { server.write("MATCH (n:Person) REMOVE n:Named") }
            .hasMessageContaining("with label Person is required to have label Named")
        server.write("CREATE (n {name: 'Cy'}) SET n:Named SET n:Person")
        server.write("MATCH (n {name: 'Cy'}) REMOVE n:Person, n:Named")
        server.write("MATCH (n {name: 'Cy'}) DELETE n")
        server.restart()

        assertThatThrownBy { server.write("CREATE (:Person)") }
            .hasMessageContaining("with label Person is required to have label Named")
        assertThatThrownBy { server.write("CREATE (n:Named) SET n:Person REMOVE n:Named") }
            .hasMessageContaining("with label Person is required to have label Named")
        assertThat(server.count("MATCH (n:Person) RETURN count(n)")).isEqualTo(1)
    }

    @Test
    fun `endpoint label constraints are enforced and survive a restart`() {
        val server = start()
        server.write("CREATE (:Person {name: 'Ann'})-[:KNOWS]->(:City {name: 'Gent'})")
        server.knowsFromPerson()
        server.knowsToCity()

        assertThatThrownBy { server.write("CREATE (:Robot)-[:KNOWS]->(:City)") }
            .hasMessageContaining("requires its start Node(")
            .hasMessageContaining("to have label Person")
        assertThatThrownBy { server.write("CREATE (:Person)-[:KNOWS]->(:Robot)") }
            .hasMessageContaining("requires its end Node(")
            .hasMessageContaining("to have label City")
        assertThatThrownBy { server.write("MATCH (n:Person) REMOVE n:Person") }
            .hasMessageContaining("requires its start Node(")
        assertThatThrownBy { server.write("MATCH (n:City) REMOVE n:City") }
            .hasMessageContaining("requires its end Node(")
        // An endpoint may get its label in the same transaction, or lose it with the relationship.
        server.write("CREATE (a:Robot)-[:KNOWS]->(:City) SET a:Person")
        server.write("MATCH (a:Robot)-[k:KNOWS]->() DELETE k REMOVE a:Person")
        // A KNOWS that ends at a person who is not a city goes the other way.
        assertThatThrownBy {
                server.write("MATCH (a:Person {name: 'Ann'}) CREATE (a)<-[:KNOWS]-(:Person)")
            }
            .hasMessageContaining("to have label City")
        server.restart()

        assertThatThrownBy { server.write("CREATE (:Robot)-[:KNOWS]->(:City)") }
            .hasMessageContaining("to have label Person")
        assertThat(server.count("MATCH ()-[k:KNOWS]->() RETURN count(k)")).isEqualTo(1)
    }

    @Test
    fun `creating label existence and endpoint label constraints checks existing data`() {
        val server = start()
        server.write("CREATE (:Person {name: 'Ann'})-[:KNOWS]->(:Robot)")

        assertThatThrownBy { server.personNamed() }
            .hasMessageContaining("Node(0) does not satisfy Constraint( name='person_named'")
        assertThatThrownBy { server.knowsToCity() }
            .hasMessageContaining("Relationship(0) does not satisfy Constraint( name='knows_city'")
        server.knowsFromPerson()

        assertThat(server.count("SHOW CONSTRAINTS YIELD name RETURN count(name)")).isEqualTo(1)
    }

    @Test
    fun `creating existence and key constraints checks existing data`() {
        val server = start()

        assertExistingDataChecked { server.database().executeTransactionally(it) }
    }
}
