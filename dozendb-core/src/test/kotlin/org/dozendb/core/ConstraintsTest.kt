package org.dozendb.core

import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.dozendb.core.ConstraintChecks.assertEnforced
import org.dozendb.core.ConstraintChecks.assertExistingDataChecked
import org.dozendb.core.ConstraintChecks.assertTypesEnforced
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.graphdb.config.Setting
import org.neo4j.internal.kernel.api.exceptions.schema.CreateConstraintFailureException
import org.neo4j.internal.schema.EndpointType
import org.neo4j.internal.schema.SchemaDescriptors
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

    /**
     * Neither can be created from Cypher 5; only Neo4j's kernel API offers them, behind an internal
     * setting.
     */
    @Test
    fun `label existence and endpoint label constraints, which are not enforced, cannot be created`() {
        val server =
            start(
                mapOf(
                    GraphDatabaseInternalSettings
                        .relationship_endpoint_label_and_node_label_existence_constraints to true
                )
            )

        server.database().beginTx().use { tx ->
            val kernel = (tx as InternalTransaction).kernelTransaction()
            val person = kernel.tokenWrite().labelGetOrCreateForName("Person")
            val named = kernel.tokenWrite().labelGetOrCreateForName("Named")
            val knows = kernel.tokenWrite().relationshipTypeGetOrCreateForName("KNOWS")

            assertThatThrownBy {
                    kernel
                        .schemaWrite()
                        .nodeLabelExistenceConstraintCreate(
                            SchemaDescriptors.forNodeLabelExistence(person),
                            "person_named",
                            named,
                        )
                }
                .isInstanceOf(CreateConstraintFailureException::class.java)
            assertThatThrownBy {
                    kernel
                        .schemaWrite()
                        .relationshipEndpointLabelConstraintCreate(
                            SchemaDescriptors.forRelationshipEndpointLabel(knows),
                            "knows_person",
                            person,
                            EndpointType.START,
                        )
                }
                .isInstanceOf(CreateConstraintFailureException::class.java)
        }
    }

    @Test
    fun `creating existence and key constraints checks existing data`() {
        val server = start()

        assertExistingDataChecked { server.database().executeTransactionally(it) }
    }
}
