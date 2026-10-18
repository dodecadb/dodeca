package org.dozendb.cluster.core.internal

import java.util.function.Function
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet
import org.neo4j.internal.kernel.api.CursorFactory
import org.neo4j.internal.kernel.api.Read
import org.neo4j.internal.schema.ConstraintDescriptor
import org.neo4j.internal.schema.LabelSchemaDescriptor
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor
import org.neo4j.io.IOUtils
import org.neo4j.io.pagecache.context.CursorContext
import org.neo4j.kernel.impl.constraints.ConstraintChecker
import org.neo4j.kernel.impl.constraints.ConstraintDelegator
import org.neo4j.kernel.impl.constraints.DozerDbConstraintSemantics
import org.neo4j.memory.MemoryTracker
import org.neo4j.storageengine.api.RelationshipVisitorWithProperties
import org.neo4j.storageengine.api.StorageReader
import org.neo4j.storageengine.api.txstate.ReadableTransactionState
import org.neo4j.storageengine.api.txstate.RelationshipModifications
import org.neo4j.storageengine.api.txstate.TxStateVisitor

/**
 * DozerDB's constraint semantics with the defects of the public 5.26.8 source fixed, so property
 * existence constraints work:
 * - Stores that hold existence, key or type constraints can be read, as in the DozerDB 5.26.27.0
 *   release; the public source rejects them, which fails the database.
 * - Commit-time enforcement builds its checker from each constraint's schema. The public source
 *   casts the constraint itself to a schema descriptor, which fails every write transaction once an
 *   existence constraint exists.
 * - A relationship that lacks a required property fails its transaction with a constraint
 *   violation; the public source reports an unknown error, and does not check relationships whose
 *   properties change.
 *
 * Enforcement otherwise runs DozerDB's own checker. Clustered databases get an instance directly
 * from [ClusterDatabaseContextFactory]. Crash recovery instead picks the service with the highest
 * priority, so this class outranks DozerDB's only when clustering is enabled; with clustering
 * disabled DozerDB behaves exactly as shipped.
 */
class DozenDbConstraintSemantics : DozerDbConstraintSemantics(if (clustered) PRIORITY else 0) {
    override fun getName(): String = "dozenDbConstraints"

    override fun readConstraint(constraint: ConstraintDescriptor): ConstraintDescriptor = constraint

    override fun decorateTxStateVisitor(
        storageReader: StorageReader,
        read: Read,
        cursorFactory: CursorFactory,
        readableTransactionState: ReadableTransactionState,
        txStateVisitor: TxStateVisitor,
        cursorContext: CursorContext,
        memoryTracker: MemoryTracker,
    ): TxStateVisitor {
        if (!readableTransactionState.hasDataChanges()) return txStateVisitor
        val checker =
            storageReader.getOrCreateSchemaDependantState(
                ExistenceChecker::class.java,
                BUILD_CHECKER,
            )
        return Enforcer(checker, cursorContext, cursorFactory, memoryTracker, txStateVisitor, read)
    }

    /** DozerDB's checker, cached per schema state under a key of its own. */
    private class ExistenceChecker(
        storageReader: StorageReader,
        nodes: List<LabelSchemaDescriptor>,
        relationships: List<RelationTypeSchemaDescriptor>,
    ) : ConstraintChecker(storageReader, nodes, relationships)

    /** DozerDB's enforcement, with relationships checked here first, after all their changes. */
    private class Enforcer(
        private val checker: ConstraintChecker,
        cursorContext: CursorContext,
        cursorFactory: CursorFactory,
        memoryTracker: MemoryTracker,
        next: TxStateVisitor,
        private val read: Read,
    ) : ConstraintDelegator(checker, cursorContext, cursorFactory, memoryTracker, next, read) {
        private val relationships =
            cursorFactory.allocateFullAccessRelationshipScanCursor(cursorContext, memoryTracker)
        private val properties =
            cursorFactory.allocateFullAccessPropertyCursor(cursorContext, memoryTracker)
        private val present = IntHashSet()

        override fun visitRelationshipModifications(
            relationshipModifications: RelationshipModifications
        ) {
            if (!checker.relPropertyMap.isEmpty) {
                val check =
                    RelationshipVisitorWithProperties<RuntimeException> { id, type, _, _, _, _, _ ->
                        checkRelationship(id, type)
                    }
                relationshipModifications.creations().forEach(check)
                relationshipModifications.updates().forEach(check)
            }
            super.visitRelationshipModifications(relationshipModifications)
        }

        private fun checkRelationship(id: Long, type: Int) {
            val required = checker.relPropertyMap.get(type.toLong()) ?: return
            read.singleRelationship(id, relationships)
            if (!relationships.next()) return
            present.clear()
            relationships.properties(properties)
            while (properties.next()) present.add(properties.propertyKey())
            for (key in required) {
                if (!present.contains(key)) checker.relConstraintFailure(id, type, key)
            }
        }

        override fun close() {
            try {
                super.close()
            } finally {
                IOUtils.closeAllUnchecked(relationships, properties)
            }
        }
    }

    companion object {
        /** Above DozerDB's priority of 1. */
        private const val PRIORITY = 2

        @Volatile private var clustered = false

        private val BUILD_CHECKER =
            Function<StorageReader, ExistenceChecker> { reader ->
                val nodes = mutableListOf<LabelSchemaDescriptor>()
                val relationships = mutableListOf<RelationTypeSchemaDescriptor>()
                reader.constraintsGetAll().forEachRemaining { constraint ->
                    if (constraint.enforcesPropertyExistence()) {
                        val schema = constraint.schema()
                        if (schema.isSchemaDescriptorType(LabelSchemaDescriptor::class.java)) {
                            nodes +=
                                schema.asSchemaDescriptorType(LabelSchemaDescriptor::class.java)
                        }
                        if (
                            schema.isSchemaDescriptorType(RelationTypeSchemaDescriptor::class.java)
                        ) {
                            relationships +=
                                schema.asSchemaDescriptorType(
                                    RelationTypeSchemaDescriptor::class.java
                                )
                        }
                    }
                }
                ExistenceChecker(reader, nodes, relationships)
            }

        /** Called once clustering is known to be enabled, before any database starts. */
        fun enableForClustering() {
            clustered = true
        }
    }
}
