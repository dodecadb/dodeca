/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Copyright (c) DozerDB
 * https://dozerdb.org
 *
 * Modifications Copyright (c) DozenDB
 * 2026-10-19: ported to Kotlin from DozerDB's DozerDbConstraintSemantics, ConstraintChecker and
 * ConstraintDelegator, and merged with DozenDB's fixes of their defects.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */
package org.dozendb.cluster.core.internal

import java.util.function.Function
import org.dozendb.cluster.core.internal.edition.DozenDbDatabaseContextFactory
import org.eclipse.collections.api.IntIterable
import org.eclipse.collections.api.set.primitive.LongSet
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet
import org.neo4j.common.TokenNameLookup
import org.neo4j.internal.kernel.api.CursorFactory
import org.neo4j.internal.kernel.api.EntityCursor
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor
import org.neo4j.internal.kernel.api.PropertyCursor
import org.neo4j.internal.kernel.api.Read
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException
import org.neo4j.internal.kernel.api.exceptions.schema.CreateConstraintFailureException
import org.neo4j.internal.schema.ConstraintDescriptor
import org.neo4j.internal.schema.LabelSchemaDescriptor
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor
import org.neo4j.internal.schema.SchemaDescriptor
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory
import org.neo4j.internal.schema.constraints.KeyConstraintDescriptor
import org.neo4j.internal.schema.constraints.NodeLabelExistenceConstraintDescriptor
import org.neo4j.internal.schema.constraints.RelationshipEndpointLabelConstraintDescriptor
import org.neo4j.internal.schema.constraints.TypeConstraintDescriptor
import org.neo4j.internal.schema.constraints.TypeRepresentation
import org.neo4j.internal.schema.constraints.UniquenessConstraintDescriptor
import org.neo4j.io.IOUtils
import org.neo4j.io.pagecache.context.CursorContext
import org.neo4j.kernel.api.exceptions.schema.NodePropertyExistenceException
import org.neo4j.kernel.api.exceptions.schema.PropertyTypeException
import org.neo4j.kernel.api.exceptions.schema.RelationshipPropertyExistenceException
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics
import org.neo4j.memory.MemoryTracker
import org.neo4j.storageengine.api.PropertySelection
import org.neo4j.storageengine.api.RelationshipVisitorWithProperties
import org.neo4j.storageengine.api.StandardConstraintRuleAccessor
import org.neo4j.storageengine.api.StorageProperty
import org.neo4j.storageengine.api.StorageReader
import org.neo4j.storageengine.api.txstate.ReadableTransactionState
import org.neo4j.storageengine.api.txstate.RelationshipModifications
import org.neo4j.storageengine.api.txstate.TxStateVisitor

/**
 * Constraint semantics that accept every constraint type Neo4j can store and enforce property
 * existence at commit, which Neo4j Community leaves to Enterprise:
 * - Stores holding existence, key, type and other Enterprise constraints can be read; Community
 *   rejects them, which fails the database.
 * - A node or relationship that lacks a property an existence or key constraint requires fails its
 *   transaction with `Neo.ClientError.Schema.ConstraintValidationFailed`, whether it is created or
 *   its labels or properties change.
 * - Creating an existence, key or property type constraint fails if existing data violates it;
 *   label existence and relationship endpoint label constraints are neither checked nor enforced.
 *
 * Every database gets an instance from [DozenDbDatabaseContextFactory]; crash recovery picks the
 * registered service with the highest priority, which this is.
 */
class DozenDbConstraintSemantics : StandardConstraintSemantics(PRIORITY) {
    private val rules = StandardConstraintRuleAccessor()

    override fun getName(): String = "dozenDbConstraints"

    override fun readConstraint(constraint: ConstraintDescriptor): ConstraintDescriptor = constraint

    override fun assertKeyConstraintAllowed(descriptor: SchemaDescriptor) {}

    override fun createUniquenessConstraintRule(
        ruleId: Long,
        descriptor: UniquenessConstraintDescriptor,
        indexId: Long,
    ): ConstraintDescriptor = rules.createUniquenessConstraintRule(ruleId, descriptor, indexId)

    override fun createKeyConstraintRule(
        ruleId: Long,
        descriptor: KeyConstraintDescriptor,
        indexId: Long,
    ): ConstraintDescriptor = rules.createKeyConstraintRule(ruleId, descriptor, indexId)

    override fun createExistenceConstraint(
        ruleId: Long,
        descriptor: ConstraintDescriptor,
    ): ConstraintDescriptor = rules.createExistenceConstraint(ruleId, descriptor)

    override fun createPropertyTypeConstraint(
        ruleId: Long,
        descriptor: TypeConstraintDescriptor,
    ): ConstraintDescriptor = rules.createPropertyTypeConstraint(ruleId, descriptor)

    override fun createRelationshipEndpointLabelConstraint(
        ruleId: Long,
        descriptor: RelationshipEndpointLabelConstraintDescriptor,
    ): ConstraintDescriptor = rules.createRelationshipEndpointLabelConstraint(ruleId, descriptor)

    override fun createNodeLabelExistenceConstraint(
        ruleId: Long,
        descriptor: NodeLabelExistenceConstraintDescriptor,
    ): ConstraintDescriptor = rules.createNodeLabelExistenceConstraint(ruleId, descriptor)

    // Checks of existing data when a constraint is created.

    override fun validateNodePropertyExistenceConstraint(
        nodeCursor: NodeCursor,
        propertyCursor: PropertyCursor,
        schema: LabelSchemaDescriptor,
        tokenNameLookup: TokenNameLookup,
        isDependent: Boolean,
    ) {
        val constraint = ConstraintDescriptorFactory.existsForSchema(schema, isDependent)
        while (nodeCursor.next()) {
            checkPresence(constraint, schema, nodeCursor, propertyCursor, tokenNameLookup)
        }
    }

    override fun validateNodePropertyExistenceConstraint(
        allNodes: NodeLabelIndexCursor,
        nodeCursor: NodeCursor,
        propertyCursor: PropertyCursor,
        descriptor: LabelSchemaDescriptor,
        tokenNameLookup: TokenNameLookup,
        isDependent: Boolean,
    ) {
        val constraint = ConstraintDescriptorFactory.existsForSchema(descriptor, isDependent)
        forEachNode(allNodes, nodeCursor) {
            checkPresence(constraint, descriptor, nodeCursor, propertyCursor, tokenNameLookup)
        }
    }

    override fun validateRelationshipPropertyExistenceConstraint(
        relationshipCursor: RelationshipScanCursor,
        propertyCursor: PropertyCursor,
        descriptor: RelationTypeSchemaDescriptor,
        tokenNameLookup: TokenNameLookup,
        isDependent: Boolean,
    ) {
        val constraint = ConstraintDescriptorFactory.existsForSchema(descriptor, isDependent)
        while (relationshipCursor.next()) {
            checkPresence(
                constraint,
                descriptor,
                relationshipCursor,
                propertyCursor,
                tokenNameLookup,
            )
        }
    }

    override fun validateRelationshipPropertyExistenceConstraint(
        allRelationships: RelationshipTypeIndexCursor,
        propertyCursor: PropertyCursor,
        descriptor: RelationTypeSchemaDescriptor,
        tokenNameLookup: TokenNameLookup,
        isDependent: Boolean,
    ) {
        val constraint = ConstraintDescriptorFactory.existsForSchema(descriptor, isDependent)
        forEachRelationship(allRelationships) {
            checkPresence(constraint, descriptor, allRelationships, propertyCursor, tokenNameLookup)
        }
    }

    override fun validateNodeKeyConstraint(
        nodeCursor: NodeCursor,
        propertyCursor: PropertyCursor,
        descriptor: LabelSchemaDescriptor,
        tokenNameLookup: TokenNameLookup,
    ) {
        val constraint = ConstraintDescriptorFactory.keyForSchema(descriptor)
        while (nodeCursor.next()) {
            checkPresence(constraint, descriptor, nodeCursor, propertyCursor, tokenNameLookup)
        }
    }

    override fun validateNodeKeyConstraint(
        allNodes: NodeLabelIndexCursor,
        nodeCursor: NodeCursor,
        propertyCursor: PropertyCursor,
        descriptor: LabelSchemaDescriptor,
        tokenNameLookup: TokenNameLookup,
    ) {
        val constraint = ConstraintDescriptorFactory.keyForSchema(descriptor)
        forEachNode(allNodes, nodeCursor) {
            checkPresence(constraint, descriptor, nodeCursor, propertyCursor, tokenNameLookup)
        }
    }

    override fun validateRelKeyConstraint(
        relCursor: RelationshipScanCursor,
        propertyCursor: PropertyCursor,
        descriptor: RelationTypeSchemaDescriptor,
        tokenNameLookup: TokenNameLookup,
    ) {
        val constraint = ConstraintDescriptorFactory.keyForSchema(descriptor)
        while (relCursor.next()) {
            checkPresence(constraint, descriptor, relCursor, propertyCursor, tokenNameLookup)
        }
    }

    override fun validateRelKeyConstraint(
        allRelationships: RelationshipTypeIndexCursor,
        relCursor: RelationshipScanCursor,
        propertyCursor: PropertyCursor,
        descriptor: RelationTypeSchemaDescriptor,
        tokenNameLookup: TokenNameLookup,
    ) {
        val constraint = ConstraintDescriptorFactory.keyForSchema(descriptor)
        forEachRelationship(allRelationships) {
            checkPresence(constraint, descriptor, allRelationships, propertyCursor, tokenNameLookup)
        }
    }

    override fun validateNodePropertyTypeConstraint(
        nodeCursor: NodeCursor,
        propertyCursor: PropertyCursor,
        descriptor: TypeConstraintDescriptor,
        tokenNameLookup: TokenNameLookup,
    ) {
        while (nodeCursor.next()) {
            checkType(descriptor, nodeCursor, propertyCursor, tokenNameLookup)
        }
    }

    override fun validateNodePropertyTypeConstraint(
        allNodes: NodeLabelIndexCursor,
        nodeCursor: NodeCursor,
        propertyCursor: PropertyCursor,
        descriptor: TypeConstraintDescriptor,
        tokenNameLookup: TokenNameLookup,
    ) {
        forEachNode(allNodes, nodeCursor) {
            checkType(descriptor, nodeCursor, propertyCursor, tokenNameLookup)
        }
    }

    override fun validateRelationshipPropertyTypeConstraint(
        relationshipCursor: RelationshipScanCursor,
        propertyCursor: PropertyCursor,
        descriptor: TypeConstraintDescriptor,
        tokenNameLookup: TokenNameLookup,
    ) {
        while (relationshipCursor.next()) {
            checkType(descriptor, relationshipCursor, propertyCursor, tokenNameLookup)
        }
    }

    override fun validateRelationshipPropertyTypeConstraint(
        allRelationships: RelationshipTypeIndexCursor,
        propertyCursor: PropertyCursor,
        descriptor: TypeConstraintDescriptor,
        tokenNameLookup: TokenNameLookup,
    ) {
        forEachRelationship(allRelationships) {
            checkType(descriptor, allRelationships, propertyCursor, tokenNameLookup)
        }
    }

    override fun validateRelationshipEndpointLabelConstraint(
        relCursor: RelationshipScanCursor,
        nodeCursor: NodeCursor,
        descriptor: RelationshipEndpointLabelConstraintDescriptor,
        tokenNameLookup: TokenNameLookup,
    ) {}

    override fun validateNodeLabelExistenceConstraint(
        allNodes: NodeLabelIndexCursor,
        nodeCursor: NodeCursor,
        descriptor: NodeLabelExistenceConstraintDescriptor,
        tokenNameLookup: TokenNameLookup,
    ) {}

    override fun validateNodeLabelExistenceConstraint(
        nodeCursor: NodeCursor,
        descriptor: NodeLabelExistenceConstraintDescriptor,
        tokenNameLookup: TokenNameLookup,
    ) {}

    /** Runs [check] with [nodeCursor] at each node of [allNodes]. */
    private inline fun forEachNode(
        allNodes: NodeLabelIndexCursor,
        nodeCursor: NodeCursor,
        check: () -> Unit,
    ) {
        while (allNodes.next()) {
            allNodes.node(nodeCursor)
            if (nodeCursor.next()) check()
        }
    }

    /** Runs [check] at each relationship of [allRelationships] that is still stored. */
    private inline fun forEachRelationship(
        allRelationships: RelationshipTypeIndexCursor,
        check: () -> Unit,
    ) {
        while (allRelationships.next()) {
            if (allRelationships.readFromStore()) check()
        }
    }

    /** Fails creating [constraint] if the node at [node] lacks a property it requires. */
    private fun checkPresence(
        constraint: ConstraintDescriptor,
        schema: LabelSchemaDescriptor,
        node: NodeCursor,
        properties: PropertyCursor,
        tokenNameLookup: TokenNameLookup,
    ) {
        if (hasAll(schema.propertyIds, properties, node)) return
        val violation =
            NodePropertyExistenceException.propertyPresenceViolation(
                schema,
                tokenNameLookup,
                constraint,
                ConstraintValidationException.Phase.VERIFICATION,
                node.nodeReference(),
            )
        throw CreateConstraintFailureException(violation, violation.constraint(), violation)
    }

    /**
     * Fails creating [constraint] if the relationship at [relationship] lacks a required property.
     */
    private fun checkPresence(
        constraint: ConstraintDescriptor,
        schema: RelationTypeSchemaDescriptor,
        relationship: EntityCursor,
        properties: PropertyCursor,
        tokenNameLookup: TokenNameLookup,
    ) {
        if (hasAll(schema.propertyIds, properties, relationship)) return
        val violation =
            RelationshipPropertyExistenceException.propertyPresenceViolation(
                schema,
                tokenNameLookup,
                constraint,
                ConstraintValidationException.Phase.VERIFICATION,
                relationship.reference(),
            )
        throw CreateConstraintFailureException(violation, violation.constraint(), violation)
    }

    /** Whether the entity at [entity] has every property in [propertyIds]. */
    private fun hasAll(
        propertyIds: IntArray,
        properties: PropertyCursor,
        entity: EntityCursor,
    ): Boolean {
        entity.properties(properties, PropertySelection.onlyKeysSelection(*propertyIds))
        var missing = propertyIds.size
        while (properties.next()) missing--
        return missing == 0
    }

    private fun checkType(
        descriptor: TypeConstraintDescriptor,
        entity: EntityCursor,
        properties: PropertyCursor,
        tokenNameLookup: TokenNameLookup,
    ) {
        entity.properties(
            properties,
            PropertySelection.selection(descriptor.schema().propertyId),
        )
        if (!properties.next()) return
        val value = properties.propertyValue()
        if (TypeRepresentation.disallows(descriptor.propertyType(), value)) {
            val violation =
                PropertyTypeException(
                    descriptor,
                    ConstraintValidationException.Phase.VERIFICATION,
                    entity.reference(),
                    tokenNameLookup,
                    value,
                )
            throw CreateConstraintFailureException(violation, violation.constraint(), violation)
        }
    }

    // Enforcement at commit.

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
        val required =
            storageReader.getOrCreateSchemaDependantState(
                RequiredProperties::class.java,
                BUILD_REQUIRED_PROPERTIES,
            )
        if (required.isEmpty) return txStateVisitor
        return Enforcer(
            required,
            storageReader,
            read,
            cursorFactory,
            cursorContext,
            memoryTracker,
            txStateVisitor,
        )
    }

    /**
     * The properties that existence and key constraints require, by label and by relationship type.
     * Cached with the schema, so it holds no transaction's state.
     */
    private class RequiredProperties(
        val nodeSchemas: List<LabelSchemaDescriptor>,
        val relationshipSchemas: List<RelationTypeSchemaDescriptor>,
    ) {
        val byLabel = IntObjectHashMap<IntArray>()
        val byType = IntObjectHashMap<IntArray>()

        init {
            for (schema in nodeSchemas) merge(byLabel, schema.labelId, schema.propertyIds)
            for (schema in relationshipSchemas) merge(byType, schema.relTypeId, schema.propertyIds)
        }

        val isEmpty: Boolean
            get() = byLabel.isEmpty && byType.isEmpty

        private fun merge(map: IntObjectHashMap<IntArray>, token: Int, keys: IntArray) {
            val current = map.get(token) ?: IntArray(0)
            map.put(token, (current + keys).distinct().sorted().toIntArray())
        }
    }

    /** Checks every changed node and relationship before passing the changes on. */
    private class Enforcer(
        private val required: RequiredProperties,
        private val storageReader: StorageReader,
        private val read: Read,
        cursorFactory: CursorFactory,
        cursorContext: CursorContext,
        memoryTracker: MemoryTracker,
        next: TxStateVisitor,
    ) : TxStateVisitor.Delegator(next) {
        private val nodes = cursorFactory.allocateFullAccessNodeCursor(cursorContext, memoryTracker)
        private val relationships =
            cursorFactory.allocateFullAccessRelationshipScanCursor(cursorContext, memoryTracker)
        private val properties =
            cursorFactory.allocateFullAccessPropertyCursor(cursorContext, memoryTracker)
        private val present = IntHashSet()

        override fun visitNodePropertyChanges(
            id: Long,
            added: Iterable<StorageProperty>,
            changed: Iterable<StorageProperty>,
            removed: IntIterable,
        ) {
            checkNode(id)
            super.visitNodePropertyChanges(id, added, changed, removed)
        }

        override fun visitNodeLabelChanges(id: Long, added: LongSet, removed: LongSet) {
            checkNode(id)
            super.visitNodeLabelChanges(id, added, removed)
        }

        override fun visitRelationshipModifications(modifications: RelationshipModifications) {
            if (!required.byType.isEmpty) {
                val check =
                    RelationshipVisitorWithProperties<RuntimeException> { id, type, _, _, _, _, _ ->
                        checkRelationship(id, type)
                    }
                modifications.creations().forEach(check)
                modifications.updates().forEach(check)
            }
            super.visitRelationshipModifications(modifications)
        }

        private fun checkNode(id: Long) {
            if (required.byLabel.isEmpty) return
            read.singleNode(id, nodes)
            // Deleted in this transaction: nothing left to violate a constraint.
            if (!nodes.next()) return
            val labels = nodes.labels()
            var loaded = false
            for (i in 0 until labels.numberOfTokens()) {
                val label = labels.token(i)
                val keys = required.byLabel.get(label) ?: continue
                if (!loaded) {
                    load(nodes)
                    loaded = true
                }
                for (key in keys) if (!present.contains(key)) throw nodeViolation(id, label, key)
            }
        }

        private fun checkRelationship(id: Long, type: Int) {
            val keys = required.byType.get(type) ?: return
            read.singleRelationship(id, relationships)
            if (!relationships.next()) return
            load(relationships)
            for (key in keys) {
                if (!present.contains(key)) throw relationshipViolation(id, type, key)
            }
        }

        private fun load(entity: EntityCursor) {
            present.clear()
            entity.properties(properties)
            while (properties.next()) present.add(properties.propertyKey())
        }

        private fun nodeViolation(id: Long, label: Int, key: Int): ConstraintValidationException {
            for (schema in required.nodeSchemas) {
                if (schema.labelId != label || key !in schema.propertyIds) continue
                val constraint = existenceConstraint(schema) ?: continue
                return NodePropertyExistenceException.propertyPresenceViolation(
                    schema,
                    storageReader.tokenNameLookup(),
                    constraint,
                    ConstraintValidationException.Phase.VALIDATION,
                    id,
                )
            }
            error("no constraint requires property $key of label $label")
        }

        private fun relationshipViolation(
            id: Long,
            type: Int,
            key: Int,
        ): ConstraintValidationException {
            for (schema in required.relationshipSchemas) {
                if (schema.relTypeId != type || key !in schema.propertyIds) continue
                val constraint = existenceConstraint(schema) ?: continue
                return RelationshipPropertyExistenceException.propertyPresenceViolation(
                    schema,
                    storageReader.tokenNameLookup(),
                    constraint,
                    ConstraintValidationException.Phase.VALIDATION,
                    id,
                )
            }
            error("no constraint requires property $key of relationship type $type")
        }

        private fun existenceConstraint(schema: SchemaDescriptor): ConstraintDescriptor? =
            storageReader.constraintsGetForSchema(schema).asSequence().firstOrNull {
                it.enforcesPropertyExistence()
            }

        override fun close() {
            try {
                super.close()
            } finally {
                IOUtils.closeAllUnchecked(nodes, relationships, properties)
            }
        }
    }

    private companion object {
        /** Above Neo4j Community's standard constraint semantics, priority 1. */
        const val PRIORITY = 2

        val BUILD_REQUIRED_PROPERTIES =
            Function<StorageReader, RequiredProperties> { reader ->
                val nodes = mutableListOf<LabelSchemaDescriptor>()
                val relationships = mutableListOf<RelationTypeSchemaDescriptor>()
                reader.constraintsGetAll().forEachRemaining { constraint ->
                    if (!constraint.enforcesPropertyExistence()) return@forEachRemaining
                    val schema = constraint.schema()
                    if (schema.isSchemaDescriptorType(LabelSchemaDescriptor::class.java)) {
                        nodes += schema.asSchemaDescriptorType(LabelSchemaDescriptor::class.java)
                    }
                    if (schema.isSchemaDescriptorType(RelationTypeSchemaDescriptor::class.java)) {
                        relationships +=
                            schema.asSchemaDescriptorType(RelationTypeSchemaDescriptor::class.java)
                    }
                }
                RequiredProperties(nodes, relationships)
            }
    }
}
