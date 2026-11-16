package dev.dodeca.core.graphtype

import org.neo4j.common.EntityType
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.logical.plans.GraphType
import org.neo4j.cypher.internal.logical.plans.GraphTypeCreateConstraint
import org.neo4j.cypher.internal.logical.plans.GraphTypeEntry
import org.neo4j.cypher.internal.logical.plans.GraphTypeForAdd
import org.neo4j.cypher.internal.logical.plans.GraphTypeForAlter
import org.neo4j.cypher.internal.logical.plans.GraphTypeForDrop
import org.neo4j.cypher.internal.logical.plans.GraphTypeForSet
import org.neo4j.cypher.internal.logical.plans.NodeElementType
import org.neo4j.cypher.internal.logical.plans.NodeElementTypeReferenceByIdentifyingLabel
import org.neo4j.cypher.internal.logical.plans.NodeElementTypeReferenceForRelationshipElementType
import org.neo4j.cypher.internal.logical.plans.RelationshipElementType
import org.neo4j.cypher.internal.logical.plans.RelationshipElementTypeReferenceByIdentifyingLabel
import org.neo4j.cypher.internal.notification.InternalNotification
import org.neo4j.cypher.internal.optionsmap.IndexBackedConstraintsOptionsConverter
import org.neo4j.cypher.internal.optionsmap.PropertyExistenceOrTypeConstraintOptionsConverter
import org.neo4j.cypher.internal.procs.SchemaExecutionResult
import org.neo4j.cypher.internal.procs.SuccessResult
import org.neo4j.cypher.internal.runtime.IndexProviderContext
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.internal.kernel.api.security.PrivilegeAction
import org.neo4j.values.virtual.MapValue
import scala.Option
import scala.jdk.javaapi.CollectionConverters

/**
 * ALTER CURRENT GRAPH TYPE, as Neo4j Enterprise has it: SET replaces the graph type, constraints
 * made with CREATE CONSTRAINT included; ADD adds element types and constraints; ALTER redefines
 * element types but their key and uniqueness constraints; DROP drops element types, but their key
 * and uniqueness constraints, and constraints by name.
 *
 * The graph type is the database's constraints (see [GraphTypeModel]), which the command changes in
 * the transaction it runs in: dropped first, then created, the ones a SET or ALTER keeps left as
 * they are, so their indexes and checks of existing data stay. Neo4j's kernel creates and drops
 * them: it checks privileges, names the dependent ones, refuses dependent and independent
 * constraints on the same label or relationship type, and checks existing data.
 */
internal class AlterGraphTypeCommand(
    private val plan: GraphType,
    private val cypherVersion: CypherVersion,
) {
    fun run(ctx: QueryContext, params: MapValue): SchemaExecutionResult {
        val tx = ctx.transactionalContext()
        val current = CurrentGraphType.read(tx.schemaRead(), tx.tokenRead())
        val notifications =
            when (plan) {
                is GraphTypeForSet -> {
                    authorize(
                        ctx,
                        PrivilegeAction.CREATE_CONSTRAINT,
                        PrivilegeAction.DROP_CONSTRAINT,
                    )
                    set(ctx, params, current, plan)
                }
                is GraphTypeForAdd -> {
                    authorize(ctx, PrivilegeAction.CREATE_CONSTRAINT)
                    add(ctx, params, current, plan)
                }
                is GraphTypeForAlter -> {
                    authorize(
                        ctx,
                        PrivilegeAction.CREATE_CONSTRAINT,
                        PrivilegeAction.DROP_CONSTRAINT,
                    )
                    alter(ctx, current, plan)
                }
                is GraphTypeForDrop -> {
                    authorize(ctx, PrivilegeAction.DROP_CONSTRAINT)
                    drop(ctx, current, plan)
                }
                else -> error("Unknown graph type operation: $plan")
            }
        return SuccessResult(GraphTypePlans.set(notifications))
    }

    private fun set(
        ctx: QueryContext,
        params: MapValue,
        current: CurrentGraphType,
        plan: GraphTypeForSet,
    ): List<InternalNotification> {
        val entries = entries(plan.elementTypes())
        val statement = CollectionConverters.asJava(plan.constraints()).toList()
        val target =
            GraphTypeModel(
                nodes(entries).associateByTo(sortedMapOf()) { it.label },
                relationships(entries).associateByTo(sortedMapOf()) { it.type },
                statement.map { GraphTypePlans.constraint(it) },
            )
        check(target, entries, statement)
        // The constraints a statement names keep their name; a renamed one is made anew.
        val unmatched = target.constraints.toMutableList()
        val dropped =
            current.graphType.constraints.filter { existing ->
                val match = unmatched.firstOrNull {
                    it.spec == existing.spec && (it.name == null || it.name == existing.name)
                }
                if (match != null) unmatched.remove(match)
                match == null
            }
        val wanted = target.requirements
        for ((requirement, name) in current.names) {
            if (requirement !in wanted) ctx.dropNamedConstraint(name, true)
        }
        for (constraint in dropped) ctx.dropNamedConstraint(constraint.name!!, false)
        for (requirement in wanted) {
            if (requirement !in current.names) ctx.createConstraint(requirement.create)
        }
        return unmatched.flatMap { create(ctx, params, it) }
    }

    private fun add(
        ctx: QueryContext,
        params: MapValue,
        current: CurrentGraphType,
        plan: GraphTypeForAdd,
    ): List<InternalNotification> {
        val existing = current.graphType
        val entries = entries(plan.elementTypes())
        val nodes = nodes(entries)
        val relationships = relationships(entries)
        nodes
            .firstOrNull { it.label in existing.nodes }
            ?.let {
                throw GraphTypeErrors.nodeElementTypeExists(it.label)
            }
        relationships
            .firstOrNull { it.type in existing.relationships }
            ?.let {
                throw GraphTypeErrors.relationshipElementTypeExists(it.type)
            }
        val statement = CollectionConverters.asJava(plan.constraints()).toList()
        val added = statement.map { GraphTypePlans.constraint(it) }
        val target =
            GraphTypeModel(
                (existing.nodes + nodes.associateBy { it.label }).toSortedMap(),
                (existing.relationships + relationships.associateBy { it.type }).toSortedMap(),
                existing.constraints + added,
            )
        check(target, entries, statement)
        for (element in nodes) element.requirements.forEach { ctx.createConstraint(it.create) }
        for (element in relationships) {
            element.requirements.forEach { ctx.createConstraint(it.create) }
        }
        return added.flatMap { create(ctx, params, it) }
    }

    private fun alter(
        ctx: QueryContext,
        current: CurrentGraphType,
        plan: GraphTypeForAlter,
    ): List<InternalNotification> {
        val existing = current.graphType
        val entries = entries(plan.elementTypes())
        val nodes = nodes(entries)
        val relationships = relationships(entries)
        nodes
            .firstOrNull { it.label !in existing.nodes }
            ?.let {
                throw GraphTypeErrors.noNodeElementType(it.label)
            }
        relationships
            .firstOrNull { it.type !in existing.relationships }
            ?.let {
                throw GraphTypeErrors.noRelationshipElementType(it.type)
            }
        val target =
            existing.copy(
                nodes = (existing.nodes + nodes.associateBy { it.label }).toSortedMap(),
                relationships =
                    (existing.relationships + relationships.associateBy { it.type }).toSortedMap(),
            )
        check(target, entries, emptyList())
        val wanted = target.requirements
        for ((requirement, name) in current.names) {
            if (requirement !in wanted) ctx.dropNamedConstraint(name, true)
        }
        for (requirement in wanted) {
            if (requirement !in current.names) ctx.createConstraint(requirement.create)
        }
        return emptyList()
    }

    private fun drop(
        ctx: QueryContext,
        current: CurrentGraphType,
        plan: GraphTypeForDrop,
    ): List<InternalNotification> {
        val existing = current.graphType
        val nodes = mutableListOf<NodeElement>()
        val relationships = mutableListOf<RelationshipElement>()
        for (entry in entries(plan.elementTypes())) {
            when (entry) {
                is NodeElementType -> {
                    val label = entry.identifyingLabel().name()
                    val element =
                        existing.nodes[label] ?: throw GraphTypeErrors.noNodeElementType(label)
                    if (!entry.isJustAnIdentifier && GraphTypePlans.node(entry) != element) {
                        throw GraphTypeErrors.nodeElementTypeDiffers(
                            GraphTypePlans.node(element),
                            entry,
                        )
                    }
                    nodes += element
                }
                is RelationshipElementType -> {
                    val type = entry.identifyingLabel().name()
                    val element =
                        existing.relationships[type]
                            ?: throw GraphTypeErrors.noRelationshipElementType(type)
                    if (!entry.isJustAnIdentifier && !sameForDrop(existing, element, entry)) {
                        throw GraphTypeErrors.relationshipElementTypeDiffers(
                            GraphTypePlans.relationship(existing, element),
                            entry,
                        )
                    }
                    relationships += element
                }
            }
        }
        val droppedTypes = relationships.map { it.type }.toSet()
        for (node in nodes) {
            existing.relationships.values
                .firstOrNull {
                    it.type !in droppedTypes && (it.source == node.label || it.target == node.label)
                }
                ?.let { throw GraphTypeErrors.nodeElementTypeInUse(node.label, it.type) }
        }
        val names = CollectionConverters.asJava(plan.constraints()).map { it.name() }.sorted()
        val dependent = current.names.values.toSet()
        // Neo4j's kernel refuses a constraint that does not exist, or is dependent, before this
        // command changes anything.
        for (name in names) {
            if (!ctx.constraintExists(name) || name in dependent)
                ctx.dropNamedConstraint(name, false)
        }
        for (element in
            nodes.flatMap { it.requirements } + relationships.flatMap { it.requirements }) {
            ctx.dropNamedConstraint(current.names.getValue(element), true)
        }
        for (name in names) ctx.dropNamedConstraint(name, false)
        return emptyList()
    }

    /**
     * Whether [given], a relationship element type a DROP defines in full, is [element]: an
     * endpoint by label may name one by identifying label, as Neo4j's plans have it.
     */
    private fun sameForDrop(
        graphType: GraphTypeModel,
        element: RelationshipElement,
        given: RelationshipElementType,
    ): Boolean {
        fun same(label: String?, endpoint: NodeElementTypeReferenceForRelationshipElementType) =
            label == GraphTypePlans.endpoint(endpoint) &&
                (endpoint !is NodeElementTypeReferenceByIdentifyingLabel ||
                    graphType.identifies(label!!))
        return GraphTypePlans.relationship(given).properties == element.properties &&
            same(element.source, given.sourceNode()) &&
            same(element.target, given.targetNode())
    }

    /**
     * Refuses [target], the graph type a command results in, if a label identifies a node element
     * type and is implied by another, if [entries] or [constraints] of the statement refer to an
     * element type by an identifying label or type that identifies none, or if an independent
     * constraint is on a label or relationship type that identifies an element type.
     */
    private fun check(
        target: GraphTypeModel,
        entries: List<GraphTypeEntry>,
        constraints: List<GraphTypeCreateConstraint>,
    ) {
        val implied = target.nodes.values.flatMapTo(mutableSetOf()) { it.implied }
        val both = target.nodes.keys.filter { it in implied }
        if (both.isNotEmpty()) throw GraphTypeErrors.identifyingAndImplied(both)
        for (entry in entries.filterIsInstance<RelationshipElementType>()) {
            for (endpoint in listOf(entry.sourceNode(), entry.targetNode())) {
                if (endpoint is NodeElementTypeReferenceByIdentifyingLabel) {
                    val label = endpoint.labelName().name()
                    if (!target.identifies(label)) throw GraphTypeErrors.noNodeElementType(label)
                }
            }
        }
        for (constraint in constraints) {
            when (val reference = constraint.reference()) {
                is NodeElementTypeReferenceByIdentifyingLabel -> {
                    val label = reference.labelName().name()
                    if (!target.identifies(label)) throw GraphTypeErrors.noNodeElementType(label)
                }
                is RelationshipElementTypeReferenceByIdentifyingLabel -> {
                    val type = reference.relTypeName().name()
                    if (type !in target.relationships) {
                        throw GraphTypeErrors.noRelationshipElementType(type)
                    }
                }
            }
        }
        for (constraint in target.constraints.filter { it.spec.kind.independent }) {
            val token = constraint.spec.token
            val description =
                constraint.name
                    ?: GraphTypePlans.constraint(target, constraint)
                        .kernelesqueConstraintDescriptor()
            when (constraint.spec.entity) {
                EntityType.NODE ->
                    if (target.identifies(token)) {
                        throw GraphTypeErrors.independentOnNodeElementType(description, token)
                    }
                EntityType.RELATIONSHIP ->
                    if (token in target.relationships) {
                        throw GraphTypeErrors.independentOnRelationshipElementType(
                            description,
                            token,
                        )
                    }
            }
        }
    }

    /**
     * Creates [constraint], a key, uniqueness or independent one, with its OPTIONS, which Neo4j's
     * converters check as CREATE CONSTRAINT does.
     */
    private fun create(
        ctx: QueryContext,
        params: MapValue,
        constraint: GraphConstraint,
    ): List<InternalNotification> {
        val spec = constraint.spec
        val node = spec.entity == EntityType.NODE
        val procedures = Option.apply(ctx.transactionalContext().procedures())
        val context = ctx as IndexProviderContext
        if (spec.kind.independent) {
            PropertyExistenceOrTypeConstraintOptionsConverter(
                    if (node) "node" else "relationship",
                    if (spec.kind == ConstraintKind.EXISTS) "existence" else "type",
                    context,
                )
                .convert(cypherVersion, constraint.options, params, Option.empty(), procedures)
            ctx.createConstraint(constraint.create(null))
            return emptyList()
        }
        val description =
            when (spec.kind) {
                ConstraintKind.KEY -> if (node) "node key" else "relationship key"
                else -> if (node) "node property uniqueness" else "relationship property uniqueness"
            }
        val converted =
            IndexBackedConstraintsOptionsConverter("$description constraint", context)
                .convert(
                    cypherVersion,
                    constraint.options,
                    params,
                    Option.apply(ctx.getConfig()),
                    procedures,
                )
                .toOptionNotification()
        val options = converted._1()
        val provider =
            if (options.isDefined && options.get().provider().isDefined) {
                options.get().provider().get()
            } else {
                null
            }
        ctx.createConstraint(constraint.create(provider))
        return CollectionConverters.asJava(converted._2()).toList()
    }

    /** Refuses the command unless the user may do all of [actions]. */
    private fun authorize(ctx: QueryContext, vararg actions: PrivilegeAction) {
        val tx = ctx.transactionalContext()
        for (action in actions) {
            tx.securityAuthorizationHandler().assertSchemaWrites(tx.securityContext(), action)
        }
    }

    private fun entries(
        entries: scala.collection.immutable.Set<GraphTypeEntry>
    ): List<GraphTypeEntry> = CollectionConverters.asJava(entries).toList()

    private fun nodes(entries: List<GraphTypeEntry>): List<NodeElement> =
        entries.filterIsInstance<NodeElementType>().map { GraphTypePlans.node(it) }

    private fun relationships(entries: List<GraphTypeEntry>): List<RelationshipElement> =
        entries.filterIsInstance<RelationshipElementType>().map { GraphTypePlans.relationship(it) }
}
