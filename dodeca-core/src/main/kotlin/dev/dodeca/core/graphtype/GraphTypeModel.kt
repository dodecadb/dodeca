package dev.dodeca.core.graphtype

import org.neo4j.common.EntityType
import org.neo4j.cypher.internal.ast.`NoOptions$`
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.logical.plans.`EmptyNodeElementTypeReference$`
import org.neo4j.cypher.internal.logical.plans.`ExistenceConstraint$`
import org.neo4j.cypher.internal.logical.plans.GraphElementTypeReferenceForConstraint
import org.neo4j.cypher.internal.logical.plans.GraphTypeConstraintType
import org.neo4j.cypher.internal.logical.plans.GraphTypeCreateConstraint
import org.neo4j.cypher.internal.logical.plans.GraphTypeEntry
import org.neo4j.cypher.internal.logical.plans.`KeyConstraint$`
import org.neo4j.cypher.internal.logical.plans.NodeElementType
import org.neo4j.cypher.internal.logical.plans.NodeElementTypeReferenceByIdentifyingLabel
import org.neo4j.cypher.internal.logical.plans.NodeElementTypeReferenceByLabel
import org.neo4j.cypher.internal.logical.plans.NodeElementTypeReferenceForConstraint
import org.neo4j.cypher.internal.logical.plans.NodeElementTypeReferenceForRelationshipElementType
import org.neo4j.cypher.internal.logical.plans.PropertyType
import org.neo4j.cypher.internal.logical.plans.PropertyTypeConstraint
import org.neo4j.cypher.internal.logical.plans.RelationshipElementType
import org.neo4j.cypher.internal.logical.plans.RelationshipElementTypeReferenceByIdentifyingLabel
import org.neo4j.cypher.internal.logical.plans.RelationshipElementTypeReferenceByLabel
import org.neo4j.cypher.internal.logical.plans.RelationshipElementTypeReferenceForConstraint
import org.neo4j.cypher.internal.logical.plans.`UniquenessConstraint$`
import org.neo4j.cypher.internal.procs.`PropertyTypeMapper$`
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.`InputPosition$`
import org.neo4j.cypher.internal.util.symbols.AnyType
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.internal.kernel.api.SchemaRead
import org.neo4j.internal.kernel.api.TokenRead
import org.neo4j.internal.schema.ConstraintType
import org.neo4j.internal.schema.EndpointType
import org.neo4j.internal.schema.GraphTypeDependence
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create
import org.neo4j.internal.schema.constraints.PropertyTypeSet
import scala.Option
import scala.collection.immutable.`ArraySeq$`
import scala.jdk.javaapi.CollectionConverters

/**
 * A graph type, Neo4j Enterprise's schema of a database: node element types by their identifying
 * label, relationship element types by their identifying relationship type, and the [constraints]
 * outside them. Neo4j keeps a graph type as nothing but constraints, and so does Dodeca: the
 * element types are the dependent constraints, and the constraints are the key, uniqueness and
 * independent existence and property type constraints. [CurrentGraphType.read] makes one of a
 * database's constraints.
 */
internal data class GraphTypeModel(
    val nodes: Map<String, NodeElement>,
    val relationships: Map<String, RelationshipElement>,
    val constraints: List<GraphConstraint>,
) {
    /** The dependent constraints the element types consist of. */
    val requirements: Set<Requirement>
        get() =
            nodes.values.flatMapTo(mutableSetOf()) { it.requirements } +
                relationships.values.flatMap { it.requirements }

    /** Whether [label] identifies a node element type. */
    fun identifies(label: String): Boolean = label in nodes

    companion object {
        val EMPTY = GraphTypeModel(emptyMap(), emptyMap(), emptyList())
    }
}

/** A property of an element type: the types it may have, any if null, and whether it must exist. */
internal data class PropertySpec(val types: PropertyTypeSet?, val required: Boolean)

/** A node element type: its [label] implies the [implied] labels and constrains [properties]. */
internal data class NodeElement(
    val label: String,
    val implied: Set<String>,
    val properties: Map<String, PropertySpec>,
) {
    val requirements: List<Requirement>
        get() =
            implied.map { Requirement.Implied(label, it) } +
                properties.flatMap { (name, spec) ->
                    Requirement.ofProperty(EntityType.NODE, label, name, spec)
                }
}

/**
 * A relationship element type: relationships of its [type] start at a node with the label [source],
 * end at one with [target], either unconstrained if null, and have [properties].
 */
internal data class RelationshipElement(
    val type: String,
    val source: String?,
    val target: String?,
    val properties: Map<String, PropertySpec>,
) {
    val requirements: List<Requirement>
        get() =
            listOfNotNull(
                source?.let { Requirement.Endpoint(type, it, EndpointType.START) },
                target?.let { Requirement.Endpoint(type, it, EndpointType.END) },
            ) +
                properties.flatMap { (name, spec) ->
                    Requirement.ofProperty(EntityType.RELATIONSHIP, type, name, spec)
                }
}

/** A dependent constraint, part of an element type, which the kernel names. */
internal sealed interface Requirement {
    /** The command that creates it. */
    val create: Create

    /** Nodes with [label] also have [implied]. */
    data class Implied(val label: String, val implied: String) : Requirement {
        override val create: Create
            get() = Create.NodeLabelExistence(null, label, implied, false)
    }

    /** The [end] of relationships of [type] has [label]. */
    data class Endpoint(val type: String, val label: String, val end: EndpointType) : Requirement {
        override val create: Create
            get() = Create.RelationshipEndpointLabel(null, type, label, end, false)
    }

    /** Nodes with the label, or relationships of the type, [token] have [property]. */
    data class Exists(val entity: EntityType, val token: String, val property: String) :
        Requirement {
        override val create: Create
            get() =
                when (entity) {
                    EntityType.NODE -> Create.NodeExistence(null, token, property, true, false)
                    EntityType.RELATIONSHIP ->
                        Create.RelationshipExistence(null, token, property, true, false)
                }
    }

    /** [property] of the nodes or relationships of [token] is one of [types]. */
    data class Typed(
        val entity: EntityType,
        val token: String,
        val property: String,
        val types: PropertyTypeSet,
    ) : Requirement {
        override val create: Create
            get() =
                when (entity) {
                    EntityType.NODE ->
                        Create.NodePropertyType(null, token, property, types, null, true, false)
                    EntityType.RELATIONSHIP ->
                        Create.RelationshipPropertyType(
                            null,
                            token,
                            property,
                            types,
                            null,
                            true,
                            false,
                        )
                }
    }

    companion object {
        fun ofProperty(
            entity: EntityType,
            token: String,
            property: String,
            spec: PropertySpec,
        ): List<Requirement> =
            listOfNotNull(
                Exists(entity, token, property).takeIf { spec.required },
                spec.types?.let { Typed(entity, token, property, it) },
            )
    }
}

/** What a constraint outside the element types requires. */
internal enum class ConstraintKind {
    KEY,
    UNIQUE,
    EXISTS,
    TYPE;

    /** Whether constraints of this kind are independent, rather than undesignated. */
    val independent: Boolean
        get() = this == EXISTS || this == TYPE
}

/**
 * A constraint outside the element types: that the nodes with the label, or relationships of the
 * type, [token] satisfy [kind] for [properties], of [types] for a property type constraint.
 */
internal data class ConstraintSpec(
    val entity: EntityType,
    val token: String,
    val properties: List<String>,
    val kind: ConstraintKind,
    val types: PropertyTypeSet?,
)

/** A constraint outside the element types, by [name] if it has one yet. */
internal data class GraphConstraint(
    val name: String?,
    val spec: ConstraintSpec,
    val options: Options = `NoOptions$`.`MODULE$`,
) {
    /** The command that creates it, with [provider] for the index of a key or uniqueness one. */
    fun create(provider: org.neo4j.internal.schema.IndexProviderDescriptor?): Create {
        val (entity, token, properties, kind, types) = spec
        val node = entity == EntityType.NODE
        return when (kind) {
            ConstraintKind.KEY ->
                if (node) {
                    Create.NodeKey(name, token, properties, provider, false)
                } else {
                    Create.RelationshipKey(name, token, properties, provider, false)
                }
            ConstraintKind.UNIQUE ->
                if (node) {
                    Create.NodeUniqueness(name, token, properties, provider, false)
                } else {
                    Create.RelationshipUniqueness(name, token, properties, provider, false)
                }
            ConstraintKind.EXISTS ->
                if (node) {
                    Create.NodeExistence(name, token, properties.single(), false, false)
                } else {
                    Create.RelationshipExistence(name, token, properties.single(), false, false)
                }
            ConstraintKind.TYPE ->
                if (node) {
                    Create.NodePropertyType(
                        name,
                        token,
                        properties.single(),
                        types,
                        null,
                        false,
                        false,
                    )
                } else {
                    Create.RelationshipPropertyType(
                        name,
                        token,
                        properties.single(),
                        types,
                        null,
                        false,
                        false,
                    )
                }
        }
    }
}

/** The graph type of a database, with the names of the constraints its element types consist of. */
internal class CurrentGraphType(
    val graphType: GraphTypeModel,
    val names: Map<Requirement, String>,
) {
    companion object {
        /** The graph type the constraints [schema] holds make, with names from [tokens]. */
        fun read(schema: SchemaRead, tokens: TokenRead): CurrentGraphType {
            val names = linkedMapOf<Requirement, String>()
            val constraints = mutableListOf<GraphConstraint>()
            schema.constraintsGetAll().forEachRemaining { constraint ->
                val descriptor = constraint.schema()
                val entity = descriptor.entityType()
                val tokenId = descriptor.entityTokenIds.single()
                val token =
                    when (entity) {
                        EntityType.NODE -> tokens.nodeLabelName(tokenId)
                        EntityType.RELATIONSHIP -> tokens.relationshipTypeName(tokenId)
                    }
                val properties = descriptor.propertyIds.map { tokens.propertyKeyName(it) }
                val dependent = constraint.graphTypeDependence() == GraphTypeDependence.DEPENDENT
                when {
                    constraint.isNodeLabelExistenceConstraint -> {
                        val required = constraint.asNodeLabelExistenceConstraint().requiredLabelId()
                        names[Requirement.Implied(token, tokens.nodeLabelName(required))] =
                            constraint.name
                    }
                    constraint.isRelationshipEndpointLabelConstraint -> {
                        val endpoint = constraint.asRelationshipEndpointLabelConstraint()
                        names[
                            Requirement.Endpoint(
                                token,
                                tokens.nodeLabelName(endpoint.endpointLabelId()),
                                endpoint.endpointType(),
                            )] = constraint.name
                    }
                    dependent && constraint.type() == ConstraintType.EXISTS ->
                        names[Requirement.Exists(entity, token, properties.single())] =
                            constraint.name
                    dependent && constraint.type() == ConstraintType.PROPERTY_TYPE ->
                        names[
                            Requirement.Typed(
                                entity,
                                token,
                                properties.single(),
                                constraint.asPropertyTypeConstraint().propertyType(),
                            )] = constraint.name
                    else -> {
                        val kind =
                            when (constraint.type()) {
                                ConstraintType.UNIQUE_EXISTS -> ConstraintKind.KEY
                                ConstraintType.UNIQUE -> ConstraintKind.UNIQUE
                                ConstraintType.EXISTS -> ConstraintKind.EXISTS
                                else -> ConstraintKind.TYPE
                            }
                        val types =
                            if (kind == ConstraintKind.TYPE) {
                                constraint.asPropertyTypeConstraint().propertyType()
                            } else {
                                null
                            }
                        constraints +=
                            GraphConstraint(
                                constraint.name,
                                ConstraintSpec(entity, token, properties, kind, types),
                            )
                    }
                }
            }
            return CurrentGraphType(elements(names.keys, constraints), names)
        }

        /** The graph type whose element types consist of [requirements]. */
        fun elements(
            requirements: Collection<Requirement>,
            constraints: List<GraphConstraint>,
        ): GraphTypeModel {
            val nodes = sortedMapOf<String, NodeElement>()
            val relationships = sortedMapOf<String, RelationshipElement>()
            fun node(label: String) = nodes.getOrPut(label) { NodeElement(label, setOf(), mapOf()) }
            fun relationship(type: String) =
                relationships.getOrPut(type) { RelationshipElement(type, null, null, mapOf()) }
            fun property(
                properties: Map<String, PropertySpec>,
                name: String,
                change: (PropertySpec) -> PropertySpec,
            ) = properties + (name to change(properties[name] ?: PropertySpec(null, false)))
            for (requirement in requirements) {
                when (requirement) {
                    is Requirement.Implied ->
                        node(requirement.label).let {
                            nodes[it.label] = it.copy(implied = it.implied + requirement.implied)
                        }
                    is Requirement.Endpoint ->
                        relationship(requirement.type).let {
                            relationships[it.type] =
                                when (requirement.end) {
                                    EndpointType.START -> it.copy(source = requirement.label)
                                    EndpointType.END -> it.copy(target = requirement.label)
                                }
                        }
                    is Requirement.Exists ->
                        if (requirement.entity == EntityType.NODE) {
                            node(requirement.token).let {
                                nodes[it.label] =
                                    it.copy(
                                        properties =
                                            property(it.properties, requirement.property) { spec ->
                                                spec.copy(required = true)
                                            }
                                    )
                            }
                        } else {
                            relationship(requirement.token).let {
                                relationships[it.type] =
                                    it.copy(
                                        properties =
                                            property(it.properties, requirement.property) { spec ->
                                                spec.copy(required = true)
                                            }
                                    )
                            }
                        }
                    is Requirement.Typed ->
                        if (requirement.entity == EntityType.NODE) {
                            node(requirement.token).let {
                                nodes[it.label] =
                                    it.copy(
                                        properties =
                                            property(it.properties, requirement.property) { spec ->
                                                spec.copy(types = requirement.types)
                                            }
                                    )
                            }
                        } else {
                            relationship(requirement.token).let {
                                relationships[it.type] =
                                    it.copy(
                                        properties =
                                            property(it.properties, requirement.property) { spec ->
                                                spec.copy(types = requirement.types)
                                            }
                                    )
                            }
                        }
                }
            }
            return GraphTypeModel(nodes, relationships, constraints)
        }
    }
}

/**
 * Conversions between [GraphTypeModel] and the classes of Neo4j's graph type plans, whose
 * stringifiers give graph types the canonical form Neo4j shows them in.
 */
internal object GraphTypePlans {
    private val NONE: InputPosition = `InputPosition$`.`MODULE$`.NONE()
    private val mapper = `PropertyTypeMapper$`.`MODULE$`

    // From plans.

    fun node(element: NodeElementType): NodeElement =
        NodeElement(
            element.identifyingLabel().name(),
            CollectionConverters.asJava(element.additionalLabels()).mapTo(sortedSetOf()) {
                it.name()
            },
            properties(element.propertyTypes()),
        )

    fun relationship(element: RelationshipElementType): RelationshipElement =
        RelationshipElement(
            element.identifyingLabel().name(),
            endpoint(element.sourceNode()),
            endpoint(element.targetNode()),
            properties(element.propertyTypes()),
        )

    fun constraint(constraint: GraphTypeCreateConstraint): GraphConstraint {
        val reference = constraint.reference()
        val (entity, token) =
            when (reference) {
                is NodeElementTypeReferenceForConstraint ->
                    EntityType.NODE to reference.labelName().name()
                is RelationshipElementTypeReferenceForConstraint ->
                    EntityType.RELATIONSHIP to reference.relTypeName().name()
                else -> error("Unknown graph type constraint reference: $reference")
            }
        val type = constraint.constraintType()
        val kind =
            when (type) {
                is `KeyConstraint$` -> ConstraintKind.KEY
                is `UniquenessConstraint$` -> ConstraintKind.UNIQUE
                is `ExistenceConstraint$` -> ConstraintKind.EXISTS
                else -> ConstraintKind.TYPE
            }
        val types =
            (type as? PropertyTypeConstraint)?.let { mapper.asPropertyTypeSet(it.propertyType()) }
        return GraphConstraint(
            constraint.name().getOrElse { null },
            ConstraintSpec(
                entity,
                token,
                CollectionConverters.asJava(constraint.properties()).map { it.name() },
                kind,
                types,
            ),
            constraint.options(),
        )
    }

    /** The label of [endpoint], null if it is empty. */
    fun endpoint(endpoint: NodeElementTypeReferenceForRelationshipElementType): String? =
        when (endpoint) {
            is NodeElementTypeReferenceByLabel -> endpoint.labelName().name()
            is NodeElementTypeReferenceByIdentifyingLabel -> endpoint.labelName().name()
            else -> null
        }

    private fun properties(
        types: scala.collection.immutable.Set<PropertyType>
    ): Map<String, PropertySpec> =
        CollectionConverters.asJava(types).associateTo(sortedMapOf()) {
            val type = it.propertyType()
            it.name().name() to
                PropertySpec(
                    if (type is AnyType) null else mapper.asPropertyTypeSet(type),
                    !type.isNullable,
                )
        }

    // To plans.

    fun entries(graphType: GraphTypeModel): scala.collection.immutable.Set<GraphTypeEntry> =
        set(
            graphType.nodes.values.map { node(it) } +
                graphType.relationships.values.map { relationship(graphType, it) }
        )

    fun constraints(
        graphType: GraphTypeModel,
        constraints: Collection<GraphConstraint> = graphType.constraints,
    ): scala.collection.immutable.Set<GraphTypeCreateConstraint> =
        set(constraints.map { constraint(graphType, it) })

    fun node(element: NodeElement): NodeElementType =
        NodeElementType(
            LabelName(element.label, NONE),
            set(element.implied.map { LabelName(it, NONE) }),
            propertyTypes(element.properties),
        )

    /** [element], whose endpoints are by identifying label if they identify node element types. */
    fun relationship(
        graphType: GraphTypeModel,
        element: RelationshipElement,
    ): RelationshipElementType =
        RelationshipElementType(
            RelTypeName(element.type, NONE),
            reference(graphType, element.source),
            reference(graphType, element.target),
            propertyTypes(element.properties),
        )

    /** [constraint], by identifying label or type if it is one in [graphType]. */
    fun constraint(
        graphType: GraphTypeModel,
        constraint: GraphConstraint,
    ): GraphTypeCreateConstraint {
        val spec = constraint.spec
        val reference: GraphElementTypeReferenceForConstraint =
            when (spec.entity) {
                EntityType.NODE ->
                    if (graphType.identifies(spec.token)) {
                        NodeElementTypeReferenceByIdentifyingLabel(LabelName(spec.token, NONE))
                    } else {
                        NodeElementTypeReferenceByLabel(LabelName(spec.token, NONE))
                    }
                EntityType.RELATIONSHIP ->
                    if (spec.token in graphType.relationships) {
                        RelationshipElementTypeReferenceByIdentifyingLabel(
                            RelTypeName(spec.token, NONE)
                        )
                    } else {
                        RelationshipElementTypeReferenceByLabel(RelTypeName(spec.token, NONE))
                    }
            }
        val type: GraphTypeConstraintType =
            when (spec.kind) {
                ConstraintKind.KEY -> `KeyConstraint$`.`MODULE$`
                ConstraintKind.UNIQUE -> `UniquenessConstraint$`.`MODULE$`
                ConstraintKind.EXISTS -> `ExistenceConstraint$`.`MODULE$`
                ConstraintKind.TYPE -> PropertyTypeConstraint(mapper.asCypherType(spec.types))
            }
        return GraphTypeCreateConstraint(
            Option.apply(constraint.name),
            reference,
            `ArraySeq$`.`MODULE$`.unsafeWrapArray(
                spec.properties.map { PropertyKeyName(it, NONE) }.toTypedArray()
            ),
            type,
            `NoOptions$`.`MODULE$`,
        )
    }

    /** The Cypher type of [spec], NOT NULL if the property must exist. */
    fun cypherType(spec: PropertySpec): CypherType {
        val type = spec.types?.let { mapper.asCypherType(it) } ?: AnyType(true, NONE)
        return if (spec.required) type.withIsNullable(false) else type
    }

    private fun reference(
        graphType: GraphTypeModel,
        label: String?,
    ): NodeElementTypeReferenceForRelationshipElementType =
        when {
            label == null -> `EmptyNodeElementTypeReference$`.`MODULE$`
            graphType.identifies(label) ->
                NodeElementTypeReferenceByIdentifyingLabel(LabelName(label, NONE))
            else -> NodeElementTypeReferenceByLabel(LabelName(label, NONE))
        }

    private fun propertyTypes(
        properties: Map<String, PropertySpec>
    ): scala.collection.immutable.Set<PropertyType> =
        set(
            properties.map { (name, spec) ->
                PropertyType(PropertyKeyName(name, NONE), cypherType(spec))
            }
        )

    fun <T> set(items: Collection<T>): scala.collection.immutable.Set<T> =
        CollectionConverters.asScala(items.toList()).toSet()
}
