package dev.dodeca.core.graphtype

import org.neo4j.common.EntityType
import org.neo4j.cypher.internal.ast.ShowCurrentGraphTypeClause
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.logical.plans.CommandDefaultColumn
import org.neo4j.cypher.internal.logical.plans.CommandYieldColumn
import org.neo4j.cypher.internal.logical.plans.`GraphType$`
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.Command
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.`InputPosition$`
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.NodeValue
import org.neo4j.values.virtual.RelationshipValue
import org.neo4j.values.virtual.VirtualValues
import scala.collection.immutable.List as ScalaList
import scala.collection.immutable.Map as ScalaMap
import scala.collection.immutable.`Map$`
import scala.jdk.javaapi.CollectionConverters

/**
 * SHOW CURRENT GRAPH TYPE, as Neo4j Enterprise has it: the graph type of the database in Neo4j's
 * canonical form, or, AS GRAPH, as a graph of virtual nodes and relationships. Requires the SHOW
 * CONSTRAINTS privilege.
 */
internal class ShowGraphTypeCommand(
    private val asGraph: Boolean,
    defaultColumns: ScalaList<CommandDefaultColumn>,
    yieldColumns: ScalaList<CommandYieldColumn>,
) : Command(defaultColumns, yieldColumns) {
    override fun originalNameRows(
        state: QueryState,
        baseRow: CypherRow,
    ): ClosingIterator<ScalaMap<String, AnyValue>> {
        val query = state.query()
        query.assertShowConstraintAllowed()
        val tx = query.transactionalContext()
        val graphType = CurrentGraphType.read(tx.schemaRead(), tx.tokenRead()).graphType
        val row =
            if (asGraph) {
                GraphTypeGraph(graphType).columns()
            } else {
                mapOf(
                    ShowCurrentGraphTypeClause.specificationColumn() to
                        Values.utf8Value(GraphTypePlans.specification(graphType))
                )
            }
        return ClosingIterator.single(`Map$`.`MODULE$`.from(CollectionConverters.asScala(row)))
    }
}

/**
 * A graph type as a graph: a NodeElementType node per node element type, a NodeLabel node per other
 * label it implies, has as an endpoint or has constraints on, IMPLIES relationships from node
 * element types to their implied labels, a RELATIONSHIP_ELEMENT_TYPE relationship per relationship
 * element type and a RELATIONSHIP_TYPE one per other relationship type with constraints. An
 * endpoint that is not constrained is an AnyLabel node, one per relationship. The ids are negative,
 * as Neo4j's for virtual entities.
 */
private class GraphTypeGraph(private val graphType: GraphTypeModel) {
    private val graph = `GraphType$`.`MODULE$`
    private var lastId = 0L
    private val anyNodes = mutableListOf<NodeValue>()
    private val elementNodes = linkedMapOf<String, NodeValue>()
    private val labelNodes = sortedMapOf<String, NodeValue>()
    private val relationships = mutableListOf<RelationshipValue>()

    fun columns(): Map<String, AnyValue> {
        for (element in graphType.nodes.values) {
            elementNodes[element.label] =
                node(
                    "NodeElementType",
                    "label" to Values.utf8Value(element.label),
                    "properties" to properties(element.properties),
                    "constraints" to constraints(EntityType.NODE, element.label),
                )
        }
        val labels =
            graphType.nodes.values.flatMap { it.implied } +
                graphType.relationships.values.flatMap { listOfNotNull(it.source, it.target) } +
                graphType.constraints
                    .filter { it.spec.entity == EntityType.NODE }
                    .map { it.spec.token }
        for (label in labels.filterNot { graphType.identifies(it) }.toSortedSet()) {
            labelNodes[label] =
                node(
                    "NodeLabel",
                    "label" to Values.utf8Value(label),
                    "constraints" to constraints(EntityType.NODE, label),
                )
        }
        for (element in graphType.nodes.values) {
            for (implied in element.implied.sorted()) {
                relationship(
                    elementNodes.getValue(element.label),
                    labelNodes.getValue(implied),
                    "IMPLIES",
                )
            }
        }
        for (element in graphType.relationships.values) {
            val any = lazy { anyNode() }
            relationship(
                element.source?.let { endpoint(it) } ?: any.value,
                element.target?.let { endpoint(it) } ?: any.value,
                "RELATIONSHIP_ELEMENT_TYPE",
                "relationshipType" to Values.utf8Value(element.type),
                "properties" to properties(element.properties),
                "constraints" to constraints(EntityType.RELATIONSHIP, element.type),
            )
        }
        val types =
            graphType.constraints
                .filter { it.spec.entity == EntityType.RELATIONSHIP }
                .map { it.spec.token }
                .filterNot { it in graphType.relationships }
                .toSortedSet()
        for (type in types) {
            val any = anyNode()
            relationship(
                any,
                any,
                "RELATIONSHIP_TYPE",
                "relationshipType" to Values.utf8Value(type),
                "constraints" to constraints(EntityType.RELATIONSHIP, type),
            )
        }
        val nodes = anyNodes + elementNodes.values + labelNodes.values
        return mapOf(
            ShowCurrentGraphTypeClause.nodesColumn() to VirtualValues.list(*nodes.toTypedArray()),
            ShowCurrentGraphTypeClause.relationshipsColumn() to
                VirtualValues.list(*relationships.toTypedArray()),
        )
    }

    private fun endpoint(label: String): NodeValue =
        elementNodes[label] ?: labelNodes.getValue(label)

    private fun anyNode(): NodeValue = node("AnyLabel").also { anyNodes += it }

    private fun node(label: String, vararg properties: Pair<String, AnyValue>): NodeValue {
        val id = --lastId
        return VirtualValues.nodeValue(
            id,
            id.toString(),
            Values.stringArray(label),
            VirtualValues.map(
                properties.map { it.first }.toTypedArray(),
                properties.map { it.second }.toTypedArray(),
            ),
        )
    }

    private fun relationship(
        start: NodeValue,
        end: NodeValue,
        type: String,
        vararg properties: Pair<String, AnyValue>,
    ) {
        val id = --lastId
        relationships +=
            VirtualValues.relationshipValue(
                id,
                id.toString(),
                start,
                end,
                Values.utf8Value(type),
                VirtualValues.map(
                    properties.map { it.first }.toTypedArray(),
                    properties.map { it.second }.toTypedArray(),
                ),
            )
    }

    /** The properties of an element type as Neo4j writes them: `name :: STRING NOT NULL`. */
    private fun properties(properties: Map<String, PropertySpec>): AnyValue =
        Values.stringArray(
            *properties.entries
                .sortedBy { it.key }
                .map { (name, spec) ->
                    val key = PropertyKeyName(name, `InputPosition$`.`MODULE$`.NONE())
                    "${graph.stringifyPropertyName(key)} :: " +
                        GraphTypePlans.cypherType(spec).normalizedCypherTypeString()
                }
                .toTypedArray()
        )

    /** The constraints of the graph type on the label or relationship type [token]. */
    private fun constraints(entity: EntityType, token: String): AnyValue {
        val on = graphType.constraints.filter { it.spec.entity == entity && it.spec.token == token }
        val strings = graph.getConstraintsStrings(GraphTypePlans.constraints(graphType, on))
        return Values.stringArray(*CollectionConverters.asJava(strings).toTypedArray())
    }
}
