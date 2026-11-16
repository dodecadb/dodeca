package dev.dodeca.core.graphtype

import dev.dodeca.core.Gql
import org.neo4j.cypher.internal.ast.prettifier.`GraphTypeStringifier$`
import org.neo4j.cypher.internal.logical.plans.NodeElementType
import org.neo4j.cypher.internal.logical.plans.RelationshipElementType
import org.neo4j.exceptions.SyntaxException
import org.neo4j.gqlstatus.GqlParams.ListParam
import org.neo4j.gqlstatus.GqlParams.StringParam
import org.neo4j.gqlstatus.GqlStatusInfoCodes

/**
 * The errors of graph type commands that their existing graph type causes, which Neo4j's semantic
 * checks of one statement cannot see: 42001, caused by the 22NC status Neo4j's status code
 * documentation gives for each, with the same message.
 */
internal object GraphTypeErrors {
    private val stringifier = `GraphTypeStringifier$`.`MODULE$`

    /** 22NC4: [labels] identify node element types and are implied by others. */
    fun identifyingAndImplied(labels: Collection<String>): SyntaxException =
        error(GqlStatusInfoCodes.STATUS_22NC4, ListParam.labelList to labels.sorted())

    /** 22NC5: no node element type is identified by [label]. */
    fun noNodeElementType(label: String): SyntaxException =
        error(
            GqlStatusInfoCodes.STATUS_22NC5,
            StringParam.entityType to "node",
            StringParam.graphTypeReference to
                node(GraphTypePlans.node(NodeElement(label, setOf(), mapOf()))),
        )

    /** 22NC5: no relationship element type is identified by [type]. */
    fun noRelationshipElementType(type: String): SyntaxException =
        error(
            GqlStatusInfoCodes.STATUS_22NC5,
            StringParam.entityType to "relationship",
            StringParam.graphTypeReference to identifier(type),
        )

    /** 22NC6: [constraint], independent, is on [label], which identifies a node element type. */
    fun independentOnNodeElementType(constraint: String, label: String): SyntaxException =
        error(
            GqlStatusInfoCodes.STATUS_22NC6,
            StringParam.constrDescrOrName to constraint,
            StringParam.label to label,
        )

    /**
     * 22NC7: [constraint], independent, is on [type], which identifies a relationship element type.
     */
    fun independentOnRelationshipElementType(constraint: String, type: String): SyntaxException =
        error(
            GqlStatusInfoCodes.STATUS_22NC7,
            StringParam.constrDescrOrName to constraint,
            StringParam.relType to type,
        )

    /** 22NCA: a node element type identified by [label] exists already. */
    fun nodeElementTypeExists(label: String): SyntaxException =
        error(GqlStatusInfoCodes.STATUS_22NCA, StringParam.label to label)

    /** 22NCB: a relationship element type identified by [type] exists already. */
    fun relationshipElementTypeExists(type: String): SyntaxException =
        error(GqlStatusInfoCodes.STATUS_22NCB, StringParam.relType to type)

    /** 22NCC: the node element type [existing] differs from [given], which a DROP names. */
    fun nodeElementTypeDiffers(existing: NodeElementType, given: NodeElementType): SyntaxException =
        error(
            GqlStatusInfoCodes.STATUS_22NCC,
            StringParam.graphTypeElement1 to node(existing),
            StringParam.label to existing.identifyingLabel().name(),
            StringParam.graphTypeElement2 to node(given),
        )

    /** 22NCD: the relationship element type [existing] differs from [given], which a DROP names. */
    fun relationshipElementTypeDiffers(
        existing: RelationshipElementType,
        given: RelationshipElementType,
    ): SyntaxException =
        error(
            GqlStatusInfoCodes.STATUS_22NCD,
            StringParam.graphTypeElement1 to relationship(existing),
            StringParam.relType to existing.identifyingLabel().name(),
            StringParam.graphTypeElement2 to relationship(given),
        )

    /**
     * 22NCE: the node element type of [label] is an endpoint of the relationship element type of
     * [type].
     */
    fun nodeElementTypeInUse(label: String, type: String): SyntaxException =
        error(
            GqlStatusInfoCodes.STATUS_22NCE,
            StringParam.label to label,
            StringParam.graphTypeReference to identifier(type),
        )

    private fun node(element: NodeElementType): String =
        stringifier.stringifyNodeType(element.toAstObject())

    private fun relationship(element: RelationshipElementType): String =
        stringifier.stringifyEdgeType(element.toAstObject())

    /** The relationship element type of [type] as a DROP names it: `()-[:TYPE =>]->()`. */
    private fun identifier(type: String): String =
        relationship(
            GraphTypePlans.relationship(
                GraphTypeModel.EMPTY,
                RelationshipElement(type, null, null, mapOf()),
            )
        )

    private fun error(
        code: GqlStatusInfoCodes,
        vararg params: Pair<org.neo4j.gqlstatus.GqlParams.GqlParam, Any>,
    ): SyntaxException {
        val cause = Gql.status(code, *params)
        return SyntaxException(
            Gql.status(GqlStatusInfoCodes.STATUS_42001, cause = cause),
            cause.message.substringAfter(": "),
        )
    }
}
