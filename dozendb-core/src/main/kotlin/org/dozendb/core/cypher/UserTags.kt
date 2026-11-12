package org.dozendb.core.cypher

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AddTags
import org.neo4j.cypher.internal.ast.RemoveAllTags
import org.neo4j.cypher.internal.ast.RemoveTags
import org.neo4j.cypher.internal.ast.SetTags
import org.neo4j.cypher.internal.ast.UserTagsAction
import org.neo4j.cypher.internal.evaluator.Evaluator
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_LABEL
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_NAME_PROPERTY
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_TAGS_PROPERTY
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlParams
import org.neo4j.gqlstatus.GqlStatusInfoCodes
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Transaction
import org.neo4j.internal.kernel.api.security.AdminActionOnResource
import org.neo4j.internal.kernel.api.security.PrivilegeAction
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.internal.kernel.api.security.Segment
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.MapValue

/**
 * The tags of native users, as Neo4j Enterprise 2026.06 has them: strings that `SET TAGS` replaces,
 * `ADD TAGS` adds to and `REMOVE [ALL] TAGS` removes from, kept in the user's `tags` property.
 * Where the docs say nothing, each tag is kept once, in the order it was first set or added, and a
 * user without tags has no property.
 */
internal object UserTags {
    /**
     * Applies [actions] to the tags of the user [name] in [tx], their values given with [params] in
     * Cypher [version], to [operation] the user ("create" or "alter"). Nothing happens if there is
     * no such user, as `IF EXISTS` has it. Whether the tags changed.
     */
    fun write(
        tx: Transaction,
        name: String,
        actions: List<UserTagsAction>,
        params: MapValue,
        version: CypherVersion,
        operation: String,
    ): Boolean {
        val user = tx.findNode(USER_LABEL, USER_NAME_PROPERTY, name) ?: return false
        return write(user, actions, params, version, operation)
    }

    /** Applies [actions] to the tags of [user]; whether they changed. */
    fun write(
        user: Node,
        actions: List<UserTagsAction>,
        params: MapValue,
        version: CypherVersion,
        operation: String,
    ): Boolean {
        val current = read(user)
        val tags = LinkedHashSet(current)
        fun values(expression: Expression, clause: String): List<String> =
            values(Evaluator.expressionEvaluator(version).evaluate(expression, params)) {
                invalid(it, clause, operation, user.getProperty(USER_NAME_PROPERTY) as String)
            }
        for (action in actions) {
            when (action) {
                is RemoveAllTags -> tags.clear()
                is RemoveTags -> tags.removeAll(values(action.tags(), "REMOVE TAGS").toSet())
                is AddTags -> tags.addAll(values(action.tags(), "ADD TAGS"))
                is SetTags -> {
                    tags.clear()
                    tags.addAll(values(action.tags(), "SET TAGS"))
                }
            }
        }
        val next = tags.toList()
        if (next == current) return false
        if (next.isEmpty()) user.removeProperty(USER_TAGS_PROPERTY)
        else user.setProperty(USER_TAGS_PROPERTY, next.toTypedArray())
        return true
    }

    /** The tags of [user], none if it has no tags. */
    fun read(user: Node): List<String> =
        when (val tags = user.getProperty(USER_TAGS_PROPERTY, null)) {
            is Array<*> -> tags.map { it as String }
            else -> emptyList()
        }

    /** Whether [securityContext] may see the tags of users: SHOW USER METADATA. */
    fun visible(securityContext: SecurityContext): Boolean =
        securityContext
            .allowsAdminAction(
                AdminActionOnResource(
                    PrivilegeAction.SHOW_USER_METADATA,
                    AdminActionOnResource.DatabaseScope.ALL,
                    Segment.ALL,
                )
            )
            .allowsAccess()

    /**
     * The tags [value] gives: a non-empty string or a list of them, as Neo4j's semantic checks have
     * literals, which parameters must be too; else [invalid].
     */
    private fun values(value: AnyValue, invalid: (AnyValue) -> Nothing): List<String> {
        val strings =
            when (value) {
                is TextValue -> listOf(value.stringValue())
                is ListValue -> value.map { (it as? TextValue)?.stringValue() ?: invalid(value) }
                else -> invalid(value)
            }
        if (strings.any { it.isEmpty() }) invalid(value)
        return strings
    }

    private fun invalid(value: AnyValue, clause: String, operation: String, user: String): Nothing {
        val shown = if (value === Values.NO_VALUE) "null" else value.toString()
        throw InvalidArgumentException(
            ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N04)
                .withParam(GqlParams.StringParam.input, shown)
                .withParam(GqlParams.StringParam.context, clause)
                .withParam(
                    GqlParams.ListParam.inputList,
                    listOf("non-empty String", "List of non-empty Strings"),
                )
                .build(),
            "Failed to $operation the specified user '$user': $clause takes a non-empty String " +
                "or a List of non-empty Strings, not $shown.",
        )
    }
}
