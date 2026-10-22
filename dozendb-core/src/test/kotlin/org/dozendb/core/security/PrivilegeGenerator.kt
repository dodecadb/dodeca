package org.dozendb.core.security

import java.util.UUID
import kotlin.random.Random
import org.neo4j.internal.kernel.api.security.PatternSegment
import org.neo4j.internal.kernel.api.security.PrivilegeAction
import org.neo4j.internal.kernel.api.security.PropertyRule
import org.neo4j.values.storable.Values

/**
 * Privileges as commands can give them, at random, about [databases] and a few names: labels A, B
 * and Z (which no node has), types R and S, properties p and q, users alice and bob.
 */
class PrivilegeGenerator(private val random: Random, private val databases: List<UUID>) {
    fun next(patterns: Boolean = true): Privilege {
        val access = Access.entries.random(random)
        val action = PrivilegeAction.entries.filter { it != PrivilegeAction.ADMIN }.random(random)
        val scopes =
            listOf(
                PrivilegeScope.AllDatabases,
                PrivilegeScope.Home,
            ) + databases.map(PrivilegeScope::Database)
        fun <T> pick(vararg options: T): T = options.random(random)
        fun labelled() =
            pick(
                PrivilegeSegment.Nodes(null),
                PrivilegeSegment.Nodes("A"),
                PrivilegeSegment.Nodes("B"),
                PrivilegeSegment.Nodes("Z"),
                PrivilegeSegment.Relationships(null),
                PrivilegeSegment.Relationships("R"),
                PrivilegeSegment.Database,
            )
        fun properties() =
            pick(
                PrivilegeResource.AllProperties,
                PrivilegeResource.Property("p"),
                PrivilegeResource.Property("q"),
            )
        return when (action) {
            PrivilegeAction.TRAVERSE ->
                Privilege(
                    access,
                    action,
                    scopes.random(random),
                    PrivilegeResource.Graph,
                    if (patterns) pick(labelled(), pattern()) else labelled(),
                )
            PrivilegeAction.READ,
            PrivilegeAction.MATCH ->
                Privilege(
                    access,
                    action,
                    scopes.random(random),
                    properties(),
                    if (patterns) pick(labelled(), pattern()) else labelled(),
                )
            PrivilegeAction.SET_PROPERTY,
            PrivilegeAction.MERGE ->
                Privilege(access, action, scopes.random(random), properties(), labelled())
            PrivilegeAction.SET_LABEL,
            PrivilegeAction.REMOVE_LABEL ->
                Privilege(
                    access,
                    action,
                    scopes.random(random),
                    pick(
                        PrivilegeResource.AllLabels,
                        PrivilegeResource.Label("A"),
                        PrivilegeResource.Label("C"),
                    ),
                    PrivilegeSegment.Nodes(null),
                )
            PrivilegeAction.CREATE_ELEMENT,
            PrivilegeAction.DELETE_ELEMENT,
            PrivilegeAction.WRITE ->
                Privilege(
                    access,
                    action,
                    scopes.random(random),
                    PrivilegeResource.Graph,
                    labelled(),
                )
            PrivilegeAction.GRAPH_ACTIONS ->
                Privilege(access, action, scopes.random(random), PrivilegeResource.Graph)
            PrivilegeAction.SHOW_TRANSACTION,
            PrivilegeAction.TERMINATE_TRANSACTION,
            PrivilegeAction.TRANSACTION_MANAGEMENT ->
                Privilege(
                    access,
                    action,
                    scopes.random(random),
                    segment =
                        pick(
                            PrivilegeSegment.Users("alice"),
                            PrivilegeSegment.Users("bob"),
                            PrivilegeSegment.Users(null),
                        ),
                )
            PrivilegeAction.IMPERSONATE ->
                Privilege(
                    access,
                    action,
                    PrivilegeScope.Dbms,
                    segment =
                        pick(
                            PrivilegeSegment.Users("alice"),
                            PrivilegeSegment.Users("bob"),
                            PrivilegeSegment.Users(null),
                        ),
                )
            PrivilegeAction.EXECUTE,
            PrivilegeAction.EXECUTE_BOOSTED ->
                Privilege(
                    access,
                    action,
                    PrivilegeScope.Dbms,
                    segment =
                        pick(
                            PrivilegeSegment.Procedures(pick(*GLOBS)),
                            PrivilegeSegment.Functions(pick(*GLOBS)),
                        ),
                )
            PrivilegeAction.SHOW_SETTING ->
                Privilege(
                    access,
                    action,
                    PrivilegeScope.Dbms,
                    segment = PrivilegeSegment.Settings(pick(*GLOBS)),
                )
            PrivilegeAction.LOAD ->
                Privilege(access, action, PrivilegeScope.Dbms, segment = PrivilegeSegment.AllData)
            PrivilegeAction.LOAD_CIDR,
            PrivilegeAction.LOAD_URL ->
                Privilege(
                    access,
                    action,
                    PrivilegeScope.Dbms,
                    segment = PrivilegeSegment.Cidr(pick("10.0.0.0/8", "10.1.0.0/16", "::1/128")),
                )
            else ->
                if (ReferencePolicy.COVERS.getValue(action).any { it in DATABASE_ACTIONS }) {
                    Privilege(access, action, scopes.random(random))
                } else {
                    Privilege(access, action, PrivilegeScope.Dbms)
                }
        }
    }

    private fun pattern(): PrivilegeSegment =
        PrivilegeSegment.NodePattern(
            if (random.nextBoolean()) {
                PatternSegment.ValuePatternSegment(
                    listOf(emptySet(), setOf("A"), setOf("A", "B")).random(random),
                    "p",
                    Values.intValue(random.nextInt(0, 3)),
                    PropertyRule.ComparisonOperator.entries.random(random),
                )
            } else {
                PatternSegment.NullPatternSegment(
                    listOf(emptySet(), setOf("B")).random(random),
                    "p",
                    PropertyRule.NullOperator.entries.random(random),
                )
            }
        )

    private companion object {
        val GLOBS = arrayOf("*", "db.*", "apoc.load.?son", "dbms.security*", "DB.LABELS")
        val DATABASE_ACTIONS =
            setOf(
                PrivilegeAction.ACCESS,
                PrivilegeAction.CREATE_LABEL,
                PrivilegeAction.CREATE_RELTYPE,
                PrivilegeAction.CREATE_PROPERTYKEY,
                PrivilegeAction.CREATE_INDEX,
                PrivilegeAction.DROP_INDEX,
                PrivilegeAction.SHOW_INDEX,
                PrivilegeAction.CREATE_CONSTRAINT,
                PrivilegeAction.DROP_CONSTRAINT,
                PrivilegeAction.SHOW_CONSTRAINT,
                PrivilegeAction.START_DATABASE,
                PrivilegeAction.STOP_DATABASE,
                PrivilegeAction.SHOW_CONNECTION,
                PrivilegeAction.TERMINATE_CONNECTION,
            )
    }
}
