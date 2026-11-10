package org.dozendb.core.security

import java.util.UUID
import org.neo4j.graphdb.Direction
import org.neo4j.graphdb.Entity
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.Transaction
import org.neo4j.internal.kernel.api.security.PatternSegment
import org.neo4j.internal.kernel.api.security.PrivilegeAction
import org.neo4j.internal.kernel.api.security.PropertyRule
import org.neo4j.values.storable.Values

/**
 * How roles and their privileges are kept in the system graph, with labels of DozenDB's own, never
 * Neo4j Enterprise's:
 * - `(:DozenDbRole {name, id, immutable})`, `id` staying the same when the role is renamed;
 * - `(:User)-[:DOZENDB_HAS_ROLE]->(:DozenDbRole)`; PUBLIC is nobody's, as it is everybody's;
 * - `(:DozenDbRole)-[:GRANTED|DENIED]->(:DozenDbPrivilege)`, one node per role and privilege, whose
 *   unique `key` makes granting twice a no-op.
 */
object PrivilegeRecords {
    val ROLE: Label = Label.label("DozenDbRole")
    val PRIVILEGE: Label = Label.label("DozenDbPrivilege")
    val HAS_ROLE: RelationshipType = RelationshipType.withName("DOZENDB_HAS_ROLE")
    val GRANTED: RelationshipType = RelationshipType.withName("GRANTED")
    val DENIED: RelationshipType = RelationshipType.withName("DENIED")

    const val ROLE_NAME = "name"
    const val ROLE_ID = "id"
    const val IMMUTABLE = "immutable"
    const val KEY = "key"

    /**
     * A privilege on one database has [SCOPE] [ONE_DATABASE] and the database's id as [DATABASE];
     * one on a remote alias has [SCOPE] [ONE_ALIAS] and the alias's name as [ALIAS].
     */
    const val SCOPE = "scope"
    const val ONE_DATABASE = "DATABASE"
    const val DATABASE = "database"
    const val ONE_ALIAS = "ALIAS"
    const val ALIAS = "alias"

    private const val ACTION = "action"
    private const val RESOURCE = "resource"
    private const val RESOURCE_NAME = "resourceName"
    private const val SEGMENT = "segment"
    private const val QUALIFIER = "qualifier"
    private const val PATTERN_LABELS = "patternLabels"
    private const val PATTERN_PROPERTY = "patternProperty"
    private const val PATTERN_OPERATOR = "patternOperator"
    private const val PATTERN_VALUE = "patternValue"

    fun relationship(access: Access): RelationshipType =
        if (access == Access.GRANTED) GRANTED else DENIED

    /** Writes [privilege] of the role with the id [roleId] into the new [node]. */
    fun write(node: Node, roleId: String, privilege: Privilege) {
        record(roleId, privilege).forEach { (key, value) -> node.setProperty(key, value) }
    }

    /**
     * The properties of the node of [privilege] of the role with the id [roleId], [KEY] included.
     */
    fun record(roleId: String, privilege: Privilege): Map<String, Any> {
        val properties = properties(privilege)
        return properties + (KEY to key(roleId, privilege.access, properties))
    }

    /** The [KEY] of [privilege] of the role with the id [roleId]. */
    fun key(roleId: String, privilege: Privilege): String =
        key(roleId, privilege.access, properties(privilege))

    /**
     * Makes the privileges on the database with the id [from] privileges on the one with the id
     * [to], as when a database is recreated under a new id, which keeps its privileges; returns how
     * many it changed.
     */
    fun retarget(tx: Transaction, from: UUID, to: UUID): Int {
        val nodes =
            tx.findNodes(PRIVILEGE, SCOPE, ONE_DATABASE).use { found ->
                found.asSequence().filter { it.getProperty(DATABASE, null) == "$from" }.toList()
            }
        for (node in nodes) {
            node.setProperty(DATABASE, "$to")
            val granting =
                node.getRelationships(Direction.INCOMING, GRANTED, DENIED).use { it.single() }
            val access = if (granting.isType(GRANTED)) Access.GRANTED else Access.DENIED
            val roleId = granting.startNode.getProperty(ROLE_ID) as String
            node.setProperty(KEY, key(roleId, read(node, access)))
        }
        return nodes.size
    }

    /** The privilege [entity] holds, given with [access]; fails if it is not one. */
    fun read(entity: Entity, access: Access): Privilege {
        val properties = entity.allProperties
        fun text(key: String): String =
            properties[key] as? String ?: throw IllegalArgumentException("no $key")
        fun optional(key: String): String? = properties[key] as String?
        val action =
            PrivilegeAction.from(text(ACTION))
                ?: throw IllegalArgumentException("unknown action ${text(ACTION)}")
        val scope =
            when (val scope = text(SCOPE)) {
                "DBMS" -> PrivilegeScope.Dbms
                "ALL" -> PrivilegeScope.AllDatabases
                "HOME" -> PrivilegeScope.Home
                ONE_DATABASE -> PrivilegeScope.Database(UUID.fromString(text(DATABASE)))
                ONE_ALIAS -> PrivilegeScope.Alias(text(ALIAS))
                else -> throw IllegalArgumentException("unknown scope $scope")
            }
        val resource =
            when (val resource = text(RESOURCE)) {
                "DATABASE" -> PrivilegeResource.Database
                "GRAPH" -> PrivilegeResource.Graph
                "ALL_PROPERTIES" -> PrivilegeResource.AllProperties
                "PROPERTY" -> PrivilegeResource.Property(text(RESOURCE_NAME))
                "ALL_LABELS" -> PrivilegeResource.AllLabels
                "LABEL" -> PrivilegeResource.Label(text(RESOURCE_NAME))
                else -> throw IllegalArgumentException("unknown resource $resource")
            }
        val segment =
            when (val segment = text(SEGMENT)) {
                "DATABASE" -> PrivilegeSegment.Database
                "NODES" -> PrivilegeSegment.Nodes(optional(QUALIFIER))
                "RELATIONSHIPS" -> PrivilegeSegment.Relationships(optional(QUALIFIER))
                "NODE_PATTERN" -> PrivilegeSegment.NodePattern(pattern(properties))
                "USERS" -> PrivilegeSegment.Users(optional(QUALIFIER))
                "PROCEDURES" -> PrivilegeSegment.Procedures(text(QUALIFIER))
                "FUNCTIONS" -> PrivilegeSegment.Functions(text(QUALIFIER))
                "SETTINGS" -> PrivilegeSegment.Settings(text(QUALIFIER))
                "ALL_DATA" -> PrivilegeSegment.AllData
                "CIDR" -> PrivilegeSegment.Cidr(text(QUALIFIER))
                else -> throw IllegalArgumentException("unknown segment $segment")
            }
        return Privilege(
            access,
            action,
            scope,
            resource,
            segment,
            properties[IMMUTABLE] as? Boolean ?: false,
        )
    }

    /**
     * What a privilege that cannot be read stands for when it was denied: everything it could have
     * denied, so that users with it are not given more than they should have.
     */
    val DENY_ALL: List<Privilege> =
        listOf(
            Privilege(Access.DENIED, PrivilegeAction.DBMS_ACTIONS, PrivilegeScope.Dbms),
            Privilege(Access.DENIED, PrivilegeAction.DATABASE_ACTIONS, PrivilegeScope.AllDatabases),
            Privilege(Access.DENIED, PrivilegeAction.START_DATABASE, PrivilegeScope.AllDatabases),
            Privilege(Access.DENIED, PrivilegeAction.STOP_DATABASE, PrivilegeScope.AllDatabases),
            Privilege(
                Access.DENIED,
                PrivilegeAction.TRANSACTION_MANAGEMENT,
                PrivilegeScope.AllDatabases,
            ),
            Privilege(
                Access.DENIED,
                PrivilegeAction.GRAPH_ACTIONS,
                PrivilegeScope.AllDatabases,
                PrivilegeResource.Graph,
            ),
            Privilege(
                Access.DENIED,
                PrivilegeAction.LOAD,
                PrivilegeScope.Dbms,
                segment = PrivilegeSegment.AllData,
            ),
        )

    private fun properties(privilege: Privilege): Map<String, Any> = buildMap {
        put(ACTION, privilege.action.name)
        when (val scope = privilege.scope) {
            PrivilegeScope.Dbms -> put(SCOPE, "DBMS")
            PrivilegeScope.AllDatabases -> put(SCOPE, "ALL")
            PrivilegeScope.Home -> put(SCOPE, "HOME")
            is PrivilegeScope.Database -> {
                put(SCOPE, ONE_DATABASE)
                put(DATABASE, scope.id.toString())
            }
            is PrivilegeScope.Alias -> {
                put(SCOPE, ONE_ALIAS)
                put(ALIAS, scope.name)
            }
        }
        when (val resource = privilege.resource) {
            PrivilegeResource.Database -> put(RESOURCE, "DATABASE")
            PrivilegeResource.Graph -> put(RESOURCE, "GRAPH")
            PrivilegeResource.AllProperties -> put(RESOURCE, "ALL_PROPERTIES")
            is PrivilegeResource.Property -> {
                put(RESOURCE, "PROPERTY")
                put(RESOURCE_NAME, resource.name)
            }
            PrivilegeResource.AllLabels -> put(RESOURCE, "ALL_LABELS")
            is PrivilegeResource.Label -> {
                put(RESOURCE, "LABEL")
                put(RESOURCE_NAME, resource.name)
            }
        }
        fun segment(name: String, qualifier: String?) {
            put(SEGMENT, name)
            qualifier?.let { put(QUALIFIER, it) }
        }
        when (val segment = privilege.segment) {
            PrivilegeSegment.Database -> segment("DATABASE", null)
            is PrivilegeSegment.Nodes -> segment("NODES", segment.label)
            is PrivilegeSegment.Relationships -> segment("RELATIONSHIPS", segment.type)
            is PrivilegeSegment.NodePattern -> {
                segment("NODE_PATTERN", null)
                val pattern = segment.pattern
                put(PATTERN_LABELS, pattern.labels().sorted().toTypedArray())
                put(PATTERN_PROPERTY, pattern.property())
                when (pattern) {
                    is PatternSegment.ValuePatternSegment -> {
                        put(PATTERN_OPERATOR, pattern.operator().name)
                        put(PATTERN_VALUE, pattern.value().asObjectCopy())
                    }
                    is PatternSegment.NullPatternSegment ->
                        put(PATTERN_OPERATOR, pattern.operator().name)
                    else -> throw IllegalArgumentException("unknown property rule $pattern")
                }
            }
            is PrivilegeSegment.Users -> segment("USERS", segment.user)
            is PrivilegeSegment.Procedures -> segment("PROCEDURES", segment.glob)
            is PrivilegeSegment.Functions -> segment("FUNCTIONS", segment.glob)
            is PrivilegeSegment.Settings -> segment("SETTINGS", segment.glob)
            PrivilegeSegment.AllData -> segment("ALL_DATA", null)
            is PrivilegeSegment.Cidr -> segment("CIDR", segment.range)
        }
        put(IMMUTABLE, privilege.immutable)
    }

    private fun pattern(properties: Map<String, Any>): PatternSegment {
        val labels =
            (properties[PATTERN_LABELS] as? Array<*>
                    ?: throw IllegalArgumentException("no $PATTERN_LABELS"))
                .map { it as String }
                .toSet()
        val property =
            properties[PATTERN_PROPERTY] as? String
                ?: throw IllegalArgumentException("no $PATTERN_PROPERTY")
        val operator =
            properties[PATTERN_OPERATOR] as? String
                ?: throw IllegalArgumentException("no $PATTERN_OPERATOR")
        val value = properties[PATTERN_VALUE]
        return if (value == null) {
            PatternSegment.NullPatternSegment(
                labels,
                property,
                PropertyRule.NullOperator.valueOf(operator),
            )
        } else {
            PatternSegment.ValuePatternSegment(
                labels,
                property,
                Values.of(value),
                PropertyRule.ComparisonOperator.valueOf(operator),
            )
        }
    }

    /** One line naming the role and everything that tells privileges apart. */
    private fun key(roleId: String, access: Access, properties: Map<String, Any>): String =
        (listOf(roleId, access.name) +
                properties.toSortedMap().map { (key, value) -> "$key=${show(key, value)}" })
            .joinToString("|")

    private fun show(key: String, value: Any): String =
        when {
            // Typed, so that 1, 1.0 and '1' stay apart.
            key == PATTERN_VALUE -> Values.of(value).toString()
            value is Array<*> -> value.joinToString(",", "[", "]")
            else -> value.toString()
        }
}
