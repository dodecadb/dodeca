package org.dozendb.core.security

import inet.ipaddr.IPAddressString
import java.net.InetAddress
import java.util.UUID
import org.neo4j.internal.kernel.api.security.PatternSegment
import org.neo4j.internal.kernel.api.security.PermissionState
import org.neo4j.internal.kernel.api.security.PermissionState.EXPLICIT_DENY
import org.neo4j.internal.kernel.api.security.PermissionState.EXPLICIT_GRANT
import org.neo4j.internal.kernel.api.security.PermissionState.NOT_GRANTED
import org.neo4j.internal.kernel.api.security.PrivilegeAction
import org.neo4j.string.Globbing
import org.neo4j.values.storable.Value

/**
 * What a user may do with the [privileges] of their roles, PUBLIC's included: what a privilege
 * grants and none denies. [home] is the id of the user's home database, else the default database,
 * or null if it does not exist.
 *
 * The rules of Neo4j's privileges, as Dodeca applies them.
 */
class PrivilegePolicy(privileges: Collection<Privilege>, private val home: UUID?) {
    private val privileges = privileges.toList()

    /** ACCESS to the database [id]. */
    fun access(id: UUID): PermissionState = admin(PrivilegeAction.ACCESS, id)

    /**
     * An administration [action] on the database [database], or on the DBMS for null, about
     * [segment], such as the user whose transactions are terminated. Reads and writes of the graph
     * are [graph]'s.
     */
    fun admin(
        action: PrivilegeAction,
        database: UUID?,
        segment: PrivilegeSegment = PrivilegeSegment.Database,
    ): PermissionState {
        require(action !in GRAPH_ACTIONS) { "$action is a graph action" }
        return decide { privilege ->
            val covers =
                privilege.action.satisfies(action) ||
                    (privilege.access == Access.DENIED && action.satisfies(privilege.action))
            covers && inScope(privilege, database) && privilege.segment.covers(segment)
        }
    }

    /** Executing the procedure [name], an @Admin procedure if [admin]. */
    fun procedure(name: String, admin: Boolean): Execution =
        procedure(
            Execution(
                executable(PrivilegeAction.EXECUTE, name, ::procedures),
                executable(PrivilegeAction.EXECUTE_BOOSTED, name, ::procedures),
            ),
            dbms(PrivilegeAction.EXECUTE_ADMIN),
            admin,
        )

    /** Executing the user-defined function [name]; built-in functions need nothing. */
    fun function(name: String): Execution =
        Execution(
            executable(PrivilegeAction.EXECUTE, name, ::functions),
            executable(PrivilegeAction.EXECUTE_BOOSTED, name, ::functions),
        )

    /** Seeing the setting [name] in SHOW SETTINGS. */
    fun setting(name: String): PermissionState =
        executable(PrivilegeAction.SHOW_SETTING, name) { segment ->
            (segment as? PrivilegeSegment.Settings)?.glob
        }

    /** LOAD from any source, as LOAD ON ALL DATA gives it. */
    fun loadAllData(): PermissionState = decide { privilege ->
        privilege.action.satisfies(PrivilegeAction.LOAD) &&
            privilege.segment == PrivilegeSegment.AllData
    }

    /** LOAD from a source at [address] through LOAD ON CIDR; none for a source without. */
    fun loadFrom(address: InetAddress?): PermissionState {
        if (address == null) return PermissionState.NOT_GRANTED
        val source = IPAddressString(address.hostAddress)
        return decide { privilege ->
            val segment = privilege.segment
            privilege.action.satisfies(PrivilegeAction.LOAD_CIDR) &&
                segment is PrivilegeSegment.Cidr &&
                segment.covers(source, privilege.access)
        }
    }

    /** What the user may do with the graph of the database [id]. */
    fun graph(id: UUID): GraphPolicy = GraphPolicy(graphPrivileges(id))

    /** The graph privileges that apply to the database [id]. */
    fun graphPrivileges(id: UUID): List<Privilege> = privileges.filter {
        it.action in GRAPH_ACTIONS && inScope(it, id)
    }

    /**
     * [procedure] for the procedures by id: [ids] gives the ids of those a glob matches, [admin]
     * those of @Admin procedures.
     */
    fun procedures(ids: (String) -> IntArray, admin: IntArray): (Int) -> Execution {
        val anyAdmin = dbms(PrivilegeAction.EXECUTE_ADMIN)
        val admins = admin.toHashSet()
        val executions =
            executions(ids, ::procedures).mapValues { (id, given) ->
                procedure(given, anyAdmin, id in admins)
            }
        // Procedures that no privilege names.
        val none = Execution(NOT_GRANTED, NOT_GRANTED)
        val other = procedure(none, anyAdmin, false)
        val otherAdmin = procedure(none, anyAdmin, true)
        return { id -> executions[id] ?: if (id in admins) otherAdmin else other }
    }

    /** [function] for the user-defined functions by id, [ids] giving those a glob matches. */
    fun functions(ids: (String) -> IntArray): Map<Int, Execution> = executions(ids, ::functions)

    /** Whether EXECUTE ADMIN PROCEDURES applies, or ALL DBMS PRIVILEGES. */
    fun executeAdmin(): PermissionState = dbms(PrivilegeAction.EXECUTE_ADMIN)

    private fun executions(
        ids: (String) -> IntArray,
        glob: (PrivilegeSegment) -> String?,
    ): MutableMap<Int, Execution> {
        val executions = mutableMapOf<Int, Execution>()
        for (privilege in privileges) {
            val matching =
                when (privilege.action) {
                    PrivilegeAction.DBMS_ACTIONS -> ids("*")
                    PrivilegeAction.EXECUTE,
                    PrivilegeAction.EXECUTE_BOOSTED -> glob(privilege.segment)?.let(ids)
                    else -> null
                } ?: continue
            val state = if (privilege.access == Access.DENIED) EXPLICIT_DENY else EXPLICIT_GRANT
            val execute = privilege.action != PrivilegeAction.EXECUTE_BOOSTED
            val boost = privilege.action != PrivilegeAction.EXECUTE
            for (id in matching) {
                val current = executions[id] ?: Execution(NOT_GRANTED, NOT_GRANTED)
                executions[id] =
                    Execution(
                        if (execute) current.execute.combine(state) else current.execute,
                        if (boost) current.boost.combine(state) else current.boost,
                    )
            }
        }
        return executions
    }

    /**
     * A procedure that EXECUTE and EXECUTE BOOSTED on it give [given]. Its caller is an admin with
     * it with EXECUTE ADMIN PROCEDURES, [anyAdmin], or both, and no DENY of the three; Neo4j asks
     * so before it shows or ends the connections of other users. An @Admin procedure, if [admin],
     * runs only so, boosted. EXECUTE alone passes Neo4j's first check of it, so that the second
     * names the admin procedure it refuses.
     */
    private fun procedure(given: Execution, anyAdmin: PermissionState, admin: Boolean): Execution {
        val asAdmin =
            when {
                EXPLICIT_DENY in listOf(given.execute, given.boost, anyAdmin) -> EXPLICIT_DENY
                anyAdmin.allowsAccess() ||
                    (given.execute.allowsAccess() && given.boost.allowsAccess()) -> EXPLICIT_GRANT
                else -> NOT_GRANTED
            }
        if (!admin) return given.copy(admin = asAdmin)
        val execute =
            if (given.execute == EXPLICIT_DENY) EXPLICIT_DENY
            else
                PermissionState.fromAllowList(
                    given.execute.allowsAccess() || asAdmin.allowsAccess()
                )
        return Execution(execute, asAdmin, asAdmin)
    }

    /** Whether [privilege] applies to [database], or to the DBMS for null. */
    private fun inScope(privilege: Privilege, database: UUID?): Boolean =
        when (val scope = privilege.scope) {
            PrivilegeScope.Dbms,
            PrivilegeScope.AllDatabases -> true
            PrivilegeScope.Home -> database != null && database == home
            is PrivilegeScope.Database -> database == scope.id
            is PrivilegeScope.Alias -> database == scope.id
        }

    /** A DBMS-wide [action], or ALL DBMS PRIVILEGES. */
    private fun dbms(action: PrivilegeAction): PermissionState = decide { privilege ->
        privilege.action == action || privilege.action == PrivilegeAction.DBMS_ACTIONS
    }

    /**
     * [action] on [name], through privileges whose segment has a glob that matches it, or ALL DBMS
     * PRIVILEGES.
     */
    private fun executable(
        action: PrivilegeAction,
        name: String,
        glob: (PrivilegeSegment) -> String?,
    ): PermissionState = decide { privilege ->
        when (privilege.action) {
            PrivilegeAction.DBMS_ACTIONS -> true
            action -> glob(privilege.segment)?.let { Globbing.globPredicate(it).test(name) } == true
            else -> false
        }
    }

    private fun procedures(segment: PrivilegeSegment): String? =
        (segment as? PrivilegeSegment.Procedures)?.glob

    private fun functions(segment: PrivilegeSegment): String? =
        (segment as? PrivilegeSegment.Functions)?.glob

    private inline fun decide(applies: (Privilege) -> Boolean): PermissionState =
        decide(privileges, applies)
}

/**
 * Whether a procedure or function may run, and whether boosted, with full access; for a procedure,
 * whether its caller is an admin with it, as @Admin procedures need.
 */
data class Execution(
    val execute: PermissionState,
    val boost: PermissionState,
    val admin: PermissionState = NOT_GRANTED,
)

/**
 * What a user may do with one graph, from the graph privileges that apply to its database. Nodes
 * are given by their labels and relationships by their type, and both by their properties for
 * property rules: a missing one is [org.neo4j.values.storable.Values.NO_VALUE].
 */
class GraphPolicy(private val privileges: List<Privilege>) {
    /** Finding a node. */
    fun traverseNode(labels: Set<String>, properties: (String) -> Value): Boolean =
        decide(privileges) { it.traverses() && it.segment.coversNode(labels, properties) }
            .allowsAccess()

    /** Finding a relationship of [type]; its nodes must be found too. */
    fun traverseRelationship(type: String, properties: (String) -> Value): Boolean =
        decide(privileges) { it.traverses() && it.segment.coversRelationship(type, properties) }
            .allowsAccess()

    /** Reading the property [key] of a node that is found. */
    fun readNodeProperty(labels: Set<String>, key: String, properties: (String) -> Value): Boolean =
        decide(privileges) { it.reads(key) && it.segment.coversNode(labels, properties) }
            .allowsAccess()

    /** Reading the property [key] of a relationship of [type] that is found. */
    fun readRelationshipProperty(
        type: String,
        key: String,
        properties: (String) -> Value,
    ): Boolean =
        decide(privileges) { it.reads(key) && it.segment.coversRelationship(type, properties) }
            .allowsAccess()

    /** Creating a node with [labels]. */
    fun createNode(labels: Set<String>): Boolean =
        forEveryLabel(labels) { it.action.satisfies(PrivilegeAction.CREATE_ELEMENT) }

    /** Deleting a node with [labels]. */
    fun deleteNode(labels: Set<String>): Boolean =
        forEveryLabel(labels) { it.action.satisfies(PrivilegeAction.DELETE_ELEMENT) }

    /** Creating a relationship of [type]. */
    fun createRelationship(type: String): Boolean =
        decide(privileges) {
                it.action.satisfies(PrivilegeAction.CREATE_ELEMENT) &&
                    it.segment.coversRelationship(type)
            }
            .allowsAccess()

    /** Deleting a relationship of [type]. */
    fun deleteRelationship(type: String): Boolean =
        decide(privileges) {
                it.action.satisfies(PrivilegeAction.DELETE_ELEMENT) &&
                    it.segment.coversRelationship(type)
            }
            .allowsAccess()

    /** Adding [label] to a node. */
    fun setLabel(label: String): Boolean =
        decide(privileges) {
                it.action.satisfies(PrivilegeAction.SET_LABEL) &&
                    it.resource.coversLabel(label) &&
                    it.segment.coversSomeNodes()
            }
            .allowsAccess()

    /** Removing [label] from a node. */
    fun removeLabel(label: String): Boolean =
        decide(privileges) {
                it.action.satisfies(PrivilegeAction.REMOVE_LABEL) &&
                    it.resource.coversLabel(label) &&
                    it.segment.coversSomeNodes()
            }
            .allowsAccess()

    /** Setting the property [key] of a node with [labels]. */
    fun setNodeProperty(labels: Set<String>, key: String): Boolean =
        forEveryLabel(labels) {
            it.action.satisfies(PrivilegeAction.SET_PROPERTY) && it.resource.coversProperty(key)
        }

    /** Setting the property [key] of a relationship of [type]. */
    fun setRelationshipProperty(type: String, key: String): Boolean =
        decide(privileges) {
                it.action.satisfies(PrivilegeAction.SET_PROPERTY) &&
                    it.resource.coversProperty(key) &&
                    it.segment.coversRelationship(type)
            }
            .allowsAccess()

    /**
     * Whether the privileges that [apply] allow writing a node with [labels]: denied for none of
     * them, and granted for every node or for each of them, so a node without labels needs a
     * privilege for every node.
     */
    private inline fun forEveryLabel(labels: Set<String>, apply: (Privilege) -> Boolean): Boolean {
        val applying = privileges.filter { apply(it) && it.segment.coversSomeNodes() }
        if (applying.any { it.access == Access.DENIED && it.segment.coversLabels(labels) }) {
            return false
        }
        val granted = applying.filter { it.access == Access.GRANTED }
        if (granted.any { it.segment.coversLabels(emptySet()) }) return true
        return labels.isNotEmpty() &&
            labels.all { label -> granted.any { it.segment.coversLabels(setOf(label)) } }
    }

    /** Whether this privilege finds, or for a DENY hides, elements. */
    private fun Privilege.traverses(): Boolean =
        action.satisfies(PrivilegeAction.TRAVERSE) &&
            (access == Access.GRANTED || resource !is PrivilegeResource.Property)

    private fun Privilege.reads(key: String): Boolean =
        action.satisfies(PrivilegeAction.READ) && resource.coversProperty(key)
}

/**
 * GRANTED if a privilege of [privileges] that [applies] grants, DENIED if one denies, which wins.
 */
private inline fun decide(
    privileges: List<Privilege>,
    applies: (Privilege) -> Boolean,
): PermissionState {
    var state = PermissionState.NOT_GRANTED
    for (privilege in privileges) {
        if (!applies(privilege)) continue
        if (privilege.access == Access.DENIED) return PermissionState.EXPLICIT_DENY
        state = PermissionState.EXPLICIT_GRANT
    }
    return state
}

/** Whether a privilege about this segment is about [requested] too. */
private fun PrivilegeSegment.covers(requested: PrivilegeSegment): Boolean =
    when (this) {
        PrivilegeSegment.Database -> true
        is PrivilegeSegment.Users ->
            requested is PrivilegeSegment.Users && (user == null || user == requested.user)
        else -> this == requested
    }

private fun PrivilegeSegment.coversSomeNodes(): Boolean =
    this == PrivilegeSegment.Database || this is PrivilegeSegment.Nodes

/** Whether this segment covers every node with [labels]; for no labels, every node. */
private fun PrivilegeSegment.coversLabels(labels: Set<String>): Boolean =
    when (this) {
        PrivilegeSegment.Database -> true
        is PrivilegeSegment.Nodes -> label == null || label in labels
        else -> false
    }

/** Whether this segment covers a node with [labels] and [properties]. */
private fun PrivilegeSegment.coversNode(
    labels: Set<String>,
    properties: (String) -> Value,
): Boolean =
    when (this) {
        PrivilegeSegment.Database -> true
        is PrivilegeSegment.Nodes -> label == null || label in labels
        is PrivilegeSegment.NodePattern -> pattern.covers(labels, properties)
        else -> false
    }

/** Whether this segment covers every relationship of [type], as writes ask. */
private fun PrivilegeSegment.coversRelationship(type: String): Boolean =
    when (this) {
        PrivilegeSegment.Database -> true
        is PrivilegeSegment.Relationships -> this.type == null || this.type == type
        else -> false
    }

/** Whether this segment covers a relationship of [type] with [properties]. */
private fun PrivilegeSegment.coversRelationship(
    type: String,
    properties: (String) -> Value,
): Boolean =
    when (this) {
        PrivilegeSegment.Database -> true
        is PrivilegeSegment.Relationships -> this.type == null || this.type == type
        is PrivilegeSegment.RelationshipPattern -> pattern.covers(setOf(type), properties)
        else -> false
    }

/**
 * Whether the rule applies to the element: it has one of the labels or is of one of the types, if
 * any, and satisfies it.
 */
private fun PatternSegment.covers(labels: Set<String>, properties: (String) -> Value): Boolean {
    if (elementTypes().isNotEmpty() && elementTypes().none { it in labels }) return false
    val value = properties(property())
    return when (this) {
        is PatternSegment.ValuePatternSegment -> operator().test(value, value())
        is PatternSegment.NullPatternSegment -> operator().test(value)
        else -> error("Unknown property rule: $this")
    }
}

/**
 * Whether [source] is in the range; a range that is not one, which GRANT and DENY refuse, covers
 * every source when denied and none when granted.
 */
private fun PrivilegeSegment.Cidr.covers(source: IPAddressString, access: Access): Boolean {
    val range = IPAddressString(range)
    return if (range.isValid) range.contains(source) else access == Access.DENIED
}

private fun PrivilegeResource.coversProperty(key: String): Boolean =
    when (this) {
        PrivilegeResource.Graph,
        PrivilegeResource.AllProperties -> true
        is PrivilegeResource.Property -> name == key
        else -> false
    }

private fun PrivilegeResource.coversLabel(label: String): Boolean =
    when (this) {
        PrivilegeResource.Graph,
        PrivilegeResource.AllLabels -> true
        is PrivilegeResource.Label -> name == label
        else -> false
    }
