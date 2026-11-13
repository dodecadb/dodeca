package dev.dodeca.core.security

import java.util.UUID
import org.neo4j.internal.kernel.api.security.PatternSegment
import org.neo4j.internal.kernel.api.security.PrivilegeAction

/**
 * One privilege of a role, as GRANT and DENY give it once Neo4j's planner split the command into
 * single privileges: one [action] in one [scope], about one [resource] of one [segment].
 */
data class Privilege(
    val access: Access,
    val action: PrivilegeAction,
    val scope: PrivilegeScope,
    val resource: PrivilegeResource = PrivilegeResource.Database,
    val segment: PrivilegeSegment = PrivilegeSegment.Database,
    val immutable: Boolean = false,
) {
    override fun toString(): String =
        "$access $action $resource $scope $segment" + if (immutable) " immutable" else ""
}

/** Whether a privilege allows or forbids; SHOW PRIVILEGES' `access`. */
enum class Access {
    GRANTED,
    DENIED,
}

/** Where a privilege applies; SHOW PRIVILEGES' `graph`. */
sealed interface PrivilegeScope {
    /** The DBMS: DBMS and LOAD privileges. */
    data object Dbms : PrivilegeScope {
        override fun toString() = "DBMS"
    }

    /** Every database, those created later too: `DATABASE *` and `GRAPH *`. */
    data object AllDatabases : PrivilegeScope {
        override fun toString() = "*"
    }

    /** The user's home database, else the default database: `HOME DATABASE` and `HOME GRAPH`. */
    data object Home : PrivilegeScope {
        override fun toString() = "HOME"
    }

    /**
     * One database, by its id rather than its name, so that a database dropped and created again
     * under the same name has none of its privileges.
     */
    data class Database(val id: UUID) : PrivilegeScope {
        override fun toString() = "database $id"
    }

    /**
     * One remote alias, by its name: a remote alias has no id that ALTER ALIAS keeps, and DROP
     * ALIAS removes its privileges. Its [id] stands for it where privileges apply to databases.
     */
    data class Alias(val name: String) : PrivilegeScope {
        val id: UUID
            get() = remoteAliasId(name)

        override fun toString() = "alias $name"
    }
}

/**
 * The id that stands for the remote alias [name] where privileges apply: derived from its name, and
 * never stored, so no database has it.
 */
fun remoteAliasId(name: String): UUID =
    UUID.nameUUIDFromBytes("dodeca-remote-alias:$name".toByteArray(Charsets.UTF_8))

/** What of the elements a graph privilege is about; SHOW PRIVILEGES' `resource`. */
sealed interface PrivilegeResource {
    /** Not about elements: DBMS, database and LOAD privileges. */
    data object Database : PrivilegeResource {
        override fun toString() = "database"
    }

    /** The elements themselves: TRAVERSE, CREATE, DELETE, WRITE and ALL GRAPH PRIVILEGES. */
    data object Graph : PrivilegeResource {
        override fun toString() = "graph"
    }

    /** Every property: `{*}`. */
    data object AllProperties : PrivilegeResource {
        override fun toString() = "all_properties"
    }

    data class Property(val name: String) : PrivilegeResource {
        override fun toString() = "property($name)"
    }

    /** Every label: `SET LABEL *` and `REMOVE LABEL *`. */
    data object AllLabels : PrivilegeResource {
        override fun toString() = "all_labels"
    }

    data class Label(val name: String) : PrivilegeResource {
        override fun toString() = "label($name)"
    }
}

/**
 * Which elements, users, procedures, settings or data sources a privilege is about; SHOW
 * PRIVILEGES' `segment`, in Neo4j's notation.
 */
sealed interface PrivilegeSegment {
    /** All of the scope: DBMS and database privileges, and ALL GRAPH PRIVILEGES. */
    data object Database : PrivilegeSegment {
        override fun toString() = "database"
    }

    /** Nodes with [label], every node for null: `NODES *`. */
    data class Nodes(val label: String?) : PrivilegeSegment {
        override fun toString() = "NODE(${label ?: "*"})"
    }

    /** Relationships of [type], every relationship for null: `RELATIONSHIPS *`. */
    data class Relationships(val type: String?) : PrivilegeSegment {
        override fun toString() = "RELATIONSHIP(${type ?: "*"})"
    }

    /**
     * Elements whose property satisfies a rule: [NodePattern] or [RelationshipPattern]. Equal by
     * the rule, which Neo4j's segments are not: they compare by identity since Neo4j 2026.
     */
    sealed class PropertyRule(val pattern: PatternSegment) : PrivilegeSegment {
        private val rule: List<Any?> =
            when (pattern) {
                is PatternSegment.ValuePatternSegment ->
                    listOf(
                        pattern.javaClass,
                        pattern.elementTypes(),
                        pattern.property(),
                        pattern.operator(),
                        pattern.value(),
                    )
                is PatternSegment.NullPatternSegment ->
                    listOf(
                        pattern.javaClass,
                        pattern.elementTypes(),
                        pattern.property(),
                        pattern.operator(),
                    )
                is PatternSegment.AbacPatternSegment ->
                    listOf(
                        pattern.javaClass,
                        pattern.elementTypes(),
                        pattern.property(),
                        pattern.operator(),
                        pattern.functionName(),
                    )
                else -> listOf(pattern)
            }

        override fun equals(other: Any?): Boolean =
            other is PropertyRule && other.javaClass == javaClass && other.rule == rule

        override fun hashCode(): Int = rule.hashCode()

        override fun toString() = pattern.toString()
    }

    /**
     * Nodes whose property satisfies a rule: `FOR (n:A) WHERE n.p > 1`, or `WHERE n.p IN
     * abac.native.user_tags()`, whose value each user's tags give.
     */
    class NodePattern(pattern: PatternSegment) : PropertyRule(pattern)

    /** Relationships whose property satisfies a rule: `FOR ()-[r:T]-() WHERE r.p > 1`. */
    class RelationshipPattern(pattern: PatternSegment) : PropertyRule(pattern)

    /** Transactions or impersonation of [user], every user for null. */
    data class Users(val user: String?) : PrivilegeSegment {
        override fun toString() = "USER(${user ?: "*"})"
    }

    /** Procedures whose name matches [glob]. */
    data class Procedures(val glob: String) : PrivilegeSegment {
        override fun toString() = "PROCEDURE($glob)"
    }

    /** User-defined functions whose name matches [glob]. */
    data class Functions(val glob: String) : PrivilegeSegment {
        override fun toString() = "FUNCTION($glob)"
    }

    /** Settings whose name matches [glob]. */
    data class Settings(val glob: String) : PrivilegeSegment {
        override fun toString() = "SETTING($glob)"
    }

    /** Every data source of LOAD CSV. */
    data object AllData : PrivilegeSegment {
        override fun toString() = "ALL DATA"
    }

    /** The data sources of LOAD CSV whose address is in [range], such as `10.0.0.0/8`. */
    data class Cidr(val range: String) : PrivilegeSegment {
        override fun toString() = "CIDR('$range')"
    }
}

/** The actions of graph privileges; the others are about databases and the DBMS. */
internal val GRAPH_ACTIONS: Set<PrivilegeAction> =
    setOf(
        PrivilegeAction.TRAVERSE,
        PrivilegeAction.READ,
        PrivilegeAction.MATCH,
        PrivilegeAction.MERGE,
        PrivilegeAction.WRITE,
        PrivilegeAction.GRAPH_ACTIONS,
        PrivilegeAction.CREATE_ELEMENT,
        PrivilegeAction.DELETE_ELEMENT,
        PrivilegeAction.SET_LABEL,
        PrivilegeAction.REMOVE_LABEL,
        PrivilegeAction.SET_PROPERTY,
    )

/** Stands for a database that does not exist: only privileges on all databases reach it. */
internal val NO_DATABASE: UUID = UUID(0, 0)
