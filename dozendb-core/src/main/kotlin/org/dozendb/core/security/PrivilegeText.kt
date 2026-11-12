package org.dozendb.core.security

import java.util.UUID
import java.util.regex.Pattern
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.internal.kernel.api.security.PatternSegment
import org.neo4j.internal.kernel.api.security.PrivilegeAction
import org.neo4j.util.Stringifier
import org.neo4j.values.storable.ArrayValue
import org.neo4j.values.storable.DateTimeValue
import org.neo4j.values.storable.DateValue
import org.neo4j.values.storable.DurationValue
import org.neo4j.values.storable.LocalDateTimeValue
import org.neo4j.values.storable.LocalTimeValue
import org.neo4j.values.storable.PointValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.TimeValue
import org.neo4j.values.storable.Value

/**
 * Privileges as SHOW PRIVILEGES shows them: rows of its columns, and the commands that grant, deny
 * or revoke them, in the formats of Neo4j's docs.
 */
object PrivilegeText {
    /** The columns of SHOW PRIVILEGES that tell a privilege of a role apart. */
    data class Row(
        val access: String,
        val action: String,
        val resource: String,
        val graph: String,
        val segment: String,
        val immutable: Boolean,
    )

    /** A command of SHOW PRIVILEGES AS COMMANDS. */
    data class Command(val command: String, val immutable: Boolean)

    /** The row of [privilege]; null for one on a database that [databaseName] does not know. */
    fun row(privilege: Privilege, databaseName: (UUID) -> String?): Row? {
        val graph =
            when (val scope = privilege.scope) {
                PrivilegeScope.Dbms,
                PrivilegeScope.AllDatabases -> "*"
                PrivilegeScope.Home -> "HOME"
                is PrivilegeScope.Database -> databaseName(scope.id) ?: return null
                is PrivilegeScope.Alias -> scope.name
            }
        return Row(
            privilege.access.name,
            privilege.action.toString(),
            privilege.resource.toString(),
            graph,
            privilege.segment.toString(),
            privilege.immutable,
        )
    }

    /**
     * The command that gives [privilege] to the role [role], or to `$role` for null; with [revoke],
     * the one that takes it away, in Cypher [version]. Null for a privilege on a database that
     * [databaseName] does not know; the database [parameter] is `$database`. WRITE on every node
     * and on every relationship give the same command.
     */
    fun command(
        privilege: Privilege,
        role: String?,
        revoke: Boolean,
        databaseName: (UUID) -> String?,
        version: CypherVersion,
        parameter: UUID? = null,
    ): Command? {
        val body = body(privilege, databaseName, version, parameter) ?: return null
        val immutable = if (privilege.immutable) "IMMUTABLE " else ""
        val access = if (privilege.access == Access.GRANTED) "GRANT" else "DENY"
        val grantee = role?.let { Stringifier.backtick(it, true) } ?: "\$role"
        val text =
            if (revoke) "REVOKE $immutable$access $body FROM $grantee"
            else "$access $immutable$body TO $grantee"
        return Command(text, privilege.immutable)
    }

    /**
     * Whether only Cypher 25 has a command for [privilege]: one of [DATABASE_AND_DBMS] on a
     * database, or on the home database, as Neo4j Enterprise 2025.08 grants them.
     */
    fun cypher25Only(privilege: Privilege): Boolean =
        privilege.action in DATABASE_AND_DBMS &&
            (privilege.scope is PrivilegeScope.Database || privilege.scope == PrivilegeScope.Home)

    private fun body(
        privilege: Privilege,
        databaseName: (UUID) -> String?,
        version: CypherVersion,
        parameter: UUID?,
    ): String? {
        val action = privilege.action
        val segment = privilege.segment
        // Neo4j keeps these on the DBMS as on every database, which Cypher 5 names the DBMS.
        if (
            privilege.scope == PrivilegeScope.AllDatabases &&
                action in DATABASE_AND_DBMS &&
                version == CypherVersion.Cypher5
        ) {
            return "${keyword(action)} ON DBMS"
        }
        return when (val scope = privilege.scope) {
            PrivilegeScope.Dbms ->
                when (action) {
                    PrivilegeAction.LOAD,
                    PrivilegeAction.LOAD_CIDR,
                    PrivilegeAction.LOAD_URL ->
                        when (segment) {
                            is PrivilegeSegment.Cidr -> "LOAD ON CIDR \"${segment.range}\""
                            else -> "LOAD ON ALL DATA"
                        }
                    else -> "${dbms(action, segment)} ON DBMS"
                }
            else -> {
                val name =
                    when (scope) {
                        is PrivilegeScope.Database ->
                            if (scope.id == parameter) "\$database"
                            else name(databaseName(scope.id) ?: return null)
                        is PrivilegeScope.Alias -> name(scope.name)
                        PrivilegeScope.AllDatabases -> "*"
                        else -> null
                    }
                if (action in GRAPH_ACTIONS) {
                    val graph = if (name == null) "HOME GRAPH" else "GRAPH $name"
                    graph(action, privilege.resource, segment, graph)
                } else {
                    val database = if (name == null) "HOME DATABASE" else "DATABASE $name"
                    "${keyword(action)}${users(segment)} ON $database"
                }
            }
        }
    }

    /** A DBMS privilege with its qualifier. */
    private fun dbms(action: PrivilegeAction, segment: PrivilegeSegment): String =
        when (segment) {
            is PrivilegeSegment.Procedures ->
                when (action) {
                    PrivilegeAction.EXECUTE -> "EXECUTE PROCEDURE ${glob(segment.glob)}"
                    PrivilegeAction.EXECUTE_BOOSTED ->
                        "EXECUTE BOOSTED PROCEDURE ${glob(segment.glob)}"
                    else -> "${keyword(action)} ${glob(segment.glob)}"
                }
            is PrivilegeSegment.Functions ->
                when (action) {
                    PrivilegeAction.EXECUTE -> "EXECUTE FUNCTION ${glob(segment.glob)}"
                    PrivilegeAction.EXECUTE_BOOSTED ->
                        "EXECUTE BOOSTED FUNCTION ${glob(segment.glob)}"
                    else -> "${keyword(action)} ${glob(segment.glob)}"
                }
            is PrivilegeSegment.Settings -> "${keyword(action)} ${glob(segment.glob)}"
            is PrivilegeSegment.Users -> "${keyword(action)}${users(segment)}"
            else -> keyword(action)
        }

    /** A graph privilege: `MATCH {*} ON GRAPH * NODE *`, `SET LABEL A ON GRAPH g`. */
    private fun graph(
        action: PrivilegeAction,
        resource: PrivilegeResource,
        segment: PrivilegeSegment,
        graph: String,
    ): String {
        val properties =
            when (resource) {
                PrivilegeResource.AllProperties -> " {*}"
                is PrivilegeResource.Property -> " {${name(resource.name)}}"
                PrivilegeResource.AllLabels -> " *"
                is PrivilegeResource.Label -> " ${name(resource.name)}"
                else -> ""
            }
        val elements =
            when {
                action in UNQUALIFIED -> ""
                else ->
                    when (segment) {
                        is PrivilegeSegment.Nodes -> " NODE ${segment.label?.let(::name) ?: "*"}"
                        is PrivilegeSegment.Relationships ->
                            " RELATIONSHIP ${segment.type?.let(::name) ?: "*"}"
                        is PrivilegeSegment.NodePattern -> " FOR ${pattern(segment.pattern)}"
                        else -> ""
                    }
            }
        return "${keyword(action)}$properties ON $graph$elements"
    }

    /** A property rule, as `(n:A|B) WHERE n.p > 1`. */
    private fun pattern(pattern: PatternSegment): String {
        val labels = pattern.elementTypes().sorted().joinToString("|") { name(it) }
        val node = if (labels.isEmpty()) "(n)" else "(n:$labels)"
        val property = "n.${name(pattern.property())}"
        val rule =
            when (pattern) {
                is PatternSegment.ValuePatternSegment ->
                    pattern.operator().toPredicateString(property, literal(pattern.value()))
                is PatternSegment.NullPatternSegment ->
                    pattern.operator().toPredicateString(property)
                else -> throw IllegalArgumentException("Unknown property rule: $pattern")
            }
        return "$node WHERE $rule"
    }

    /** [value] as a Cypher literal, a temporal value or point as the function that makes it. */
    private fun literal(value: Value): String =
        when (value) {
            is TextValue -> "'${value.stringValue().replace("\\", "\\\\").replace("'", "\\'")}'"
            is ArrayValue -> value.joinToString(", ", "[", "]") { literal(it as Value) }
            is DateValue -> "date('${value.prettyPrint()}')"
            is LocalDateTimeValue -> "localdatetime('${value.prettyPrint()}')"
            is DateTimeValue -> "datetime('${value.prettyPrint()}')"
            is LocalTimeValue -> "localtime('${value.prettyPrint()}')"
            is TimeValue -> "time('${value.prettyPrint()}')"
            is DurationValue -> "duration('${value.prettyPrint()}')"
            is PointValue -> value.toString()
            else -> value.prettyPrint()
        }

    private fun users(segment: PrivilegeSegment): String =
        when (segment) {
            is PrivilegeSegment.Users -> " (${segment.user?.let(::name) ?: "*"})"
            else -> ""
        }

    /** A name as Cypher takes it, in backticks where it needs them. */
    private fun name(name: String): String = Stringifier.backtick(name)

    /**
     * A procedure, function or setting glob, whose parts each get backticks if they need them, as
     * Neo4j writes it: an empty part in backticks, a trailing dot as is.
     */
    private fun glob(glob: String): String {
        if (glob.contains("..")) return Stringifier.backtick(glob, false, true, true)
        // As Java splits, without trailing empty parts.
        val parts =
            DOT.split(glob).joinToString(".") { Stringifier.backtick(it, false, true, true) }
        return when {
            glob.isEmpty() -> "``"
            glob.endsWith('.') -> "$parts."
            else -> parts
        }
    }

    /**
     * The keyword of [action]; for an action no command grants, such as the legacy `admin`, its
     * name, so that SHOW PRIVILEGES shows whatever the system graph holds.
     */
    private fun keyword(action: PrivilegeAction): String =
        KEYWORDS[action] ?: action.name.replace('_', ' ')

    /**
     * The database privileges that Cypher 5 grants on the DBMS only, and Cypher 25 also on
     * databases; Neo4j keeps those on the DBMS as on every database.
     */
    val DATABASE_AND_DBMS =
        setOf(
            PrivilegeAction.ALTER_DATABASE,
            PrivilegeAction.SET_DATABASE_ACCESS,
            PrivilegeAction.SET_DEFAULT_LANGUAGE,
        )

    private val DOT = Pattern.compile("\\.")

    /** Graph privileges whose commands name no elements. */
    private val UNQUALIFIED =
        setOf(
            PrivilegeAction.WRITE,
            PrivilegeAction.SET_LABEL,
            PrivilegeAction.REMOVE_LABEL,
            PrivilegeAction.GRAPH_ACTIONS,
        )

    /** The privileges as the GRANT command names them. */
    private val KEYWORDS: Map<PrivilegeAction, String> =
        mapOf(
            PrivilegeAction.ACCESS to "ACCESS",
            PrivilegeAction.START_DATABASE to "START",
            PrivilegeAction.STOP_DATABASE to "STOP",
            PrivilegeAction.INDEX to "INDEX MANAGEMENT",
            PrivilegeAction.CREATE_INDEX to "CREATE INDEX",
            PrivilegeAction.DROP_INDEX to "DROP INDEX",
            PrivilegeAction.SHOW_INDEX to "SHOW INDEX",
            PrivilegeAction.CONSTRAINT to "CONSTRAINT MANAGEMENT",
            PrivilegeAction.CREATE_CONSTRAINT to "CREATE CONSTRAINT",
            PrivilegeAction.DROP_CONSTRAINT to "DROP CONSTRAINT",
            PrivilegeAction.SHOW_CONSTRAINT to "SHOW CONSTRAINT",
            PrivilegeAction.TOKEN to "NAME MANAGEMENT",
            PrivilegeAction.CREATE_LABEL to "CREATE NEW NODE LABEL",
            PrivilegeAction.CREATE_RELTYPE to "CREATE NEW RELATIONSHIP TYPE",
            PrivilegeAction.CREATE_PROPERTYKEY to "CREATE NEW PROPERTY NAME",
            PrivilegeAction.DATABASE_ACTIONS to "ALL DATABASE PRIVILEGES",
            PrivilegeAction.TRANSACTION_MANAGEMENT to "TRANSACTION MANAGEMENT",
            PrivilegeAction.SHOW_TRANSACTION to "SHOW TRANSACTION",
            PrivilegeAction.TERMINATE_TRANSACTION to "TERMINATE TRANSACTION",
            PrivilegeAction.TRAVERSE to "TRAVERSE",
            PrivilegeAction.READ to "READ",
            PrivilegeAction.MATCH to "MATCH",
            PrivilegeAction.MERGE to "MERGE",
            PrivilegeAction.WRITE to "WRITE",
            PrivilegeAction.CREATE_ELEMENT to "CREATE",
            PrivilegeAction.DELETE_ELEMENT to "DELETE",
            PrivilegeAction.SET_LABEL to "SET LABEL",
            PrivilegeAction.REMOVE_LABEL to "REMOVE LABEL",
            PrivilegeAction.SET_PROPERTY to "SET PROPERTY",
            PrivilegeAction.GRAPH_ACTIONS to "ALL GRAPH PRIVILEGES",
            PrivilegeAction.DBMS_ACTIONS to "ALL DBMS PRIVILEGES",
            PrivilegeAction.ROLE_MANAGEMENT to "ROLE MANAGEMENT",
            PrivilegeAction.CREATE_ROLE to "CREATE ROLE",
            PrivilegeAction.RENAME_ROLE to "RENAME ROLE",
            PrivilegeAction.DROP_ROLE to "DROP ROLE",
            PrivilegeAction.ASSIGN_ROLE to "ASSIGN ROLE",
            PrivilegeAction.REMOVE_ROLE to "REMOVE ROLE",
            PrivilegeAction.SHOW_ROLE to "SHOW ROLE",
            PrivilegeAction.USER_MANAGEMENT to "USER MANAGEMENT",
            PrivilegeAction.CREATE_USER to "CREATE USER",
            PrivilegeAction.RENAME_USER to "RENAME USER",
            PrivilegeAction.DROP_USER to "DROP USER",
            PrivilegeAction.SHOW_USER to "SHOW USER",
            PrivilegeAction.ALTER_USER to "ALTER USER",
            PrivilegeAction.SET_USER_STATUS to "SET USER STATUS",
            PrivilegeAction.SET_PASSWORDS to "SET PASSWORDS",
            PrivilegeAction.SET_AUTH to "SET AUTH",
            PrivilegeAction.SET_USER_HOME_DATABASE to "SET USER HOME DATABASE",
            PrivilegeAction.DATABASE_MANAGEMENT to "DATABASE MANAGEMENT",
            PrivilegeAction.CREATE_DATABASE to "CREATE DATABASE",
            PrivilegeAction.DROP_DATABASE to "DROP DATABASE",
            PrivilegeAction.ALTER_DATABASE to "ALTER DATABASE",
            PrivilegeAction.SET_DATABASE_ACCESS to "SET DATABASE ACCESS",
            PrivilegeAction.SET_DEFAULT_LANGUAGE to "SET DATABASE DEFAULT LANGUAGE",
            PrivilegeAction.COMPOSITE_DATABASE_MANAGEMENT to "COMPOSITE DATABASE MANAGEMENT",
            PrivilegeAction.CREATE_COMPOSITE_DATABASE to "CREATE COMPOSITE DATABASE",
            PrivilegeAction.DROP_COMPOSITE_DATABASE to "DROP COMPOSITE DATABASE",
            PrivilegeAction.ALIAS_MANAGEMENT to "ALIAS MANAGEMENT",
            PrivilegeAction.CREATE_ALIAS to "CREATE ALIAS",
            PrivilegeAction.DROP_ALIAS to "DROP ALIAS",
            PrivilegeAction.ALTER_ALIAS to "ALTER ALIAS",
            PrivilegeAction.SHOW_ALIAS to "SHOW ALIAS",
            PrivilegeAction.PRIVILEGE_MANAGEMENT to "PRIVILEGE MANAGEMENT",
            PrivilegeAction.SHOW_PRIVILEGE to "SHOW PRIVILEGE",
            PrivilegeAction.ASSIGN_PRIVILEGE to "ASSIGN PRIVILEGE",
            PrivilegeAction.REMOVE_PRIVILEGE to "REMOVE PRIVILEGE",
            PrivilegeAction.SERVER_MANAGEMENT to "SERVER MANAGEMENT",
            PrivilegeAction.SHOW_SERVER to "SHOW SERVERS",
            PrivilegeAction.IMPERSONATE to "IMPERSONATE",
            PrivilegeAction.EXECUTE_ADMIN to "EXECUTE ADMIN PROCEDURES",
            PrivilegeAction.SHOW_SETTING to "SHOW SETTING",
        )
}
