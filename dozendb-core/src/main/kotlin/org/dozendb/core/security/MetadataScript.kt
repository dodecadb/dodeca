package org.dozendb.core.security

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.ast.prettifier.`ExpressionStringifier$`
import org.neo4j.graphdb.Transaction
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.logging.InternalLog
import org.neo4j.server.security.SystemGraphCredential
import org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.USER_CREDENTIALS
import org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.USER_EXPIRED
import org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.USER_LABEL
import org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.USER_NAME

/**
 * The Cypher that gives a DBMS back what concerns one database of its roles, privileges and users,
 * as `neo4j-admin database backup --include-metadata` keeps it with a backup, and a restore writes
 * to `restore_metadata.cypher`. The database is the script's parameter `$database`, so that it can
 * be restored under another name.
 *
 * The roles are those with privileges on the database, on every database or on users' home
 * databases, which they get again; privileges on the DBMS or on other databases are left out, as
 * Neo4j's docs say. The users are those whose roles give them access to the database, suspended or
 * not, with their encrypted passwords, status and home database, and the roles of the script they
 * have. Every statement can run again: roles and users are created if they do not exist.
 */
object MetadataScript {
    /** What `--include-metadata` asks for. */
    enum class Include(val roles: Boolean, val users: Boolean) {
        NONE(false, false),
        ALL(true, true),
        USERS(false, true),
        ROLES(true, false),
    }

    /**
     * The script for [database], as the system graph in [tx] has it, or null for none: nothing was
     * asked for, the database is `system` or there is no such database. [defaultDatabase] is the
     * DBMS's default database, the home of users without their own.
     */
    fun generate(
        tx: Transaction,
        database: String,
        include: Include,
        defaultDatabase: String,
        log: InternalLog,
    ): String? {
        val name = NormalizedDatabaseName(database).name()
        if (include == Include.NONE || name == GraphDatabaseSettings.SYSTEM_DATABASE_NAME) {
            return null
        }
        val graph = SecurityGraph.read(tx, log)
        val id = graph.databaseId(name) ?: return null
        fun affects(privilege: Privilege) =
            when (val scope = privilege.scope) {
                PrivilegeScope.AllDatabases,
                PrivilegeScope.Home -> true
                is PrivilegeScope.Database -> scope.id == id
                PrivilegeScope.Dbms -> false
            }
        val roles =
            graph.roles.filterValues { role -> role.privileges.any(::affects) }.keys.sorted()
        val statements = mutableListOf<String>()
        if (include.roles) {
            for (role in roles) {
                val record = graph.roles.getValue(role)
                if (role != BuiltInRoles.PUBLIC) {
                    val immutable = if (record.immutable) "IMMUTABLE " else ""
                    statements += "CREATE ${immutable}ROLE ${name(role)} IF NOT EXISTS"
                }
                statements +=
                    record.privileges
                        .filter(::affects)
                        .mapNotNull {
                            PrivilegeText.command(it, role, false, graph::databaseName, id)?.command
                        }
                        .distinct()
                        .sorted()
            }
        }
        if (include.users) {
            for (user in users(tx).sortedBy { it.name }) {
                val record = graph.users[user.name] ?: continue
                val home =
                    graph.databaseId(NormalizedDatabaseName(record.home ?: defaultDatabase).name())
                val held = record.roles + BuiltInRoles.PUBLIC
                if (!PrivilegePolicy(graph.privileges(held), home).access(id).allowsAccess()) {
                    continue
                }
                val credentials =
                    user.credentials?.let {
                        runCatching { SystemGraphCredential.maskSerialized(it) }.getOrNull()
                    }
                if (credentials == null) {
                    log.warn(
                        "The user '${user.name}' has no native password; the metadata of " +
                            "database '$name' leaves it out."
                    )
                    continue
                }
                val change =
                    if (user.passwordChangeRequired) "CHANGE REQUIRED" else "CHANGE NOT REQUIRED"
                val status = if (record.suspended) "SUSPENDED" else "ACTIVE"
                val homeClause =
                    record.home?.let {
                        val target =
                            if (NormalizedDatabaseName(it).name() == name) "\$database"
                            else name(it)
                        " SET HOME DATABASE $target"
                    } ?: ""
                statements +=
                    "CREATE USER ${name(user.name)} IF NOT EXISTS SET ENCRYPTED PASSWORD " +
                        "'$credentials' $change SET STATUS $status$homeClause"
                for (role in record.roles.filter { it in roles }.sorted()) {
                    statements += "GRANT ROLE ${name(role)} TO ${name(user.name)}"
                }
            }
        }
        return statements.joinToString("") { "$it;\n" }
    }

    /** A user's native credentials, as the system graph keeps them. */
    private class User(
        val name: String,
        val credentials: String?,
        val passwordChangeRequired: Boolean,
    )

    private fun users(tx: Transaction): List<User> =
        tx.findNodes(USER_LABEL).use { nodes ->
            nodes.asSequence().toList().map {
                User(
                    it.getProperty(USER_NAME) as String,
                    it.getProperty(USER_CREDENTIALS, null) as String?,
                    it.getProperty(USER_EXPIRED, false) as Boolean,
                )
            }
        }

    private fun name(name: String): String =
        `ExpressionStringifier$`.`MODULE$`.backtick(name, true, false)
}
