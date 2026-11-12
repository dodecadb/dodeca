package org.dozendb.core.security

import org.dozendb.core.edition.DefaultDatabase
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_CREDENTIALS_EXPIRED_PROPERTY as USER_EXPIRED
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_CREDENTIALS_PROPERTY as USER_CREDENTIALS
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_LABEL
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_NAME_PROPERTY as USER_NAME
import org.neo4j.graphdb.Transaction
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.logging.InternalLog
import org.neo4j.server.security.SystemGraphCredential
import org.neo4j.util.Stringifier

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
     * asked for, the database is `system` or there is no such database. The default database that
     * the system graph names is the home of users without their own. [users], if given, are the
     * only users it may hold, as `--include-metadata=users=alice,bob` asks.
     */
    fun generate(
        tx: Transaction,
        database: String,
        include: Include,
        log: InternalLog,
        users: Set<String>? = null,
    ): String? {
        val name = NormalizedDatabaseName(database).name()
        if (include == Include.NONE || name == GraphDatabaseSettings.SYSTEM_DATABASE_NAME) {
            return null
        }
        val graph = SecurityGraph.read(tx, log)
        val id = graph.databaseId(name) ?: return null
        val defaultDatabase =
            DefaultDatabase.name(tx)
                ?: GraphDatabaseSettings.initial_default_database.defaultValue()
        fun affects(privilege: Privilege) =
            when (val scope = privilege.scope) {
                // Those Cypher 5 grants on the DBMS are the DBMS's, which Neo4j keeps as on every
                // database.
                PrivilegeScope.AllDatabases -> privilege.action !in PrivilegeText.DATABASE_AND_DBMS
                PrivilegeScope.Home -> true
                is PrivilegeScope.Database -> scope.id == id
                // A remote alias's privileges are no database's.
                is PrivilegeScope.Alias,
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
                        .mapNotNull { privilege ->
                            // Prefixed where only Cypher 25 has the command.
                            PrivilegeText.command(
                                    privilege,
                                    role,
                                    false,
                                    graph::databaseName,
                                    CypherVersion.Cypher5,
                                    id,
                                )
                                ?.command
                                ?.let {
                                    if (PrivilegeText.cypher25Only(privilege)) "CYPHER 25 $it"
                                    else it
                                }
                        }
                        .distinct()
                        .sorted()
            }
        }
        if (include.users) {
            for (user in
                users(tx).filter { users == null || it.name in users }.sortedBy { it.name }) {
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

    private fun name(name: String): String = Stringifier.backtick(name, true)
}
