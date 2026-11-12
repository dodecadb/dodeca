package org.dozendb.core.security

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import org.neo4j.dbms.database.SystemGraphComponent
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_HOME_DB_PROPERTY as USER_HOME_DB
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_LABEL
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_NAME_PROPERTY as USER_NAME
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_SUSPENDED_PROPERTY as USER_SUSPENDED
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_TAGS_PROPERTY as USER_TAGS
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel
import org.neo4j.graphdb.Direction
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Transaction
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.logging.InternalLog

/**
 * The users, roles, privileges and auth rules of the system graph as one transaction read them.
 * [initialized] tells whether the roles exist yet: in a cluster formed before DozenDB had roles,
 * not until its leader created them.
 */
class SecurityGraph(
    val initialized: Boolean,
    val users: Map<String, User>,
    val roles: Map<String, Role>,
    private val databases: Map<String, UUID>,
    private val aliases: Map<String, UUID> = emptyMap(),
    private val remoteAliases: Set<String> = emptySet(),
    val authRules: List<AuthRule> = emptyList(),
) {
    class User(
        val suspended: Boolean,
        val home: String?,
        val roles: Set<String>,
        val tags: List<String> = emptyList(),
    )

    class Role(val immutable: Boolean, val privileges: List<Privilege>)

    fun isSuspended(user: String): Boolean = users[user]?.suspended == true

    /**
     * The home database of [user], or null for the DBMS default database. A local alias stands for
     * its database, as Neo4j Enterprise resolves home databases set to aliases.
     */
    fun homeDatabase(user: String): String? =
        users[user]?.home?.let { home ->
            aliases[NormalizedDatabaseName(home).name()]?.let(names::get) ?: home
        }

    /**
     * The id of the database [name], or of the target of the local alias [name], whose privileges
     * are those of its database, or the one that stands for the remote alias [name]
     * ([remoteAliasId]); null if there is none of them.
     */
    fun databaseId(name: String): UUID? =
        databases[name]
            ?: aliases[name]
            ?: name.takeIf { it in remoteAliases }?.let(::remoteAliasId)

    private val names: Map<UUID, String> by lazy {
        databases.entries.associate { (name, id) -> id to name }
    }

    /** The name of the database with the id [id], or null if there is none. */
    fun databaseName(id: UUID): String? = names[id]

    /**
     * The roles granted to [user], and PUBLIC, or none while the user is suspended or unknown;
     * [AttributeBasedAccess] adds those of auth rules.
     */
    fun roles(user: String): Set<String> {
        val record = users[user]
        if (record == null || record.suspended) return emptySet()
        return record.roles + BuiltInRoles.PUBLIC
    }

    /** The privileges of the roles called [names]. */
    fun privileges(names: Set<String>): List<Privilege> = names.flatMap {
        roles[it]?.privileges.orEmpty()
    }

    /** The access modes compiled for this read of the system graph, which is their lifetime. */
    internal val accessModes = ConcurrentHashMap<AccessModeKey, DozenDbAccessMode>()

    /**
     * Who an access mode is for: the same roles and home database give the same privileges, and the
     * same [tags] where property rules compare with them.
     */
    internal data class AccessModeKey(
        val roles: Set<String>,
        val home: UUID?,
        val database: UUID,
        val tags: List<String>? = null,
    )

    /** The conditions of [authRules], compiled for this read of the system graph. */
    internal val conditions = ConcurrentHashMap<AuthRule, AttributeBasedAccess.Condition>()

    private val logged = ConcurrentHashMap.newKeySet<String>()

    /** Whether [message] is new for this read of the system graph, so to be logged. */
    internal fun firstTime(message: String): Boolean = logged.add(message)

    companion object {
        /**
         * Reads the system graph. A privilege that cannot be read is logged to [log]; if denied, it
         * stands for everything it could deny.
         */
        fun read(tx: Transaction, log: InternalLog): SecurityGraph {
            val initialized =
                SystemGraphComponent.getVersionNumber(tx, DozenDbSecurityComponent.NAME) != null
            val users =
                tx.findNodes(USER_LABEL).use { nodes ->
                    nodes.asSequence().associate { node ->
                        val roles =
                            node
                                .getRelationships(Direction.OUTGOING, PrivilegeRecords.HAS_ROLE)
                                .map {
                                    it.endNode.getProperty(PrivilegeRecords.ROLE_NAME) as String
                                }
                        node.getProperty(USER_NAME) as String to
                            User(
                                node.getProperty(USER_SUSPENDED, false) == true,
                                node.getProperty(USER_HOME_DB, null) as String?,
                                roles.toSet(),
                                (node.getProperty(USER_TAGS, null) as Array<*>?)
                                    ?.map { it as String }
                                    .orEmpty(),
                            )
                    }
                }
            val roles =
                tx.findNodes(PrivilegeRecords.ROLE).use { nodes ->
                    nodes.asSequence().associate { node ->
                        val name = node.getProperty(PrivilegeRecords.ROLE_NAME) as String
                        name to
                            Role(
                                node.getProperty(PrivilegeRecords.IMMUTABLE, false) == true,
                                privileges(node, name, log),
                            )
                    }
                }
            val databases =
                tx.findNodes(TopologyGraphDbmsModel.DATABASE_LABEL).use { nodes ->
                    nodes.asSequence().associate { node ->
                        node.getProperty(TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY) as String to
                            UUID.fromString(
                                node.getProperty(TopologyGraphDbmsModel.DATABASE_UUID_PROPERTY)
                                    as String
                            )
                    }
                }
            val ids = databases.values.toSet()
            val aliases =
                tx.findNodes(TopologyGraphDbmsModel.DATABASE_NAME_LABEL).use { nodes ->
                    nodes
                        .asSequence()
                        .filter {
                            it.getProperty(TopologyGraphDbmsModel.PRIMARY_PROPERTY, true) ==
                                false &&
                                it.getProperty(TopologyGraphDbmsModel.NAMESPACE_PROPERTY, null) ==
                                    TopologyGraphDbmsModel.DEFAULT_NAMESPACE
                        }
                        .mapNotNull { alias ->
                            val database =
                                alias
                                    .getSingleRelationship(
                                        TopologyGraphDbmsModel.TARGETS_RELATIONSHIP,
                                        Direction.OUTGOING,
                                    )
                                    ?.endNode
                                    ?.getProperty(TopologyGraphDbmsModel.DATABASE_UUID_PROPERTY)
                                    as String?
                            database
                                ?.let(UUID::fromString)
                                ?.takeIf { it in ids }
                                ?.let {
                                    alias.getProperty(TopologyGraphDbmsModel.NAME_PROPERTY)
                                        as String to it
                                }
                        }
                        .toMap()
                }
            val remoteAliases =
                tx.findNodes(TopologyGraphDbmsModel.REMOTE_DATABASE_LABEL).use { nodes ->
                    nodes
                        .asSequence()
                        .filter {
                            it.getProperty(TopologyGraphDbmsModel.NAMESPACE_PROPERTY, null) ==
                                TopologyGraphDbmsModel.DEFAULT_NAMESPACE
                        }
                        .map { it.getProperty(TopologyGraphDbmsModel.NAME_PROPERTY) as String }
                        .toSet()
                }
            val authRules =
                tx.findNodes(AuthRule.LABEL).use { nodes ->
                    nodes
                        .asSequence()
                        .mapNotNull { node ->
                            try {
                                AuthRule.read(node)
                            } catch (e: RuntimeException) {
                                log.error(
                                    "An auth rule cannot be read, so gives no roles: " +
                                        "${node.allProperties}.",
                                    e,
                                )
                                null
                            }
                        }
                        .sortedBy { it.name }
                        .toList()
                }
            return SecurityGraph(
                initialized,
                users,
                roles,
                databases,
                aliases,
                remoteAliases,
                authRules,
            )
        }

        private fun privileges(role: Node, name: String, log: InternalLog): List<Privilege> =
            Access.entries.flatMap { access ->
                role
                    .getRelationships(Direction.OUTGOING, PrivilegeRecords.relationship(access))
                    .flatMap { relationship ->
                        val node = relationship.endNode
                        try {
                            listOf(PrivilegeRecords.read(node, access))
                        } catch (e: RuntimeException) {
                            val denied = access == Access.DENIED
                            log.error(
                                "A privilege of the role '$name' cannot be read: " +
                                    "${node.allProperties}. It is " +
                                    (if (denied) "taken to deny everything." else "ignored."),
                                e,
                            )
                            if (denied) PrivilegeRecords.DENY_ALL else emptyList()
                        }
                    }
            }
    }
}
