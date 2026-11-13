package dev.dodeca.core.security

import java.util.UUID
import org.neo4j.commandline.admin.security.SetDefaultAdminCommand
import org.neo4j.configuration.Config
import org.neo4j.dbms.database.AbstractSystemGraphComponent
import org.neo4j.dbms.database.ComponentVersion
import org.neo4j.dbms.database.SystemGraphComponent
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.ROLE_LABEL
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_LABEL
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_NAME_PROPERTY as USER_NAME
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Transaction
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.kernel.api.security.AuthManager
import org.neo4j.logging.InternalLogProvider
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.server.security.auth.CommunitySecurityModule
import org.neo4j.server.security.auth.FileUserRepository

/**
 * Roles and privileges in the system graph, in [PrivilegeRecords]' model, and auth rules, whose
 * names are unique ([AuthRule]). The first start with it creates the built-in roles, gives `admin`
 * to one user and leaves the others with PUBLIC alone, as Neo4j Enterprise does: the user named by
 * `neo4j-admin dbms set-default-admin`, else `neo4j`, else the only user.
 *
 * A system graph with Neo4j Enterprise's roles is refused: Dodeca cannot read them, and starting
 * without them would give their users other privileges than they had.
 */
class DodecaSecurityComponent(
    config: Config,
    private val fs: FileSystemAbstraction,
    private val logProvider: InternalLogProvider,
) : AbstractSystemGraphComponent(config) {
    private val log = logProvider.getLog(javaClass)

    override fun componentName(): SystemGraphComponent.Name = NAME

    override fun detect(tx: Transaction): SystemGraphComponent.Status =
        when (SystemGraphComponent.getVersionNumber(tx, NAME)) {
            null -> SystemGraphComponent.Status.UNINITIALIZED
            VERSION -> SystemGraphComponent.Status.CURRENT
            else -> SystemGraphComponent.Status.UNSUPPORTED_FUTURE
        }

    override fun initializeSystemGraph(system: GraphDatabaseService, firstInitialization: Boolean) {
        system.beginTx().use(::refuseEnterpriseRoles)
        super.initializeSystemGraph(system, firstInitialization)
    }

    /** Initializes roles where an older Dodeca version left none, as in a cluster. */
    override fun upgradeToCurrent(system: GraphDatabaseService) {
        system.beginTx().use(::refuseEnterpriseRoles)
        if (detect(system) == SystemGraphComponent.Status.UNINITIALIZED) {
            initializeSystemGraphSchema(system)
            initializeSystemGraphModel(system)
        }
    }

    override fun initializeSystemGraphSchema(system: GraphDatabaseService) {
        initializeSystemGraphConstraint(system, PrivilegeRecords.ROLE, PrivilegeRecords.ROLE_NAME)
        initializeSystemGraphConstraint(system, PrivilegeRecords.ROLE, PrivilegeRecords.ROLE_ID)
        initializeSystemGraphConstraint(system, PrivilegeRecords.PRIVILEGE, PrivilegeRecords.KEY)
        initializeSystemGraphConstraint(system, AuthRule.LABEL, AuthRule.NAME)
    }

    override fun initializeSystemGraphModel(tx: Transaction, system: GraphDatabaseService) {
        val (admin, why) = defaultAdmin(tx)
        val roles =
            BuiltInRoles.ALL.mapValues { (name, privileges) -> createRole(tx, name, privileges) }
        tx.findNode(USER_LABEL, USER_NAME, admin)
            .createRelationshipTo(roles.getValue(BuiltInRoles.ADMIN), PrivilegeRecords.HAS_ROLE)
        tx.findNodes(SystemGraphComponent.VERSION_LABEL).use { versions ->
            val version =
                if (versions.hasNext()) versions.next()
                else tx.createNode(SystemGraphComponent.VERSION_LABEL)
            version.setProperty(NAME.name(), VERSION)
        }
        log.info(
            "Created the built-in roles and gave the role ${BuiltInRoles.ADMIN} to the user " +
                "'$admin', $why. Every other user has only the role ${BuiltInRoles.PUBLIC}."
        )
    }

    private fun createRole(tx: Transaction, name: String, privileges: List<Privilege>) =
        tx.createNode(PrivilegeRecords.ROLE).also { role ->
            val id = UUID.randomUUID().toString()
            role.setProperty(PrivilegeRecords.ROLE_NAME, name)
            role.setProperty(PrivilegeRecords.ROLE_ID, id)
            role.setProperty(PrivilegeRecords.IMMUTABLE, false)
            for (privilege in privileges) {
                val node = tx.createNode(PrivilegeRecords.PRIVILEGE)
                PrivilegeRecords.write(node, id, privilege)
                role.createRelationshipTo(node, PrivilegeRecords.relationship(privilege.access))
            }
        }

    /** The user that gets `admin`, and why. */
    private fun defaultAdmin(tx: Transaction): Pair<String, String> {
        val users =
            tx.findNodes(USER_LABEL).use { nodes ->
                nodes.asSequence().map { it.getProperty(USER_NAME) as String }.toSortedSet()
            }
        val chosen = defaultAdminFile()
        return when {
            chosen != null ->
                if (chosen in users) {
                    chosen to "chosen with 'neo4j-admin dbms set-default-admin'"
                } else {
                    throw IllegalStateException(
                        "$NO_CANDIDATE The user '$chosen', chosen with 'neo4j-admin dbms " +
                            "set-default-admin', does not exist."
                    )
                }
            AuthManager.INITIAL_USER_NAME in users ->
                AuthManager.INITIAL_USER_NAME to "the initial user"
            users.size == 1 -> users.single() to "the only user"
            else ->
                throw IllegalStateException(
                    "$NO_CANDIDATE There are several users, and none is called " +
                        "'${AuthManager.INITIAL_USER_NAME}': ${users.joinToString()}."
                )
        }
    }

    /** The user that `neo4j-admin dbms set-default-admin` chose, if any. */
    private fun defaultAdminFile(): String? {
        val file =
            CommunitySecurityModule.getInitialUserRepositoryFile(config)
                .resolveSibling(SetDefaultAdminCommand.ADMIN_INI)
        if (!fs.fileExists(file)) return null
        val repository = FileUserRepository(fs, file, logProvider, EmptyMemoryTracker.INSTANCE)
        repository.init()
        repository.start()
        try {
            return repository.allUsernames.singleOrNull()
                ?: throw IllegalStateException(
                    "$NO_CANDIDATE The file $file should name one user, but names " +
                        "${repository.allUsernames.ifEmpty { listOf("none") }.joinToString()}."
                )
        } finally {
            repository.stop()
            repository.shutdown()
        }
    }

    private fun refuseEnterpriseRoles(tx: Transaction) {
        val versioned =
            tx.findNodes(SystemGraphComponent.VERSION_LABEL).use { versions ->
                versions.asSequence().any {
                    it.hasProperty(ComponentVersion.SECURITY_PRIVILEGE_COMPONENT.name())
                }
            }
        val roles = tx.findNodes(ROLE_LABEL).use { it.hasNext() }
        check(!versioned && !roles) {
            "The system database holds roles and privileges of Neo4j Enterprise Edition, which " +
                "Dodeca cannot read: starting without them would give their users other " +
                "privileges than they had. Export them with Neo4j Enterprise, using SHOW ROLES " +
                "WITH USERS and SHOW PRIVILEGES AS COMMANDS, and run the commands on Dodeca " +
                "with a new system database, as Dodeca's operator guide describes."
        }
    }

    companion object {
        val NAME = SystemGraphComponent.Name("dodeca-security")
        const val VERSION = 1

        private const val NO_CANDIDATE =
            "No admin user candidate is found. Use `neo4j-admin dbms set-default-admin` to " +
                "select a valid admin."
    }
}
