package dev.dodeca.core.security

import dev.dodeca.core.StandaloneServer
import java.nio.file.Path
import java.time.LocalDate
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.neo4j.commandline.admin.security.SetDefaultAdminCommand
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.dbms.database.ComponentVersion
import org.neo4j.dbms.database.SystemGraphComponent
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Transaction
import org.neo4j.internal.kernel.api.security.PatternSegment
import org.neo4j.internal.kernel.api.security.PrivilegeAction
import org.neo4j.internal.kernel.api.security.PropertyRule
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import org.neo4j.kernel.impl.security.User
import org.neo4j.logging.NullLog
import org.neo4j.logging.NullLogProvider
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.server.security.auth.CommunitySecurityModule
import org.neo4j.server.security.auth.FileUserRepository
import org.neo4j.server.security.auth.LegacyCredential
import org.neo4j.values.storable.CoordinateReferenceSystem
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values

/** Roles and privileges in the system graph, from the first start of a server. */
class SecurityComponentTest {
    // Kept when a test fails, for the server's logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private val server by lazy { StandaloneServer(root) }

    @AfterEach
    fun stop() {
        server.close()
    }

    private fun <T> system(work: (Transaction) -> T): T =
        server.database(StandaloneServer.SYSTEM).beginTx().use { tx ->
            work(tx).also { tx.commit() }
        }

    private fun security(): SecurityGraph = system { SecurityGraph.read(it, NullLog.getInstance()) }

    private fun roles(user: String): Set<String> = security().users.getValue(user).roles

    /** Makes the system graph one written before Dodeca had roles, with the users it has. */
    private fun forgetRoles() = system { tx ->
        for (label in listOf(PrivilegeRecords.PRIVILEGE, PrivilegeRecords.ROLE)) {
            tx.findNodes(label).forEach { node ->
                node.relationships.forEach { it.delete() }
                node.delete()
            }
        }
        tx.findNodes(SystemGraphComponent.VERSION_LABEL).forEach {
            it.removeProperty(DodecaSecurityComponent.NAME.name())
        }
    }

    private fun chooseDefaultAdmin(user: String) {
        val file =
            CommunitySecurityModule.getInitialUserRepositoryFile(
                    Config.defaults(GraphDatabaseSettings.neo4j_home, server.home)
                )
                .resolveSibling(SetDefaultAdminCommand.ADMIN_INI)
        val fs = DefaultFileSystemAbstraction()
        // As set-default-admin does.
        fs.deleteFile(file)
        FileUserRepository(fs, file, NullLogProvider.getInstance(), EmptyMemoryTracker.INSTANCE)
            .apply {
                init()
                start()
                create(User(user, null, LegacyCredential.INACCESSIBLE, false, false))
                stop()
                shutdown()
            }
    }

    @Test
    fun `a new DBMS has the built-in roles, and its initial user is admin`() {
        server.start()

        val security = security()

        assertThat(security.initialized).isTrue
        assertThat(security.roles.keys)
            .containsExactlyInAnyOrder(
                "PUBLIC",
                "reader",
                "editor",
                "publisher",
                "architect",
                "admin",
            )
        for ((role, privileges) in BuiltInRoles.ALL) {
            assertThat(security.roles.getValue(role).privileges)
                .describedAs(role)
                .containsExactlyInAnyOrderElementsOf(privileges)
        }
        assertThat(BuiltInRoles.ALL.getValue("admin")).hasSize(15)
        assertThat(roles("neo4j")).containsExactly("admin")
    }

    @Test
    fun `the roles are created once`() {
        server.start()
        server.administer("CREATE USER alice SET PASSWORD 'alice-secret-1'")

        server.restart()

        assertThat(security().roles).hasSize(6)
        assertThat(roles("neo4j")).containsExactly("admin")
        assertThat(roles("alice")).isEmpty()
    }

    @Test
    fun `existing users keep only PUBLIC, but neo4j, who becomes admin`() {
        server.start()
        server.administer("CREATE USER alice SET PASSWORD 'alice-secret-1'")
        server.administer("CREATE USER bob SET PASSWORD 'bob-secret-1'")
        forgetRoles()

        server.restart()

        assertThat(roles("neo4j")).containsExactly("admin")
        assertThat(roles("alice")).isEmpty()
        assertThat(roles("bob")).isEmpty()
    }

    @Test
    fun `without neo4j, the only user becomes admin`() {
        server.start()
        server.administer("CREATE USER alice SET PASSWORD 'alice-secret-1'")
        server.administer("DROP USER neo4j")
        forgetRoles()

        server.restart()

        assertThat(roles("alice")).containsExactly("admin")
    }

    @Test
    fun `the user chosen with set-default-admin becomes admin`() {
        server.start()
        server.administer("CREATE USER alice SET PASSWORD 'alice-secret-1'")
        server.administer("CREATE USER bob SET PASSWORD 'bob-secret-1'")
        forgetRoles()
        chooseDefaultAdmin("bob")

        server.restart()

        assertThat(roles("bob")).containsExactly("admin")
        assertThat(roles("neo4j")).isEmpty()
    }

    @Test
    fun `refuses to start without a user to make admin`() {
        server.start()
        server.administer("CREATE USER alice SET PASSWORD 'alice-secret-1'")
        server.administer("CREATE USER bob SET PASSWORD 'bob-secret-1'")
        server.administer("DROP USER neo4j")
        forgetRoles()
        server.stop()

        assertThat(catchThrowable { server.start() })
            .hasStackTraceContaining(
                "No admin user candidate is found. Use `neo4j-admin dbms set-default-admin` to " +
                    "select a valid admin. There are several users, and none is called 'neo4j': " +
                    "alice, bob."
            )

        chooseDefaultAdmin("carol")
        assertThat(catchThrowable { server.start() })
            .hasStackTraceContaining(
                "The user 'carol', chosen with 'neo4j-admin dbms set-default-admin', does not exist."
            )

        chooseDefaultAdmin("alice")
        server.start()
        assertThat(roles("alice")).containsExactly("admin")
    }

    @ParameterizedTest
    @ValueSource(strings = ["Role nodes", "a security-privileges version"])
    fun `refuses a system graph with Neo4j Enterprise's roles`(what: String) {
        server.start()
        system { tx ->
            if (what == "Role nodes") {
                tx.createNode(Label.label("Role")).setProperty("name", "admin")
            } else {
                tx.findNodes(SystemGraphComponent.VERSION_LABEL).forEach {
                    it.setProperty(ComponentVersion.SECURITY_PRIVILEGE_COMPONENT.name(), 16)
                }
            }
        }
        server.stop()

        assertThat(catchThrowable { server.start() })
            .hasStackTraceContaining(
                "The system database holds roles and privileges of Neo4j Enterprise Edition, " +
                    "which Dodeca cannot read"
            )
    }

    @Test
    fun `a privilege that cannot be read grants nothing, or denies everything`() {
        server.start()
        system { tx ->
            val publisher =
                tx.findNode(PrivilegeRecords.ROLE, PrivilegeRecords.ROLE_NAME, "publisher")
            publisher.relationships.first().endNode.setProperty("action", "FLY")
            val reader = tx.findNode(PrivilegeRecords.ROLE, PrivilegeRecords.ROLE_NAME, "reader")
            val denied = tx.createNode(PrivilegeRecords.PRIVILEGE)
            denied.setProperty("action", "MATCH")
            reader.createRelationshipTo(denied, PrivilegeRecords.DENIED)
        }

        val security = security()

        assertThat(security.roles.getValue("publisher").privileges)
            .hasSize(BuiltInRoles.ALL.getValue("publisher").size - 1)
        assertThat(security.roles.getValue("reader").privileges)
            .containsAll(BuiltInRoles.ALL.getValue("reader"))
            .containsAll(PrivilegeRecords.DENY_ALL)
    }

    @Test
    fun `privileges are written and read back alike, each with its own key`() {
        val database = UUID.randomUUID()
        val privileges =
            listOf(
                Privilege(Access.GRANTED, PrivilegeAction.ACCESS, PrivilegeScope.Home),
                Privilege(Access.DENIED, PrivilegeAction.ACCESS, PrivilegeScope.Database(database)),
                Privilege(
                    Access.GRANTED,
                    PrivilegeAction.READ,
                    PrivilegeScope.AllDatabases,
                    PrivilegeResource.Property("name"),
                    PrivilegeSegment.Nodes("Person"),
                ),
                Privilege(
                    Access.GRANTED,
                    PrivilegeAction.SET_LABEL,
                    PrivilegeScope.AllDatabases,
                    PrivilegeResource.Label("Draft"),
                    PrivilegeSegment.Nodes(null),
                    immutable = true,
                ),
                Privilege(
                    Access.GRANTED,
                    PrivilegeAction.TERMINATE_TRANSACTION,
                    PrivilegeScope.AllDatabases,
                    segment = PrivilegeSegment.Users("bob"),
                ),
                Privilege(
                    Access.GRANTED,
                    PrivilegeAction.EXECUTE_BOOSTED,
                    PrivilegeScope.Dbms,
                    segment = PrivilegeSegment.Procedures("apoc.*"),
                ),
                Privilege(
                    Access.DENIED,
                    PrivilegeAction.LOAD_CIDR,
                    PrivilegeScope.Dbms,
                    segment = PrivilegeSegment.Cidr("10.0.0.0/8"),
                ),
                pattern(Values.intValue(1)),
                pattern(Values.longValue(1)),
                pattern(Values.doubleValue(1.0)),
                pattern(Values.stringValue("1")),
                pattern(
                    Values.stringArray("PUBLIC", "UNCLASSIFIED"),
                    PropertyRule.ComparisonOperator.IN,
                ),
                pattern(Values.temporalValue(LocalDate.of(2024, 10, 25))),
                pattern(Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 1.0, 2.0)),
                Privilege(
                    Access.DENIED,
                    PrivilegeAction.MATCH,
                    PrivilegeScope.AllDatabases,
                    PrivilegeResource.AllProperties,
                    PrivilegeSegment.NodePattern(
                        PatternSegment.NodeNullPatternSegment(
                            setOf("A", "B"),
                            "level",
                            PropertyRule.NullOperator.IS_NULL,
                        )
                    ),
                ),
            )
        server.start()

        val (read, keys) =
            server.database().beginTx().use { tx ->
                privileges
                    .map { privilege ->
                        val node = tx.createNode()
                        PrivilegeRecords.write(node, "role", privilege)
                        PrivilegeRecords.read(node, privilege.access) to
                            node.getProperty(PrivilegeRecords.KEY)
                    }
                    .unzip()
            }

        assertThat(read).containsExactlyElementsOf(privileges)
        assertThat(keys).doesNotHaveDuplicates()
        assertThat(keys)
            .containsExactlyElementsOf(privileges.map { PrivilegeRecords.key("role", it) })
    }

    private fun pattern(
        value: Value,
        operator: PropertyRule.ComparisonOperator = PropertyRule.ComparisonOperator.GREATER_THAN,
    ) =
        Privilege(
            Access.GRANTED,
            PrivilegeAction.TRAVERSE,
            PrivilegeScope.AllDatabases,
            PrivilegeResource.Graph,
            PrivilegeSegment.NodePattern(
                PatternSegment.NodeValuePatternSegment(
                    setOf("Doc"),
                    "level",
                    value,
                    operator,
                )
            ),
        )
}
