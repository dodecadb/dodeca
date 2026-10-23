package org.dozendb.core.cypher

import java.lang.reflect.Modifier
import java.nio.file.Path
import java.util.jar.JarFile
import kotlin.io.path.toPath
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.dozendb.core.StandaloneServer
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.cypher.internal.logical.plans.AdministrationCommandLogicalPlan

/**
 * Every administration command Neo4j plans is run by DozenDB, run by Community, or refused on
 * purpose. A Neo4j upgrade that adds a plan fails here until someone decides which.
 */
class AdministrationPlansTest {
    // Kept when a test fails, for the server's logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path

    @Test
    fun `every administration plan is run or refused on purpose`() {
        assertThat(DOZENDB intersect COMMUNITY).isEmpty()
        assertThat((DOZENDB + COMMUNITY) intersect REFUSED.keys).isEmpty()
        assertThat(plans()).containsExactlyInAnyOrderElementsOf(DOZENDB + COMMUNITY + REFUSED.keys)
    }

    @Test
    fun `refused commands fail as in Community`() {
        StandaloneServer(root).use { server ->
            server.start()
            for (command in REFUSED_COMMANDS) {
                val error = catchThrowable { server.administer(command) }
                assertThat(error).describedAs(command).isNotNull()
                assertThat(error.message).isEqualTo("Unsupported administration command: $command")
            }
            assertThat(server.rows("SHOW DATABASES YIELD name", database = StandaloneServer.SYSTEM))
                .extracting<Any?> { it["name"] }
                .containsExactlyInAnyOrder("neo4j", "system")
        }
    }

    /** The administration plans Neo4j has, by name. */
    private fun plans(): Set<String> {
        val base = AdministrationCommandLogicalPlan::class.java
        val jar = base.protectionDomain.codeSource.location.toURI().toPath()
        val prefix = base.packageName.replace('.', '/') + "/"
        return JarFile(jar.toFile()).use { file ->
            file
                .stream()
                .map { it.name }
                .filter { it.startsWith(prefix) && it.endsWith(".class") }
                .map { it.removeSuffix(".class").replace('/', '.') }
                .map { Class.forName(it, false, base.classLoader) }
                .filter { base.isAssignableFrom(it) && !Modifier.isAbstract(it.modifiers) }
                .map { it.simpleName }
                .toList()
                .toSet()
        }
    }

    private companion object {
        /**
         * Planned by DozenDB's commands. Of DoNothingIfExists, DoNothingIfNotExists and
         * EnsureNodeExists, it plans those for roles and Community those for users.
         */
        val DOZENDB =
            setOf(
                // Users
                "CreateUser",
                "AlterUser",
                "ShowUsers",
                "ShowCurrentUser",
                // Roles
                "ShowRoles",
                "CreateRole",
                "DropRole",
                "RenameRole",
                "AssertRoleCanBeDropped",
                "AssertRoleCanBeReplaced",
                "AssertRoleCanBeRenamed",
                "AssertSecurityDisabled",
                "RequireRole",
                "CopyRolePrivileges",
                "AssertRolePrivilegesCanAllBeCopied",
                "AssertRolePrivilegesAreAllImmutable",
                "GrantRoleToUser",
                "RevokeRoleFromUser",
                "DoNothingIfExists",
                "DoNothingIfNotExists",
                "EnsureNodeExists",
                // Privileges
                "GrantDbmsAction",
                "DenyDbmsAction",
                "RevokeDbmsAction",
                "AssertDbmsPrivilegeCanBeMutated",
                "AssertDbmsActionIsAssignable",
                "GrantDatabaseAction",
                "DenyDatabaseAction",
                "RevokeDatabaseAction",
                "AssertDatabasePrivilegeCanBeMutated",
                "GrantGraphAction",
                "DenyGraphAction",
                "RevokeGraphAction",
                "AssertGraphPrivilegeCanBeMutated",
                "GrantLoadAction",
                "DenyLoadAction",
                "RevokeLoadAction",
                "AssertLoadPrivilegeCanBeMutated",
                "AssertMutablePrivilegesCanBeAssignedToRole",
                "ShowPrivileges",
                "ShowPrivilegeCommands",
                "ShowSupportedPrivileges",
                // Databases
                "CreateDatabase",
                "EnsureValidNumberOfDatabases",
                "DropDatabase",
                "StartDatabase",
                "StopDatabase",
                "EnsureValidNonSystemDatabase",
                "AssertCanDropDatabase",
                "AssertManagementActionNotBlocked",
                "EnsureNameIsNotAmbiguous",
                "EnsureDatabaseSafeToDelete",
                "WaitForCompletion",
                // Aliases
                "CreateLocalDatabaseAlias",
                "DropDatabaseAlias",
                "ShowAliases",
                "EnsureDatabaseNodeExists",
                "AssertNotBlockedDropAlias",
                "AssertNotBlockedRemoteAliasManagement",
            )

        /** Planned by Community's runtime, which DozenDB's falls back to. */
        val COMMUNITY =
            setOf(
                "AllowedNonAdministrationCommands",
                "LogSystemCommand",
                "CheckNativeAuthentication",
                "AssertAllowedDbmsActions",
                "AssertAllowedDbmsActionsOrSelf",
                "AssertAllowedDatabaseAction",
                "AssertNotCurrentUser",
                "RenameUser",
                "DropUser",
                "SetOwnPassword",
                "ShowDatabase",
                "DoNothingIfDatabaseExists",
                "DoNothingIfDatabaseNotExists",
            )

        const val ALIASES = "aliases are not supported yet"
        const val SERVERS = "the dozendb.cluster procedures manage the servers"

        /** Planned by neither, so they fail as they do in Community: why. */
        val REFUSED =
            mapOf(
                "CreateRemoteDatabaseAlias" to ALIASES,
                "AlterLocalDatabaseAlias" to ALIASES,
                "AlterRemoteDatabaseAlias" to ALIASES,
                "EnsureAliasIsNotRemote" to ALIASES,
                "EnableServer" to SERVERS,
                "AlterServer" to SERVERS,
                "RenameServer" to SERVERS,
                "DropServer" to SERVERS,
                "ShowServers" to SERVERS,
                "DeallocateServer" to SERVERS,
                "ReallocateDatabases" to SERVERS,
                "AlterDatabase" to
                    "databases are read-write, on every member, with the options they were " +
                        "created with",
            )

        /** A command for each refused plan that is a command of its own. */
        val REFUSED_COMMANDS =
            listOf(
                "CREATE ALIAS remote FOR DATABASE neo4j AT 'neo4j+s://example.com' " +
                    "USER alice PASSWORD 'alice-secret-1'",
                "ALTER ALIAS films SET DATABASE TARGET neo4j",
                "ALTER ALIAS remote SET DATABASE USER bob",
                "ENABLE SERVER 'one'",
                "ALTER SERVER 'one' SET OPTIONS {modeConstraint: 'NONE'}",
                "RENAME SERVER 'one' TO 'two'",
                "DROP SERVER 'one'",
                "SHOW SERVERS",
                "DEALLOCATE DATABASES FROM SERVER 'one'",
                "REALLOCATE DATABASES",
                "ALTER DATABASE neo4j SET ACCESS READ ONLY",
                "ALTER DATABASE neo4j SET TOPOLOGY 1 PRIMARY",
            )
    }
}
