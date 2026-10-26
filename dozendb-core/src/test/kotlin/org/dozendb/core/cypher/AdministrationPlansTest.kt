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
                "AlterDatabase",
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
                "CreateRemoteDatabaseAlias",
                "AlterLocalDatabaseAlias",
                "AlterRemoteDatabaseAlias",
                "DropDatabaseAlias",
                "ShowAliases",
                "EnsureDatabaseNodeExists",
                "DoNothingIfDatabaseExists",
                "DoNothingIfDatabaseNotExists",
                "AssertNotBlockedDropAlias",
                "AssertNotBlockedRemoteAliasManagement",
                // Servers
                "ShowServers",
                "EnableServer",
                "AlterServer",
                "RenameServer",
                "DeallocateServer",
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
            )

        const val PLACEMENT = "every enabled server hosts every database: none moves off one"

        /** Planned by neither, so they fail as they do in Community: why. */
        val REFUSED =
            mapOf(
                "EnsureAliasIsNotRemote" to "Neo4j 5.26 does not plan it",
                "DropServer" to PLACEMENT,
                "ReallocateDatabases" to PLACEMENT,
            )

        /** A command for each refused plan that is a command of its own. */
        val REFUSED_COMMANDS =
            listOf(
                "DROP SERVER 'one'",
                "REALLOCATE DATABASES",
            )
    }
}
