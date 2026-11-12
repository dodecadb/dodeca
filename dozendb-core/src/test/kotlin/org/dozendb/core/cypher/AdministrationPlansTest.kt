package org.dozendb.core.cypher

import java.lang.reflect.Modifier
import java.util.jar.JarFile
import kotlin.io.path.toPath
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.neo4j.cypher.internal.logical.plans.AdministrationCommandLogicalPlan

/**
 * Every administration command Neo4j plans is run by DozenDB, run by Community, or refused on
 * purpose. A Neo4j upgrade that adds a plan fails here until someone decides which.
 */
class AdministrationPlansTest {
    @Test
    fun `every administration plan is run or refused on purpose`() {
        assertThat(DOZENDB intersect COMMUNITY).isEmpty()
        assertThat((DOZENDB + COMMUNITY) intersect REFUSED.keys).isEmpty()
        assertThat(plans()).containsExactlyInAnyOrderElementsOf(DOZENDB + COMMUNITY + REFUSED.keys)
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
         * EnsureNodeExists, it plans those for roles and auth rules, and Community those for users.
         */
        val DOZENDB =
            setOf(
                // Users
                "CreateUser",
                "AlterUser",
                "AlterUsers",
                "ShowUsers",
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
                // Auth rules
                "ShowAuthRules",
                "CreateAuthRule",
                "AlterAuthRule",
                "RenameAuthRule",
                "DropAuthRule",
                "GrantRoleToAuthRule",
                "RevokeRoleFromAuthRule",
                "EnsureRoleHasNoDeniedPrivileges",
                // Privileges
                "GrantDbmsAction",
                "DenyDbmsAction",
                "RevokeDbmsAction",
                "AssertDbmsPrivilegeCanBeMutated",
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
                "EnsureRoleNotGrantedToAnyAuthRules",
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
                "AssertNotShardedDatabase",
                "AssertNotInvalidActionOnShard",
                "AssertNotStandard",
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
                "ReallocateDatabases",
                "DropServer",
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
                "AssertCanAlterDatabase",
                "AssertNotGraphShard",
                "AssertNotPropertyShard",
                "AssertNotVirtualSpd",
            )

        /** Planned by neither, so they fail as they do in Community: why. */
        val REFUSED =
            mapOf(
                "EnsureAliasIsNotRemote" to "Neo4j does not plan it",
                "CreateShardedDatabase" to "Neo4j Enterprise's sharded databases",
                "AlterShardedDatabase" to "Neo4j Enterprise's sharded databases",
            )
    }
}
