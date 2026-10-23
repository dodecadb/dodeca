/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Modifications Copyright (c) DozenDB
 * 2026-10-20: Neo4j Community's CommunitySecurityModule with a realm that refuses suspended users.
 * 2026-10-23: logins whose transactions have the privileges of their users' roles,
 * `dbms.showCurrentUser()` with their roles, and impersonation.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */
package org.dozendb.core.security

import java.util.UUID
import org.dozendb.core.logging.SecurityLog
import org.neo4j.collection.Dependencies
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.function.Suppliers
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlStatusInfoCodes
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog
import org.neo4j.internal.kernel.api.security.AuthSubject
import org.neo4j.internal.kernel.api.security.AuthenticationResult
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.internal.kernel.api.security.PermissionState
import org.neo4j.internal.kernel.api.security.PrivilegeAction
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.procedure.GlobalProcedures
import org.neo4j.kernel.api.security.AuthManager
import org.neo4j.kernel.api.security.AuthToken
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.kernel.database.PrivilegeDatabaseReference
import org.neo4j.kernel.impl.security.User
import org.neo4j.logging.internal.LogService
import org.neo4j.messages.MessageUtil
import org.neo4j.server.security.SecureHasher
import org.neo4j.server.security.auth.AuthenticationStrategy
import org.neo4j.server.security.auth.BasicLoginContext
import org.neo4j.server.security.auth.CommunitySecurityModule
import org.neo4j.server.security.systemgraph.BasicSystemGraphRealm
import org.neo4j.server.security.systemgraph.SecurityGraphHelper

/**
 * Neo4j Community's security module, whose logins have the privileges of their users' roles in each
 * transaction, as the system graph has them when it starts. Suspended users cannot log in, as with
 * a wrong password, and sessions they already have lose access at their next transaction. Users
 * with IMPERSONATE can act as other users.
 */
class DozenDbSecurityModule(
    private val logService: LogService,
    private val config: Config,
    private val dependencies: Dependencies,
    private val securityLog: SecurityLog,
    private val security: SystemGraphSecurity,
    private val defaultDatabase: (String) -> String,
) : CommunitySecurityModule(logService, config, dependencies, securityLog) {
    private lateinit var realm: AuthManager

    override fun setup() {
        val system =
            Suppliers.lazySingleton<GraphDatabaseService> {
                dependencies
                    .resolveDependency(DatabaseContextProvider::class.java)
                    .getDatabaseContext(NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID)
                    .orElseThrow { IllegalStateException("No database called `system` was found.") }
                    .databaseFacade()
            }
        realm =
            Realm(
                SecurityGraphHelper(system, SecureHasher(), securityLog),
                createAuthenticationStrategy(config),
                security,
                securityLog,
                defaultDatabase,
            )
        registerProcedure(
            dependencies.resolveDependency(GlobalProcedures::class.java),
            logService.internalLogProvider.getLog(javaClass),
            DozenDbAuthProcedures::class.java,
        )
    }

    override fun authManager(): AuthManager = realm

    private class Realm(
        helper: SecurityGraphHelper,
        strategy: AuthenticationStrategy,
        private val security: SystemGraphSecurity,
        private val securityLog: SecurityLog,
        private val defaultDatabase: (String) -> String,
    ) : BasicSystemGraphRealm(helper, strategy) {
        override fun login(
            authToken: MutableMap<String, Any>,
            connectionInfo: ClientConnectionInfo,
        ): LoginContext {
            val login = super.login(authToken, connectionInfo)
            val user = authToken[AuthToken.PRINCIPAL] as? String ?: ""
            when (login.subject().authenticationResult) {
                AuthenticationResult.SUCCESS,
                AuthenticationResult.PASSWORD_CHANGE_REQUIRED -> {}
                AuthenticationResult.TOO_MANY_ATTEMPTS -> {
                    securityLog.loginFailed(user, connectionInfo, "too many failed attempts")
                    return login
                }
                else -> {
                    securityLog.loginFailed(
                        user,
                        connectionInfo,
                        "invalid principal or credentials",
                    )
                    return login
                }
            }
            if (security.isSuspended(login.subject().executingUser())) {
                securityLog.loginFailed(user, connectionInfo, "the user is suspended")
                return BasicLoginContext(
                    null as User?,
                    AuthenticationResult.FAILURE,
                    connectionInfo,
                )
            }
            return DozenDbLoginContext(login, security, defaultDatabase).also {
                securityLog.loggedIn(it)
            }
        }

        override fun impersonate(originalAuth: LoginContext, userToImpersonate: String) =
            (originalAuth as DozenDbLoginContext).impersonate(userToImpersonate, securityLog)
    }
}

/**
 * A user's login: each of their transactions gets the privileges their roles have when it starts,
 * as [SystemGraphSecurity] reads them. Community's [login] answers for failed logins and expired
 * passwords. The [subject] of an impersonating login executes as another user.
 */
internal class DozenDbLoginContext(
    private val login: LoginContext,
    private val security: SystemGraphSecurity,
    private val defaultDatabase: (String) -> String,
    subject: AuthSubject = login.subject(),
) : LoginContext(subject, login.connectionInfo()) {
    override fun authorize(
        idLookup: IdLookup,
        dbReference: PrivilegeDatabaseReference,
        securityLog: AbstractSecurityLog,
    ): SecurityContext {
        val result = subject().authenticationResult
        if (
            result != AuthenticationResult.SUCCESS &&
                result != AuthenticationResult.PASSWORD_CHANGE_REQUIRED
        ) {
            return login.authorize(idLookup, dbReference, securityLog)
        }
        val user = subject().executingUser()
        val graph = current(subject().authenticatedUser(), user)
        val roles = graph.roles(user)
        val home = graph.databaseId(NormalizedDatabaseName(defaultDatabase(user)).name())
        val policy = PrivilegePolicy(graph.privileges(roles), home)
        val name = NormalizedDatabaseName(owner(dbReference)).name()
        val database = graph.databaseId(name) ?: NO_DATABASE
        val mode = accessMode(graph, roles, home, database, policy, idLookup)
        val authorization =
            DozenDbSecurityContext.Authorization(name, policy, roles, graph, this, mode)
        val context =
            DozenDbSecurityContext(
                subject(),
                if (result == AuthenticationResult.PASSWORD_CHANGE_REQUIRED) {
                    // Community's refusal outside system, and its mode in it.
                    login.authorize(idLookup, dbReference, securityLog).mode()
                } else {
                    mode
                },
                connectionInfo(),
                dbReference.name(),
                authorization,
                authorization,
            )
        if (
            name != GraphDatabaseSettings.SYSTEM_DATABASE_NAME &&
                !policy.access(database).allowsAccess()
        ) {
            val message =
                "Database access is not allowed for ${MessageUtil.withUser(user, mode.name())}."
            securityLog.error(context, message)
            throw AuthorizationViolationException.authorizationViolation(message)
        }
        return context
    }

    /**
     * This login acting as [user], whose privileges its transactions get, if IMPERSONATE on them
     * allows it; drivers send the user with each transaction, which asks again. The user must exist
     * and not be suspended, and whoever impersonates must not have to change their password.
     */
    fun impersonate(user: String, securityLog: AbstractSecurityLog): LoginContext {
        if (subject().authenticationResult == AuthenticationResult.PASSWORD_CHANGE_REQUIRED) {
            throw AuthorizationViolationException(
                ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N83).build(),
                "Cannot impersonate a user while password change required.",
                Status.Security.CredentialsExpired,
            )
        }
        val self = subject().authenticatedUser()
        val graph = current(self)
        val permission =
            PrivilegePolicy(graph.privileges(graph.roles(self)), null)
                .admin(PrivilegeAction.IMPERSONATE, null, PrivilegeSegment.Users(user))
        val refusal =
            when {
                permission == PermissionState.EXPLICIT_DENY ->
                    "Permission denied for IMPERSONATE. $CHECK_PRIVILEGES"
                !permission.allowsAccess() ->
                    "Permission has not been granted for IMPERSONATE. $CHECK_PRIVILEGES"
                user !in graph.users -> "Cannot impersonate user '$user': the user does not exist."
                graph.isSuspended(user) -> "Cannot impersonate user '$user': the user is suspended."
                else ->
                    return DozenDbLoginContext(
                            login,
                            security,
                            defaultDatabase,
                            ImpersonatingSubject(subject(), user),
                        )
                        .also { (securityLog as? SecurityLog)?.impersonating(it, user) }
            }
        securityLog.error(this, "Impersonating user '$user' was refused: $refusal")
        throw AuthorizationViolationException.authorizationViolation(refusal)
    }

    override fun nativelyAuthenticated(): Boolean = login.nativelyAuthenticated()

    /**
     * The system graph as it is now, for transactions of [users], who must not be suspended. Until
     * a cluster's leader created the roles, nobody may run any.
     */
    @Suppress("DEPRECATION") // Neo4j 5.26 has no GQL status for roles that do not exist yet.
    private fun current(vararg users: String): SecurityGraph {
        val graph = security.current()
        for (user in users) {
            if (graph.isSuspended(user)) {
                throw AuthorizationViolationException.authorizationViolation(
                    "Database access is not allowed for user '$user': the user is suspended."
                )
            }
        }
        if (!graph.initialized) {
            throw AuthorizationViolationException(
                "Database access is not allowed for user '${users.last()}' yet: this server does " +
                    "not have the roles of the cluster yet. Try again in a moment.",
                Status.General.DatabaseUnavailable,
            )
        }
        return graph
    }

    /** The access mode of [roles] in [database], compiled once per read of the system graph. */
    private fun accessMode(
        graph: SecurityGraph,
        roles: Set<String>,
        home: UUID?,
        database: UUID,
        policy: PrivilegePolicy,
        lookup: IdLookup,
    ): DozenDbAccessMode {
        if (!lookup.isCachableLookup) return DozenDbAccessMode(policy, database, lookup, roles)
        val key = SecurityGraph.AccessModeKey(roles, home, database)
        val cached = graph.accessModes[key]
        if (cached != null && !cached.stale(lookup)) return cached
        return DozenDbAccessMode(policy, database, lookup, roles).also {
            graph.accessModes[key] = it
        }
    }
}

/** The subject of [original]'s login executing as [user], whose privileges apply. */
private class ImpersonatingSubject(private val original: AuthSubject, private val user: String) :
    AuthSubject {
    override fun getAuthenticationResult(): AuthenticationResult = original.authenticationResult

    override fun hasUsername(username: String): Boolean = username == user

    override fun executingUser(): String = user

    override fun authenticatedUser(): String = original.authenticatedUser()

    override fun nativelyAuthenticated(): Boolean = original.nativelyAuthenticated()
}

/** How Neo4j's refusals of administration commands go on. */
private const val CHECK_PRIVILEGES =
    "Try executing SHOW USER PRIVILEGES to determine the missing or denied privileges. In case " +
        "of missing privileges, they need to be granted (See GRANT). In case of denied " +
        "privileges, they need to be revoked (See REVOKE) and granted."

/** The database [reference] is in, an alias's target; Neo4j leaves it out for full access. */
internal fun owner(reference: PrivilegeDatabaseReference): String =
    reference.owningDatabaseName() ?: reference.name()
