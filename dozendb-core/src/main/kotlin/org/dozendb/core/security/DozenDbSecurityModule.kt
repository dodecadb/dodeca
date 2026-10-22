/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Modifications Copyright (c) DozenDB
 * 2026-10-20: Neo4j Community's CommunitySecurityModule with a realm that refuses suspended users.
 * 2026-10-23: logins whose transactions have the privileges of their users' roles, and
 * `dbms.showCurrentUser()` with their roles.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */
package org.dozendb.core.security

import java.util.UUID
import org.neo4j.collection.Dependencies
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.function.Suppliers
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog
import org.neo4j.internal.kernel.api.security.AuthenticationResult
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.procedure.GlobalProcedures
import org.neo4j.kernel.api.security.AuthManager
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
 * a wrong password, and sessions they already have lose access at their next transaction.
 */
class DozenDbSecurityModule(
    private val logService: LogService,
    private val config: Config,
    private val dependencies: Dependencies,
    private val securityLog: AbstractSecurityLog,
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
        private val defaultDatabase: (String) -> String,
    ) : BasicSystemGraphRealm(helper, strategy) {
        override fun login(
            authToken: MutableMap<String, Any>,
            connectionInfo: ClientConnectionInfo,
        ): LoginContext {
            val login = super.login(authToken, connectionInfo)
            val result = login.subject().authenticationResult
            if (
                result != AuthenticationResult.SUCCESS &&
                    result != AuthenticationResult.PASSWORD_CHANGE_REQUIRED
            ) {
                return login
            }
            if (security.isSuspended(login.subject().executingUser())) {
                return BasicLoginContext(
                    null as User?,
                    AuthenticationResult.FAILURE,
                    connectionInfo,
                )
            }
            return DozenDbLoginContext(login, security, defaultDatabase)
        }
    }
}

/**
 * A user's login: each of their transactions gets the privileges their roles have when it starts,
 * as [SystemGraphSecurity] reads them. Community's [login] answers for failed logins and expired
 * passwords.
 */
internal class DozenDbLoginContext(
    private val login: LoginContext,
    private val security: SystemGraphSecurity,
    private val defaultDatabase: (String) -> String,
) : LoginContext(login.subject(), login.connectionInfo()) {
    @Suppress("DEPRECATION") // Neo4j 5.26 has no GQL status for roles that do not exist yet.
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
        val graph = security.current()
        val user = subject().executingUser()
        if (graph.isSuspended(user)) {
            throw AuthorizationViolationException.authorizationViolation(
                "Database access is not allowed for user '$user': the user is suspended."
            )
        }
        if (!graph.initialized) {
            throw AuthorizationViolationException(
                "Database access is not allowed for user '$user' yet: this server does not have " +
                    "the roles of the cluster yet. Try again in a moment.",
                Status.General.DatabaseUnavailable,
            )
        }
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

    override fun impersonating(): Boolean = login.impersonating()

    override fun nativelyAuthenticated(): Boolean = login.nativelyAuthenticated()

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

/** The database [reference] is in, an alias's target; Neo4j leaves it out for full access. */
internal fun owner(reference: PrivilegeDatabaseReference): String =
    reference.owningDatabaseName() ?: reference.name()
