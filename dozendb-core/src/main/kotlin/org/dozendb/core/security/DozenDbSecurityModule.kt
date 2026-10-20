/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Modifications Copyright (c) DozenDB
 * 2026-10-20: Neo4j Community's CommunitySecurityModule with a realm that refuses suspended users.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */
package org.dozendb.core.security

import org.neo4j.collection.Dependencies
import org.neo4j.configuration.Config
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.function.Suppliers
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog
import org.neo4j.internal.kernel.api.security.AuthenticationResult
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.kernel.api.procedure.GlobalProcedures
import org.neo4j.kernel.api.security.AuthManager
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.database.PrivilegeDatabaseReference
import org.neo4j.kernel.impl.security.User
import org.neo4j.logging.internal.LogService
import org.neo4j.server.security.SecureHasher
import org.neo4j.server.security.auth.AuthProcedures
import org.neo4j.server.security.auth.AuthenticationStrategy
import org.neo4j.server.security.auth.BasicLoginContext
import org.neo4j.server.security.auth.CommunitySecurityModule
import org.neo4j.server.security.systemgraph.BasicSystemGraphRealm
import org.neo4j.server.security.systemgraph.SecurityGraphHelper

/**
 * Neo4j Community's security module, whose realm refuses suspended users: their login fails as with
 * a wrong password, and sessions they already have lose access at their next transaction.
 */
class DozenDbSecurityModule(
    private val logService: LogService,
    private val config: Config,
    private val dependencies: Dependencies,
    private val securityLog: AbstractSecurityLog,
    private val users: SystemGraphUsers,
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
                users,
            )
        registerProcedure(
            dependencies.resolveDependency(GlobalProcedures::class.java),
            logService.internalLogProvider.getLog(javaClass),
            AuthProcedures::class.java,
        )
    }

    override fun authManager(): AuthManager = realm

    private class Realm(
        helper: SecurityGraphHelper,
        strategy: AuthenticationStrategy,
        private val users: SystemGraphUsers,
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
            if (users.isSuspended(login.subject().executingUser())) {
                return BasicLoginContext(
                    null as User?,
                    AuthenticationResult.FAILURE,
                    connectionInfo,
                )
            }
            return ActiveUserLogin(login, users)
        }
    }

    /** A login that loses access once its user is suspended. */
    private class ActiveUserLogin(
        private val login: LoginContext,
        private val users: SystemGraphUsers,
    ) : LoginContext(login.subject(), login.connectionInfo()) {
        override fun authorize(
            idLookup: IdLookup,
            dbReference: PrivilegeDatabaseReference,
            securityLog: AbstractSecurityLog,
        ): SecurityContext {
            val user = subject().executingUser()
            if (users.isSuspended(user)) {
                throw AuthorizationViolationException.authorizationViolation(
                    "Database access is not allowed for user '$user': the user is suspended."
                )
            }
            return login.authorize(idLookup, dbReference, securityLog)
        }

        override fun impersonating(): Boolean = login.impersonating()

        override fun nativelyAuthenticated(): Boolean = login.nativelyAuthenticated()
    }
}
