/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Modifications Copyright (c) DozenDB
 * 2026-10-20: Neo4j Community's planners of CREATE USER, ALTER USER, SHOW USERS and SHOW CURRENT
 * USER, which keep and show the status and home database that Community refuses.
 * 2026-10-23: SHOW USERS and SHOW CURRENT USER show the roles of the users.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */
package org.dozendb.core.cypher

import org.dozendb.core.security.BuiltInRoles
import org.dozendb.core.security.PrivilegeRecords
import org.neo4j.common.DependencyResolver
import org.neo4j.configuration.Config
import org.neo4j.cypher.internal.`AdministrationCommandRuntime$`
import org.neo4j.cypher.internal.AdministrationCommandRuntimeContext
import org.neo4j.cypher.internal.AdministrationShowCommandUtils
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.administration.ShowUsersExecutionPlanner
import org.neo4j.cypher.internal.ast.ExternalAuth
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.Yield
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.logical.plans.AlterUser
import org.neo4j.cypher.internal.logical.plans.CreateUser
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ShowCurrentUser
import org.neo4j.cypher.internal.logical.plans.ShowUsers
import org.neo4j.cypher.internal.procs.ParameterTransformer
import org.neo4j.cypher.internal.procs.PredicateExecutionPlan
import org.neo4j.cypher.internal.procs.SystemCommandExecutionPlan
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.server.security.systemgraph.UserSecurityGraphComponent
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues
import scala.Function2
import scala.Function3
import scala.Option
import scala.PartialFunction
import scala.collection.immutable.List as ScalaList
import scala.collection.immutable.Seq
import scala.jdk.javaapi.CollectionConverters
import scala.util.Either

/**
 * CREATE USER and ALTER USER with `SET STATUS` and `SET HOME DATABASE`, which Neo4j Community
 * refuses, through Neo4j's own plans that store them; and SHOW USERS and SHOW CURRENT USER, which
 * show them where Community shows null. Only native authentication is supported.
 */
internal class UserCommands(
    private val engine: ExecutionEngine,
    private val dependencies: DependencyResolver,
) : PartialFunction<LogicalPlan, Planner> {
    /** Compiles a nested plan with all commands, set by the runtime that owns these commands. */
    lateinit var compile: (LogicalPlan, AdministrationCommandRuntimeContext) -> ExecutionPlan

    private val config = dependencies.resolveDependency(Config::class.java)

    // Resolved on first use, like Community's runtime does.
    private val authorization by lazy {
        SecurityAuthorizationHandler(
            dependencies.resolveDependency(AbstractSecurityLog::class.java)
        )
    }
    private val userSecurity by lazy {
        dependencies.resolveDependency(UserSecurityGraphComponent::class.java)
    }

    private val admin = `AdministrationCommandRuntime$`.`MODULE$`

    override fun isDefinedAt(plan: LogicalPlan): Boolean = planner(plan) != null

    override fun apply(plan: LogicalPlan): Planner =
        planner(plan) ?: throw IllegalArgumentException("Not a user command: $plan")

    private fun planner(plan: LogicalPlan): Planner? =
        when (plan) {
            is CreateUser -> Planner { createUser(plan, it) }
            is AlterUser -> Planner { alterUser(plan, it) }
            is ShowUsers ->
                Planner {
                    showUsers(
                        plan.returnColumns(),
                        plan.withAuth(),
                        plan.yields(),
                        plan.returns(),
                        compile(plan.source(), it),
                    )
                }
            is ShowCurrentUser -> Planner { showCurrentUser(plan) }
            else -> null
        }

    private fun createUser(
        plan: CreateUser,
        context: AdministrationCommandRuntimeContext,
    ): ExecutionPlan {
        val source = compile(plan.source(), context)
        if (!plan.externalAuths().isEmpty) {
            return failure(source, "create", plan.userName(), external(plan.externalAuths()))
        }
        return admin.makeCreateUserExecutionPlan(
            plan.userName(),
            plan.suspended().getOrElse { false } as Boolean,
            plan.defaultDatabase(),
            plan.nativeAuth(),
            plan.externalAuths(),
            admin.`makeCreateUserExecutionPlan$default$6`(),
            Option.apply(source),
            engine,
            authorization,
            config,
        )
    }

    private fun alterUser(
        plan: AlterUser,
        context: AdministrationCommandRuntimeContext,
    ): ExecutionPlan {
        val source = compile(plan.source(), context)
        if (!plan.externalAuths().isEmpty) {
            return failure(source, "alter", plan.userName(), external(plan.externalAuths()))
        }
        if (!plan.removeAuth().isEmpty) {
            return failure(source, "alter", plan.userName(), "`REMOVE AUTH` is")
        }
        return admin.makeAlterUserExecutionPlan(
            plan.userName(),
            plan.suspended(),
            plan.defaultDatabase(),
            plan.nativeAuth(),
            plan.externalAuths(),
            plan.removeAuth(),
            admin.`makeAlterUserExecutionPlan$default$7`(),
            Option.apply(source),
            engine,
            authorization,
            userSecurity,
            config,
        )
    }

    private fun external(auths: Seq<ExternalAuth>): String =
        CollectionConverters.asJava(auths).joinToString(", ") { "`SET AUTH '${it.provider()}'`" } +
            if (auths.size() == 1) " is" else " are"

    /** Fails with "[what] not supported" once the [source] checks, such as privileges, passed. */
    @Suppress("DEPRECATION") // Neo4j 5.26 has no GQL status for unsupported features.
    private fun failure(
        source: ExecutionPlan,
        operation: String,
        userName: Either<String, Parameter>,
        what: String,
    ): ExecutionPlan =
        PredicateExecutionPlan.apply(
            Function2 { _, _ -> false },
            Option.apply(source),
            Function3 { params, _, _ ->
                InvalidArgumentException(
                    "Failed to $operation the specified user " +
                        "'${admin.runtimeStringValue(userName, params)}': $what not supported."
                )
            },
        )

    private fun showUsers(
        columns: ScalaList<LogicalVariable>,
        withAuth: Boolean,
        yields: Option<Yield>,
        returns: Option<Return>,
        source: ExecutionPlan,
    ): ExecutionPlan {
        val auth = if (withAuth) ShowUsersExecutionPlanner.getAuthCypher("u") else null
        return SystemCommandExecutionPlan(
            "ShowUsers",
            engine,
            authorization,
            """
            MATCH (u:${admin.userLabel()})
            ${auth?._1() ?: ""}
            WITH ${userColumns("u")}
            ${auth?._2() ?: ""}
            ${AdministrationShowCommandUtils.generateReturnClause(columns, yields, returns, seqOf("user"))}
            """
                .trimIndent(),
            VirtualValues.EMPTY_MAP,
            Option.apply(source),
            SystemCommandExecutionPlan.`apply$default$7`(),
            SystemCommandExecutionPlan.`apply$default$8`(),
            SystemCommandExecutionPlan.`apply$default$9`(),
        )
    }

    private fun showCurrentUser(plan: ShowCurrentUser): ExecutionPlan {
        val currentUser = "__internal_currentUser"
        return SystemCommandExecutionPlan(
            "ShowCurrentUser",
            engine,
            authorization,
            """
            MATCH (u:${admin.userLabel()})
            WITH ${userColumns("u")}
            WHERE user = ${'$'}`$currentUser`
            ${AdministrationShowCommandUtils.generateReturnClause(plan.returnColumns(), plan.yields(), plan.returns(), seqOf("user"))}
            """
                .trimIndent(),
            VirtualValues.EMPTY_MAP,
            Option.empty(),
            SystemCommandExecutionPlan.`apply$default$7`(),
            ParameterTransformer.apply { _, securityContext, _ ->
                VirtualValues.map(
                    arrayOf(currentUser),
                    arrayOf(Values.utf8Value(securityContext.subject().executingUser())),
                )
            },
            SystemCommandExecutionPlan.`apply$default$9`(),
        )
    }

    /** The columns of SHOW USERS: the roles are those granted, by name, then PUBLIC. */
    private fun userColumns(user: String): String =
        "$user.${admin.userNamePropKey()} AS user, " +
            "COLLECT { MATCH ($user)-[:${PrivilegeRecords.HAS_ROLE.name()}]->" +
            "(role:${PrivilegeRecords.ROLE.name()}) RETURN role.${PrivilegeRecords.ROLE_NAME} " +
            "ORDER BY role.${PrivilegeRecords.ROLE_NAME} } + ['${BuiltInRoles.PUBLIC}'] AS roles, " +
            "$user.${admin.userPwChangeReqPropKey()} AS passwordChangeRequired, " +
            "coalesce($user.${admin.userSuspendedPropKey()}, false) AS suspended, " +
            "$user.${admin.userHomeDbPropKey()} AS home"

    private fun <T> seqOf(vararg items: T): Seq<T> =
        CollectionConverters.asScala(items.toList()).toSeq()
}
