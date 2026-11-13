/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Modifications Copyright (c) Dodeca
 * 2026-10-20: Neo4j Community's planners of CREATE USER, ALTER USER, SHOW USERS and SHOW CURRENT
 * USER, which keep and show the status and home database that Community refuses.
 * 2026-10-23: SHOW USERS and SHOW CURRENT USER show the roles of the users.
 * 2026-11-11: ported to Neo4j 2026, whose SHOW USERS lists the users in Java and runs SHOW CURRENT
 * USER outside the administration commands; user tags are refused.
 * 2026-11-12: user tags, with ALTER USERS, shown with SHOW USER METADATA.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */
package dev.dodeca.core.cypher

import dev.dodeca.core.security.BuiltInRoles
import dev.dodeca.core.security.PrivilegeRecords
import org.neo4j.common.DependencyResolver
import org.neo4j.configuration.Config
import org.neo4j.cypher.internal.`AdministrationCommandRuntime$`
import org.neo4j.cypher.internal.AdministrationCommandRuntimeContext
import org.neo4j.cypher.internal.AdministrationShowCommandUtils
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.ast.ExternalAuth
import org.neo4j.cypher.internal.ast.UserTagsAction
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.logical.plans.AlterUser
import org.neo4j.cypher.internal.logical.plans.AlterUsers
import org.neo4j.cypher.internal.logical.plans.CreateUser
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ShowUsers
import org.neo4j.cypher.internal.procs.ParameterTransformer
import org.neo4j.cypher.internal.procs.PredicateExecutionPlan
import org.neo4j.cypher.internal.procs.SystemCommandExecutionPlan
import org.neo4j.cypher.internal.procs.SystemGraphWriteExecutionPlan
import org.neo4j.cypher.internal.procs.WriteEffect
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_LABEL
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_NAME_PROPERTY
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.graphdb.Transaction
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.server.security.systemgraph.ShowUsersOutput
import org.neo4j.server.security.systemgraph.UserSecurityGraphComponent
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues
import scala.Function1
import scala.Function2
import scala.Function3
import scala.Option
import scala.PartialFunction
import scala.collection.immutable.Seq
import scala.jdk.javaapi.CollectionConverters
import scala.util.Either

/**
 * CREATE USER and ALTER USER with `SET STATUS`, `SET HOME DATABASE` and tags, which Neo4j Community
 * refuses, through Neo4j's own plans that store them; ALTER USERS, which sets the tags of many
 * users; and SHOW USERS, which shows them, and the users' roles, where Community shows null. Only
 * native authentication is supported.
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
            is AlterUsers -> Planner { alterUsers(plan, it) }
            is ShowUsers -> Planner { showUsers(plan, it) }
            else -> null
        }

    private fun createUser(
        plan: CreateUser,
        context: AdministrationCommandRuntimeContext,
    ): ExecutionPlan {
        val source = compile(plan.source(), context)
        val unsupported = external(plan.externalAuths())
        if (unsupported.isNotEmpty()) {
            return failure(source, "create", plan.userName(), unsupported)
        }
        return admin.makeCreateUserExecutionPlan(
            plan.userName(),
            plan.suspended().getOrElse { false } as Boolean,
            plan.defaultDatabase(),
            plan.nativeAuth(),
            plan.externalAuths(),
            admin.`makeCreateUserExecutionPlan$default$6`(),
            tagWriter(listOfNotNull(plan.tags().getOrElse { null }), "create", context),
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
        val unsupported =
            external(plan.externalAuths()) +
                listOfNotNull("`REMOVE AUTH`".takeUnless { plan.removeAuth().isEmpty })
        if (unsupported.isNotEmpty()) {
            return failure(source, "alter", plan.userName(), unsupported)
        }
        return admin.makeAlterUserExecutionPlan(
            plan.userName(),
            plan.suspended(),
            plan.defaultDatabase(),
            plan.nativeAuth(),
            plan.externalAuths(),
            plan.removeAuth(),
            admin.`makeAlterUserExecutionPlan$default$7`(),
            tagWriter(CollectionConverters.asJava(plan.tags()), "alter", context),
            Option.apply(source),
            engine,
            authorization,
            userSecurity,
            config,
        )
    }

    /** What writes the tags of [actions] once Neo4j's plan created or altered the user. */
    private fun tagWriter(
        actions: List<UserTagsAction>,
        operation: String,
        context: AdministrationCommandRuntimeContext,
    ): Option<Function3<Transaction, String, MapValue, Any>> {
        if (actions.isEmpty()) return Option.empty()
        val version = context.runtimeContext().cypherVersion()
        return Option.apply(
            Function3 { tx, user, params ->
                UserTags.write(tx, user, actions, params, version, operation)
            }
        )
    }

    /**
     * ALTER USERS: the tags of each user named, all or none, failing as ALTER USER does for a user
     * that does not exist, unless `IF EXISTS`, which leaves it out.
     */
    private fun alterUsers(
        plan: AlterUsers,
        context: AdministrationCommandRuntimeContext,
    ): ExecutionPlan {
        val actions = CollectionConverters.asJava(plan.tags())
        val version = context.runtimeContext().cypherVersion()
        return SystemGraphWriteExecutionPlan(
            "AlterUsers",
            authorization,
            Option.apply(compile(plan.source(), context)),
            WriteEffect.Counted.apply(
                Function3<Transaction, SecurityContext, MapValue, Any> { tx, _, params ->
                    val users =
                        CollectionConverters.asJava(plan.userNames()).mapNotNull { name ->
                            val user = admin.runtimeStringValue(name, params, IDENTITY)
                            tx.findNode(USER_LABEL, USER_NAME_PROPERTY, user)
                                ?: if (plan.ifExists()) null
                                else
                                    throw InvalidArgumentException.alterMissingUser(
                                        user,
                                        admin.getParameterName(name).getOrElse { null },
                                    )
                        }
                    users.distinct().count { UserTags.write(it, actions, params, version, "alter") }
                }
            ),
            SystemGraphWriteExecutionPlan.`$lessinit$greater$default$5`(),
            SystemGraphWriteExecutionPlan.`$lessinit$greater$default$6`(),
        )
    }

    private fun external(auths: Seq<ExternalAuth>): List<String> =
        CollectionConverters.asJava(auths).map { "`SET AUTH '${it.provider()}'`" }

    /**
     * Fails with "[clauses] not supported" once the [source] checks, such as privileges, passed.
     */
    private fun failure(
        source: ExecutionPlan,
        operation: String,
        userName: Either<String, Parameter>,
        clauses: List<String>,
    ): ExecutionPlan {
        val what = clauses.joinToString(", ")
        return PredicateExecutionPlan.apply(
            Function2 { _, _ -> false },
            Option.apply(source),
            Function3 { params, _, _ ->
                InvalidArgumentException(
                    notSupported(what),
                    "Failed to $operation the specified user " +
                        "'${nameOf(userName, params)}': $what " +
                        "${if (clauses.size == 1) "is" else "are"} not supported.",
                )
            },
        )
    }

    /**
     * The users as Neo4j Community lists them, with their status and home database, and the roles
     * granted to them in Dodeca's model, by name, then PUBLIC.
     */
    private fun showUsers(
        plan: ShowUsers,
        context: AdministrationCommandRuntimeContext,
    ): ExecutionPlan {
        val rows = admin.internalKey(ShowUsersOutput.USERS)
        return SystemCommandExecutionPlan(
            "ShowUsers",
            engine,
            authorization,
            """
            UNWIND ${'$'}`$rows` AS row
            MATCH (u:${USER_LABEL.name()} {$USER_NAME_PROPERTY: row.${ShowUsersOutput.USER}})
            WITH
              row.${ShowUsersOutput.COMMAND} AS command,
              row.${ShowUsersOutput.USER} AS user,
              COLLECT { MATCH (u)-[:${PrivilegeRecords.HAS_ROLE.name()}]->(role:${PrivilegeRecords.ROLE.name()})
                RETURN role.${PrivilegeRecords.ROLE_NAME} ORDER BY role.${PrivilegeRecords.ROLE_NAME} } +
                ['${BuiltInRoles.PUBLIC}'] AS roles,
              row.${ShowUsersOutput.PASSWORD_CHANGE_REQ} AS passwordChangeRequired,
              row.${ShowUsersOutput.SUSPENDED} AS suspended,
              row.${ShowUsersOutput.HOME} AS home,
              row.${ShowUsersOutput.PROVIDER} AS provider,
              row.${ShowUsersOutput.AUTH} AS auth,
              row.${ShowUsersOutput.TAGS} AS tags
            ${AdministrationShowCommandUtils.generateReturnClause(plan.returnColumns(), plan.yields(), plan.returns(), seqOf("user"))}
            """
                .trimIndent(),
            VirtualValues.EMPTY_MAP,
            Option.apply(compile(plan.source(), context)),
            SystemCommandExecutionPlan.`$lessinit$greater$default$7`(),
            ParameterTransformer.apply { tx, securityContext, _ ->
                userSecurity
                    .showUsers(
                        tx,
                        plan.withAuth(),
                        UserTags.visible(securityContext),
                        plan.asCommands(),
                        true,
                    )
                    .toMapValue()
            },
            SystemCommandExecutionPlan.`$lessinit$greater$default$9`(),
            context.runtimeContext().cypherVersion(),
        )
    }

    private fun <T> seqOf(vararg items: T): Seq<T> =
        CollectionConverters.asScala(items.toList()).toSeq()

    private companion object {
        val IDENTITY = Function1<String, String> { it }
    }
}
