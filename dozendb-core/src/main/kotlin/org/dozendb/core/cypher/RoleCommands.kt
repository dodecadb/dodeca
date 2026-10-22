/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Modifications Copyright (c) DozenDB
 * 2026-10-23: Neo4j Community's DoNothingExecutionPlanner and EnsureNodeExistsExecutionPlanner for
 * the roles of DozenDB, with the role commands that Community refuses.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */
package org.dozendb.core.cypher

import org.dozendb.core.security.BuiltInRoles
import org.dozendb.core.security.PrivilegeRecords
import org.neo4j.common.DependencyResolver
import org.neo4j.cypher.internal.`AdministrationCommandRuntime$`
import org.neo4j.cypher.internal.AdministrationCommandRuntimeContext
import org.neo4j.cypher.internal.AdministrationShowCommandUtils
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.PrivilegeGQLCodeEntity
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.logical.plans.AssertRoleCanBeDropped
import org.neo4j.cypher.internal.logical.plans.AssertRoleCanBeRenamed
import org.neo4j.cypher.internal.logical.plans.AssertRoleCanBeReplaced
import org.neo4j.cypher.internal.logical.plans.AssertRolePrivilegesAreAllImmutable
import org.neo4j.cypher.internal.logical.plans.AssertRolePrivilegesCanAllBeCopied
import org.neo4j.cypher.internal.logical.plans.AssertSecurityDisabled
import org.neo4j.cypher.internal.logical.plans.CopyRolePrivileges
import org.neo4j.cypher.internal.logical.plans.CreateRole
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExists
import org.neo4j.cypher.internal.logical.plans.DoNothingIfNotExists
import org.neo4j.cypher.internal.logical.plans.DropRole
import org.neo4j.cypher.internal.logical.plans.EnsureNodeExists
import org.neo4j.cypher.internal.logical.plans.GrantRoleToUser
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NameValidator
import org.neo4j.cypher.internal.logical.plans.RenameRole
import org.neo4j.cypher.internal.logical.plans.RequireRole
import org.neo4j.cypher.internal.logical.plans.RevokeRoleFromUser
import org.neo4j.cypher.internal.logical.plans.`RoleEntity$`
import org.neo4j.cypher.internal.logical.plans.ShowRoles
import org.neo4j.cypher.internal.procs.`Continue$`
import org.neo4j.cypher.internal.procs.NotifyAndContinue
import org.neo4j.cypher.internal.procs.ParameterTransformer
import org.neo4j.cypher.internal.procs.PredicateExecutionPlan
import org.neo4j.cypher.internal.procs.QueryHandler
import org.neo4j.cypher.internal.procs.QueryHandlerBuilder
import org.neo4j.cypher.internal.procs.QueryHandlerResult
import org.neo4j.cypher.internal.procs.SystemCommandExecutionPlan
import org.neo4j.cypher.internal.procs.ThrowException
import org.neo4j.cypher.internal.procs.UpdatingSystemCommandExecutionPlan
import org.neo4j.cypher.internal.util.GrantRoleCommandHasNoEffectNotification
import org.neo4j.cypher.internal.util.ImpossibleRevokeCommandWarning
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.internal.util.RevokeRoleCommandHasNoEffectNotification
import org.neo4j.exceptions.DatabaseAdministrationOnFollowerException
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.Transaction
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues
import scala.Function1
import scala.Function2
import scala.Function3
import scala.Option
import scala.PartialFunction
import scala.Tuple2
import scala.collection.immutable.Seq
import scala.collection.immutable.Set
import scala.collection.immutable.`Set$`
import scala.jdk.javaapi.CollectionConverters
import scala.util.Either

/**
 * SHOW ROLES, CREATE, RENAME and DROP ROLE, and GRANT and REVOKE ROLE, which Neo4j Community plans
 * but refuses to run, on the roles of [PrivilegeRecords].
 *
 * PUBLIC, which every user has, cannot be created, renamed, dropped or revoked. Immutable roles
 * cannot be created, replaced, renamed or dropped while authentication is enabled.
 */
internal class RoleCommands(
    private val engine: ExecutionEngine,
    private val dependencies: DependencyResolver,
) : PartialFunction<LogicalPlan, Planner> {
    /** Compiles a nested plan with all commands, set by the runtime that owns these commands. */
    lateinit var compile: (LogicalPlan, AdministrationCommandRuntimeContext) -> ExecutionPlan

    // Resolved on first use, like Community's runtime does.
    private val authorization by lazy {
        SecurityAuthorizationHandler(
            dependencies.resolveDependency(AbstractSecurityLog::class.java)
        )
    }

    private val admin = `AdministrationCommandRuntime$`.`MODULE$`

    override fun isDefinedAt(plan: LogicalPlan): Boolean = planner(plan) != null

    override fun apply(plan: LogicalPlan): Planner =
        planner(plan) ?: throw IllegalArgumentException("Not a role command: $plan")

    private fun planner(plan: LogicalPlan): Planner? =
        when (plan) {
            is ShowRoles -> Planner { showRoles(plan, compile(plan.source(), it)) }
            is CreateRole -> Planner { createRole(plan, compile(plan.source(), it)) }
            is DropRole -> Planner { dropRole(plan.roleName(), compile(plan.source(), it)) }
            is RenameRole -> Planner { renameRole(plan, compile(plan.source(), it)) }
            is AssertRoleCanBeDropped ->
                Planner {
                    assertMutable(
                        "AssertRoleCanBeDropped",
                        plan.roleName(),
                        "delete",
                        compile(plan.source(), it),
                    ) {
                        AuthorizationViolationException.droppingImmutableRoles()
                    }
                }
            is AssertRoleCanBeReplaced ->
                Planner {
                    assertMutable(
                        "AssertRoleCanBeReplaced",
                        plan.roleName(),
                        "replace",
                        compile(plan.source(), it),
                    ) {
                        AuthorizationViolationException.replacingImmutableRoles()
                    }
                }
            is AssertRoleCanBeRenamed ->
                Planner {
                    assertMutable(
                        "AssertRoleCanBeRenamed",
                        plan.roleName(),
                        "rename",
                        compile(plan.source(), it),
                    ) {
                        AuthorizationViolationException.renamingImmutableRoles()
                    }
                }
            is AssertSecurityDisabled ->
                Planner {
                    PredicateExecutionPlan.apply(
                        Function2 { _, securityContext -> securityContext.isAuthDisabled },
                        Option.empty(),
                        Function3 { _, _, _ -> plan.onViolation().apply() },
                    )
                }
            is RequireRole -> Planner { requireRole(plan.name(), compile(plan.source(), it)) }
            is CopyRolePrivileges -> Planner { copyPrivileges(plan, compile(plan.source(), it)) }
            is AssertRolePrivilegesCanAllBeCopied ->
                Planner { assertCopyable(plan.roleName(), compile(plan.source(), it)) }
            is AssertRolePrivilegesAreAllImmutable ->
                Planner { assertAllImmutable(plan.roleName(), compile(plan.source(), it)) }
            is GrantRoleToUser -> Planner { grantRole(plan, compile(plan.source(), it)) }
            is RevokeRoleFromUser -> Planner { revokeRole(plan, compile(plan.source(), it)) }
            is DoNothingIfExists ->
                if (plan.entity() != `RoleEntity$`.`MODULE$`) null
                else
                    Planner {
                        doNothing(
                            "DoNothingIfExists",
                            plan.command(),
                            "create",
                            plan.name(),
                            plan.valueMapper(),
                            QueryHandler.ignoreOnResult(),
                            compile(plan.source(), it),
                        )
                    }
            is DoNothingIfNotExists ->
                if (plan.entity() != `RoleEntity$`.`MODULE$`) null
                else
                    Planner {
                        doNothing(
                            "DoNothingIfNotExists",
                            plan.command(),
                            plan.operation(),
                            plan.name(),
                            plan.valueMapper(),
                            QueryHandler.ignoreNoResult(),
                            compile(plan.source(), it),
                        )
                    }
            is EnsureNodeExists ->
                if (plan.entity() != `RoleEntity$`.`MODULE$`) null
                else Planner { ensureExists(plan, compile(plan.source(), it)) }
            else -> null
        }

    /**
     * Every role, or those with users; PUBLIC has every user, and a role without users has null.
     */
    private fun showRoles(plan: ShowRoles, source: ExecutionPlan): ExecutionPlan {
        val user = admin.userLabel()
        val userName = admin.userNamePropKey()
        return SystemCommandExecutionPlan(
            "ShowRoles",
            engine,
            authorization,
            """
            MATCH (role:$ROLE)
            OPTIONAL MATCH (assigned:$user)-[:$HAS_ROLE]->(role)
            WITH role, collect(assigned.$userName) AS assigned
            OPTIONAL MATCH (user:$user) WHERE role.$NAME = '${BuiltInRoles.PUBLIC}'
            WITH role, assigned, collect(user.$userName) AS everyone
            WITH role, assigned + everyone AS members
            ${if (plan.showAll()) "" else "WHERE size(members) > 0"}
            ${if (plan.withUsers()) "UNWIND CASE size(members) WHEN 0 THEN [null] ELSE members END AS member" else ""}
            WITH role.$NAME AS role, role.$IMMUTABLE AS immutable${if (plan.withUsers()) ", member" else ""}
            ${AdministrationShowCommandUtils.generateReturnClause(plan.returnColumns(), plan.yields(), plan.returns(), seqOf("role", "member"))}
            """
                .trimIndent(),
            VirtualValues.EMPTY_MAP,
            Option.apply(source),
            SystemCommandExecutionPlan.`apply$default$7`(),
            SystemCommandExecutionPlan.`apply$default$8`(),
            SystemCommandExecutionPlan.`apply$default$9`(),
        )
    }

    private fun createRole(plan: CreateRole, source: ExecutionPlan): ExecutionPlan {
        val name = admin.getNameFields("role", plan.roleName(), IDENTITY)
        return updating(
            "CreateRole",
            """
            CREATE (role:$ROLE {
                $NAME: ${parameter(name.nameKey())},
                ${PrivilegeRecords.ROLE_ID}: randomUUID(),
                $IMMUTABLE: ${parameter(IMMUTABLE_ROLE)}})
            RETURN role.$NAME AS name
            """
                .trimIndent(),
            VirtualValues.map(
                arrayOf(name.nameKey(), IMMUTABLE_ROLE),
                arrayOf(name.nameValue(), Values.booleanValue(plan.immutable())),
            ),
            QueryHandler.handleError(
                Function2 { error, params ->
                    val role = show(plan.roleName(), params)
                    if (hasStatus(error, Status.Schema.ConstraintValidationFailed)) {
                        InvalidArgumentException(
                            PrivilegeGQLCodeEntity.entityAlreadyExistsGqlStatus(ROLE_ENTITY, role),
                            "Failed to create the specified role '$role': Role already exists.",
                            error,
                        )
                    } else {
                        failed("CREATE ROLE", "create the specified role '$role'", error)
                    }
                }
            ),
            source,
            transformer(name.nameConverter()) { params ->
                val role = show(plan.roleName(), params)
                assertValidRoleName(role)
                NameValidator.assertUnreservedRoleName("create", role, Option.empty())
            },
        )
    }

    /** Drops a role with its privileges; one that does not exist is left to the plans before. */
    private fun dropRole(
        roleName: Either<String, Parameter>,
        source: ExecutionPlan,
    ): ExecutionPlan {
        val name = admin.getNameFields("role", roleName, IDENTITY)
        return updating(
            "DropRole",
            """
            MATCH (role:$ROLE {$NAME: ${parameter(name.nameKey())}})
            OPTIONAL MATCH (role)-[:$GRANTED|$DENIED]->(privilege:$PRIVILEGE)
            DETACH DELETE role, privilege
            """
                .trimIndent(),
            VirtualValues.map(arrayOf(name.nameKey()), arrayOf(name.nameValue())),
            QueryHandler.handleError(
                Function2 { error, params ->
                    failed(
                        "DROP ROLE",
                        "delete the specified role '${show(roleName, params)}'",
                        error,
                    )
                }
            ),
            source,
            transformer(name.nameConverter()) { params ->
                NameValidator.assertUnreservedRoleName(
                    "delete",
                    show(roleName, params),
                    Option.empty(),
                )
            },
        )
    }

    private fun renameRole(plan: RenameRole, source: ExecutionPlan): ExecutionPlan {
        val from = admin.getNameFields("from", plan.fromRoleName(), IDENTITY)
        val to = admin.getNameFields("to", plan.toRoleName(), IDENTITY)
        return updating(
            "RenameRole",
            """
            MATCH (role:$ROLE {$NAME: ${parameter(from.nameKey())}})
            SET role.$NAME = ${parameter(to.nameKey())}
            RETURN role.$NAME AS name
            """
                .trimIndent(),
            VirtualValues.map(
                arrayOf(from.nameKey(), to.nameKey()),
                arrayOf(from.nameValue(), to.nameValue()),
            ),
            QueryHandler.handleNoResult(
                    Function1 { params ->
                        val fromName = show(plan.fromRoleName(), params)
                        Option.apply(
                            ThrowException(
                                InvalidArgumentException.renameEntityNotFound(
                                    PrivilegeGQLCodeEntity.entityNotFoundGqlStatus(
                                        ROLE_ENTITY,
                                        fromName,
                                    ),
                                    "role",
                                    fromName,
                                    show(plan.toRoleName(), params),
                                )
                            )
                        )
                    }
                )
                .handleError(
                    Function2 { error, params ->
                        val fromName = show(plan.fromRoleName(), params)
                        val toName = show(plan.toRoleName(), params)
                        if (hasStatus(error, Status.Schema.ConstraintValidationFailed)) {
                            InvalidArgumentException.renameEntityAlreadyExists(
                                PrivilegeGQLCodeEntity.entityAlreadyExistsGqlStatus(
                                    ROLE_ENTITY,
                                    toName,
                                ),
                                "Role",
                                fromName,
                                toName,
                                error,
                            )
                        } else {
                            failed(
                                "RENAME ROLE",
                                "rename the specified role '$fromName' to '$toName'",
                                error,
                            )
                        }
                    }
                ),
            source,
            transformer(from.nameConverter(), to.nameConverter()) { params ->
                val fromName = show(plan.fromRoleName(), params)
                val toName = show(plan.toRoleName(), params)
                assertValidRoleName(toName)
                NameValidator.assertUnreservedRoleName("rename", fromName, Option.apply(toName))
            },
        )
    }

    /**
     * Refuses to [verb] PUBLIC, and an immutable role while authentication is enabled; a role that
     * does not exist is left to the plans after.
     */
    private fun assertMutable(
        planName: String,
        roleName: Either<String, Parameter>,
        verb: String,
        source: ExecutionPlan,
        violation: () -> Throwable,
    ): ExecutionPlan =
        refusing(
            planName,
            roleName,
            {
                "MATCH (role:$ROLE {$NAME: $it}) RETURN coalesce(role.$IMMUTABLE, false) AS immutable"
            },
            { violation() },
            source,
        ) { params ->
            NameValidator.assertUnreservedRoleName(verb, show(roleName, params), Option.empty())
        }

    private fun requireRole(
        roleName: Either<String, Parameter>,
        source: ExecutionPlan,
    ): ExecutionPlan {
        val name = admin.getNameFields("role", roleName, IDENTITY)
        return updating(
            "RequireRole",
            "MATCH (role:$ROLE {$NAME: ${parameter(name.nameKey())}}) RETURN role.$NAME AS name",
            VirtualValues.map(arrayOf(name.nameKey()), arrayOf(name.nameValue())),
            QueryHandler.handleNoResult(
                    Function1 { params ->
                        val role = show(roleName, params)
                        Option.apply(
                            ThrowException(
                                InvalidArgumentException(
                                    PrivilegeGQLCodeEntity.entityNotFoundGqlStatus(
                                        ROLE_ENTITY,
                                        role,
                                    ),
                                    "Failed to create a role as copy of '$role': " +
                                        "Role does not exist.",
                                )
                            )
                        )
                    }
                )
                .handleError(
                    Function2 { error, params ->
                        failed(
                            "CREATE ROLE",
                            "create a role as copy of '${show(roleName, params)}'",
                            error,
                        )
                    }
                ),
            source,
            transformer(name.nameConverter()) {},
        )
    }

    /** Copies the GRANTED or DENIED privileges of one role to another, each with its own key. */
    private fun copyPrivileges(plan: CopyRolePrivileges, source: ExecutionPlan): ExecutionPlan {
        val to = admin.getNameFields("to", plan.to(), IDENTITY)
        val from = admin.getNameFields("from", plan.from(), IDENTITY)
        val access: RelationshipType =
            when (plan.grantDeny()) {
                "GRANTED" -> PrivilegeRecords.GRANTED
                "DENIED" -> PrivilegeRecords.DENIED
                else ->
                    throw IllegalArgumentException(
                        "Neither GRANTED nor DENIED: ${plan.grantDeny()}"
                    )
            }
        return updating(
            "CopyRolePrivileges",
            """
            MATCH (to:$ROLE {$NAME: ${parameter(to.nameKey())}})
            MATCH (from:$ROLE {$NAME: ${parameter(from.nameKey())}})-[:${access.name()}]->(privilege:$PRIVILEGE)
            CREATE (to)-[:${access.name()}]->(copy:$PRIVILEGE)
            SET copy = privilege {.*, $KEY: to.$ID + substring(privilege.$KEY, size(from.$ID))}
            """
                .trimIndent(),
            VirtualValues.map(
                arrayOf(to.nameKey(), from.nameKey()),
                arrayOf(to.nameValue(), from.nameValue()),
            ),
            QueryHandler.handleError(
                Function2 { error, params ->
                    failed(
                        "CREATE ROLE",
                        "create a role as copy of '${show(plan.from(), params)}'",
                        error,
                    )
                }
            ),
            source,
            transformer(to.nameConverter(), from.nameConverter()) {},
        )
    }

    /** A mutable copy cannot have immutable privileges while authentication is enabled. */
    private fun assertCopyable(
        roleName: Either<String, Parameter>,
        source: ExecutionPlan,
    ): ExecutionPlan =
        refusing(
            "AssertRolePrivilegesCanAllBeCopied",
            roleName,
            {
                """
                MATCH (role:$ROLE {$NAME: $it})-[:$GRANTED|$DENIED]->(privilege:$PRIVILEGE)
                WHERE privilege.$IMMUTABLE
                RETURN true AS immutable
                LIMIT 1
                """
                    .trimIndent()
            },
            { AuthorizationViolationException.copyingRoleWithImmutablePrivileges(it) },
            source,
        ) {}

    /** An immutable copy can only have immutable privileges, even with authentication disabled. */
    private fun assertAllImmutable(
        roleName: Either<String, Parameter>,
        source: ExecutionPlan,
    ): ExecutionPlan {
        val name = admin.getNameFields("role", roleName, IDENTITY)
        return updating(
            "AssertRolePrivilegesAreAllImmutable",
            """
            MATCH (role:$ROLE {$NAME: ${parameter(name.nameKey())}})-[:$GRANTED|$DENIED]->(privilege:$PRIVILEGE)
            WHERE NOT privilege.$IMMUTABLE
            RETURN privilege.$KEY AS mutable
            LIMIT 1
            """
                .trimIndent(),
            VirtualValues.map(arrayOf(name.nameKey()), arrayOf(name.nameValue())),
            QueryHandler.handleResult(
                    Function3 { _, _, params ->
                        ThrowException(
                            AuthorizationViolationException.copyingRoleWithMutablePrivileges(
                                show(roleName, params)
                            )
                        )
                    }
                )
                .handleError(
                    Function2 { error, params ->
                        failed(
                            "CREATE ROLE",
                            "create a role as copy of '${show(roleName, params)}'",
                            error,
                        )
                    }
                ),
            source,
            transformer(name.nameConverter()) {},
        )
    }

    /** Grants a role, but not PUBLIC, which every user has; a user who has it is a notification. */
    private fun grantRole(plan: GrantRoleToUser, source: ExecutionPlan): ExecutionPlan {
        val role = admin.getNameFields("role", plan.roleName(), IDENTITY)
        val user = admin.getNameFields("user", plan.userName(), IDENTITY)
        return updating(
            "GrantRoleToUser",
            """
            OPTIONAL MATCH (role:$ROLE {$NAME: ${parameter(role.nameKey())}})
            OPTIONAL MATCH (user:${admin.userLabel()} {${admin.userNamePropKey()}: ${parameter(user.nameKey())}})
            OPTIONAL MATCH (user)-[had:$HAS_ROLE]->(role)
            WITH role, user, count(had) > 0 AS had
            FOREACH (_ IN CASE WHEN role IS NOT NULL AND user IS NOT NULL AND NOT had THEN [1] ELSE [] END |
                CREATE (user)-[:$HAS_ROLE]->(role))
            RETURN {role: role IS NOT NULL, user: user IS NOT NULL, had: had} AS result
            """
                .trimIndent(),
            VirtualValues.map(
                arrayOf(role.nameKey(), user.nameKey()),
                arrayOf(role.nameValue(), user.nameValue()),
            ),
            QueryHandler.handleResult(
                    Function3 { _, value, params ->
                        val result = value as MapValue
                        val roleName = show(plan.roleName(), params)
                        val userName = show(plan.userName(), params)
                        when {
                            !result.flag("role") ->
                                ThrowException(
                                    InvalidArgumentException(
                                        PrivilegeGQLCodeEntity.entityNotFoundGqlStatus(
                                            ROLE_ENTITY,
                                            roleName,
                                        ),
                                        "Failed to grant role '$roleName' to user '$userName': " +
                                            "Role does not exist.",
                                    )
                                )
                            !result.flag("user") ->
                                ThrowException(
                                    InvalidArgumentException.roleMissingUser(
                                        roleName,
                                        userName,
                                        null,
                                    )
                                )
                            result.flag("had") ->
                                notify(GrantRoleCommandHasNoEffectNotification(plan.command()))
                            else -> `Continue$`.`MODULE$`
                        }
                    }
                )
                .handleError(
                    Function2 { error, params ->
                        failed(
                            "GRANT ROLE",
                            "grant role '${show(plan.roleName(), params)}' to user " +
                                "'${show(plan.userName(), params)}'",
                            error,
                        )
                    }
                ),
            source,
            transformer(role.nameConverter(), user.nameConverter()) { params ->
                NameValidator.assertUnreservedRoleName(
                    "grant",
                    show(plan.roleName(), params),
                    Option.empty(),
                )
            },
        )
    }

    /**
     * Revokes a role, but not PUBLIC; a role the user does not have is a notification, and so is a
     * role or user that does not exist.
     */
    private fun revokeRole(plan: RevokeRoleFromUser, source: ExecutionPlan): ExecutionPlan {
        val role = admin.getNameFields("role", plan.roleName(), IDENTITY)
        val user = admin.getNameFields("user", plan.userName(), IDENTITY)
        return updating(
            "RevokeRoleFromUser",
            """
            OPTIONAL MATCH (role:$ROLE {$NAME: ${parameter(role.nameKey())}})
            OPTIONAL MATCH (user:${admin.userLabel()} {${admin.userNamePropKey()}: ${parameter(user.nameKey())}})
            OPTIONAL MATCH (user)-[had:$HAS_ROLE]->(role)
            WITH role, user, collect(had) AS had
            FOREACH (relationship IN had | DELETE relationship)
            RETURN {role: role IS NOT NULL, user: user IS NOT NULL, had: size(had) > 0} AS result
            """
                .trimIndent(),
            VirtualValues.map(
                arrayOf(role.nameKey(), user.nameKey()),
                arrayOf(role.nameValue(), user.nameValue()),
            ),
            QueryHandler.handleResult(
                    Function3 { _, value, _ ->
                        val result = value as MapValue
                        when {
                            !result.flag("role") ->
                                notify(
                                    ImpossibleRevokeCommandWarning(
                                        plan.command(),
                                        "Role does not exist.",
                                    )
                                )
                            !result.flag("user") ->
                                notify(
                                    ImpossibleRevokeCommandWarning(
                                        plan.command(),
                                        "User does not exist.",
                                    )
                                )
                            !result.flag("had") ->
                                notify(RevokeRoleCommandHasNoEffectNotification(plan.command()))
                            else -> `Continue$`.`MODULE$`
                        }
                    }
                )
                .handleError(
                    Function2 { error, params ->
                        failed(
                            "REVOKE ROLE",
                            "revoke role '${show(plan.roleName(), params)}' from user " +
                                "'${show(plan.userName(), params)}'",
                            error,
                        )
                    }
                ),
            source,
            transformer(role.nameConverter(), user.nameConverter()) { params ->
                NameValidator.assertUnreservedRoleName(
                    "revoke",
                    show(plan.roleName(), params),
                    Option.empty(),
                )
            },
        )
    }

    /** IF EXISTS and IF NOT EXISTS, as Community's for its `Role` nodes: [handler] stops. */
    private fun doNothing(
        planName: String,
        command: String,
        operation: String,
        roleName: Either<String, Parameter>,
        valueMapper: Function1<String, String>,
        handler: QueryHandlerBuilder,
        source: ExecutionPlan,
    ): ExecutionPlan {
        val name = admin.getNameFields("name", roleName, valueMapper)
        return updating(
            planName,
            "MATCH (role:$ROLE {$NAME: ${parameter(name.nameKey())}}) RETURN role.$NAME AS name",
            VirtualValues.map(arrayOf(name.nameKey()), arrayOf(name.nameValue())),
            handler.handleError(
                Function2 { error, params ->
                    failed(
                        command,
                        "$operation the specified role '${show(roleName, params)}'",
                        error,
                    )
                }
            ),
            source,
            transformer(name.nameConverter()) {},
        )
    }

    private fun ensureExists(plan: EnsureNodeExists, source: ExecutionPlan): ExecutionPlan {
        val name = admin.getNameFields("name", plan.name(), plan.valueMapper())
        val operation = "${plan.action()} the specified role"
        return updating(
            "EnsureNodeExists",
            """
            MATCH (role:$ROLE {$NAME: ${parameter(name.nameKey())}})
            ${plan.extraFilter().apply("role")}
            RETURN role.$NAME AS name
            """
                .trimIndent(),
            VirtualValues.map(arrayOf(name.nameKey()), arrayOf(name.nameValue())),
            QueryHandler.handleNoResult(
                    Function1 { params ->
                        val role = show(plan.name(), params)
                        Option.apply(
                            ThrowException(
                                InvalidArgumentException(
                                    PrivilegeGQLCodeEntity.entityNotFoundGqlStatus(
                                        ROLE_ENTITY,
                                        role,
                                    ),
                                    "Failed to $operation '$role': Role does not exist.",
                                )
                            )
                        )
                    }
                )
                .handleError(
                    Function2 { error, params ->
                        failed(plan.command(), "$operation '${show(plan.name(), params)}'", error)
                    }
                ),
            source,
            transformer(name.nameConverter()) {},
        )
    }

    /**
     * Fails with [violation] of the role when [query] of the role's parameter returns true, unless
     * authentication is disabled; [check]s the role's name first.
     */
    private fun refusing(
        planName: String,
        roleName: Either<String, Parameter>,
        query: (String) -> String,
        violation: (String) -> Throwable,
        source: ExecutionPlan,
        check: (MapValue) -> Unit,
    ): ExecutionPlan {
        val name = admin.getNameFields("role", roleName, IDENTITY)
        return updating(
            planName,
            query(parameter(name.nameKey())),
            VirtualValues.map(arrayOf(name.nameKey()), arrayOf(name.nameValue())),
            QueryHandler.handleResult(
                Function3 { _, value, params ->
                    val authDisabled = (params.get(AUTH_DISABLED) as BooleanValue).booleanValue()
                    if ((value as BooleanValue).booleanValue() && !authDisabled) {
                        ThrowException(violation(show(roleName, params)))
                    } else {
                        `Continue$`.`MODULE$`
                    }
                }
            ),
            source,
            transformer(name.nameConverter(), check = check)
                .generate(
                    Function3 { _, securityContext, _ ->
                        VirtualValues.map(
                            arrayOf(AUTH_DISABLED),
                            arrayOf(Values.booleanValue(securityContext.isAuthDisabled)),
                        )
                    }
                ),
        )
    }

    private fun updating(
        name: String,
        query: String,
        parameters: MapValue,
        handler: QueryHandler,
        source: ExecutionPlan,
        transformer: ParameterTransformer,
    ): ExecutionPlan =
        UpdatingSystemCommandExecutionPlan(
            name,
            engine,
            authorization,
            query,
            parameters,
            handler,
            Option.apply(source),
            UpdatingSystemCommandExecutionPlan.`$lessinit$greater$default$8`(),
            UpdatingSystemCommandExecutionPlan.`$lessinit$greater$default$9`(),
            transformer,
            UpdatingSystemCommandExecutionPlan.`$lessinit$greater$default$11`(),
        )

    /** Converts the parameters of names, then [check]s them before the command runs. */
    private fun transformer(
        vararg converters: Function2<Transaction, MapValue, MapValue>,
        check: (MapValue) -> Unit,
    ): ParameterTransformer =
        converters
            .fold(
                ParameterTransformer(
                    ParameterTransformer.`$lessinit$greater$default$1`(),
                    ParameterTransformer.`$lessinit$greater$default$2`(),
                )
            ) { transformer, converter ->
                transformer.convert(converter)
            }
            .validate(
                Function2 { _, params ->
                    check(params)
                    Tuple2(params, `Set$`.`MODULE$`.empty<InternalNotification>())
                }
            )

    /**
     * Neo4j's check of role names, and that they begin with a letter, as its docs require and its
     * check does not.
     */
    private fun assertValidRoleName(name: String) {
        NameValidator.assertValidRoleName(name)
        if (name.first() !in 'a'..'z' && name.first() !in 'A'..'Z') {
            throw InvalidArgumentException.inputContainsInvalidCharacters(
                name,
                "role name",
                "Role name '$name' must begin with an ascii letter.",
            )
        }
    }

    private fun notify(notification: InternalNotification): QueryHandlerResult =
        NotifyAndContinue(Set.Set1(notification))

    private fun failed(command: String, operation: String, error: Throwable): Throwable =
        when {
            hasStatus(error, Status.Cluster.NotALeader) ->
                DatabaseAdministrationOnFollowerException.notALeader(
                    command,
                    "Failed to $operation",
                    error,
                )
            error is Status.HasStatus -> error
            else -> IllegalStateException("Failed to $operation.", error)
        }

    private fun hasStatus(error: Throwable, status: Status): Boolean =
        generateSequence(error) { it.cause }.any { it is Status.HasStatus && it.status() == status }

    private fun MapValue.flag(key: String): Boolean = (get(key) as BooleanValue).booleanValue()

    private fun show(name: Either<String, Parameter>, params: MapValue): String =
        admin.runtimeStringValue(name, params)

    private fun parameter(key: String) = "${'$'}`$key`"

    private fun <T> seqOf(vararg items: T): Seq<T> =
        CollectionConverters.asScala(items.toList()).toSeq()

    private companion object {
        val ROLE = PrivilegeRecords.ROLE.name()
        val PRIVILEGE = PrivilegeRecords.PRIVILEGE.name()
        val HAS_ROLE = PrivilegeRecords.HAS_ROLE.name()
        val GRANTED = PrivilegeRecords.GRANTED.name()
        val DENIED = PrivilegeRecords.DENIED.name()
        const val NAME = PrivilegeRecords.ROLE_NAME
        const val ID = PrivilegeRecords.ROLE_ID
        const val IMMUTABLE = PrivilegeRecords.IMMUTABLE
        const val KEY = PrivilegeRecords.KEY

        val ROLE_ENTITY: PrivilegeGQLCodeEntity = PrivilegeGQLCodeEntity.Role()
        val IDENTITY = Function1<String, String> { it }

        /** The parameter of whether a new role is immutable. */
        const val IMMUTABLE_ROLE = "__dozendb_immutable"

        /** The parameter of whether authentication is disabled. */
        const val AUTH_DISABLED = "__dozendb_authDisabled"
    }
}
