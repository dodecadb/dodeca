package org.dozendb.core.cypher

import org.dozendb.core.security.AuthRule
import org.dozendb.core.security.PrivilegeRecords
import org.neo4j.common.DependencyResolver
import org.neo4j.cypher.internal.AdministrationCommandRuntimeContext
import org.neo4j.cypher.internal.AdministrationShowCommandUtils
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.ast.prettifier.`ExpressionStringifier$`
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.logical.plans.AlterAuthRule
import org.neo4j.cypher.internal.logical.plans.`AuthRuleEntity$`
import org.neo4j.cypher.internal.logical.plans.CreateAuthRule
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExists
import org.neo4j.cypher.internal.logical.plans.DoNothingIfNotExists
import org.neo4j.cypher.internal.logical.plans.DropAuthRule
import org.neo4j.cypher.internal.logical.plans.EnsureNodeExists
import org.neo4j.cypher.internal.logical.plans.EnsureRoleHasNoDeniedPrivileges
import org.neo4j.cypher.internal.logical.plans.EnsureRoleNotGrantedToAnyAuthRules
import org.neo4j.cypher.internal.logical.plans.GrantRoleToAuthRule
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NameValidator
import org.neo4j.cypher.internal.logical.plans.RenameAuthRule
import org.neo4j.cypher.internal.logical.plans.RevokeRoleFromAuthRule
import org.neo4j.cypher.internal.logical.plans.ShowAuthRules
import org.neo4j.cypher.internal.notification.GrantRoleCommandHasNoEffectNotification
import org.neo4j.cypher.internal.notification.RevokeRoleCommandHasNoEffectNotification
import org.neo4j.cypher.internal.procs.`Continue$`
import org.neo4j.cypher.internal.procs.ParameterTransformer
import org.neo4j.cypher.internal.procs.QueryHandler
import org.neo4j.cypher.internal.procs.QueryHandlerBuilder
import org.neo4j.cypher.internal.procs.SystemCommandExecutionPlan
import org.neo4j.cypher.internal.procs.ThrowException
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlHelper
import org.neo4j.gqlstatus.GqlParams
import org.neo4j.gqlstatus.GqlStatusInfoCodes
import org.neo4j.gqlstatus.PrivilegeGqlCodeEntity
import org.neo4j.graphdb.Transaction
import org.neo4j.internal.kernel.api.security.AdminActionOnResource
import org.neo4j.internal.kernel.api.security.PrivilegeAction
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.internal.kernel.api.security.Segment
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.util.Stringifier.backtick
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues
import scala.Function1
import scala.Function2
import scala.Function3
import scala.Option
import scala.util.Either

/**
 * The commands of auth rules, as Neo4j Enterprise 2026.10 has them in Cypher 25: SHOW, CREATE,
 * ALTER, RENAME and DROP AUTH RULE, GRANT and REVOKE ROLE ... TO AUTH RULE, and the checks that
 * keep denied privileges away from the roles auth rules give. Rules are [AuthRule] nodes; whom they
 * give roles to, [org.dozendb.core.security.AttributeBasedAccess] works out.
 */
internal class AuthRuleCommands(engine: ExecutionEngine, dependencies: DependencyResolver) :
    SecurityCommands(engine, dependencies) {
    override fun planner(plan: LogicalPlan): Planner? =
        when (plan) {
            is ShowAuthRules -> Planner { show(plan, it) }
            is CreateAuthRule -> Planner { create(plan, compile(plan.source(), it), it) }
            is AlterAuthRule -> Planner { alter(plan, compile(plan.source(), it), it) }
            is RenameAuthRule -> Planner { rename(plan, compile(plan.source(), it)) }
            is DropAuthRule -> Planner { drop(plan.authRuleName(), compile(plan.source(), it)) }
            is GrantRoleToAuthRule -> Planner { grant(plan, compile(plan.source(), it)) }
            is RevokeRoleFromAuthRule -> Planner { revoke(plan, compile(plan.source(), it)) }
            is EnsureRoleHasNoDeniedPrivileges ->
                Planner {
                    refuseRole(
                        "EnsureRoleHasNoDeniedPrivileges",
                        plan.roleName(),
                        "MATCH (role:$ROLE {$ROLE_NAME: %s})-[:$DENIED]->() RETURN true LIMIT 1",
                        GqlStatusInfoCodes.STATUS_22ND2,
                        plan.subquery(),
                        if (plan.source().isDefined) compile(plan.source().get(), it) else null,
                    )
                }
            is EnsureRoleNotGrantedToAnyAuthRules ->
                Planner {
                    refuseRole(
                        "EnsureRoleNotGrantedToAnyAuthRules",
                        plan.roleName(),
                        "MATCH (:$RULE)-[:$GIVES_ROLE]->(role:$ROLE {$ROLE_NAME: %s}) " +
                            "RETURN true LIMIT 1",
                        GqlStatusInfoCodes.STATUS_22ND1,
                        plan.subquery(),
                        if (plan.source().isDefined) compile(plan.source().get(), it) else null,
                    )
                }
            is DoNothingIfExists ->
                if (plan.entity() != `AuthRuleEntity$`.`MODULE$`) null
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
                if (plan.entity() != `AuthRuleEntity$`.`MODULE$`) null
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
                if (plan.entity() != `AuthRuleEntity$`.`MODULE$`) null
                else Planner { ensureExists(plan, compile(plan.source(), it)) }
            else -> null
        }

    /**
     * The rules, by name, as Neo4j's docs show them: the roles they give only with SHOW ROLE, else
     * null; as commands, those that create them, without the roles, which SHOW ROLES WITH AUTH
     * RULES AS COMMANDS gives.
     */
    private fun show(
        plan: ShowAuthRules,
        context: AdministrationCommandRuntimeContext,
    ): ExecutionPlan {
        val rows = admin.internalKey("authRules")
        val columns =
            if (plan.asCommands()) "row.command AS command, row.roles AS roles"
            else
                "row.name AS name, row.condition AS condition, row.enabled AS enabled, " +
                    "row.roles AS roles"
        return SystemCommandExecutionPlan(
            "ShowAuthRules",
            engine,
            authorization,
            """
            UNWIND ${parameter(rows)} AS row
            WITH $columns
            ${AdministrationShowCommandUtils.generateReturnClause(plan.returnColumns(), plan.yields(), plan.returns(), if (plan.asCommands()) seqOf() else seqOf("name"))}
            """
                .trimIndent(),
            VirtualValues.EMPTY_MAP,
            Option.apply(compile(plan.source(), context)),
            SystemCommandExecutionPlan.`$lessinit$greater$default$7`(),
            ParameterTransformer.apply { tx, securityContext, _ ->
                VirtualValues.map(arrayOf(rows), arrayOf(rules(tx, securityContext)))
            },
            SystemCommandExecutionPlan.`$lessinit$greater$default$9`(),
            context.runtimeContext().cypherVersion(),
        )
    }

    private fun rules(tx: Transaction, securityContext: SecurityContext): ListValue {
        val roles =
            securityContext
                .allowsAdminAction(
                    AdminActionOnResource(
                        PrivilegeAction.SHOW_ROLE,
                        AdminActionOnResource.DatabaseScope.ALL,
                        Segment.ALL,
                    )
                )
                .allowsAccess()
        val rules =
            tx.findNodes(AuthRule.LABEL).use { nodes ->
                nodes.asSequence().map(AuthRule::read).sortedBy { it.name }.toList()
            }
        return VirtualValues.list(
            *rules
                .map { rule ->
                    VirtualValues.map(
                        arrayOf("name", "condition", "enabled", "roles", "command"),
                        arrayOf(
                            Values.utf8Value(rule.name),
                            Values.utf8Value(rule.condition),
                            Values.booleanValue(rule.enabled),
                            if (roles)
                                VirtualValues.fromList(rule.roles.sorted().map(Values::utf8Value))
                            else Values.NO_VALUE,
                            Values.utf8Value(
                                "CREATE AUTH RULE ${backtick(rule.name)} SET CONDITION " +
                                    "${rule.condition} SET ENABLED ${rule.enabled}"
                            ),
                        ),
                    )
                }
                .toTypedArray()
        )
    }

    /**
     * Creates a rule, enabled unless it says otherwise, with its condition as Neo4j prints it,
     * parsed again in the Cypher version of the command when transactions start.
     */
    private fun create(
        plan: CreateAuthRule,
        source: ExecutionPlan,
        context: AdministrationCommandRuntimeContext,
    ): ExecutionPlan {
        val name = admin.getNameFields("authRule", plan.authRuleName(), IDENTITY)
        val enabled = if (plan.enabled().isDefined) plan.enabled().get() as Boolean else true
        return updating(
            "CreateAuthRule",
            """
            OPTIONAL MATCH (existing:$RULE {$NAME: ${parameter(name.nameKey())}})
            WITH count(existing) > 0 AS exists
            FOREACH (_ IN CASE WHEN exists THEN [] ELSE [1] END |
                CREATE (:$RULE {
                    $NAME: ${parameter(name.nameKey())},
                    $CONDITION: ${parameter(CONDITION_PARAMETER)},
                    $ENABLED: ${parameter(ENABLED_PARAMETER)},
                    $VERSION: ${parameter(VERSION_PARAMETER)}}))
            RETURN exists
            """
                .trimIndent(),
            VirtualValues.map(
                arrayOf(name.nameKey(), CONDITION_PARAMETER, ENABLED_PARAMETER, VERSION_PARAMETER),
                arrayOf(
                    name.nameValue(),
                    Values.utf8Value(text(plan.condition())),
                    Values.booleanValue(enabled),
                    Values.utf8Value(context.runtimeContext().cypherVersion().persistedValue),
                ),
            ),
            QueryHandler.handleResult(
                    Function3 { _, exists, params ->
                        if (exists == Values.TRUE) {
                            ThrowException(alreadyExists(plan.authRuleName(), params))
                        } else {
                            `Continue$`.`MODULE$`
                        }
                    }
                )
                .handleError(
                    Function2 { error, params ->
                        if (hasStatus(error, Status.Schema.ConstraintValidationFailed)) {
                            alreadyExists(plan.authRuleName(), params)
                        } else {
                            failed(
                                "CREATE AUTH RULE",
                                "create the specified auth rule " +
                                    "'${show(plan.authRuleName(), params)}'",
                                error,
                            )
                        }
                    }
                ),
            source,
            transformer(name.nameConverter()) {},
        )
    }

    private fun alreadyExists(name: Either<String, Parameter>, params: MapValue) =
        InvalidArgumentException.createEntityAlreadyExists(RULE_ENTITY, show(name, params))

    /** Sets the condition, in the command's Cypher version, or whether a rule is enabled. */
    private fun alter(
        plan: AlterAuthRule,
        source: ExecutionPlan,
        context: AdministrationCommandRuntimeContext,
    ): ExecutionPlan {
        val name = admin.getNameFields("authRule", plan.authRuleName(), IDENTITY)
        val condition = if (plan.condition().isDefined) text(plan.condition().get()) else null
        val enabled = if (plan.enabled().isDefined) plan.enabled().get() as Boolean else null
        return updating(
            "AlterAuthRule",
            """
            MATCH (rule:$RULE {$NAME: ${parameter(name.nameKey())}})
            SET rule.$CONDITION = coalesce(${parameter(CONDITION_PARAMETER)}, rule.$CONDITION),
                rule.$VERSION = CASE WHEN ${parameter(CONDITION_PARAMETER)} IS NULL
                    THEN rule.$VERSION ELSE ${parameter(VERSION_PARAMETER)} END,
                rule.$ENABLED = coalesce(${parameter(ENABLED_PARAMETER)}, rule.$ENABLED)
            RETURN rule.$NAME AS name
            """
                .trimIndent(),
            VirtualValues.map(
                arrayOf(name.nameKey(), CONDITION_PARAMETER, ENABLED_PARAMETER, VERSION_PARAMETER),
                arrayOf(
                    name.nameValue(),
                    condition?.let(Values::utf8Value) ?: Values.NO_VALUE,
                    enabled?.let(Values::booleanValue) ?: Values.NO_VALUE,
                    Values.utf8Value(context.runtimeContext().cypherVersion().persistedValue),
                ),
            ),
            QueryHandler.handleNoResult(
                    Function1 { params ->
                        Option.apply(
                            ThrowException(
                                InvalidArgumentException.alterMissingAuthRule(
                                    show(plan.authRuleName(), params),
                                    parameterName(plan.authRuleName()),
                                )
                            )
                        )
                    }
                )
                .handleError(
                    Function2 { error, params ->
                        failed(
                            "ALTER AUTH RULE",
                            "alter the specified auth rule '${show(plan.authRuleName(), params)}'",
                            error,
                        )
                    }
                ),
            source,
            transformer(name.nameConverter()) {},
        )
    }

    private fun rename(plan: RenameAuthRule, source: ExecutionPlan): ExecutionPlan {
        val from = admin.getNameFields("from", plan.fromAuthRuleName(), IDENTITY)
        val to = admin.getNameFields("to", plan.toAuthRuleName(), IDENTITY)
        fun operation(params: MapValue) =
            "rename the specified auth rule '${show(plan.fromAuthRuleName(), params)}' to " +
                "'${show(plan.toAuthRuleName(), params)}'"
        return updating(
            "RenameAuthRule",
            """
            MATCH (rule:$RULE {$NAME: ${parameter(from.nameKey())}})
            OPTIONAL MATCH (other:$RULE {$NAME: ${parameter(to.nameKey())}}) WHERE other <> rule
            WITH rule, count(other) > 0 AS taken
            FOREACH (_ IN CASE WHEN taken THEN [] ELSE [1] END |
                SET rule.$NAME = ${parameter(to.nameKey())})
            RETURN taken
            """
                .trimIndent(),
            VirtualValues.map(
                arrayOf(from.nameKey(), to.nameKey()),
                arrayOf(from.nameValue(), to.nameValue()),
            ),
            QueryHandler.handleNoResult(
                    Function1 { params ->
                        Option.apply(
                            ThrowException(
                                InvalidArgumentException.renameEntityNotFound(
                                    RULE_ENTITY,
                                    show(plan.fromAuthRuleName(), params),
                                    show(plan.toAuthRuleName(), params),
                                    parameterName(plan.fromAuthRuleName()),
                                )
                            )
                        )
                    }
                )
                .handleResult(
                    Function3 { _, taken, params ->
                        if (taken == Values.TRUE) {
                            ThrowException(
                                InvalidArgumentException.renameEntityAlreadyExists(
                                    RULE_ENTITY,
                                    show(plan.fromAuthRuleName(), params),
                                    show(plan.toAuthRuleName(), params),
                                )
                            )
                        } else {
                            `Continue$`.`MODULE$`
                        }
                    }
                )
                .handleError(
                    Function2 { error, params ->
                        if (hasStatus(error, Status.Schema.ConstraintValidationFailed)) {
                            InvalidArgumentException.renameEntityAlreadyExists(
                                RULE_ENTITY,
                                show(plan.fromAuthRuleName(), params),
                                show(plan.toAuthRuleName(), params),
                            )
                        } else {
                            failed("RENAME AUTH RULE", operation(params), error)
                        }
                    }
                ),
            source,
            transformer(from.nameConverter(), to.nameConverter()) {},
        )
    }

    /** Drops a rule; one that does not exist is left to the plans before. */
    private fun drop(ruleName: Either<String, Parameter>, source: ExecutionPlan): ExecutionPlan {
        val name = admin.getNameFields("authRule", ruleName, IDENTITY)
        return updating(
            "DropAuthRule",
            "MATCH (rule:$RULE {$NAME: ${parameter(name.nameKey())}}) DETACH DELETE rule",
            VirtualValues.map(arrayOf(name.nameKey()), arrayOf(name.nameValue())),
            QueryHandler.handleError(
                Function2 { error, params ->
                    failed(
                        "DROP AUTH RULE",
                        "delete the specified auth rule '${show(ruleName, params)}'",
                        error,
                    )
                }
            ),
            source,
            transformer(name.nameConverter()) {},
        )
    }

    /** Gives a role, but not PUBLIC, to a rule; a rule that gives it already is a notification. */
    private fun grant(plan: GrantRoleToAuthRule, source: ExecutionPlan): ExecutionPlan {
        val role = admin.getNameFields("role", plan.roleName(), IDENTITY)
        val rule = admin.getNameFields("authRule", plan.ruleName(), IDENTITY)
        fun operation(params: MapValue) =
            "grant role '${show(plan.roleName(), params)}' to auth rule " +
                "'${show(plan.ruleName(), params)}'"
        return updating(
            "GrantRoleToAuthRule",
            """
            OPTIONAL MATCH (role:$ROLE {$ROLE_NAME: ${parameter(role.nameKey())}})
            OPTIONAL MATCH (rule:$RULE {$NAME: ${parameter(rule.nameKey())}})
            OPTIONAL MATCH (rule)-[had:$GIVES_ROLE]->(role)
            WITH role, rule, count(had) > 0 AS had
            FOREACH (_ IN CASE WHEN role IS NOT NULL AND rule IS NOT NULL AND NOT had THEN [1] ELSE [] END |
                CREATE (rule)-[:$GIVES_ROLE]->(role))
            RETURN {role: role IS NOT NULL, rule: rule IS NOT NULL, had: had} AS result
            """
                .trimIndent(),
            VirtualValues.map(
                arrayOf(role.nameKey(), rule.nameKey()),
                arrayOf(role.nameValue(), rule.nameValue()),
            ),
            QueryHandler.handleResult(
                    Function3 { _, value, params ->
                        val result = value as MapValue
                        val roleName = show(plan.roleName(), params)
                        when {
                            !result.flag("role") ->
                                ThrowException(
                                    InvalidArgumentException.failedActionEntityNotFound(
                                        operation(params),
                                        PrivilegeGqlCodeEntity.ROLE,
                                        roleName,
                                        parameterName(plan.roleName()),
                                    )
                                )
                            !result.flag("rule") ->
                                ThrowException(
                                    InvalidArgumentException.grantRoleToAuthRuleMissingAuthRule(
                                        roleName,
                                        show(plan.ruleName(), params),
                                        parameterName(plan.ruleName()),
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
                        failed("GRANT ROLE", operation(params), error)
                    }
                ),
            source,
            transformer(role.nameConverter(), rule.nameConverter()) { params ->
                NameValidator.assertUnreservedRoleName(
                    "grant",
                    show(plan.roleName(), params),
                    Option.empty(),
                )
            },
        )
    }

    /**
     * Takes a role, but not PUBLIC, from a rule; a role or rule that does not exist is an error, as
     * in Cypher 25, and a rule that does not give the role a notification.
     */
    private fun revoke(plan: RevokeRoleFromAuthRule, source: ExecutionPlan): ExecutionPlan {
        val role = admin.getNameFields("role", plan.roleName(), IDENTITY)
        val rule = admin.getNameFields("authRule", plan.ruleName(), IDENTITY)
        return updating(
            "RevokeRoleFromAuthRule",
            """
            OPTIONAL MATCH (role:$ROLE {$ROLE_NAME: ${parameter(role.nameKey())}})
            OPTIONAL MATCH (rule:$RULE {$NAME: ${parameter(rule.nameKey())}})
            OPTIONAL MATCH (rule)-[had:$GIVES_ROLE]->(role)
            WITH role, rule, collect(had) AS had
            FOREACH (relationship IN had | DELETE relationship)
            RETURN {role: role IS NOT NULL, rule: rule IS NOT NULL, had: size(had) > 0} AS result
            """
                .trimIndent(),
            VirtualValues.map(
                arrayOf(role.nameKey(), rule.nameKey()),
                arrayOf(role.nameValue(), rule.nameValue()),
            ),
            QueryHandler.handleResult(
                    Function3 { _, value, params ->
                        val result = value as MapValue
                        when {
                            !result.flag("role") ->
                                ThrowException(
                                    InvalidArgumentException.invalidCommandMissingRole(
                                        plan.command(),
                                        show(plan.roleName(), params),
                                        parameterName(plan.roleName()),
                                    )
                                )
                            !result.flag("rule") ->
                                ThrowException(
                                    InvalidArgumentException.invalidCommandMissingAuthRule(
                                        plan.command(),
                                        show(plan.ruleName(), params),
                                        parameterName(plan.ruleName()),
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
                            "revoke role '${show(plan.roleName(), params)}' from auth rule " +
                                "'${show(plan.ruleName(), params)}'",
                            error,
                        )
                    }
                ),
            source,
            transformer(role.nameConverter(), rule.nameConverter()) { params ->
                NameValidator.assertUnreservedRoleName(
                    "revoke",
                    show(plan.roleName(), params),
                    Option.empty(),
                )
            },
        )
    }

    /**
     * Refuses [subquery], with the GQL status [code], when [query] of the role finds anything: auth
     * rules give no roles with denied privileges, so that a rule never takes privileges away.
     */
    private fun refuseRole(
        planName: String,
        roleName: Either<String, Parameter>,
        query: String,
        code: GqlStatusInfoCodes,
        subquery: String,
        source: ExecutionPlan?,
    ): ExecutionPlan {
        val name = admin.getNameFields("role", roleName, IDENTITY)
        return updating(
            planName,
            query.format(parameter(name.nameKey())),
            VirtualValues.map(arrayOf(name.nameKey()), arrayOf(name.nameValue())),
            QueryHandler.handleResult(
                Function3 { _, _, _ ->
                    val gql =
                        ErrorGqlStatusObjectImplementation.from(code)
                            .withParam(GqlParams.StringParam.query, subquery)
                            .build()
                    ThrowException(InvalidArgumentException(gql, GqlHelper.getCompleteMessage(gql)))
                }
            ),
            source,
            transformer(name.nameConverter()) {},
        )
    }

    /** IF EXISTS and IF NOT EXISTS, as Community's for its `User` nodes: [handler] stops. */
    private fun doNothing(
        planName: String,
        command: String,
        operation: String,
        ruleName: Either<String, Parameter>,
        valueMapper: Function1<String, String>,
        handler: QueryHandlerBuilder,
        source: ExecutionPlan,
    ): ExecutionPlan {
        val name = admin.getNameFields("name", ruleName, valueMapper)
        return updating(
            planName,
            "MATCH (rule:$RULE {$NAME: ${parameter(name.nameKey())}}) RETURN rule.$NAME AS name",
            VirtualValues.map(arrayOf(name.nameKey()), arrayOf(name.nameValue())),
            handler.handleError(
                Function2 { error, params ->
                    failed(
                        command,
                        "$operation the specified auth rule '${show(ruleName, params)}'",
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
        val operation = "${plan.action()} the specified auth rule"
        return updating(
            "EnsureNodeExists",
            """
            MATCH (rule:$RULE {$NAME: ${parameter(name.nameKey())}})
            ${plan.extraFilter().apply("rule")}
            RETURN rule.$NAME AS name
            """
                .trimIndent(),
            VirtualValues.map(arrayOf(name.nameKey()), arrayOf(name.nameValue())),
            QueryHandler.handleNoResult(
                    Function1 { params ->
                        val rule = show(plan.name(), params)
                        Option.apply(
                            ThrowException(
                                InvalidArgumentException.failedActionEntityNotFound(
                                    "$operation '$rule'",
                                    RULE_ENTITY,
                                    rule,
                                    parameterName(plan.name()),
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

    /** [condition] as Neo4j prints conditions: with single quotes, as in its docs. */
    private fun text(condition: Expression): String =
        `ExpressionStringifier$`.`MODULE$`
            .apply(`ExpressionStringifier$`.`MODULE$`.failingExtender(), false, false, true, false)
            .apply(condition)

    private companion object {
        val RULE = AuthRule.LABEL.name()
        val GIVES_ROLE = AuthRule.GIVES_ROLE.name()
        val ROLE = PrivilegeRecords.ROLE.name()
        val DENIED = PrivilegeRecords.DENIED.name()
        const val ROLE_NAME = PrivilegeRecords.ROLE_NAME
        const val NAME = AuthRule.NAME
        const val CONDITION = AuthRule.CONDITION
        const val ENABLED = AuthRule.ENABLED
        const val VERSION = AuthRule.VERSION

        val RULE_ENTITY = PrivilegeGqlCodeEntity.AUTHRULE

        const val CONDITION_PARAMETER = "__dozendb_condition"
        const val ENABLED_PARAMETER = "__dozendb_enabled"
        const val VERSION_PARAMETER = "__dozendb_cypherVersion"
    }
}
