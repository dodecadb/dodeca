package org.dozendb.core.cypher

import inet.ipaddr.IPAddressString
import java.util.UUID
import org.dozendb.core.security.Access
import org.dozendb.core.security.Privilege
import org.dozendb.core.security.PrivilegeRecords
import org.dozendb.core.security.PrivilegeResource
import org.dozendb.core.security.PrivilegeScope
import org.dozendb.core.security.PrivilegeSegment
import org.neo4j.common.DependencyResolver
import org.neo4j.cypher.internal.`AdministrationCommandRuntime$`
import org.neo4j.cypher.internal.AdministrationCommandRuntimeContext
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.PrivilegeGQLCodeEntity
import org.neo4j.cypher.internal.ast.AdministrationAction
import org.neo4j.cypher.internal.ast.AllDatabasesQualifier
import org.neo4j.cypher.internal.ast.AllQualifier
import org.neo4j.cypher.internal.ast.DatabaseName
import org.neo4j.cypher.internal.ast.FunctionAllQualifier
import org.neo4j.cypher.internal.ast.FunctionQualifier
import org.neo4j.cypher.internal.ast.LoadAllQualifier
import org.neo4j.cypher.internal.ast.LoadCidrQualifier
import org.neo4j.cypher.internal.ast.PrivilegeQualifier
import org.neo4j.cypher.internal.ast.ProcedureAllQualifier
import org.neo4j.cypher.internal.ast.ProcedureQualifier
import org.neo4j.cypher.internal.ast.SettingAllQualifier
import org.neo4j.cypher.internal.ast.SettingQualifier
import org.neo4j.cypher.internal.ast.UnassignableAction
import org.neo4j.cypher.internal.ast.UserAllQualifier
import org.neo4j.cypher.internal.ast.UserQualifier
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.logical.plans.`AllScope$`
import org.neo4j.cypher.internal.logical.plans.AssertDatabasePrivilegeCanBeMutated
import org.neo4j.cypher.internal.logical.plans.AssertDbmsActionIsAssignable
import org.neo4j.cypher.internal.logical.plans.AssertDbmsPrivilegeCanBeMutated
import org.neo4j.cypher.internal.logical.plans.AssertLoadPrivilegeCanBeMutated
import org.neo4j.cypher.internal.logical.plans.AssertMutablePrivilegesCanBeAssignedToRole
import org.neo4j.cypher.internal.logical.plans.DenyDatabaseAction
import org.neo4j.cypher.internal.logical.plans.DenyDbmsAction
import org.neo4j.cypher.internal.logical.plans.DenyLoadAction
import org.neo4j.cypher.internal.logical.plans.GrantDatabaseAction
import org.neo4j.cypher.internal.logical.plans.GrantDbmsAction
import org.neo4j.cypher.internal.logical.plans.GrantLoadAction
import org.neo4j.cypher.internal.logical.plans.`HomeScope$`
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NamedScope
import org.neo4j.cypher.internal.logical.plans.PrivilegeCommandScope
import org.neo4j.cypher.internal.logical.plans.RevokeDatabaseAction
import org.neo4j.cypher.internal.logical.plans.RevokeDbmsAction
import org.neo4j.cypher.internal.logical.plans.RevokeLoadAction
import org.neo4j.cypher.internal.procs.ActionMapper
import org.neo4j.cypher.internal.procs.`Continue$`
import org.neo4j.cypher.internal.procs.NotifyAndContinue
import org.neo4j.cypher.internal.procs.ParameterTransformer
import org.neo4j.cypher.internal.procs.PredicateExecutionPlan
import org.neo4j.cypher.internal.procs.QueryHandler
import org.neo4j.cypher.internal.procs.QueryHandlerResult
import org.neo4j.cypher.internal.procs.ThrowException
import org.neo4j.cypher.internal.procs.UpdatingSystemCommandExecutionPlan
import org.neo4j.cypher.internal.util.AssignPrivilegeCommandHasNoEffectNotification
import org.neo4j.cypher.internal.util.ImpossibleRevokeCommandWarning
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.internal.util.RevokePrivilegeCommandHasNoEffectNotification
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel
import org.neo4j.exceptions.DatabaseAdministrationOnFollowerException
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlHelper
import org.neo4j.gqlstatus.GqlStatusInfoCodes
import org.neo4j.graphdb.Transaction
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog
import org.neo4j.internal.kernel.api.security.PrivilegeAction
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues
import scala.Function1
import scala.Function2
import scala.Function3
import scala.Option
import scala.PartialFunction
import scala.collection.immutable.Set
import scala.util.Either

/**
 * GRANT, DENY and REVOKE of DBMS, database and LOAD privileges, which Neo4j Community plans but
 * refuses to run. Neo4j's planner splits each command into commands of one privilege for one role,
 * which these run on the records of [PrivilegeRecords].
 *
 * IMMUTABLE privileges need authentication disabled, as nobody can be granted the privilege to
 * assign or remove them; immutable roles take only immutable privileges.
 */
internal class PrivilegeCommands(
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
        planner(plan) ?: throw IllegalArgumentException("Not a privilege command: $plan")

    private fun planner(plan: LogicalPlan): Planner? =
        when (plan) {
            is GrantDbmsAction ->
                Planner {
                    assign(
                        Access.GRANTED,
                        dbms(plan.action(), plan.qualifier()),
                        plan.roleName(),
                        plan.immutable(),
                        plan.command(),
                        compile(plan.source(), it),
                    )
                }
            is DenyDbmsAction ->
                Planner {
                    assign(
                        Access.DENIED,
                        dbms(plan.action(), plan.qualifier()),
                        plan.roleName(),
                        plan.immutable(),
                        plan.command(),
                        compile(plan.source(), it),
                    )
                }
            is RevokeDbmsAction ->
                Planner {
                    revoke(
                        access(plan.revokeType()),
                        dbms(plan.action(), plan.qualifier()),
                        plan.roleName(),
                        plan.immutableOnly(),
                        plan.command(),
                        compile(plan.source(), it),
                    )
                }
            is AssertDbmsPrivilegeCanBeMutated ->
                Planner {
                    assertRevocable(
                        access(plan.revokeType()),
                        dbms(plan.action(), plan.qualifier()),
                        plan.roleName(),
                        compile(plan.source(), it),
                    )
                }
            is GrantDatabaseAction ->
                Planner {
                    assign(
                        Access.GRANTED,
                        database(plan.action(), plan.database(), plan.qualifier()),
                        plan.roleName(),
                        plan.immutable(),
                        plan.command(),
                        compile(plan.source(), it),
                    )
                }
            is DenyDatabaseAction ->
                Planner {
                    assign(
                        Access.DENIED,
                        database(plan.action(), plan.database(), plan.qualifier()),
                        plan.roleName(),
                        plan.immutable(),
                        plan.command(),
                        compile(plan.source(), it),
                    )
                }
            is RevokeDatabaseAction ->
                Planner {
                    revoke(
                        access(plan.revokeType()),
                        database(plan.action(), plan.database(), plan.qualifier()),
                        plan.roleName(),
                        plan.immutableOnly(),
                        plan.command(),
                        compile(plan.source(), it),
                    )
                }
            is AssertDatabasePrivilegeCanBeMutated ->
                Planner {
                    assertRevocable(
                        access(plan.revokeType()),
                        database(plan.action(), plan.database(), plan.qualifier()),
                        plan.roleName(),
                        compile(plan.source(), it),
                    )
                }
            is GrantLoadAction ->
                Planner {
                    assign(
                        Access.GRANTED,
                        load(plan.action(), plan.qualifier()),
                        plan.roleName(),
                        plan.immutable(),
                        plan.command(),
                        compile(plan.source(), it),
                    )
                }
            is DenyLoadAction ->
                Planner {
                    assign(
                        Access.DENIED,
                        load(plan.action(), plan.qualifier()),
                        plan.roleName(),
                        plan.immutable(),
                        plan.command(),
                        compile(plan.source(), it),
                    )
                }
            is RevokeLoadAction ->
                Planner {
                    revoke(
                        access(plan.revokeType()),
                        load(plan.action(), plan.qualifier()),
                        plan.roleName(),
                        plan.immutableOnly(),
                        plan.command(),
                        compile(plan.source(), it),
                    )
                }
            is AssertLoadPrivilegeCanBeMutated ->
                Planner {
                    assertRevocable(
                        access(plan.revokeType()),
                        load(plan.action(), plan.qualifier()),
                        plan.roleName(),
                        compile(plan.source(), it),
                    )
                }
            is AssertMutablePrivilegesCanBeAssignedToRole ->
                Planner { assertMutableRole(plan.roleName(), compile(plan.source(), it)) }
            is AssertDbmsActionIsAssignable ->
                Planner { context ->
                    PredicateExecutionPlan.apply(
                        Function2 { _, _ -> plan.action() !is UnassignableAction },
                        plan.source().map { compile(it, context) },
                        Function3 { _, _, _ ->
                            AuthorizationViolationException.grantingImmutablePrivileges(
                                plan.action().name()
                            )
                        },
                    )
                }
            else -> null
        }

    /**
     * A privilege of a command, but for the names its parameters give: [resolve] makes it, or tells
     * why there is none, such as a database that does not exist.
     */
    private class Template(
        val action: PrivilegeAction,
        val resource: PrivilegeResource,
        private val scope: (MapValue, Transaction) -> Resolved<PrivilegeScope>,
        private val segment: (MapValue) -> PrivilegeSegment,
    ) {
        fun resolve(
            access: Access,
            immutable: Boolean,
            params: MapValue,
            tx: Transaction,
        ): Resolved<Privilege> =
            when (val scope = scope(params, tx)) {
                is Resolved.Missing -> scope
                is Resolved.Found ->
                    Resolved.Found(
                        Privilege(access, action, scope.value, resource, segment(params), immutable)
                    )
            }
    }

    private sealed interface Resolved<out T> {
        data class Found<T>(val value: T) : Resolved<T>

        /**
         * Nothing, because the [database] does not exist, as [reason] says in the words of Neo4j's
         * notifications.
         */
        data class Missing(val database: String, val reason: String) : Resolved<Nothing>
    }

    private fun dbms(action: AdministrationAction, qualifier: PrivilegeQualifier) =
        Template(
            ActionMapper.asKernelAction(action),
            PrivilegeResource.Database,
            { _, _ -> Resolved.Found(PrivilegeScope.Dbms) },
            segment(qualifier),
        )

    private fun database(
        action: AdministrationAction,
        scope: PrivilegeCommandScope,
        qualifier: PrivilegeQualifier,
    ) =
        Template(
            ActionMapper.asKernelAction(action),
            PrivilegeResource.Database,
            scope(scope),
            segment(qualifier),
        )

    private fun load(action: AdministrationAction, qualifier: PrivilegeQualifier) =
        Template(
            ActionMapper.asKernelAction(action),
            PrivilegeResource.Database,
            { _, _ -> Resolved.Found(PrivilegeScope.Dbms) },
            segment(qualifier),
        )

    /** The databases of [scope]; a named one by its id, as privileges keep it. */
    private fun scope(
        scope: PrivilegeCommandScope
    ): (MapValue, Transaction) -> Resolved<PrivilegeScope> =
        when (scope) {
            is `AllScope$` -> { _, _ ->
                Resolved.Found(PrivilegeScope.AllDatabases)
            }
            is `HomeScope$` -> { _, _ ->
                Resolved.Found(PrivilegeScope.Home)
            }
            is NamedScope -> { params, tx ->
                named(scope.name(), params, tx)
            }
            else -> throw IllegalArgumentException("Unknown scope: $scope")
        }

    private fun named(
        name: DatabaseName,
        params: MapValue,
        tx: Transaction,
    ): Resolved<PrivilegeScope> {
        val shown = admin.runtimeStringValue(name, params)
        if (shown == "*") {
            // Only a parameter gives `*` here: the planner makes `DATABASE *` all databases.
            return Resolved.Missing(
                shown,
                "Parameterized database and graph names do not support wildcards.",
            )
        }
        val id =
            tx.findNode(
                    TopologyGraphDbmsModel.DATABASE_LABEL,
                    TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY,
                    NormalizedDatabaseName(shown).name(),
                )
                ?.getProperty(TopologyGraphDbmsModel.DATABASE_UUID_PROPERTY) as String?
        return if (id == null) Resolved.Missing(shown, "Database `$shown` does not exist.")
        else Resolved.Found(PrivilegeScope.Database(UUID.fromString(id)))
    }

    private fun segment(qualifier: PrivilegeQualifier): (MapValue) -> PrivilegeSegment =
        when (qualifier) {
            is AllQualifier,
            is AllDatabasesQualifier -> { _ ->
                PrivilegeSegment.Database
            }
            is UserAllQualifier -> { _ ->
                PrivilegeSegment.Users(null)
            }
            is UserQualifier -> { params ->
                PrivilegeSegment.Users(
                    admin.runtimeStringValue(qualifier.username(), params, false)
                )
            }
            is ProcedureAllQualifier -> { _ ->
                PrivilegeSegment.Procedures("*")
            }
            is ProcedureQualifier -> { _ ->
                PrivilegeSegment.Procedures(qualifier.glob())
            }
            is FunctionAllQualifier -> { _ ->
                PrivilegeSegment.Functions("*")
            }
            is FunctionQualifier -> { _ ->
                PrivilegeSegment.Functions(qualifier.glob())
            }
            is SettingAllQualifier -> { _ ->
                PrivilegeSegment.Settings("*")
            }
            is SettingQualifier -> { _ ->
                PrivilegeSegment.Settings(qualifier.glob())
            }
            is LoadAllQualifier -> { _ ->
                PrivilegeSegment.AllData
            }
            is LoadCidrQualifier -> { params ->
                PrivilegeSegment.Cidr(cidr(qualifier.cidr(), params))
            }
            else -> throw IllegalArgumentException("Unsupported qualifier: $qualifier")
        }

    /** The CIDR range of [range], refused unless it is one, such as `10.0.0.0/8`. */
    private fun cidr(range: Either<String, Parameter>, params: MapValue): String {
        val shown = admin.runtimeStringValue(range, params)
        val address = IPAddressString(shown)
        if (!address.isPrefixed || address.address == null) {
            throw InvalidArgumentException.inputContainsInvalidCharacters(
                shown,
                "CIDR range",
                "'$shown' is not a CIDR range, such as '10.0.0.0/8' or '2001:db8::/32'.",
            )
        }
        return shown
    }

    private fun access(revokeType: String): Access =
        when (revokeType) {
            "GRANTED" -> Access.GRANTED
            "DENIED" -> Access.DENIED
            else -> throw IllegalArgumentException("Neither GRANTED nor DENIED: $revokeType")
        }

    /**
     * GRANT and DENY: adds the privilege unless the role has it, which is a notification. A role or
     * database that does not exist fails the command.
     */
    private fun assign(
        access: Access,
        template: Template,
        roleName: Either<String, Parameter>,
        immutable: Boolean,
        command: String,
        source: ExecutionPlan,
    ): ExecutionPlan {
        val role = admin.getNameFields("role", roleName, IDENTITY)
        val relationship = PrivilegeRecords.relationship(access).name()
        val verb = if (access == Access.GRANTED) "grant" else "deny"
        return updating(
            if (access == Access.GRANTED) "GrantPrivilege" else "DenyPrivilege",
            """
            MATCH (role:$ROLE {${PrivilegeRecords.ROLE_ID}: ${parameter(ROLE_ID)}})
            OPTIONAL MATCH (role)-[:$relationship]->(had:$PRIVILEGE {$KEY: ${parameter(KEYS)}[0]})
            WITH role, had
            FOREACH (_ IN CASE WHEN had IS NULL THEN [1] ELSE [] END |
                CREATE (role)-[:$relationship]->(privilege:$PRIVILEGE)
                SET privilege = ${parameter(RECORD)})
            RETURN had IS NOT NULL AS had
            """
                .trimIndent(),
            VirtualValues.map(arrayOf(role.nameKey()), arrayOf(role.nameValue())),
            QueryHandler.handleResult(
                    Function3 { _, value, _ ->
                        if ((value as BooleanValue).booleanValue()) {
                            notify(AssignPrivilegeCommandHasNoEffectNotification(command))
                        } else {
                            `Continue$`.`MODULE$`
                        }
                    }
                )
                .handleError(
                    Function2 { error, params ->
                        failed(
                            command,
                            "$verb privilege to role '${show(roleName, params)}'",
                            error,
                        )
                    }
                ),
            source,
            transformer(role.nameConverter()) { tx, params ->
                val name = show(roleName, params)
                val failure = "Failed to $verb privilege to role '$name'"
                val roleId =
                    roleId(tx, name)
                        ?: throw InvalidArgumentException(
                            ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                                .withCause(
                                    PrivilegeGQLCodeEntity.entityNotFoundGqlStatus(
                                        ROLE_ENTITY,
                                        name,
                                    )
                                )
                                .build(),
                            "$failure: Role does not exist.",
                        )
                when (val privilege = template.resolve(access, immutable, params, tx)) {
                    is Resolved.Missing ->
                        throw InvalidArgumentException(
                            GqlHelper.getGql42002_42N00(privilege.database),
                            "$failure: ${privilege.reason}",
                        )
                    is Resolved.Found -> {
                        val record = PrivilegeRecords.record(roleId, privilege.value)
                        params
                            .updatedWith(ROLE_ID, Values.utf8Value(roleId))
                            .updatedWith(
                                KEYS,
                                Values.stringArray(record.getValue(PrivilegeRecords.KEY) as String),
                            )
                            .updatedWith(RECORD, ValueUtils.asMapValue(record))
                    }
                }
            },
        )
    }

    /**
     * REVOKE GRANT or REVOKE DENY: removes the privilege, both its mutable and its immutable one,
     * or only the immutable one for REVOKE IMMUTABLE. A role that does not have it is a
     * notification, and so is a role or database that does not exist.
     */
    private fun revoke(
        access: Access,
        template: Template,
        roleName: Either<String, Parameter>,
        immutableOnly: Boolean,
        command: String,
        source: ExecutionPlan,
    ): ExecutionPlan {
        val role = admin.getNameFields("role", roleName, IDENTITY)
        val relationship = PrivilegeRecords.relationship(access).name()
        return updating(
            "RevokePrivilege",
            """
            OPTIONAL MATCH (:$ROLE {${PrivilegeRecords.ROLE_ID}: ${parameter(ROLE_ID)}})
                -[:$relationship]->(privilege:$PRIVILEGE)
            WHERE privilege.$KEY IN ${parameter(KEYS)}
            WITH collect(privilege) AS revoked
            FOREACH (privilege IN revoked | DETACH DELETE privilege)
            RETURN size(revoked) > 0 AS revoked
            """
                .trimIndent(),
            VirtualValues.map(arrayOf(role.nameKey()), arrayOf(role.nameValue())),
            QueryHandler.handleResult(
                    Function3 { _, value, params ->
                        val missing = params.get(MISSING)
                        when {
                            missing is TextValue ->
                                notify(
                                    ImpossibleRevokeCommandWarning(command, missing.stringValue())
                                )
                            !(value as BooleanValue).booleanValue() ->
                                notify(RevokePrivilegeCommandHasNoEffectNotification(command))
                            else -> `Continue$`.`MODULE$`
                        }
                    }
                )
                .handleError(
                    Function2 { error, params ->
                        failed(
                            command,
                            "revoke privilege from role '${show(roleName, params)}'",
                            error,
                        )
                    }
                ),
            source,
            transformer(role.nameConverter()) { tx, params ->
                matching(access, template, roleName, params, tx, immutableOnly)
            },
        )
    }

    /** Refuses to revoke an immutable privilege while authentication is enabled. */
    private fun assertRevocable(
        access: Access,
        template: Template,
        roleName: Either<String, Parameter>,
        source: ExecutionPlan,
    ): ExecutionPlan {
        val role = admin.getNameFields("role", roleName, IDENTITY)
        val relationship = PrivilegeRecords.relationship(access).name()
        return updating(
            "AssertPrivilegeCanBeMutated",
            """
            MATCH (:$ROLE {${PrivilegeRecords.ROLE_ID}: ${parameter(ROLE_ID)}})
                -[:$relationship]->(privilege:$PRIVILEGE)
            WHERE privilege.$KEY IN ${parameter(KEYS)} AND privilege.$IMMUTABLE
            RETURN true AS immutable
            LIMIT 1
            """
                .trimIndent(),
            VirtualValues.map(arrayOf(role.nameKey()), arrayOf(role.nameValue())),
            QueryHandler.handleResult(
                Function3 { _, _, params ->
                    if ((params.get(AUTH_DISABLED) as BooleanValue).booleanValue()) {
                        `Continue$`.`MODULE$`
                    } else {
                        ThrowException(
                            AuthorizationViolationException.revokingImmutablePrivileges()
                        )
                    }
                }
            ),
            source,
            transformer(role.nameConverter()) { tx, params ->
                    matching(access, template, roleName, params, tx, immutableOnly = false)
                }
                .generate(authDisabled()),
        )
    }

    /**
     * The parameters naming the privileges that a REVOKE of [template] removes, with why there are
     * none if the role or database does not exist.
     */
    private fun matching(
        access: Access,
        template: Template,
        roleName: Either<String, Parameter>,
        params: MapValue,
        tx: Transaction,
        immutableOnly: Boolean,
    ): MapValue {
        val roleId = roleId(tx, show(roleName, params))
        val privilege = template.resolve(access, true, params, tx)
        val missing =
            when {
                roleId == null -> "Role does not exist."
                privilege is Resolved.Missing -> privilege.reason
                else -> null
            }
        val keys =
            if (roleId == null || privilege !is Resolved.Found) emptyList()
            else
                listOfNotNull(
                    PrivilegeRecords.key(roleId, privilege.value),
                    if (immutableOnly) null
                    else PrivilegeRecords.key(roleId, privilege.value.copy(immutable = false)),
                )
        return params
            .updatedWith(ROLE_ID, roleId?.let(Values::utf8Value) ?: Values.NO_VALUE)
            .updatedWith(KEYS, Values.stringArray(*keys.toTypedArray()))
            .updatedWith(MISSING, missing?.let(Values::utf8Value) ?: Values.NO_VALUE)
    }

    /** Immutable roles take only immutable privileges, even with authentication disabled. */
    private fun assertMutableRole(
        roleName: Either<String, Parameter>,
        source: ExecutionPlan,
    ): ExecutionPlan {
        val role = admin.getNameFields("role", roleName, IDENTITY)
        return updating(
            "AssertMutablePrivilegesCanBeAssignedToRole",
            "MATCH (role:$ROLE {${PrivilegeRecords.ROLE_NAME}: ${parameter(role.nameKey())}}) " +
                "WHERE role.$IMMUTABLE RETURN role.${PrivilegeRecords.ROLE_NAME} AS name",
            VirtualValues.map(arrayOf(role.nameKey()), arrayOf(role.nameValue())),
            QueryHandler.handleResult(
                Function3 { _, _, _ ->
                    ThrowException(
                        AuthorizationViolationException.assigningMutablePrivilegesToImmutableRole()
                    )
                }
            ),
            source,
            transformer(role.nameConverter()) { _, params -> params },
        )
    }

    private fun roleId(tx: Transaction, name: String): String? =
        tx.findNode(PrivilegeRecords.ROLE, PrivilegeRecords.ROLE_NAME, name)
            ?.getProperty(PrivilegeRecords.ROLE_ID) as String?

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

    /**
     * Converts the role's name, then [convert]s the parameters within the command's transaction.
     */
    private fun transformer(
        role: Function2<Transaction, MapValue, MapValue>,
        convert: (Transaction, MapValue) -> MapValue,
    ): ParameterTransformer =
        ParameterTransformer(
                ParameterTransformer.`$lessinit$greater$default$1`(),
                ParameterTransformer.`$lessinit$greater$default$2`(),
            )
            .convert(role)
            .convert(Function2 { tx, params -> convert(tx, params) })

    private fun authDisabled() =
        Function3<
            Transaction,
            org.neo4j.internal.kernel.api.security.SecurityContext,
            MapValue,
            MapValue,
        > { _, securityContext, _ ->
            VirtualValues.map(
                arrayOf(AUTH_DISABLED),
                arrayOf(Values.booleanValue(securityContext.isAuthDisabled)),
            )
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

    private fun show(name: Either<String, Parameter>, params: MapValue): String =
        admin.runtimeStringValue(name, params)

    private fun parameter(key: String) = "${'$'}`$key`"

    private companion object {
        val ROLE = PrivilegeRecords.ROLE.name()
        val PRIVILEGE = PrivilegeRecords.PRIVILEGE.name()
        const val KEY = PrivilegeRecords.KEY
        const val IMMUTABLE = PrivilegeRecords.IMMUTABLE

        val ROLE_ENTITY: PrivilegeGQLCodeEntity = PrivilegeGQLCodeEntity.Role()
        val IDENTITY = Function1<String, String> { it }

        /** The parameters that a command's transformer adds. */
        const val ROLE_ID = "__dozendb_roleId"
        const val KEYS = "__dozendb_keys"
        const val RECORD = "__dozendb_record"
        const val MISSING = "__dozendb_missing"
        const val AUTH_DISABLED = "__dozendb_authDisabled"
    }
}
