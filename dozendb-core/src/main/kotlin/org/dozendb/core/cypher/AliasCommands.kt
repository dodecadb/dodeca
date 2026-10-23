package org.dozendb.core.cypher

import org.neo4j.common.DependencyResolver
import org.neo4j.cypher.internal.`AdministrationCommandRuntime$`
import org.neo4j.cypher.internal.AdministrationCommandRuntimeContext
import org.neo4j.cypher.internal.AliasMapSettingsEvaluator
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.administration.EnsureNodeExistsExecutionPlanner
import org.neo4j.cypher.internal.administration.ShowAliasesExecutionPlanner
import org.neo4j.cypher.internal.ast.DatabaseName
import org.neo4j.cypher.internal.logical.plans.AssertNotBlockedDropAlias
import org.neo4j.cypher.internal.logical.plans.AssertNotBlockedRemoteAliasManagement
import org.neo4j.cypher.internal.logical.plans.CreateLocalDatabaseAlias
import org.neo4j.cypher.internal.logical.plans.DropDatabaseAlias
import org.neo4j.cypher.internal.logical.plans.EnsureDatabaseNodeExists
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NameValidator
import org.neo4j.cypher.internal.logical.plans.ShowAliases
import org.neo4j.cypher.internal.procs.ParameterTransformer
import org.neo4j.cypher.internal.procs.PredicateExecutionPlan
import org.neo4j.cypher.internal.procs.QueryHandler
import org.neo4j.cypher.internal.procs.ThrowException
import org.neo4j.cypher.internal.procs.UpdatingSystemCommandExecutionPlan
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.dbms.api.DatabaseExistsException
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel
import org.neo4j.exceptions.DatabaseAdministrationOnFollowerException
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlParams
import org.neo4j.gqlstatus.GqlStatusInfoCodes
import org.neo4j.graphdb.Transaction
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.database.DatabaseReferenceRepository
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues
import scala.Function2
import scala.Function3
import scala.Option
import scala.PartialFunction
import scala.Tuple2
import scala.collection.immutable.`Set$`

/**
 * CREATE, DROP and SHOW ALIASES for local aliases, which Neo4j Community leaves to Neo4j
 * Enterprise. An alias is a `DatabaseName` node in `system` that is not its database's primary name
 * and `TARGETS` the database, as Neo4j models it, so that Neo4j resolves it wherever it resolves a
 * database name: Bolt and HTTP sessions, `USE`, routing tables, home databases and SHOW DATABASES.
 * Its properties are in an `AliasProperties` node, empty when it has none.
 */
internal class AliasCommands(
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
        planner(plan) ?: throw IllegalArgumentException("Not an alias command: $plan")

    private fun planner(plan: LogicalPlan): Planner? =
        when (plan) {
            is CreateLocalDatabaseAlias -> Planner { createLocal(plan, compile(plan.source(), it)) }
            is DropDatabaseAlias -> Planner { drop(plan, compile(plan.source(), it)) }
            is ShowAliases ->
                Planner {
                    ShowAliasesExecutionPlanner.apply(
                            engine,
                            authorization,
                            dependencies.resolveDependency(DatabaseReferenceRepository::class.java),
                        )
                        .planShowAliasesForDatabase(
                            Option.apply(compile(plan.source(), it)),
                            plan.aliasName(),
                            plan.verbose(),
                            plan.returnColumns(),
                            plan.yields(),
                            plan.returns(),
                        )
                }
            is EnsureDatabaseNodeExists ->
                Planner {
                    EnsureNodeExistsExecutionPlanner.apply(engine, authorization)
                        .planEnsureDatabaseNodeExists(
                            plan.command(),
                            plan.name(),
                            plan.extraFilter(),
                            plan.action(),
                            Option.apply(compile(plan.source(), it)),
                        )
                }
            // Neo4j Enterprise can block the management of aliases; nothing blocks it here.
            is AssertNotBlockedDropAlias,
            is AssertNotBlockedRemoteAliasManagement ->
                Planner {
                    PredicateExecutionPlan.apply(
                        Function2 { _, _ -> true },
                        Option.empty(),
                        Function3 { _, _, _ -> IllegalStateException("Unreachable") },
                    )
                }
            else -> null
        }

    /**
     * Creates the alias, unless a database or alias has the name; the plans before checked the
     * target and, with OR REPLACE, dropped an alias of the same name.
     */
    private fun createLocal(plan: CreateLocalDatabaseAlias, source: ExecutionPlan): ExecutionPlan {
        val alias = admin.getDatabaseNameFields("aliasName", plan.aliasName())
        val target = admin.getDatabaseNameFields("targetName", plan.targetName())
        val free =
            ensureNameFree(
                engine,
                authorization,
                "database alias",
                alias.nameKey(),
                VirtualValues.map(alias.keys(), alias.values()),
                ParameterTransformer(
                        ParameterTransformer.`$lessinit$greater$default$1`(),
                        ParameterTransformer.`$lessinit$greater$default$2`(),
                    )
                    .convert(alias.nameConverter())
                    .convert(inRoot(alias.nameKey(), alias.namespaceKey())),
                { show(plan.aliasName(), it) },
                source,
            )
        return updating(
            "CreateLocalDatabaseAlias",
            """
            MATCH (database:$DATABASE {${TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY}:
                ${parameter(target.displayNameKey())}})
            CREATE (database)<-[:$TARGETS]-(alias:$DATABASE_NAME {
                ${TopologyGraphDbmsModel.NAME_PROPERTY}: ${parameter(alias.nameKey())},
                ${TopologyGraphDbmsModel.NAMESPACE_PROPERTY}: ${parameter(alias.namespaceKey())},
                ${TopologyGraphDbmsModel.DISPLAY_NAME_PROPERTY}: ${parameter(alias.displayNameKey())},
                ${TopologyGraphDbmsModel.PRIMARY_PROPERTY}: false})
            CREATE (alias)-[:$PROPERTIES]->(properties:$ALIAS_PROPERTIES)
            SET properties = ${parameter(PROPERTIES_KEY)}
            RETURN alias.${TopologyGraphDbmsModel.NAME_PROPERTY} AS name
            """
                .trimIndent(),
            VirtualValues.map(
                alias.keys() + target.keys(),
                alias.values() + target.values(),
            ),
            QueryHandler.handleError(
                Function2 { error, params ->
                    val name = show(plan.aliasName(), params)
                    if (hasStatus(error, Status.Schema.ConstraintValidationFailed)) {
                        nameTaken(
                            "Failed to create the specified database alias '$name': A database " +
                                "or alias with that name already exists.",
                            name,
                            error,
                        )
                    } else {
                        failed("CREATE ALIAS", "create", name, error)
                    }
                }
            ),
            free,
            ParameterTransformer(
                    ParameterTransformer.`$lessinit$greater$default$1`(),
                    ParameterTransformer.`$lessinit$greater$default$2`(),
                )
                .convert(alias.nameConverter())
                .convert(inRoot(alias.nameKey(), alias.namespaceKey()))
                .convert(target.nameConverter())
                .convert(inRoot(target.nameKey(), target.namespaceKey()))
                .validate(
                    Function2 { transaction, params ->
                        val name = text(params, alias.displayNameKey())
                        assertValidAliasName(name)
                        NameValidator.assertValidTargetName(text(params, target.displayNameKey()))
                        val properties =
                            AliasMapSettingsEvaluator(
                                    (transaction as InternalTransaction)
                                        .kernelTransaction()
                                        .procedures()
                                )
                                .convertPropertiesMap(
                                    plan.properties(),
                                    params,
                                    "create the specified database alias '$name'",
                                )
                                .getOrElse { MapValue.EMPTY }
                        Tuple2(
                            params.updatedWith(PROPERTIES_KEY, properties),
                            `Set$`.`MODULE$`.empty<InternalNotification>(),
                        )
                    }
                ),
        )
    }

    /**
     * Drops the alias, local or remote, with what is attached to it; none is left to the plans
     * before, which CREATE OR REPLACE relies on. A database's own name is never dropped.
     */
    private fun drop(plan: DropDatabaseAlias, source: ExecutionPlan): ExecutionPlan {
        val alias = admin.getDatabaseNameFields("aliasName", plan.aliasName())
        return updating(
            "DropDatabaseAlias",
            """
            MATCH (alias:$DATABASE_NAME {
                ${TopologyGraphDbmsModel.NAME_PROPERTY}: ${parameter(alias.nameKey())},
                ${TopologyGraphDbmsModel.NAMESPACE_PROPERTY}: ${parameter(alias.namespaceKey())},
                ${TopologyGraphDbmsModel.PRIMARY_PROPERTY}: false})
            OPTIONAL MATCH (alias)-[:$PROPERTIES|$CONNECTS_WITH]->(attached)
            DETACH DELETE alias, attached
            """
                .trimIndent(),
            VirtualValues.map(alias.keys(), alias.values()),
            QueryHandler.handleError(
                Function2 { error, params ->
                    failed("DROP ALIAS", "delete", show(plan.aliasName(), params), error)
                }
            ),
            source,
            ParameterTransformer(
                    ParameterTransformer.`$lessinit$greater$default$1`(),
                    ParameterTransformer.`$lessinit$greater$default$2`(),
                )
                .convert(alias.nameConverter())
                .convert(inRoot(alias.nameKey(), alias.namespaceKey())),
        )
    }

    /**
     * Neo4j's check of alias names, and the rules its documentation adds: names that begin with an
     * underscore are reserved too, and names cannot end with a dot.
     */
    @Suppress("DEPRECATION") // Like Neo4j's own name validation.
    private fun assertValidAliasName(name: String) {
        NameValidator.assertValidAliasName(name)
        if (name.startsWith("_")) {
            throw InvalidArgumentException("Alias name '$name' is invalid, due to the prefix '_'.")
        }
        if (name.endsWith(".")) {
            throw InvalidArgumentException(
                "Alias name '$name' is invalid: alias names cannot end with a dot."
            )
        }
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

    private fun failed(command: String, operation: String, name: String, error: Throwable) =
        when {
            hasStatus(error, Status.Cluster.NotALeader) ->
                DatabaseAdministrationOnFollowerException.notALeader(
                    command,
                    "Failed to $operation the specified database alias '$name'",
                    error,
                )
            error is Status.HasStatus -> error
            else ->
                IllegalStateException(
                    "Failed to $operation the specified database alias '$name'.",
                    error,
                )
        }

    private fun hasStatus(error: Throwable, status: Status): Boolean =
        generateSequence(error) { it.cause }.any { it is Status.HasStatus && it.status() == status }

    private fun show(name: DatabaseName, params: MapValue): String =
        admin.runtimeStringValue(name, params)

    private fun text(params: MapValue, key: String): String =
        (params.get(key) as TextValue).stringValue()

    private fun parameter(key: String) = "${'$'}`$key`"

    private companion object {
        val DATABASE = TopologyGraphDbmsModel.DATABASE_LABEL.name()
        val DATABASE_NAME = TopologyGraphDbmsModel.DATABASE_NAME_LABEL.name()
        val TARGETS = TopologyGraphDbmsModel.TARGETS_RELATIONSHIP.name()
        val PROPERTIES = TopologyGraphDbmsModel.PROPERTIES_RELATIONSHIP.name()
        val CONNECTS_WITH = TopologyGraphDbmsModel.CONNECTS_WITH_RELATIONSHIP.name()
        val ALIAS_PROPERTIES = TopologyGraphDbmsModel.ALIAS_PROPERTIES_LABEL.name()

        /** The parameter of the properties of a new alias. */
        const val PROPERTIES_KEY = "__dozendb_aliasProperties"
    }
}

/**
 * Refuses to create the [what], "database" or "database alias", whose name the [transformer] puts
 * at [nameKey], when a database or an alias has that name, saying which. The uniqueness constraint
 * on names would refuse it too, without saying which.
 */
internal fun ensureNameFree(
    engine: ExecutionEngine,
    authorization: SecurityAuthorizationHandler,
    what: String,
    nameKey: String,
    parameters: MapValue,
    transformer: ParameterTransformer,
    show: (MapValue) -> String,
    source: ExecutionPlan,
): ExecutionPlan =
    UpdatingSystemCommandExecutionPlan(
        "EnsureNameIsFree",
        engine,
        authorization,
        """
        MATCH (taken:${TopologyGraphDbmsModel.DATABASE_NAME_LABEL.name()} {
            ${TopologyGraphDbmsModel.NAME_PROPERTY}: ${'$'}`$nameKey`,
            ${TopologyGraphDbmsModel.NAMESPACE_PROPERTY}: '${TopologyGraphDbmsModel.DEFAULT_NAMESPACE}'})
        RETURN taken.${TopologyGraphDbmsModel.PRIMARY_PROPERTY} AS primary
        """
            .trimIndent(),
        parameters,
        QueryHandler.handleResult(
            Function3 { _, primary, params ->
                val database = (primary as BooleanValue).booleanValue()
                val reason =
                    when {
                        database == (what == "database") ->
                            "${what.replaceFirstChar(Char::uppercase)} already exists."
                        database -> "A database with that name already exists."
                        else -> "A database alias with that name already exists."
                    }
                ThrowException(
                    nameTaken(
                        "Failed to create the specified $what '${show(params)}': $reason",
                        show(params),
                    )
                )
            }
        ),
        Option.apply(source),
        UpdatingSystemCommandExecutionPlan.`$lessinit$greater$default$8`(),
        UpdatingSystemCommandExecutionPlan.`$lessinit$greater$default$9`(),
        transformer,
        UpdatingSystemCommandExecutionPlan.`$lessinit$greater$default$11`(),
    )

/** That the database or alias [name] exists already, as [message] says. */
internal fun nameTaken(message: String, name: String, cause: Throwable? = null) =
    DatabaseExistsException(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N11)
            .withParam(GqlParams.StringParam.db, name)
            .build(),
        message,
        cause,
    )

/**
 * Converts the name at [nameKey] in namespace [namespaceKey] to a name of the root namespace:
 * DozenDB has no composite databases, so a dotted name such as `films.old` is a name of its own, as
 * Neo4j reads it when no composite database has the part before the dot.
 */
internal fun inRoot(nameKey: String, namespaceKey: String) =
    Function2<Transaction, MapValue, MapValue> { _, params ->
        val namespace = (params.get(namespaceKey) as TextValue).stringValue()
        if (namespace == TopologyGraphDbmsModel.DEFAULT_NAMESPACE) {
            params
        } else {
            val name = (params.get(nameKey) as TextValue).stringValue()
            params
                .updatedWith(nameKey, Values.utf8Value("$namespace.$name"))
                .updatedWith(
                    namespaceKey,
                    Values.utf8Value(TopologyGraphDbmsModel.DEFAULT_NAMESPACE),
                )
        }
    }
