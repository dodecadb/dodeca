/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Modifications Copyright (c) DozerDB
 * https://dozerdb.org
 *
 * Modifications Copyright (c) DozenDB
 * 2026-10-19: ported to Kotlin from DozerDB's DozerDbAdministrationCommandRuntime (Scala), now a
 * subclass of Neo4j's CommunityAdministrationCommandRuntime that adds only the database commands.
 * Fixed parameterised and dotted database names, privilege checks that DROP DATABASE skipped, START
 * and STOP of a missing database succeeding, CREATE OR REPLACE of a missing database failing, and
 * the database limit counting one short. Composite databases, DUMP DATA and WAIT are rejected
 * instead of ignored or failing internally, and so is dropping the default database.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */
package org.dozendb.cluster.core.internal.cypher

import org.dozendb.cluster.core.internal.DozenDbSettings
import org.neo4j.common.DependencyResolver
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.helpers.DatabaseNameValidator
import org.neo4j.cypher.internal.`AdministrationCommandRuntime$`
import org.neo4j.cypher.internal.AdministrationCommandRuntimeContext
import org.neo4j.cypher.internal.CommunityAdministrationCommandRuntime
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.ast.AdministrationAction
import org.neo4j.cypher.internal.ast.DatabaseName
import org.neo4j.cypher.internal.ast.`DumpData$`
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.logical.plans.AssertCanDropDatabase
import org.neo4j.cypher.internal.logical.plans.AssertManagementActionNotBlocked
import org.neo4j.cypher.internal.logical.plans.CreateDatabase
import org.neo4j.cypher.internal.logical.plans.DropDatabase
import org.neo4j.cypher.internal.logical.plans.EnsureDatabaseSafeToDelete
import org.neo4j.cypher.internal.logical.plans.EnsureNameIsNotAmbiguous
import org.neo4j.cypher.internal.logical.plans.EnsureValidNonSystemDatabase
import org.neo4j.cypher.internal.logical.plans.EnsureValidNumberOfDatabases
import org.neo4j.cypher.internal.logical.plans.LogSystemCommand
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.StartDatabase
import org.neo4j.cypher.internal.logical.plans.StopDatabase
import org.neo4j.cypher.internal.logical.plans.WaitForCompletion
import org.neo4j.cypher.internal.procs.ActionMapper
import org.neo4j.cypher.internal.procs.AuthorizationAndPredicateExecutionPlan
import org.neo4j.cypher.internal.procs.`Continue$`
import org.neo4j.cypher.internal.procs.ParameterTransformer
import org.neo4j.cypher.internal.procs.PredicateExecutionPlan
import org.neo4j.cypher.internal.procs.QueryHandler
import org.neo4j.cypher.internal.procs.QueryHandlerResult
import org.neo4j.cypher.internal.procs.ThrowException
import org.neo4j.cypher.internal.procs.UpdatingSystemCommandExecutionPlan
import org.neo4j.dbms.api.DatabaseExistsException
import org.neo4j.dbms.api.DatabaseLimitReachedException
import org.neo4j.dbms.api.DatabaseNotFoundHelper
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DatabaseStatus
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.exceptions.DatabaseAdministrationOnFollowerException
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog
import org.neo4j.internal.kernel.api.security.AdminActionOnResource
import org.neo4j.internal.kernel.api.security.PermissionState
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.internal.kernel.api.security.Segment
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.values.storable.LongValue
import org.neo4j.values.storable.TextValue
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
import scala.jdk.javaapi.CollectionConverters
import scala.util.Either

private typealias Planner = Function1<AdministrationCommandRuntimeContext, ExecutionPlan>

/**
 * Neo4j Community's administration commands, plus CREATE, DROP, START and STOP DATABASE, which
 * Community leaves to Neo4j Enterprise. The commands only change `Database` nodes in `system`; the
 * databases themselves follow those nodes.
 */
internal class DozenDbAdministrationCommandRuntime
private constructor(
    engine: ExecutionEngine,
    dependencies: DependencyResolver,
    private val databaseCommands: DatabaseCommands,
) : CommunityAdministrationCommandRuntime(engine, dependencies, databaseCommands) {
    constructor(
        engine: ExecutionEngine,
        dependencies: DependencyResolver,
    ) : this(engine, dependencies, DatabaseCommands(engine, dependencies))

    // Community compiles nested plans with its commands and ours, but the top-level plan only with
    // its own; this runtime uses both everywhere.
    private val commands = databaseCommands.orElse(logicalToExecutable())

    init {
        databaseCommands.compile = ::compile
    }

    override fun name(): String = "dozendb administration-commands"

    override fun compileToExecutable(
        state: LogicalQuery,
        context: RuntimeContext,
        databaseMode: TransactionalContext.DatabaseMode,
    ): ExecutionPlan =
        compile(state.logicalPlan(), AdministrationCommandRuntimeContext.apply(context))

    override fun isApplicableAdministrationCommand(logicalPlan: LogicalPlan): Boolean {
        val command = if (logicalPlan is LogSystemCommand) logicalPlan.source() else logicalPlan
        return databaseCommands.isDefinedAt(command) ||
            super.isApplicableAdministrationCommand(logicalPlan)
    }

    private fun compile(
        plan: LogicalPlan,
        context: AdministrationCommandRuntimeContext,
    ): ExecutionPlan {
        if (!commands.isDefinedAt(plan)) {
            throw CantCompileQueryException(
                "Plan is not a recognized database administration command: ${plan.javaClass.simpleName}"
            )
        }
        return commands.apply(plan).apply(context)
    }
}

private class DatabaseCommands(
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

    private val admin = `AdministrationCommandRuntime$`.`MODULE$`

    override fun isDefinedAt(plan: LogicalPlan): Boolean = planner(plan) != null

    override fun apply(plan: LogicalPlan): Planner =
        planner(plan) ?: throw IllegalArgumentException("Not a database command: $plan")

    private fun planner(plan: LogicalPlan): Planner? =
        when (plan) {
            is CreateDatabase -> Planner { createDatabase(plan, it) }
            is EnsureValidNumberOfDatabases -> Planner { ensureValidNumberOfDatabases(plan, it) }
            is DropDatabase -> Planner { dropDatabase(plan, it) }
            is StartDatabase ->
                Planner {
                    setStatus(plan.source(), plan.databaseName(), DatabaseStatus.ONLINE, it)
                }
            is StopDatabase ->
                Planner {
                    setStatus(plan.source(), plan.databaseName(), DatabaseStatus.OFFLINE, it)
                }
            is EnsureValidNonSystemDatabase -> Planner { ensureValidNonSystemDatabase(plan, it) }
            is AssertCanDropDatabase -> Planner { assertCanDropDatabase(plan, it) }
            // Neo4j Enterprise can block management actions; nothing blocks them here.
            is AssertManagementActionNotBlocked ->
                Planner {
                    PredicateExecutionPlan.apply(
                        Function2 { _, _ -> true },
                        Option.empty(),
                        Function3 { _, _, _ -> IllegalStateException("Unreachable") },
                    )
                }
            // There are neither composite databases nor aliases that could make a name ambiguous
            // or a database unsafe to delete.
            is EnsureNameIsNotAmbiguous -> Planner { compile(plan.source(), it) }
            is EnsureDatabaseSafeToDelete -> Planner { compile(plan.source(), it) }
            is WaitForCompletion ->
                Planner { context ->
                    failure(compile(plan.source(), context)) {
                        "Failed to wait for the specified database " +
                            "'${show(plan.databaseName(), it)}': WAIT is not supported."
                    }
                }
            else -> null
        }

    @Suppress("DEPRECATION") // Neo4j 5.26 has no GQL status for an existing database.
    private fun createDatabase(
        plan: CreateDatabase,
        context: AdministrationCommandRuntimeContext,
    ): ExecutionPlan {
        val source = compile(plan.source(), context)
        val databaseName = plan.databaseName()
        if (plan.isComposite) {
            return failure(source) {
                "Failed to create the specified database '${show(databaseName, it)}': " +
                    "composite databases are not supported."
            }
        }
        val name = admin.getNameFields("databaseName", databaseName, Function1(::validName))
        val key = parameter(name.nameKey())
        return updating(
            "CreateDatabase",
            """
            CREATE (database:Database)<-[:TARGETS]-(:DatabaseName {
                displayName: $key, name: $key, namespace: ${'$'}namespace, primary: true})
            SET database.access = 'READ_WRITE',
                database.created_at = datetime(),
                database.default = false,
                database.name = $key,
                database.status = ${'$'}status,
                database.uuid = randomUUID()
            RETURN database.name AS name
            """
                .trimIndent(),
            VirtualValues.map(
                arrayOf(name.nameKey(), "namespace", "status"),
                arrayOf(
                    name.nameValue(),
                    Values.utf8Value(TopologyGraphDbmsModel.DEFAULT_NAMESPACE),
                    Values.utf8Value(DatabaseStatus.ONLINE.statusName()),
                ),
            ),
            QueryHandler.handleError(
                Function2 { error, params ->
                    if (hasStatus(error, Status.Schema.ConstraintValidationFailed)) {
                        DatabaseExistsException(
                            "Failed to create the specified database " +
                                "'${show(databaseName, params)}': Database already exists."
                        )
                    } else {
                        failed("CREATE DATABASE", "create", show(databaseName, params), error)
                    }
                }
            ),
            source,
            transformer().convert(name.nameConverter()),
        )
    }

    @Suppress("DEPRECATION") // Like Neo4j's own name validation.
    private fun validName(name: String): String {
        val normalized = NormalizedDatabaseName(name)
        try {
            DatabaseNameValidator.validateExternalDatabaseName(normalized)
        } catch (e: IllegalArgumentException) {
            throw InvalidArgumentException(e.message)
        }
        return normalized.name()
    }

    /** Runs after the database was created, so the count includes it. */
    @Suppress("DEPRECATION") // Neo4j 5.26 has no GQL status for the database limit.
    private fun ensureValidNumberOfDatabases(
        plan: EnsureValidNumberOfDatabases,
        context: AdministrationCommandRuntimeContext,
    ): ExecutionPlan =
        updating(
            "EnsureValidNumberOfDatabases",
            "MATCH (database:Database) RETURN count(database) AS count",
            MapValue.EMPTY,
            QueryHandler.handleResult(
                Function3 { _, count, params ->
                    val limit = config.get(DozenDbSettings.maxDatabases)
                    if ((count as LongValue).longValue() <= limit) {
                        `Continue$`.`MODULE$`
                    } else {
                        ThrowException(
                            DatabaseLimitReachedException(
                                "Failed to create the specified database " +
                                    "'${show(plan.source().databaseName(), params)}': " +
                                    "there may be at most $limit databases, including system."
                            )
                        )
                    }
                }
            ),
            compile(plan.source(), context),
        )

    /**
     * Refuses the default database: it is created at startup, but only on the member that founds a
     * cluster, so dropping it would keep every other member from starting.
     */
    @Suppress("DEPRECATION") // Neo4j 5.26 has no GQL status for this.
    private fun dropDatabase(
        plan: DropDatabase,
        context: AdministrationCommandRuntimeContext,
    ): ExecutionPlan {
        val databaseName = plan.databaseName()
        if (plan.additionalAction() == `DumpData$`.`MODULE$`) {
            return failure(compile(plan.source(), context)) {
                "Failed to delete the specified database '${show(databaseName, it)}': " +
                    "DUMP DATA is not supported."
            }
        }
        val defaultDatabase = config.get(GraphDatabaseSettings.initial_default_database)
        val source =
            PredicateExecutionPlan.apply(
                Function2 { params, _ ->
                    NormalizedDatabaseName(show(databaseName, params)).name() != defaultDatabase
                },
                Option.apply(compile(plan.source(), context)),
                Function3 { params, _, _ ->
                    InvalidArgumentException(
                        "Not allowed to delete the default database " +
                            "'${show(databaseName, params)}'."
                    )
                },
            )
        val name = admin.getDatabaseNameFields("databaseName", databaseName)
        val key = parameter(name.displayNameKey())
        return updating(
            "DropDatabase",
            """
            MATCH (database:Database {name: $key})
            OPTIONAL MATCH (database)-[r]-()
            OPTIONAL MATCH (databaseName:DatabaseName {name: $key})
            OPTIONAL MATCH (databaseName)-[r2]-()
            DELETE r, database, r2, databaseName
            WITH DISTINCT $key AS droppedName
            CREATE (:DeletedDatabase {name: droppedName, timestamp: timestamp()})
            RETURN droppedName
            """
                .trimIndent(),
            VirtualValues.map(name.keys(), name.values()),
            QueryHandler.handleError(errors("DROP DATABASE", "delete", databaseName)),
            source,
            transformer().convert(name.nameConverter()),
        )
    }

    private fun setStatus(
        sourcePlan: LogicalPlan,
        databaseName: DatabaseName,
        status: DatabaseStatus,
        context: AdministrationCommandRuntimeContext,
    ): ExecutionPlan {
        val online = status == DatabaseStatus.ONLINE
        val (command, operation) =
            if (online) "START DATABASE" to "start" else "STOP DATABASE" to "stop"
        val name = admin.getDatabaseNameFields("databaseName", databaseName)
        return updating(
            if (online) "StartDatabase" else "StopDatabase",
            """
            MATCH (database:Database {name: ${parameter(name.displayNameKey())}})
            SET database.status = ${'$'}status,
                database.started_at = ${if (online) "datetime()" else "null"},
                database.stopped_at = ${if (online) "null" else "datetime()"},
                database.updated_at = datetime()
            RETURN database.name AS name
            """
                .trimIndent(),
            VirtualValues.map(
                name.keys() + "status",
                name.values() + Values.utf8Value(status.statusName()),
            ),
            QueryHandler.handleNoResult(notFound(operation, databaseName))
                .handleError(errors(command, operation, databaseName)),
            compile(sourcePlan, context),
            transformer().convert(name.nameConverter()),
        )
    }

    @Suppress("DEPRECATION") // Neo4j 5.26 has no GQL status for this.
    private fun ensureValidNonSystemDatabase(
        plan: EnsureValidNonSystemDatabase,
        context: AdministrationCommandRuntimeContext,
    ): ExecutionPlan {
        val databaseName = plan.databaseName()
        val name = admin.getDatabaseNameFields("databaseName", databaseName)
        return updating(
            "EnsureValidNonSystemDatabase",
            "MATCH (database:Database {name: ${parameter(name.displayNameKey())}}) " +
                "RETURN database.name AS name",
            VirtualValues.map(name.keys(), name.values()),
            QueryHandler.handleNoResult(notFound(plan.action(), databaseName))
                .handleResult(
                    Function3 { _, value, _ ->
                        if (
                            (value as TextValue).stringValue() ==
                                GraphDatabaseSettings.SYSTEM_DATABASE_NAME
                        ) {
                            ThrowException(
                                InvalidArgumentException(
                                    "Not allowed to ${plan.action()} the system database."
                                )
                            )
                        } else {
                            `Continue$`.`MODULE$`
                        }
                    }
                )
                .handleError(errors(plan.command(), plan.action(), databaseName)),
            compile(plan.source(), context),
            transformer().convert(name.nameConverter()),
        )
    }

    private fun assertCanDropDatabase(
        plan: AssertCanDropDatabase,
        context: AdministrationCommandRuntimeContext,
    ): ExecutionPlan {
        val action = plan.defaultAction()
        return AuthorizationAndPredicateExecutionPlan.apply(
            authorization,
            Function2<
                MapValue,
                SecurityContext,
                Seq<Tuple2<AdministrationAction, PermissionState>>,
            > { params, securityContext ->
                val resource =
                    AdminActionOnResource(
                        ActionMapper.asKernelAction(action),
                        AdminActionOnResource.DatabaseScope(
                            admin.runtimeStringValue(plan.namespacedName(), params)
                        ),
                        Segment.ALL,
                    )
                seqOf(Tuple2(action, securityContext.allowsAdminAction(resource)))
            },
            Option.apply(compile(plan.source(), context)),
            AuthorizationAndPredicateExecutionPlan.`apply$default$4`(),
        )
    }

    private fun updating(
        name: String,
        query: String,
        parameters: MapValue,
        handler: QueryHandler,
        source: ExecutionPlan,
        transformer: ParameterTransformer = transformer(),
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

    /** Fails with [message] once the [source] checks, such as privileges, passed. */
    @Suppress("DEPRECATION") // Neo4j 5.26 has no GQL status for unsupported features.
    private fun failure(source: ExecutionPlan, message: (MapValue) -> String): ExecutionPlan =
        PredicateExecutionPlan.apply(
            Function2 { _, _ -> false },
            Option.apply(source),
            Function3 { params, _, _ -> InvalidArgumentException(message(params)) },
        )

    private fun notFound(operation: String, databaseName: DatabaseName) =
        Function1<MapValue, Option<QueryHandlerResult>> { params ->
            Option.apply(
                ThrowException(
                    DatabaseNotFoundHelper.failedAction(operation, show(databaseName, params))
                )
            )
        }

    private fun errors(command: String, operation: String, databaseName: DatabaseName) =
        Function2<Throwable, MapValue, Throwable> { error, params ->
            failed(command, operation, show(databaseName, params), error)
        }

    private fun failed(
        command: String,
        operation: String,
        name: String,
        error: Throwable,
    ): Throwable =
        when {
            hasStatus(error, Status.Cluster.NotALeader) ->
                DatabaseAdministrationOnFollowerException.notALeader(
                    command,
                    "Failed to $operation the specified database '$name'",
                    error,
                )
            error is Status.HasStatus -> error
            else ->
                IllegalStateException("Failed to $operation the specified database '$name'.", error)
        }

    private fun hasStatus(error: Throwable, status: Status): Boolean =
        generateSequence(error) { it.cause }.any { it is Status.HasStatus && it.status() == status }

    private fun show(name: DatabaseName, params: MapValue): String =
        admin.runtimeStringValue(name, params)

    private fun show(name: Either<String, Parameter>, params: MapValue): String =
        admin.runtimeStringValue(name, params)

    private fun transformer(): ParameterTransformer =
        ParameterTransformer(
            ParameterTransformer.`$lessinit$greater$default$1`(),
            ParameterTransformer.`$lessinit$greater$default$2`(),
        )

    private fun parameter(key: String) = "${'$'}`$key`"

    private fun <T> seqOf(vararg items: T): Seq<T> =
        CollectionConverters.asScala(items.toList()).toSeq()
}
