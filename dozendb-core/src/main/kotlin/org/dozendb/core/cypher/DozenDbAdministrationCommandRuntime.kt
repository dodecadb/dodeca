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
 * 2026-10-20: OPTIONS and TOPOLOGY of CREATE DATABASE are rejected instead of ignored, the user
 * commands of UserCommands are added, and WAIT waits for the servers.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */
package org.dozendb.core.cypher

import java.time.Duration
import java.util.UUID
import org.dozendb.core.DozenDbSettings
import org.dozendb.core.edition.DatabaseProgress
import org.dozendb.core.edition.DatabaseSeeder
import org.dozendb.core.edition.SeedServers
import org.dozendb.core.store.StorePlacement
import org.neo4j.common.DependencyResolver
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.helpers.DatabaseNameValidator
import org.neo4j.cypher.internal.`AdministrationCommandRuntime$`
import org.neo4j.cypher.internal.AdministrationCommandRuntimeContext
import org.neo4j.cypher.internal.CommunityAdministrationCommandRuntime
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.ast.AdministrationAction
import org.neo4j.cypher.internal.ast.DatabaseName
import org.neo4j.cypher.internal.ast.`DumpData$`
import org.neo4j.cypher.internal.ast.`IfExistsReplace$`
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.OptionsParam
import org.neo4j.cypher.internal.evaluator.Evaluator
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
import org.neo4j.cypher.internal.optionsmap.`ExistingDataOption$`
import org.neo4j.cypher.internal.optionsmap.`ExistingSeedServerOption$`
import org.neo4j.cypher.internal.optionsmap.`SeedURIOption$`
import org.neo4j.cypher.internal.optionsmap.StringOptionValidator
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
import org.neo4j.graphdb.Transaction
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog
import org.neo4j.internal.kernel.api.security.AdminActionOnResource
import org.neo4j.internal.kernel.api.security.PermissionState
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.internal.kernel.api.security.Segment
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.io.layout.Neo4jLayout
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.values.storable.LongValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.MapValueBuilder
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

internal typealias Planner = Function1<AdministrationCommandRuntimeContext, ExecutionPlan>

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
    private val userCommands: UserCommands,
) :
    CommunityAdministrationCommandRuntime(
        engine,
        dependencies,
        databaseCommands.orElse(userCommands),
    ) {
    constructor(
        engine: ExecutionEngine,
        dependencies: DependencyResolver,
    ) : this(
        engine,
        dependencies,
        DatabaseCommands(engine, dependencies),
        UserCommands(engine, dependencies),
    )

    // Community compiles nested plans with its commands and ours, but the top-level plan only with
    // its own; this runtime uses both everywhere.
    private val commands = databaseCommands.orElse(userCommands).orElse(logicalToExecutable())

    init {
        databaseCommands.compile = ::compile
        userCommands.compile = ::compile
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
            userCommands.isDefinedAt(command) ||
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
    private val fs = dependencies.resolveDependency(FileSystemAbstraction::class.java)

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
                    WaitExecutionPlan(
                        compile(plan.source(), context),
                        plan.databaseName(),
                        Duration.ofSeconds(plan.waitForCompletion().timeout()),
                        { dependencies.resolveDependency(DatabaseProgress::class.java) },
                        engine,
                        authorization,
                    )
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
        if (plan.topology().isDefined) {
            return failure(source) {
                "Failed to create the specified database '${show(databaseName, it)}': " +
                    "TOPOLOGY is not supported."
            }
        }
        val name = admin.getNameFields("databaseName", databaseName, Function1(::validName))
        val key = parameter(name.nameKey())
        val version = context.runtimeContext().cypherVersion()
        val seed =
            Function2<Transaction, MapValue, MapValue> { _, params ->
                val database = show(databaseName, params)
                val seed = seedOptions(plan, database, params, version)
                params
                    .updatedWith(SEED_URI, seed.uri?.let(Values::utf8Value) ?: Values.NO_VALUE)
                    .updatedWith(
                        SEEDING_SERVERS,
                        seed.server?.let { Values.stringArray(it) } ?: Values.NO_VALUE,
                    )
            }
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
                database.uuid = randomUUID(),
                database.${TopologyGraphDbmsModel.DATABASE_SEED_URI_PROPERTY} = ${parameter(SEED_URI)},
                database.${TopologyGraphDbmsModel.DATABASE_SEEDING_SERVERS_PROPERTY} =
                    ${parameter(SEEDING_SERVERS)}
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
            transformer().convert(name.nameConverter()).convert(seed),
        )
    }

    /**
     * The seed URI of the database [name] that [plan] creates, null for none, from its options,
     * which it checks: DozenDB takes `existingData`, `seedURI` and, with clustering,
     * `existingDataSeedServer`, and refuses every other option rather than ignore it. Ignoring one
     * such as a seed would create an empty database instead of the expected data.
     */
    @Suppress("DEPRECATION") // Neo4j 5.26 has no GQL status for unsupported options.
    private fun seedOptions(
        plan: CreateDatabase,
        name: String,
        params: MapValue,
        version: CypherVersion,
    ): Seed {
        val options = evaluate(plan.options(), params, version)
        if (options.isEmpty) return Seed(null, null)
        val failed = "Failed to create the specified database '$name'"
        val keys = options.keySet().toList()
        val unsupported = keys.filter { key -> SUPPORTED_OPTIONS.none { it.equals(key, true) } }
        if (unsupported.isNotEmpty()) {
            throw InvalidArgumentException(
                "$failed: unsupported option(s) ${unsupported.joinToString { "'$it'" }}. " +
                    "DozenDB supports ${SUPPORTED_OPTIONS.joinToString { "'$it'" }}."
            )
        }
        val existingData = find(`ExistingDataOption$`.`MODULE$`, options) != null
        val seedUri = find(`SeedURIOption$`.`MODULE$`, options)
        val seedServer = find(`ExistingSeedServerOption$`.`MODULE$`, options)
        if (plan.ifExistsDo() is `IfExistsReplace$`) {
            throw InvalidArgumentException(
                "$failed: OR REPLACE cannot be combined with existingData, seedURI or " +
                    "existingDataSeedServer. Drop the database first."
            )
        }
        val clustered = config.get(DozenDbSettings.clusterEnabled)
        if (seedServer != null) {
            if (!clustered) {
                throw InvalidArgumentException(
                    "$failed: existingDataSeedServer needs clustering. Without it, restore the " +
                        "database with 'neo4j-admin database restore' and create it without " +
                        "options."
                )
            }
            try {
                dependencies.resolveTypeDependencies(SeedServers::class.java).forEach {
                    it.check(UUID.fromString(seedServer))
                }
            } catch (e: IllegalArgumentException) {
                throw InvalidArgumentException("$failed: ${e.message?.removeSuffix(".")}.", e)
            }
        }
        val placement = StorePlacement(fs, Neo4jLayout.of(config))
        val normalized = NormalizedDatabaseName(name).name()
        when {
            seedUri != null -> {
                if (!clustered && placement.holdsStore(normalized)) {
                    throw InvalidArgumentException(
                        "$failed: its files are in place already. Create it without seedURI to " +
                            "use them, or delete them first."
                    )
                }
                val seeder =
                    dependencies.resolveTypeDependencies(DatabaseSeeder::class.java).firstOrNull()
                        ?: throw InvalidArgumentException(
                            "$failed: this server cannot create databases from seeds."
                        )
                try {
                    seeder.check(seedUri)
                } catch (e: IllegalArgumentException) {
                    throw InvalidArgumentException("$failed: ${e.message?.removeSuffix(".")}.", e)
                }
            }
            existingData && seedServer == null && clustered ->
                throw InvalidArgumentException(
                    "$failed: with clustering, existingData 'use' needs existingDataSeedServer, " +
                        "the server where 'neo4j-admin database restore' prepared the database, " +
                        "or a seedURI."
                )
            existingData && !clustered && !placement.holdsStore(normalized) ->
                throw InvalidArgumentException(
                    "$failed: existingData 'use' needs its files in place, as 'neo4j-admin " +
                        "database restore' puts them, or a seedURI. There are no files."
                )
        }
        return Seed(seedUri, seedServer?.let { UUID.fromString(it).toString() })
    }

    /** What a database is created from: a [uri], read by the [server] if named. */
    private class Seed(val uri: String?, val server: String?)

    /** The value of [option] in [options], checked as Neo4j checks it; null if absent. */
    private fun find(option: StringOptionValidator, options: MapValue): String? =
        option.findIn(options, Option.apply(config), CREATE_DATABASE).getOrElse { null }

    /** The values of [options], evaluated as Neo4j evaluates the options of its commands. */
    @Suppress("DEPRECATION") // Neo4j 5.26 has no GQL status for this.
    private fun evaluate(options: Options, params: MapValue, version: CypherVersion): MapValue =
        when (options) {
            is OptionsMap -> {
                val evaluator = Evaluator.expressionEvaluator(version)
                val builder = MapValueBuilder()
                CollectionConverters.asJava(options.map()).forEach { (key, expression) ->
                    builder.add(key, evaluator.evaluate(expression, params))
                }
                builder.build()
            }
            is OptionsParam ->
                params.get(options.parameter().name()) as? MapValue
                    ?: throw InvalidArgumentException(
                        "Could not $CREATE_DATABASE with options " +
                            "'${params.get(options.parameter().name())}'. Expected a map value."
                    )
            else -> MapValue.EMPTY
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

    private companion object {
        const val CREATE_DATABASE = "create database"

        /** The parameter of the seed URI the `Database` node keeps. */
        const val SEED_URI = "__dozendb_seedURI"

        /** The parameter of the servers a clustered database is created on first: one. */
        const val SEEDING_SERVERS = "__dozendb_seedingServers"

        val SUPPORTED_OPTIONS =
            listOf(
                `ExistingDataOption$`.`MODULE$`.KEY(),
                `SeedURIOption$`.`MODULE$`.KEY(),
                `ExistingSeedServerOption$`.`MODULE$`.KEY(),
            )
    }
}
