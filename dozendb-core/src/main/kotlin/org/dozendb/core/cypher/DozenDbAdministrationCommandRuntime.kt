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
 * 2026-10-23: the role commands of RoleCommands, the privilege commands of PrivilegeCommands and
 * ShowPrivilegeCommands are added, and DROP DATABASE drops the privileges on the database.
 * 2026-10-24: the alias commands of AliasCommands and RemoteAliasCommands are added, and the targets of aliases checked.
 * DROP DATABASE refuses to drop a database that local aliases target unless CASCADE ALIASES drops
 * them too, and drops only the names of the database, not those of other databases or aliases.
 * START, STOP and DROP DATABASE take the name of a local alias for its database.
 * 2026-10-26: CREATE DATABASE takes TOPOLOGY and places the database on servers; ALTER DATABASE
 * sets its topology and access.
 * 2026-11-09: CREATE DATABASE takes storeFormat and existingDataSeedInstance, warning of the
 * deprecated ones.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */
package org.dozendb.core.cypher

import java.time.Duration
import java.util.UUID
import org.dozendb.core.DozenDbSettings
import org.dozendb.core.Gql
import org.dozendb.core.edition.DatabaseProgress
import org.dozendb.core.edition.DatabaseSeeder
import org.dozendb.core.edition.SeedServers
import org.dozendb.core.logging.SecurityLog
import org.dozendb.core.security.PrivilegeRecords
import org.dozendb.core.servers.AllocationException
import org.dozendb.core.servers.CopyMode
import org.dozendb.core.servers.DatabasePlacement
import org.dozendb.core.servers.Placement
import org.dozendb.core.servers.Placements
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
import org.neo4j.cypher.internal.ast.`CascadeAliases$`
import org.neo4j.cypher.internal.ast.DatabaseName
import org.neo4j.cypher.internal.ast.`DumpData$`
import org.neo4j.cypher.internal.ast.`IfExistsReplace$`
import org.neo4j.cypher.internal.ast.`NoOptions$`
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.OptionsParam
import org.neo4j.cypher.internal.ast.ParameterName
import org.neo4j.cypher.internal.ast.`ReadOnlyAccess$`
import org.neo4j.cypher.internal.ast.Topology
import org.neo4j.cypher.internal.evaluator.Evaluator
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.logical.plans.AlterDatabase
import org.neo4j.cypher.internal.logical.plans.AssertCanDropDatabase
import org.neo4j.cypher.internal.logical.plans.AssertManagementActionNotBlocked
import org.neo4j.cypher.internal.logical.plans.AssertNotInvalidActionOnShard
import org.neo4j.cypher.internal.logical.plans.AssertNotShardedDatabase
import org.neo4j.cypher.internal.logical.plans.AssertNotStandard
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
import org.neo4j.cypher.internal.notification.DeprecatedOptionInOptionMap
import org.neo4j.cypher.internal.notification.DeprecatedStoreFormat
import org.neo4j.cypher.internal.notification.InternalNotification
import org.neo4j.cypher.internal.optionsmap.`ExistingDataOption$`
import org.neo4j.cypher.internal.optionsmap.`ExistingSeedInstanceOption$`
import org.neo4j.cypher.internal.optionsmap.`ExistingSeedServerOption$`
import org.neo4j.cypher.internal.optionsmap.`SeedURIOption$`
import org.neo4j.cypher.internal.optionsmap.`StoreFormatOption$`
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
import org.neo4j.dbms.api.DatabaseAliasExistsException
import org.neo4j.dbms.api.DatabaseLimitReachedException
import org.neo4j.dbms.api.DatabaseNotFoundHelper
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DatabaseAccess
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DatabaseStatus
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.exceptions.DatabaseAdministrationOnFollowerException
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.gqlstatus.ErrorGqlStatusObject
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlHelper
import org.neo4j.gqlstatus.GqlParams
import org.neo4j.gqlstatus.GqlStatusInfoCodes
import org.neo4j.graphdb.Direction
import org.neo4j.graphdb.Node
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
import org.neo4j.storageengine.api.StorageEngineFactory
import org.neo4j.values.storable.IntegralValue
import org.neo4j.values.storable.LongValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue
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
import scala.collection.immutable.Set
import scala.collection.immutable.`Set$`
import scala.jdk.javaapi.CollectionConverters
import scala.util.Either
import scala.util.Left
import scala.util.Right

internal typealias Planner = Function1<AdministrationCommandRuntimeContext, ExecutionPlan>

/** The name [name] gives, itself or the value of its parameter in [params]. */
internal fun nameOf(name: Either<String, Parameter>, params: MapValue): String =
    `AdministrationCommandRuntime$`.`MODULE$`.runtimeStringValue(name, params) { it }

/**
 * Fails with "[feature] is not supported." once the checks of [source], such as privileges, passed.
 */
internal fun notSupportedPlan(source: ExecutionPlan, feature: String): ExecutionPlan =
    PredicateExecutionPlan.apply(
        Function2 { _, _ -> false },
        Option.apply(source),
        Function3 { _, _, _ ->
            InvalidArgumentException(notSupported(feature), "$feature is not supported.")
        },
    )

/** Neo4j's GQL status for a [feature] that DozenDB does not support. */
internal fun notSupported(feature: String): ErrorGqlStatusObject =
    ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N31)
        .withParam(GqlParams.StringParam.item, feature)
        .withParam(GqlParams.StringParam.context, "DozenDB")
        .build()

/**
 * Neo4j Community's administration commands, plus CREATE, DROP, START and STOP DATABASE, the role
 * commands, the alias commands and the server commands, which Community leaves to Neo4j Enterprise.
 * The database commands only change `Database` nodes in `system`; the databases themselves follow
 * those nodes.
 */
internal class DozenDbAdministrationCommandRuntime
private constructor(
    engine: ExecutionEngine,
    dependencies: DependencyResolver,
    private val databaseCommands: DatabaseCommands,
    private val userCommands: UserCommands,
    private val roleCommands: RoleCommands,
    private val privilegeCommands: PrivilegeCommands,
    private val showPrivilegeCommands: ShowPrivilegeCommands,
    private val aliasCommands: AliasCommands,
    private val remoteAliasCommands: RemoteAliasCommands,
    private val serverCommands: ServerCommands,
    private val nameChecks: DatabaseNameChecks,
) :
    CommunityAdministrationCommandRuntime(
        engine,
        dependencies,
        databaseCommands
            .orElse(userCommands)
            .orElse(roleCommands)
            .orElse(privilegeCommands)
            .orElse(showPrivilegeCommands)
            .orElse(aliasCommands)
            .orElse(remoteAliasCommands)
            .orElse(serverCommands)
            .orElse(nameChecks),
    ) {
    constructor(
        engine: ExecutionEngine,
        dependencies: DependencyResolver,
    ) : this(
        engine,
        dependencies,
        DatabaseCommands(engine, dependencies),
        UserCommands(engine, dependencies),
        RoleCommands(engine, dependencies),
        PrivilegeCommands(engine, dependencies),
        ShowPrivilegeCommands(engine, dependencies),
        AliasCommands(engine, dependencies),
        RemoteAliasCommands(engine, dependencies),
        ServerCommands(engine, dependencies),
        DatabaseNameChecks(engine, dependencies),
    )

    // Community compiles nested plans with its commands and ours, but the top-level plan only with
    // its own; this runtime uses both everywhere.
    private val commands =
        databaseCommands
            .orElse(userCommands)
            .orElse(roleCommands)
            .orElse(privilegeCommands)
            .orElse(showPrivilegeCommands)
            .orElse(aliasCommands)
            .orElse(remoteAliasCommands)
            .orElse(serverCommands)
            .orElse(nameChecks)
            .orElse(logicalToExecutable())

    init {
        databaseCommands.compile = ::compile
        userCommands.compile = ::compile
        roleCommands.compile = ::compile
        privilegeCommands.compile = ::compile
        showPrivilegeCommands.compile = ::compile
        aliasCommands.compile = ::compile
        remoteAliasCommands.compile = ::compile
        serverCommands.compile = ::compile
        nameChecks.compile = ::compile
    }

    /** Where administration commands are logged; none with authentication disabled. */
    private val securityLog: SecurityLog? =
        dependencies.resolveTypeDependencies(SecurityLog::class.java).firstOrNull()

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
            roleCommands.isDefinedAt(command) ||
            privilegeCommands.isDefinedAt(command) ||
            showPrivilegeCommands.isDefinedAt(command) ||
            aliasCommands.isDefinedAt(command) ||
            remoteAliasCommands.isDefinedAt(command) ||
            serverCommands.isDefinedAt(command) ||
            nameChecks.isDefinedAt(command) ||
            super.isApplicableAdministrationCommand(logicalPlan)
    }

    private fun compile(
        plan: LogicalPlan,
        context: AdministrationCommandRuntimeContext,
    ): ExecutionPlan {
        if (plan is LogSystemCommand) {
            val source = compile(plan.source(), context)
            return securityLog?.let { LoggedCommandExecutionPlan(source, plan.command(), it) }
                ?: source
        }
        if (!commands.isDefinedAt(plan)) {
            throw CantCompileQueryException.internalError(
                "DozenDbAdministrationCommandRuntime",
                "Plan is not a recognized database administration command: ${plan.javaClass.simpleName}",
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
    private val authorizationLog by lazy {
        dependencies.resolveDependency(AbstractSecurityLog::class.java)
    }
    private val authorization by lazy {
        SecurityAuthorizationHandler(authorizationLog)
    }

    private val admin = `AdministrationCommandRuntime$`.`MODULE$`

    private val placement by lazy { dependencies.resolveDependency(DatabasePlacement::class.java) }

    override fun isDefinedAt(plan: LogicalPlan): Boolean = planner(plan) != null

    override fun apply(plan: LogicalPlan): Planner =
        planner(plan) ?: throw IllegalArgumentException("Not a database command: $plan")

    private fun planner(plan: LogicalPlan): Planner? =
        when (plan) {
            is CreateDatabase -> Planner { createDatabase(plan, it) }
            is EnsureValidNumberOfDatabases -> Planner { ensureValidNumberOfDatabases(plan, it) }
            is DropDatabase -> Planner { dropDatabase(plan, it, aliases = true) }
            is AlterDatabase -> Planner { alterDatabase(plan, it) }
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
            // There are no composite databases that could make a name ambiguous.
            is EnsureNameIsNotAmbiguous -> Planner { compile(plan.source(), it) }
            // DROP DATABASE checks for aliases, where it knows which database the name means.
            is EnsureDatabaseSafeToDelete -> Planner { compile(plan.source(), it) }
            // There are no sharded databases: no database is one, nor a shard of one. The commands
            // that need one, which this check is for, refuse themselves.
            is AssertNotShardedDatabase -> Planner { compile(plan.source(), it) }
            is AssertNotInvalidActionOnShard -> Planner { compile(plan.source(), it) }
            is AssertNotStandard -> Planner { compile(plan.source(), it) }
            is WaitForCompletion ->
                Planner { context ->
                    WaitExecutionPlan(
                        compile(plan.source(), context),
                        plan.databaseName(),
                        Duration.ofSeconds(plan.waitForCompletion().timeout()),
                        { dependencies.resolveDependency(DatabaseProgress::class.java) },
                        engine,
                        authorization,
                        context.runtimeContext().cypherVersion(),
                    )
                }
            else -> null
        }

    private fun createDatabase(
        plan: CreateDatabase,
        context: AdministrationCommandRuntimeContext,
    ): ExecutionPlan {
        val source = replacing(plan.source(), context)
        val databaseName = plan.databaseName()
        val type = plan.databaseType()
        if (type.isComposite || type.isReplica) {
            val kind = if (type.isComposite) "Composite databases" else "Replica databases"
            return failure(source, kind) {
                "Failed to create the specified database '${show(databaseName, it)}': " +
                    "${kind.lowercase()} are not supported."
            }
        }
        if (plan.defaultLanguageVersion().isDefined) {
            return failure(source, "DEFAULT LANGUAGE") {
                "Failed to create the specified database '${show(databaseName, it)}': " +
                    "a default language of its own is not supported."
            }
        }
        val name = admin.getNameFields("databaseName", databaseName, Function1(::validName))
        val key = parameter(name.nameKey())
        val free =
            ensureNameFree(
                engine,
                authorization,
                "database",
                name.nameKey(),
                VirtualValues.map(arrayOf(name.nameKey()), arrayOf(name.nameValue())),
                transformer().convert(name.nameConverter()),
                { show(databaseName, it) },
                source,
            )
        val version = context.runtimeContext().cypherVersion()
        val creating =
            Function2<Transaction, MapValue, Tuple2<MapValue, Set<InternalNotification>>> {
                tx,
                params ->
                val database = show(databaseName, params)
                val creation = creationOptions(plan, database, params, version)
                val placed =
                    placed(database, "create") {
                        val requested =
                            plan.topology().valueOrNull()?.let {
                                topology(it, database, params, "create")
                            }
                        placement.place(
                            tx,
                            NormalizedDatabaseName(database).name(),
                            requested?.primaries,
                            requested?.secondaries,
                            creation.server?.let(UUID::fromString),
                        )
                    }
                params
                    .updatedWith(SEED_URI, creation.uri?.let(Values::utf8Value) ?: Values.NO_VALUE)
                    .updatedWith(
                        SEEDING_SERVERS,
                        creation.server?.let { Values.stringArray(it) } ?: Values.NO_VALUE,
                    )
                    .updatedWith(
                        STORE_FORMAT,
                        creation.storeFormat?.let(Values::utf8Value) ?: Values.NO_VALUE,
                    )
                    .updatedWith(
                        HOSTS,
                        VirtualValues.list(
                            *placed.copies
                                .map { (server, mode) ->
                                    VirtualValues.map(
                                        arrayOf("server", "mode"),
                                        arrayOf(
                                            Values.utf8Value(server.toString()),
                                            Values.utf8Value(mode.hostedOn.modeName()),
                                        ),
                                    )
                                }
                                .toTypedArray()
                        ),
                    )
                    .updatedWith(PRIMARIES, Values.longValue(placed.primaries.toLong()))
                    .updatedWith(SECONDARIES, Values.longValue(placed.secondaries.toLong()))
                    .let { Tuple2(it, creation.warnings) }
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
                    ${parameter(SEEDING_SERVERS)},
                database.${TopologyGraphDbmsModel.DATABASE_STORE_FORMAT_NEW_DB_PROPERTY} =
                    ${parameter(STORE_FORMAT)},
                database.${TopologyGraphDbmsModel.DATABASE_PRIMARIES_PROPERTY} = ${parameter(PRIMARIES)},
                database.${TopologyGraphDbmsModel.DATABASE_SECONDARIES_PROPERTY} = ${parameter(SECONDARIES)}
            WITH database
            CALL {
                WITH database
                UNWIND ${parameter(HOSTS)} AS host
                MATCH (server:${TopologyGraphDbmsModel.INSTANCE_LABEL.name()} {
                    ${TopologyGraphDbmsModel.INSTANCE_UUID_PROPERTY}: host.server})
                CREATE (database)-[:${TopologyGraphDbmsModel.HOSTED_ON_RELATIONSHIP.name()} {
                    ${TopologyGraphDbmsModel.HOSTED_ON_MODE_PROPERTY}: host.mode,
                    ${TopologyGraphDbmsModel.HOSTED_ON_INSTALLED_AT_PROPERTY}: datetime()}]->(server)
            }
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
                        nameTaken(
                            "Failed to create the specified database " +
                                "'${show(databaseName, params)}': Database already exists.",
                            show(databaseName, params),
                            error,
                        )
                    } else {
                        failed("CREATE DATABASE", "create", show(databaseName, params), error)
                    }
                }
            ),
            free,
            transformer().convert(name.nameConverter()).validate(creating),
        )
    }

    /**
     * ALTER DATABASE: SET TOPOLOGY places the database on as many primaries as asked for (see
     * [DatabasePlacement]), SET ACCESS makes it read-only or read-write. Options, such as
     * `txLogEnrichment` for change data capture, are refused.
     */
    private fun alterDatabase(
        plan: AlterDatabase,
        context: AdministrationCommandRuntimeContext,
    ): ExecutionPlan {
        val source = compile(plan.source(), context)
        val databaseName = plan.databaseName()
        if (plan.options() !is `NoOptions$` || !plan.optionsToRemove().isEmpty) {
            return failure(source, "ALTER DATABASE options") {
                "Failed to alter the specified database '${show(databaseName, it)}': options " +
                    "are not supported."
            }
        }
        if (plan.defaultLanguageVersion().isDefined) {
            return failure(source, "SET DEFAULT LANGUAGE") {
                "Failed to alter the specified database '${show(databaseName, it)}': a default " +
                    "language of its own is not supported."
            }
        }
        if (plan.replicas().isDefined) {
            return failure(source, "SET REPLICAS") {
                "Failed to alter the specified database '${show(databaseName, it)}': replicas " +
                    "are not supported."
            }
        }
        val name = admin.getDatabaseNameFields("databaseName", databaseName, false)
        val access =
            plan.access().valueOrNull()?.let {
                if (it is `ReadOnlyAccess$`) DatabaseAccess.READ_ONLY else DatabaseAccess.READ_WRITE
            }
        val topology = plan.topology().valueOrNull()
        val placing =
            Function2<Transaction, MapValue, MapValue> { tx, params ->
                if (topology != null) {
                    val database = show(databaseName, params)
                    val requested = topology(topology, database, params, "alter")
                    val node = resolve(tx, database)
                    // Without a node the query finds none and reports it. A number not given
                    // stays as it is.
                    if (node != null) {
                        val placed =
                            placed(database, "alter") {
                                placement.resize(
                                    tx,
                                    Placements.name(node),
                                    requested.primaries
                                        ?: Placements.primaries(node)
                                        ?: Placements.hosts(node, CopyMode.PRIMARY).size,
                                    requested.secondaries
                                        ?: Placements.secondaries(node)
                                        ?: Placements.hosts(node, CopyMode.SECONDARY).size,
                                )
                            }
                        Placements.place(
                            tx,
                            node,
                            placed.copies,
                            placed.primaries,
                            placed.secondaries,
                        )
                    }
                }
                params
            }
        return updating(
            "AlterDatabase",
            """
            ${database(parameter(name.displayNameKey()))}
            SET database.access = coalesce(${'$'}access, database.access),
                database.updated_at = datetime()
            RETURN database.name AS name
            """
                .trimIndent(),
            VirtualValues.map(
                name.keys() + "access",
                name.values() + (access?.let { Values.utf8Value(it.name) } ?: Values.NO_VALUE),
            ),
            QueryHandler.handleNoResult(notFound("alter", databaseName))
                .handleError(errors("ALTER DATABASE", "alter", databaseName)),
            source,
            transformer().convert(name.nameConverter()).convert(placing),
        )
    }

    /** The `Database` node that [name] names, by its name or a local alias, if there is one. */
    private fun resolve(tx: Transaction, name: String): Node? {
        val normalized = NormalizedDatabaseName(name).name()
        tx.findNode(
                TopologyGraphDbmsModel.DATABASE_LABEL,
                TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY,
                normalized,
            )
            ?.let {
                return it
            }
        return tx.findNodes(
                TopologyGraphDbmsModel.DATABASE_NAME_LABEL,
                TopologyGraphDbmsModel.NAME_PROPERTY,
                normalized,
            )
            .use { names ->
                names
                    .asSequence()
                    .filter {
                        it.getProperty(TopologyGraphDbmsModel.NAMESPACE_PROPERTY, null) ==
                            TopologyGraphDbmsModel.DEFAULT_NAMESPACE &&
                            it.getProperty(TopologyGraphDbmsModel.PRIMARY_PROPERTY, null) == false
                    }
                    .mapNotNull {
                        it.getSingleRelationship(
                                TopologyGraphDbmsModel.TARGETS_RELATIONSHIP,
                                Direction.OUTGOING,
                            )
                            ?.endNode
                    }
                    .firstOrNull { it.hasLabel(TopologyGraphDbmsModel.DATABASE_LABEL) }
            }
    }

    /** The numbers of copies a TOPOLOGY asks for; null for a number it does not give. */
    private data class Requested(val primaries: Int?, val secondaries: Int?)

    /**
     * The numbers of copies [topology] asks for, checked for the database [name]: 1 to 11 primaries
     * and 0 to 20 secondaries, as Neo4j Enterprise takes them.
     */
    private fun topology(
        topology: Topology,
        name: String,
        params: MapValue,
        operation: String,
    ): Requested {
        val failed = "Failed to $operation the specified database '$name'"
        val primaries = topology.primaries().valueOrNull()?.let { number(it, params, failed) }
        val secondaries = topology.secondaries().valueOrNull()?.let { number(it, params, failed) }
        if (primaries != null && primaries !in 1..MAX_PRIMARIES) {
            throw InvalidArgumentException(
                outOfRange(GqlStatusInfoCodes.STATUS_51N52, operation, primaries, MAX_PRIMARIES),
                "$failed: a topology has 1 to $MAX_PRIMARIES primaries, not $primaries.",
            )
        }
        if (secondaries != null && secondaries !in 0..MAX_SECONDARIES) {
            throw InvalidArgumentException(
                outOfRange(
                    GqlStatusInfoCodes.STATUS_51N53,
                    operation,
                    secondaries,
                    MAX_SECONDARIES,
                ),
                "$failed: a topology has 0 to $MAX_SECONDARIES secondaries, not $secondaries.",
            )
        }
        return Requested(primaries?.toInt(), secondaries?.toInt())
    }

    /** 51N52 or 51N53: [count] primaries or secondaries, out of their range up to [upper]. */
    private fun outOfRange(code: GqlStatusInfoCodes, operation: String, count: Long, upper: Int) =
        Gql.status(
            code,
            GqlParams.StringParam.action to operation,
            GqlParams.NumberParam.count to count,
            GqlParams.NumberParam.upper to upper,
        )

    /** A number of a topology, given or as a parameter. */
    private fun number(given: Either<Any, Parameter>, params: MapValue, failed: String): Long =
        when (given) {
            is Left -> (given.value() as Number).toLong()
            else -> {
                val parameter = (given as Right).value()
                (params.get(parameter.name()) as? IntegralValue)?.longValue()
                    ?: throw Gql.refused(
                        "$failed: the topology parameter '${parameter.name()}' is not an integer."
                    )
            }
        }

    /** What [placing] decides, refused as a failed [operation] of the database [name]. */
    private fun placed(name: String, operation: String, placing: () -> Placement): Placement =
        try {
            placing()
        } catch (e: AllocationException) {
            throw Gql.refused("Failed to $operation the specified database '$name': ${e.message}.")
        }

    /**
     * Compiles the plans before CREATE DATABASE: with OR REPLACE, they drop the database of the
     * name, never the database of an alias with the name, which the name check then refuses.
     */
    private fun replacing(
        plan: LogicalPlan,
        context: AdministrationCommandRuntimeContext,
    ): ExecutionPlan =
        when (plan) {
            // Checks nothing here: there are no composite databases.
            is EnsureNameIsNotAmbiguous -> replacing(plan.source(), context)
            is DropDatabase -> dropDatabase(plan, context, aliases = false)
            else -> compile(plan, context)
        }

    /**
     * What the options of [plan] ask of the database [name] it creates, which it checks: DozenDB
     * takes `existingData`, `seedURI`, `storeFormat` and, with clustering, `existingDataSeedServer`
     * or its deprecated name `existingDataSeedInstance`, and refuses every other option rather than
     * ignore it. Ignoring one such as a seed would create an empty database instead of the expected
     * data.
     */
    private fun creationOptions(
        plan: CreateDatabase,
        name: String,
        params: MapValue,
        version: CypherVersion,
    ): Creation {
        val options = evaluate(plan.options(), params, version)
        if (options.isEmpty) return Creation(null, null, null, emptyList())
        val failed = "Failed to create the specified database '$name'"
        val keys = options.keySet().toList()
        val unsupported = keys.filter { key -> SUPPORTED_OPTIONS.none { it.equals(key, true) } }
        val credentials = unsupported.filter { key ->
            SEED_CREDENTIALS.any { it.equals(key, true) }
        }
        if (credentials.isNotEmpty()) {
            throw InvalidArgumentException(
                GqlHelper.getGql42001_22N04(credentials.first(), "option", SUPPORTED_OPTIONS),
                "$failed: unsupported option(s) ${credentials.sorted().joinToString { "'$it'" }}, which " +
                    "Neo4j Enterprise's deprecated S3SeedProvider takes. DozenDB reads seeds in " +
                    "cloud storage with each cloud's own credentials, which every server that " +
                    "reads the seed needs: for S3, the AWS SDK's environment variables, " +
                    "~/.aws/credentials or the instance's role.",
            )
        }
        if (unsupported.isNotEmpty()) {
            throw InvalidArgumentException(
                GqlHelper.getGql42001_22N04(unsupported.first(), "option", SUPPORTED_OPTIONS),
                "$failed: unsupported option(s) ${unsupported.joinToString { "'$it'" }}. " +
                    "DozenDB supports ${SUPPORTED_OPTIONS.joinToString { "'$it'" }}.",
            )
        }
        val existingData = find(`ExistingDataOption$`.`MODULE$`, options) != null
        // One URI: a list, for one per server, is not supported.
        val seedUri =
            `SeedURIOption$`.`MODULE$`
                .findIn(options, Option.apply(config), CREATE_DATABASE)
                .takeIf { it.isDefined }
                ?.let {
                    `SeedURIOption$`.`MODULE$`
                        .validateSingleOnly(it.get(), CREATE_DATABASE)
                        .singleUri()
                        .get()
                }
        val seedServers =
            listOf(`ExistingSeedServerOption$`.`MODULE$`, `ExistingSeedInstanceOption$`.`MODULE$`)
                .mapNotNull { option -> find(option, options)?.let { option.KEY() to it } }
        if (seedServers.size > 1) {
            throw Gql.refused(
                "$failed: existingDataSeedInstance is the deprecated name of " +
                    "existingDataSeedServer. Give existingDataSeedServer alone."
            )
        }
        val (seedKey, seedServer) = seedServers.singleOrNull() ?: (null to null)
        val storeFormat = find(`StoreFormatOption$`.`MODULE$`, options)
        if (
            plan.ifExistsDo() is `IfExistsReplace$` &&
                (existingData || seedUri != null || seedServer != null)
        ) {
            throw Gql.refused(
                "$failed: OR REPLACE cannot be combined with existingData, seedURI or " +
                    "existingDataSeedServer. Drop the database first."
            )
        }
        val clustered = config.get(DozenDbSettings.clusterEnabled)
        if (seedServer != null) {
            if (!clustered) {
                throw Gql.refused(
                    "$failed: $seedKey needs clustering. Without it, restore the database with " +
                        "'neo4j-admin database restore' and create it without options."
                )
            }
            try {
                dependencies.resolveTypeDependencies(SeedServers::class.java).forEach {
                    it.check(UUID.fromString(seedServer))
                }
            } catch (e: IllegalArgumentException) {
                throw Gql.refused("$failed: ${e.message?.removeSuffix(".")}.", e)
            }
        }
        val placement = StorePlacement(fs, Neo4jLayout.of(config))
        val normalized = NormalizedDatabaseName(name).name()
        when {
            seedUri != null -> {
                if (!clustered && placement.holdsStore(normalized)) {
                    throw Gql.refused(
                        "$failed: its files are in place already. Create it without seedURI to " +
                            "use them, or delete them first."
                    )
                }
                val seeder =
                    dependencies.resolveTypeDependencies(DatabaseSeeder::class.java).firstOrNull()
                        ?: throw Gql.refused(
                            "$failed: this server cannot create databases from seeds."
                        )
                try {
                    seeder.check(seedUri)
                } catch (e: IllegalArgumentException) {
                    throw Gql.refused("$failed: ${e.message?.removeSuffix(".")}.", e)
                }
            }
            existingData && seedServer == null && clustered ->
                throw Gql.refused(
                    "$failed: with clustering, existingData 'use' needs existingDataSeedServer, " +
                        "the server where 'neo4j-admin database restore' prepared the database, " +
                        "or a seedURI."
                )
            existingData && !clustered && !placement.holdsStore(normalized) ->
                throw Gql.refused(
                    "$failed: existingData 'use' needs its files in place, as 'neo4j-admin " +
                        "database restore' puts them, or a seedURI. There are no files."
                )
        }
        val warnings =
            listOfNotNull(
                (seedKey == `ExistingSeedInstanceOption$`.`MODULE$`.KEY())
                    .takeIf { it }
                    ?.let {
                        DeprecatedOptionInOptionMap(
                            `ExistingSeedInstanceOption$`.`MODULE$`.KEY(),
                            `ExistingSeedServerOption$`.`MODULE$`.KEY(),
                        )
                    },
                storeFormat
                    ?.takeIf { StorageEngineFactory.isFormatDeprecated(it) }
                    ?.let { DeprecatedStoreFormat(it) },
            )
        return Creation(
            seedUri,
            seedServer?.let { UUID.fromString(it).toString() },
            storeFormat,
            warnings,
        )
    }

    /**
     * What a database is created from: a seed [uri], read by the [server] if named, or else a new
     * store, in the [storeFormat] if asked for; and the [warnings] the options deserve.
     */
    private class Creation(
        val uri: String?,
        val server: String?,
        val storeFormat: String?,
        warnings: List<InternalNotification>,
    ) {
        val warnings: Set<InternalNotification> =
            warnings.fold(`Set$`.`MODULE$`.empty()) { set, warning -> set.incl(warning) }
    }

    /** The value of [option] in [options], checked as Neo4j checks it; null if absent. */
    private fun find(option: StringOptionValidator, options: MapValue): String? =
        option.findIn(options, Option.apply(config), CREATE_DATABASE).getOrElse { null }

    /** The values of [options], evaluated as Neo4j evaluates the options of its commands. */
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
                        GqlHelper.getGql22G03_22N27(
                            params.get(options.parameter().name()).toString(),
                            "OPTIONS",
                            listOf("MAP"),
                        ),
                        "Could not $CREATE_DATABASE with options " +
                            "'${params.get(options.parameter().name())}'. Expected a map value.",
                    )
            else -> MapValue.EMPTY
        }

    @Suppress("DEPRECATION") // Like Neo4j's own name validation.
    private fun validName(name: String): String {
        val normalized = NormalizedDatabaseName(name)
        try {
            DatabaseNameValidator.validateExternalDatabaseName(normalized)
        } catch (e: IllegalArgumentException) {
            throw InvalidArgumentException(GqlHelper.getGql22000_22N11(name), e.message)
        }
        return normalized.name()
    }

    /** Runs after the database was created, so the count includes it. */
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
                                ErrorGqlStatusObjectImplementation.from(
                                        GqlStatusInfoCodes.STATUS_51N55
                                    )
                                    .withParam(
                                        GqlParams.StringParam.db,
                                        show(plan.source().databaseName(), params),
                                    )
                                    .withParam(
                                        GqlParams.StringParam.cfgSetting,
                                        DozenDbSettings.maxDatabases.name(),
                                    )
                                    .build(),
                                "Failed to create the specified database " +
                                    "'${show(plan.source().databaseName(), params)}': " +
                                    "there may be at most $limit databases, including system.",
                            )
                        )
                    }
                }
            ),
            compile(plan.source(), context),
        )

    /**
     * Drops a database with the privileges on it and its names: its own and, with CASCADE ALIASES,
     * its local aliases. Without, it refuses to drop a database that local aliases target, naming
     * them; remote aliases never stop a drop, they no longer resolve once it is gone. With
     * [aliases], the name may be a local alias's, which stands for its database. Refuses the
     * default database, as Neo4j Enterprise does: `dbms.setDefaultDatabase` makes another database
     * the default first.
     */
    private fun dropDatabase(
        plan: DropDatabase,
        context: AdministrationCommandRuntimeContext,
        aliases: Boolean,
    ): ExecutionPlan {
        val databaseName = plan.databaseName()
        val source = compile(plan.source(), context)
        if (plan.additionalAction() == `DumpData$`.`MODULE$`) {
            return failure(source, "DUMP DATA") {
                "Failed to delete the specified database '${show(databaseName, it)}': " +
                    "DUMP DATA is not supported."
            }
        }
        val name = admin.getDatabaseNameFields("databaseName", databaseName, false)
        val key = parameter(name.displayNameKey())
        val restrict = plan.aliasAction() !is `CascadeAliases$`
        val droppable =
            updating(
                "EnsureDatabaseCanBeDropped",
                """
                ${database(key, aliases)}
                OPTIONAL MATCH (database)<-[:TARGETS]-(alias:DatabaseName {primary: false})
                WITH database, alias.displayName AS alias ORDER BY alias
                WITH database.name AS name, database.default AS default, collect(alias) AS aliases
                RETURN {name: name, default: default, aliases: aliases} AS database
                """
                    .trimIndent(),
                VirtualValues.map(name.keys(), name.values()),
                QueryHandler.handleResult(
                        Function3 { _, value, params ->
                            val database = value as MapValue
                            val target = (database.get("name") as TextValue).stringValue()
                            val targeting =
                                (database.get("aliases") as ListValue).map {
                                    (it as TextValue).stringValue()
                                }
                            when {
                                database.get("default") == Values.TRUE ->
                                    ThrowException(
                                        Gql.refused(
                                            "Not allowed to delete the default database " +
                                                "'$target'."
                                        )
                                    )
                                restrict && targeting.isNotEmpty() ->
                                    ThrowException(
                                        aliasesExist(
                                            show(databaseName, params),
                                            target,
                                            targeting,
                                        )
                                    )
                                else -> `Continue$`.`MODULE$`
                            }
                        }
                    )
                    .handleError(errors("DROP DATABASE", "delete", databaseName)),
                source,
                transformer().convert(name.nameConverter()),
            )
        return updating(
            "DropDatabase",
            """
            ${database(key, aliases)}
            WITH database, database.name AS droppedName
            OPTIONAL MATCH (privilege:${PrivilegeRecords.PRIVILEGE.name()} {
                ${PrivilegeRecords.SCOPE}: '${PrivilegeRecords.ONE_DATABASE}',
                ${PrivilegeRecords.DATABASE}: database.uuid})
            DETACH DELETE privilege
            WITH DISTINCT database, droppedName
            OPTIONAL MATCH (database)<-[:TARGETS]-(name:DatabaseName)
            OPTIONAL MATCH (name)-[:PROPERTIES|CONNECTS_WITH]->(attached)
            DETACH DELETE attached, name
            WITH DISTINCT database, droppedName
            DETACH DELETE database
            WITH DISTINCT droppedName
            CREATE (:DeletedDatabase {name: droppedName, timestamp: timestamp()})
            RETURN droppedName
            """
                .trimIndent(),
            VirtualValues.map(name.keys(), name.values()),
            QueryHandler.handleError(errors("DROP DATABASE", "delete", databaseName)),
            droppable,
            transformer().convert(name.nameConverter()),
        )
    }

    /**
     * Binds `database` to the database named at the parameter [key], or with [aliases] also to the
     * database of the local alias of that name. Database names and alias names never clash.
     */
    private fun database(key: String, aliases: Boolean = true): String =
        if (aliases) {
            """
            CALL {
                MATCH (database:Database {name: $key}) RETURN database
                UNION
                MATCH (:DatabaseName {name: $key, namespace: '${TopologyGraphDbmsModel.DEFAULT_NAMESPACE}',
                    primary: false})-[:TARGETS]->(database:Database)
                RETURN database
            }
            """
        } else {
            "MATCH (database:Database {name: $key})"
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
        val name = admin.getDatabaseNameFields("databaseName", databaseName, false)
        return updating(
            if (online) "StartDatabase" else "StopDatabase",
            """
            ${database(parameter(name.displayNameKey()))}
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

    private fun ensureValidNonSystemDatabase(
        plan: EnsureValidNonSystemDatabase,
        context: AdministrationCommandRuntimeContext,
    ): ExecutionPlan {
        if (plan.aliasName().isDefined)
            return ensureAliasTarget(plan, plan.aliasName().get(), context)
        val databaseName = plan.databaseName()
        val name = admin.getDatabaseNameFields("databaseName", databaseName, false)
        return updating(
            "EnsureValidNonSystemDatabase",
            "${database(parameter(name.displayNameKey()))} RETURN database.name AS name",
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
                                    ErrorGqlStatusObjectImplementation.from(
                                            GqlStatusInfoCodes.STATUS_51N69
                                        )
                                        .withParam(GqlParams.StringParam.operation, plan.command())
                                        .build(),
                                    "Not allowed to ${plan.action()} the system database.",
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

    /** That the database [target], given as [name], cannot be dropped while [aliases] exist. */
    private fun aliasesExist(
        name: String,
        target: String,
        aliases: List<String>,
    ): DatabaseAliasExistsException {
        val listed = aliases.joinToString(", ") { "'$it'" }
        return DatabaseAliasExistsException(
            ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N82)
                .withParam(GqlParams.StringParam.db1, target)
                .withParam(GqlParams.StringParam.db2, listed)
                .build(),
            "Failed to delete the specified database '$name': Database '$target' has aliases " +
                "$listed. Drop them before the database, or drop the database with CASCADE ALIASES.",
        )
    }

    /**
     * Checks that the target of the alias [aliasName] is a database other than `system`: an alias
     * cannot target another alias.
     */
    private fun ensureAliasTarget(
        plan: EnsureValidNonSystemDatabase,
        aliasName: DatabaseName,
        context: AdministrationCommandRuntimeContext,
    ): ExecutionPlan {
        val target = plan.databaseName()
        val name = admin.getDatabaseNameFields("targetName", target, false)
        val key = parameter(name.displayNameKey())
        return updating(
            "EnsureValidAliasTarget",
            """
            OPTIONAL MATCH (database:Database {name: $key})
            OPTIONAL MATCH (alias:DatabaseName {name: $key, namespace: ${'$'}namespace,
                primary: false})
            RETURN CASE
                WHEN database.name = '${GraphDatabaseSettings.SYSTEM_DATABASE_NAME}' THEN 'system'
                WHEN database IS NOT NULL THEN 'database'
                WHEN alias IS NOT NULL THEN 'alias'
                ELSE 'none'
            END AS target
            """
                .trimIndent(),
            VirtualValues.map(
                name.keys() + "namespace",
                name.values() + Values.utf8Value(TopologyGraphDbmsModel.DEFAULT_NAMESPACE),
            ),
            QueryHandler.handleResult(
                    Function3 { _, value, params ->
                        val failed =
                            "Failed to ${plan.action()} the specified database alias " +
                                "'${show(aliasName, params)}'"
                        val targetName = show(target, params)
                        when ((value as TextValue).stringValue()) {
                            "database" -> `Continue$`.`MODULE$`
                            "system" ->
                                ThrowException(
                                    Gql.refused(
                                        "$failed: Target database name '$targetName' is " +
                                            "invalid, due to the prefix 'system'."
                                    )
                                )
                            "alias" ->
                                ThrowException(
                                    Gql.refused(
                                        "$failed: '$targetName' is an alias, and aliases can " +
                                            "only target databases."
                                    )
                                )
                            else ->
                                ThrowException(
                                    DatabaseNotFoundHelper.failedActionAlias(
                                        plan.action(),
                                        show(aliasName, params),
                                        targetName,
                                        parameterName(target),
                                    )
                                )
                        }
                    }
                )
                .handleError(errors(plan.command(), plan.action(), target)),
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
            authorizationLog,
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

    /**
     * Fails with [message], that DozenDB does not support [feature], once the [source] checks, such
     * as privileges, passed.
     */
    private fun failure(
        source: ExecutionPlan,
        feature: String,
        message: (MapValue) -> String,
    ): ExecutionPlan =
        PredicateExecutionPlan.apply(
            Function2 { _, _ -> false },
            Option.apply(source),
            Function3 { params, _, _ ->
                InvalidArgumentException(notSupported(feature), message(params))
            },
        )

    private fun notFound(operation: String, databaseName: DatabaseName) =
        Function1<MapValue, Option<QueryHandlerResult>> { params ->
            Option.apply(
                ThrowException(
                    DatabaseNotFoundHelper.failedAction(
                        operation,
                        show(databaseName, params),
                        parameterName(databaseName),
                    )
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

    /** The parameter that gives [name], if a parameter does. */
    private fun parameterName(name: DatabaseName): String? =
        (name as? ParameterName)?.parameter()?.name()

    private fun show(name: Either<String, Parameter>, params: MapValue): String =
        nameOf(name, params)

    private fun transformer(): ParameterTransformer =
        ParameterTransformer(
            ParameterTransformer.`$lessinit$greater$default$1`(),
            ParameterTransformer.`$lessinit$greater$default$2`(),
        )

    private fun parameter(key: String) = "${'$'}`$key`"

    private fun <T> Option<T>.valueOrNull(): T? = if (isDefined) get() else null

    private fun <T> seqOf(vararg items: T): Seq<T> =
        CollectionConverters.asScala(items.toList()).toSeq()

    private companion object {
        const val CREATE_DATABASE = "create database"

        /** The parameter of the seed URI the `Database` node keeps. */
        const val SEED_URI = "__dozendb_seedURI"

        /** The parameter of the servers a clustered database is created on first: one. */
        const val SEEDING_SERVERS = "__dozendb_seedingServers"

        /** The parameter of the store format a new database's store is created in. */
        const val STORE_FORMAT = "__dozendb_storeFormat"

        /** The parameter of the servers a new database is placed on, by id. */
        const val HOSTS = "__dozendb_hosts"

        /** The parameter of the number of primaries a new database requests. */
        const val PRIMARIES = "__dozendb_primaries"
        const val SECONDARIES = "__dozendb_secondaries"

        /** The most primaries a topology may ask for, as in Neo4j Enterprise. */
        const val MAX_PRIMARIES = 11
        const val MAX_SECONDARIES = 20

        val SUPPORTED_OPTIONS =
            listOf(
                `ExistingDataOption$`.`MODULE$`.KEY(),
                `SeedURIOption$`.`MODULE$`.KEY(),
                `ExistingSeedServerOption$`.`MODULE$`.KEY(),
                `ExistingSeedInstanceOption$`.`MODULE$`.KEY(),
                `StoreFormatOption$`.`MODULE$`.KEY(),
            )

        /**
         * The options of Neo4j Enterprise's S3SeedProvider, refused with a message of their own.
         */
        val SEED_CREDENTIALS = listOf("seedConfig", "seedCredentials")
    }
}
