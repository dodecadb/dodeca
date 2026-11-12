package org.dozendb.core.cypher

import java.util.UUID
import org.dozendb.core.Gql
import org.dozendb.core.servers.AllocationException
import org.dozendb.core.servers.DatabasePlacement
import org.dozendb.core.servers.Move
import org.dozendb.core.servers.ServerLifecycle
import org.dozendb.core.servers.ServerOptions
import org.dozendb.core.servers.ServerOptionsInput
import org.dozendb.core.servers.ServerRecord
import org.dozendb.core.servers.ServerRecords
import org.dozendb.core.servers.Servers
import org.neo4j.common.DependencyResolver
import org.neo4j.cypher.internal.`AdministrationCommandRuntime$`
import org.neo4j.cypher.internal.AdministrationCommandRuntimeContext
import org.neo4j.cypher.internal.AdministrationShowCommandUtils
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.OptionsParam
import org.neo4j.cypher.internal.evaluator.Evaluator
import org.neo4j.cypher.internal.logical.plans.AlterServer
import org.neo4j.cypher.internal.logical.plans.DeallocateServer
import org.neo4j.cypher.internal.logical.plans.DropServer
import org.neo4j.cypher.internal.logical.plans.EnableServer
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ReallocateDatabases
import org.neo4j.cypher.internal.logical.plans.RenameServer
import org.neo4j.cypher.internal.logical.plans.ShowServers
import org.neo4j.cypher.internal.procs.ParameterTransformer
import org.neo4j.cypher.internal.procs.QueryHandler
import org.neo4j.cypher.internal.procs.SystemCommandExecutionPlan
import org.neo4j.cypher.internal.procs.UpdatingSystemCommandExecutionPlan
import org.neo4j.dbms.database.ServerDetails
import org.neo4j.dbms.database.TopologyInfoService
import org.neo4j.exceptions.DatabaseAdministrationOnFollowerException
import org.neo4j.graphdb.Transaction
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.MapValueBuilder
import org.neo4j.values.virtual.VirtualValues
import scala.Function2
import scala.Option
import scala.PartialFunction
import scala.jdk.javaapi.CollectionConverters

/**
 * The server commands of Neo4j Enterprise. SHOW SERVERS lists the servers that the
 * [TopologyInfoService] knows, with Enterprise's columns: a cluster's members, or this server alone
 * without clustering. ENABLE SERVER records a Free server (see [ServerRecords]), with the options
 * given and, for those left out, the ones its `initial.server.*` settings set; ALTER SERVER
 * replaces the options of an enabled one, RENAME SERVER its name. A server is named by its name or
 * its id. The commands that change records take the records' lock first and check under it, so that
 * they apply one after the other.
 *
 * ALTER SERVER moves the copies of databases that the server's new options no longer let it host to
 * other servers (see [DatabasePlacement.conform]); it fails, changing nothing, if one has nowhere
 * to go.
 */
internal class ServerCommands(
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

    private val topology by lazy {
        dependencies.resolveDependency(TopologyInfoService::class.java)
    }

    private val servers by lazy { dependencies.resolveDependency(Servers::class.java) }

    private val placement by lazy { dependencies.resolveDependency(DatabasePlacement::class.java) }

    private val admin = `AdministrationCommandRuntime$`.`MODULE$`

    override fun isDefinedAt(plan: LogicalPlan): Boolean = planner(plan) != null

    override fun apply(plan: LogicalPlan): Planner =
        planner(plan) ?: throw IllegalArgumentException("Not a server command: $plan")

    private fun planner(plan: LogicalPlan): Planner? =
        when (plan) {
            is ShowServers -> Planner { showServers(plan, compile(plan.source(), it), version(it)) }
            is EnableServer -> Planner { enable(plan, it) }
            is AlterServer -> Planner { alter(plan, it) }
            is RenameServer -> Planner { rename(plan, it) }
            is DeallocateServer -> Planner { deallocate(plan, it) }
            is ReallocateDatabases -> Planner { reallocate(plan, it) }
            is DropServer -> Planner { drop(plan, it) }
            else -> null
        }

    private fun showServers(
        plan: ShowServers,
        source: ExecutionPlan,
        version: CypherVersion,
    ): ExecutionPlan {
        val servers = ParameterTransformer.apply { tx, _, _ ->
            val rows = topology.servers(tx).map(::row)
            VirtualValues.map(arrayOf(SERVERS_KEY), arrayOf(VirtualValues.fromList(rows)))
        }
        return SystemCommandExecutionPlan(
            "ShowServers",
            engine,
            authorization,
            """
            UNWIND ${'$'}`$SERVERS_KEY` AS server
            WITH ${COLUMNS.joinToString { "server.$it AS $it" }}
            ${AdministrationShowCommandUtils.generateReturnClause(plan.returnColumns(), plan.yields(), plan.returns(), seqOf("name"))}
            """
                .trimIndent(),
            VirtualValues.EMPTY_MAP,
            Option.apply(source),
            SystemCommandExecutionPlan.`$lessinit$greater$default$7`(),
            servers,
            SystemCommandExecutionPlan.`$lessinit$greater$default$9`(),
            version,
        )
    }

    /**
     * ENABLE SERVER: records a Free server with its options. An enabled one keeps its options if
     * those given are the same, and the command fails if they are not: ALTER SERVER changes them; a
     * cordoned one is enabled again.
     */
    private fun enable(
        plan: EnableServer,
        context: AdministrationCommandRuntimeContext,
    ): ExecutionPlan {
        val version = context.runtimeContext().cypherVersion()
        val failure = { params: MapValue ->
            "Could not enable server '${nameOf(plan.serverName(), params)}'"
        }
        val change =
            changing("ENABLE SERVER", failure) { tx, params ->
                val name = nameOf(plan.serverName(), params)
                val failed = failure(params)
                val given = evaluate(plan.options(), params, version, failed)
                ServerRecords.lock(tx)
                val server = target(tx, name, failed)
                val id = server.id
                if (server.enabled) {
                    if (ServerOptionsInput.parse(given, server.options, failed) != server.options) {
                        throw Gql.refused(
                            "$failed: it is enabled already, with other options. ALTER SERVER " +
                                "changes them."
                        )
                    }
                    ServerLifecycle.uncordon(tx, server, failed)
                } else {
                    val options =
                        ServerOptionsInput.parse(
                            given,
                            servers.initialOptions(id),
                            failed,
                            noDefaults =
                                ", and the server does not answer, so its initial.server.* " +
                                    "settings are not known: give every option, or start it first",
                        )
                    ServerRecords.write(tx, ServerRecord(id, options = options))
                }
            }
        return updating(
            "EnableServer",
            "ENABLE SERVER",
            change,
            compile(plan.source(), context),
            failure,
        )
    }

    /**
     * ALTER SERVER: replaces every option of an enabled server with those given, and moves the
     * copies they no longer allow.
     */
    private fun alter(
        plan: AlterServer,
        context: AdministrationCommandRuntimeContext,
    ): ExecutionPlan {
        val version = context.runtimeContext().cypherVersion()
        val failure = { params: MapValue ->
            "Could not alter server '${nameOf(plan.serverName(), params)}'"
        }
        val change =
            changing("ALTER SERVER", failure) { tx, params ->
                val failed = failure(params)
                val given = evaluate(plan.options(), params, version, failed)
                ServerRecords.lock(tx)
                val name = nameOf(plan.serverName(), params)
                val server = enabled(target(tx, name, failed), failed)
                val options = ServerOptionsInput.parse(given, ServerOptions(), failed)
                ServerRecords.write(
                    tx,
                    ServerRecord(server.id, server.name, options, server.status),
                )
                try {
                    placement.conform(tx)
                } catch (e: AllocationException) {
                    throw Gql.refused("$failed: ${e.message}.")
                }
            }
        return updating(
            "AlterServer",
            "ALTER SERVER",
            change,
            compile(plan.source(), context),
            failure,
        )
    }

    /**
     * [DRYRUN] DEALLOCATE DATABASES FROM SERVER[S]: the servers become deallocating and their
     * copies move to other servers (see [ServerLifecycle.deallocate]); DRYRUN shows the moves, and
     * changes nothing.
     */
    private fun deallocate(
        plan: DeallocateServer,
        context: AdministrationCommandRuntimeContext,
    ): ExecutionPlan {
        val names = { params: MapValue ->
            CollectionConverters.asJava(plan.serverNames()).map {
                nameOf(it, params)
            }
        }
        val failure = { params: MapValue ->
            val given = names(params)
            "Could not deallocate databases from server${if (given.size == 1) "" else "s"} " +
                given.joinToString { "'$it'" }
        }
        val source = compile(plan.source(), context)
        if (plan.dryRun()) {
            return moves("DeallocateServer", source, version(context)) { tx, params ->
                ServerLifecycle.deallocate(
                    tx,
                    names(params),
                    servers,
                    placement,
                    dryRun = true,
                    failure(params),
                )
            }
        }
        val change =
            changing("DEALLOCATE DATABASES FROM SERVER", failure) { tx, params ->
                ServerRecords.lock(tx)
                ServerLifecycle.deallocate(
                    tx,
                    names(params),
                    servers,
                    placement,
                    dryRun = false,
                    failure(params),
                )
            }
        return updating(
            "DeallocateServer",
            "DEALLOCATE DATABASES FROM SERVER",
            change,
            source,
            failure,
        )
    }

    /**
     * REALLOCATE DATABASES: moves copies to even out how many each server hosts (see
     * [ServerLifecycle.reallocate]); DRYRUN shows the moves, and changes nothing.
     */
    private fun reallocate(
        plan: ReallocateDatabases,
        context: AdministrationCommandRuntimeContext,
    ): ExecutionPlan {
        val failed = "Could not reallocate databases"
        val failure = { _: MapValue -> failed }
        val source = compile(plan.source(), context)
        if (plan.dryRun()) {
            return moves("ReallocateDatabases", source, version(context)) { tx, _ ->
                ServerLifecycle.reallocate(tx, placement, null, null, dryRun = true, failed)
            }
        }
        val change =
            changing("REALLOCATE DATABASES", failure) { tx, _ ->
                ServerRecords.lock(tx)
                ServerLifecycle.reallocate(tx, placement, null, null, dryRun = false, failed)
            }
        return updating("ReallocateDatabases", "REALLOCATE DATABASES", change, source, failure)
    }

    /**
     * DROP SERVER: a Free or Deallocated server leaves the cluster (see [ServerLifecycle.drop]).
     */
    private fun drop(
        plan: DropServer,
        context: AdministrationCommandRuntimeContext,
    ): ExecutionPlan {
        val name = { params: MapValue -> nameOf(plan.serverName(), params) }
        val failure = { params: MapValue -> "Could not drop server '${name(params)}'" }
        val change =
            changing("DROP SERVER", failure) { tx, params ->
                ServerRecords.lock(tx)
                ServerLifecycle.drop(tx, name(params), servers, failure(params))
            }
        return updating(
            "DropServer",
            "DROP SERVER",
            change,
            compile(plan.source(), context),
            failure,
        )
    }

    /**
     * A DRYRUN command: the [moves] it would make, a row each, in Neo4j Enterprise's columns of
     * DRYRUN DEALLOCATE and REALLOCATE.
     */
    private fun moves(
        name: String,
        source: ExecutionPlan,
        version: CypherVersion,
        moves: (Transaction, MapValue) -> List<Move>,
    ): ExecutionPlan {
        val rows = ParameterTransformer.apply { tx, _, params ->
            val records = ServerRecords.records(tx)
            val serverName = { id: UUID -> records[id]?.name ?: id.toString() }
            val rows =
                moves(tx, params).map { move ->
                    VirtualValues.map(
                        MOVE_COLUMNS.toTypedArray(),
                        arrayOf<AnyValue>(
                            Values.stringValue(move.database),
                            Values.stringValue(serverName(move.from)),
                            Values.stringValue(move.from.toString()),
                            Values.stringValue(serverName(move.to)),
                            Values.stringValue(move.to.toString()),
                            Values.stringValue(PRIMARY),
                        ),
                    )
                }
            VirtualValues.map(arrayOf(MOVES_KEY), arrayOf(VirtualValues.fromList(rows)))
        }
        return SystemCommandExecutionPlan(
            name,
            engine,
            authorization,
            """
            UNWIND ${'$'}`$MOVES_KEY` AS move
            RETURN ${MOVE_COLUMNS.joinToString { "move.$it AS $it" }}
            """
                .trimIndent(),
            VirtualValues.EMPTY_MAP,
            Option.apply(source),
            SystemCommandExecutionPlan.`$lessinit$greater$default$7`(),
            rows,
            SystemCommandExecutionPlan.`$lessinit$greater$default$9`(),
            version,
        )
    }

    /**
     * RENAME SERVER: gives an enabled server a name that is neither empty nor another server's name
     * or id; the server keeps its id.
     */
    private fun rename(
        plan: RenameServer,
        context: AdministrationCommandRuntimeContext,
    ): ExecutionPlan {
        val failure = { params: MapValue ->
            "Could not rename server '${nameOf(plan.serverName(), params)}'"
        }
        val change =
            changing("RENAME SERVER", failure) { tx, params ->
                val failed = failure(params)
                val newName = nameOf(plan.newName(), params)
                ServerRecords.lock(tx)
                val name = nameOf(plan.serverName(), params)
                val server = enabled(target(tx, name, failed), failed)
                if (newName == server.name) return@changing
                if (newName.isBlank()) {
                    throw Gql.refused("$failed: the new name is empty.")
                }
                val records = ServerRecords.records(tx)
                records.values
                    .firstOrNull { it.name == newName && it.id != server.id }
                    ?.let {
                        throw Gql.refused("$failed: server ${it.id} has the name '$newName'.")
                    }
                uuid(newName)
                    ?.takeIf { it != server.id && (it in servers.ids() || it in records) }
                    ?.let {
                        throw Gql.refused("$failed: '$newName' is the id of another server.")
                    }
                ServerRecords.write(
                    tx,
                    ServerRecord(server.id, newName, server.options, server.status),
                )
            }
        return updating(
            "RenameServer",
            "RENAME SERVER",
            change,
            compile(plan.source(), context),
            failure,
        )
    }

    /** The server called [name], by its name or its id. */
    private fun target(tx: Transaction, name: String, failed: String): ServerLifecycle.Target =
        ServerLifecycle.target(tx, name, servers, failed)

    private fun enabled(server: ServerLifecycle.Target, failed: String) =
        ServerLifecycle.enabled(server, failed)

    /**
     * The [change] of a server [command] in its system transaction. On a member that does not lead
     * `system` it fails as the other administration commands do there.
     */
    private fun changing(
        command: String,
        failure: (MapValue) -> String,
        change: (Transaction, MapValue) -> Unit,
    ): ParameterTransformer = ParameterTransformer.apply { tx, _, params ->
        try {
            change(tx, params)
        } catch (e: RuntimeException) {
            if (!notALeader(e)) throw e
            throw DatabaseAdministrationOnFollowerException.notALeader(
                command,
                failure(params),
                e,
            )
        }
        params
    }

    /** A command whose [change] runs in its system transaction, before an empty query. */
    private fun updating(
        name: String,
        command: String,
        change: ParameterTransformer,
        source: ExecutionPlan,
        failure: (MapValue) -> String,
    ): ExecutionPlan =
        UpdatingSystemCommandExecutionPlan(
            name,
            engine,
            authorization,
            "RETURN 1 AS changed",
            VirtualValues.EMPTY_MAP,
            QueryHandler.handleError(
                Function2<Throwable, MapValue, Throwable> { error, params ->
                    if (notALeader(error)) {
                        DatabaseAdministrationOnFollowerException.notALeader(
                            command,
                            failure(params),
                            error,
                        )
                    } else {
                        error
                    }
                }
            ),
            Option.apply(source),
            UpdatingSystemCommandExecutionPlan.`$lessinit$greater$default$8`(),
            UpdatingSystemCommandExecutionPlan.`$lessinit$greater$default$9`(),
            change,
            UpdatingSystemCommandExecutionPlan.`$lessinit$greater$default$11`(),
        )

    private fun notALeader(error: Throwable): Boolean =
        generateSequence(error) { it.cause }
            .any {
                it is Status.HasStatus && it.status() == Status.Cluster.NotALeader
            }

    private fun evaluate(
        options: Options,
        params: MapValue,
        version: CypherVersion,
        failure: String,
    ): MapValue =
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
                    ?: throw Gql.refused("$failure: OPTIONS is not a map.")
            else -> MapValue.EMPTY
        }

    private fun uuid(text: String): UUID? = ServerLifecycle.uuid(text)

    /** A row of SHOW SERVERS; databases `system` first, then by name. */
    private fun row(server: ServerDetails): MapValue {
        val values =
            listOf<AnyValue>(
                Values.stringValue(server.serverId().uuid().toString()),
                Values.stringValue(server.name()),
                address(server.boltAddress().orElse(null)),
                address(server.httpAddress().orElse(null)),
                address(server.httpsAddress().orElse(null)),
                Values.stringValue(server.state().prettyPrint()),
                Values.stringValue(server.runningState().prettyPrint()),
                strings(databases(server.hostedDatabases())),
                strings(databases(server.desiredDatabases())),
                strings(server.tags().toList()),
                strings(server.allowedDatabases().toList()),
                strings(server.deniedDatabases().toList()),
                Values.stringValue(server.modeConstraint().name),
                server
                    .neo4jVersion()
                    .map<AnyValue> { Values.stringValue(it) }
                    .orElse(Values.NO_VALUE),
            )
        return VirtualValues.map(COLUMNS.toTypedArray(), values.toTypedArray())
    }

    private fun address(address: Any?): AnyValue =
        address?.let { Values.stringValue(it.toString()) } ?: Values.NO_VALUE

    private fun databases(names: Set<String>): List<String> =
        names.sortedWith(compareBy<String> { it != SYSTEM }.thenBy { it })

    private fun strings(values: List<String>): AnyValue =
        VirtualValues.fromList(values.map { Values.stringValue(it) })

    private fun seqOf(vararg names: String) = CollectionConverters.asScala(names.toList()).toSeq()

    private companion object {
        const val SERVERS_KEY = "__dozendb_servers"
        const val MOVES_KEY = "__dozendb_moves"
        const val SYSTEM = "system"

        /** The `mode` of a copy moved by DEALLOCATE or REALLOCATE: every copy is a primary. */
        const val PRIMARY = "primary"

        /** Neo4j Enterprise's columns of DRYRUN DEALLOCATE and DRYRUN REALLOCATE, in its order. */
        val MOVE_COLUMNS =
            listOf(
                "database",
                "fromServerName",
                "fromServerId",
                "toServerName",
                "toServerId",
                "mode",
            )

        /** Neo4j Enterprise's columns of SHOW SERVERS YIELD *, in its order. */
        val COLUMNS =
            listOf(
                "serverId",
                "name",
                "address",
                "httpAddress",
                "httpsAddress",
                "state",
                "health",
                "hosting",
                "requestedHosting",
                "tags",
                "allowedDatabases",
                "deniedDatabases",
                "modeConstraint",
                "version",
            )
    }
}
