package org.dozendb.core.cypher

import java.util.UUID
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
import org.neo4j.cypher.internal.logical.plans.EnableServer
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ShowServers
import org.neo4j.cypher.internal.procs.ParameterTransformer
import org.neo4j.cypher.internal.procs.QueryHandler
import org.neo4j.cypher.internal.procs.SystemCommandExecutionPlan
import org.neo4j.cypher.internal.procs.UpdatingSystemCommandExecutionPlan
import org.neo4j.dbms.database.ServerDetails
import org.neo4j.dbms.database.TopologyInfoService
import org.neo4j.dbms.systemgraph.InstanceModeConstraint
import org.neo4j.exceptions.DatabaseAdministrationOnFollowerException
import org.neo4j.exceptions.InvalidArgumentException
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
 * given and, for those left out, the ones its `initial.server.*` settings set. The commands that
 * change records take the records' lock first and check under it, so that they apply one after the
 * other.
 *
 * Every enabled server hosts every database as a primary, so the options that would make it host
 * fewer, or as a secondary, are refused for now.
 */
@Suppress("DEPRECATION") // Neo4j 5.26 has no GQL status for most refusals of server commands.
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

    private val admin = `AdministrationCommandRuntime$`.`MODULE$`

    override fun isDefinedAt(plan: LogicalPlan): Boolean = planner(plan) != null

    override fun apply(plan: LogicalPlan): Planner =
        planner(plan) ?: throw IllegalArgumentException("Not a server command: $plan")

    private fun planner(plan: LogicalPlan): Planner? =
        when (plan) {
            is ShowServers -> Planner { showServers(plan, compile(plan.source(), it)) }
            is EnableServer -> Planner { enable(plan, it) }
            else -> null
        }

    private fun showServers(plan: ShowServers, source: ExecutionPlan): ExecutionPlan {
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
            SystemCommandExecutionPlan.`apply$default$7`(),
            servers,
            SystemCommandExecutionPlan.`apply$default$9`(),
        )
    }

    /**
     * ENABLE SERVER: records a Free server with its options. An enabled one keeps its options if
     * those given are the same, and the command fails if they are not: ALTER SERVER changes them.
     */
    private fun enable(
        plan: EnableServer,
        context: AdministrationCommandRuntimeContext,
    ): ExecutionPlan {
        val version = context.runtimeContext().cypherVersion()
        val failure = { params: MapValue ->
            "Could not enable server '${admin.runtimeStringValue(plan.serverName(), params)}'"
        }
        val change =
            changing("ENABLE SERVER", failure) { tx, params ->
                val name = admin.runtimeStringValue(plan.serverName(), params)
                val failed = failure(params)
                val given = evaluate(plan.options(), params, version, failed)
                ServerRecords.lock(tx)
                val records = ServerRecords.records(tx)
                val id =
                    (records.values.firstOrNull { it.name == name }?.id
                        ?: uuid(name)?.takeIf { it in servers.ids() || it in records })
                        ?: throw InvalidArgumentException(
                            "$failed: there is no such server. SHOW SERVERS lists the servers."
                        )
                val record = records[id]
                if (record != null || !ServerRecords.initialServersEnabled(tx)) {
                    val current = record?.options ?: servers.initialOptions(id) ?: ServerOptions()
                    if (ServerOptionsInput.parse(given, current, failed) != current) {
                        throw InvalidArgumentException(
                            "$failed: it is enabled already, with other options. ALTER SERVER " +
                                "changes them."
                        )
                    }
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
                    checkPlaceable(options, failed)
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
     * Refuses [options] that would make a server host fewer databases than every one, or host them
     * other than as a primary: every enabled server hosts every database as a primary.
     */
    private fun checkPlaceable(options: ServerOptions, failure: String) {
        if (options.modeConstraint == InstanceModeConstraint.SECONDARY) {
            throw InvalidArgumentException(
                "$failure: 'modeConstraint' 'SECONDARY' is not supported yet. Every enabled " +
                    "server hosts every database as a primary."
            )
        }
        if (options.allowedDatabases.isNotEmpty() || options.deniedDatabases.isNotEmpty()) {
            throw InvalidArgumentException(
                "$failure: 'allowedDatabases' and 'deniedDatabases' are not supported yet. " +
                    "Every enabled server hosts every database."
            )
        }
    }

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
                    ?: throw InvalidArgumentException("$failure: OPTIONS is not a map.")
            else -> MapValue.EMPTY
        }

    private fun uuid(text: String): UUID? = runCatching { UUID.fromString(text) }.getOrNull()

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
        const val SYSTEM = "system"

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
