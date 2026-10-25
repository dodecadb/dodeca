package org.dozendb.core.cypher

import org.neo4j.common.DependencyResolver
import org.neo4j.cypher.internal.AdministrationCommandRuntimeContext
import org.neo4j.cypher.internal.AdministrationShowCommandUtils
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ShowServers
import org.neo4j.cypher.internal.procs.ParameterTransformer
import org.neo4j.cypher.internal.procs.SystemCommandExecutionPlan
import org.neo4j.dbms.database.ServerDetails
import org.neo4j.dbms.database.TopologyInfoService
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues
import scala.Option
import scala.PartialFunction
import scala.jdk.javaapi.CollectionConverters

/**
 * The server commands of Neo4j Enterprise. SHOW SERVERS lists the servers that the
 * [TopologyInfoService] knows, with Enterprise's columns: a cluster's members, or this server alone
 * without clustering.
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

    override fun isDefinedAt(plan: LogicalPlan): Boolean = planner(plan) != null

    override fun apply(plan: LogicalPlan): Planner =
        planner(plan) ?: throw IllegalArgumentException("Not a server command: $plan")

    private fun planner(plan: LogicalPlan): Planner? =
        when (plan) {
            is ShowServers -> Planner { showServers(plan, compile(plan.source(), it)) }
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
