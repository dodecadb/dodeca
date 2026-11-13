package dev.dodeca.core.cypher

import dev.dodeca.core.security.PrivilegeText
import dev.dodeca.core.security.SecurityGraph
import dev.dodeca.core.security.SupportedPrivileges
import org.neo4j.common.DependencyResolver
import org.neo4j.cypher.internal.`AdministrationCommandRuntime$`
import org.neo4j.cypher.internal.AdministrationCommandRuntimeContext
import org.neo4j.cypher.internal.AdministrationShowCommandUtils
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ShowAllPrivileges
import org.neo4j.cypher.internal.ast.ShowAuthRulesPrivileges
import org.neo4j.cypher.internal.ast.ShowPrivilegeScope
import org.neo4j.cypher.internal.ast.ShowRolesPrivileges
import org.neo4j.cypher.internal.ast.ShowUserPrivileges
import org.neo4j.cypher.internal.ast.ShowUsersPrivileges
import org.neo4j.cypher.internal.ast.Yield
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.PrivilegePlan
import org.neo4j.cypher.internal.logical.plans.ShowPrivilegeCommands as ShowPrivilegeCommandsPlan
import org.neo4j.cypher.internal.logical.plans.ShowPrivileges
import org.neo4j.cypher.internal.logical.plans.ShowSupportedPrivileges
import org.neo4j.cypher.internal.procs.ParameterTransformer
import org.neo4j.cypher.internal.procs.SystemCommandExecutionPlan
import org.neo4j.graphdb.Transaction
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.logging.internal.LogService
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues
import scala.Function3
import scala.Option
import scala.PartialFunction
import scala.collection.immutable.List as ScalaList
import scala.collection.immutable.Seq
import scala.jdk.javaapi.CollectionConverters

/**
 * SHOW PRIVILEGES, with AS COMMANDS or AS REVOKE COMMANDS, of all, roles, users or auth rules, and
 * SHOW SUPPORTED PRIVILEGES, which Neo4j Community plans but refuses to run. The rows come from one
 * read of the system graph, in [PrivilegeText]'s formats, and pass through Cypher, so that YIELD,
 * WHERE and RETURN work as on Neo4j's other SHOW commands.
 */
internal class ShowPrivilegeCommands(
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
    private val log by lazy {
        dependencies
            .resolveDependency(LogService::class.java)
            .getInternalLog(SecurityGraph::class.java)
    }

    private val admin = `AdministrationCommandRuntime$`.`MODULE$`

    override fun isDefinedAt(plan: LogicalPlan): Boolean = planner(plan) != null

    override fun apply(plan: LogicalPlan): Planner =
        planner(plan) ?: throw IllegalArgumentException("Not a SHOW PRIVILEGES command: $plan")

    private fun planner(plan: LogicalPlan): Planner? =
        when (plan) {
            is ShowPrivileges ->
                Planner {
                    privileges(
                        plan.scope(),
                        plan.returnColumns(),
                        plan.yields(),
                        plan.returns(),
                        source(plan.source(), it),
                        it.runtimeContext().cypherVersion(),
                    )
                }
            is ShowPrivilegeCommandsPlan ->
                Planner {
                    commands(
                        plan.scope(),
                        plan.asRevoke(),
                        plan.returnColumns(),
                        plan.yields(),
                        plan.returns(),
                        source(plan.source(), it),
                        it.runtimeContext().cypherVersion(),
                    )
                }
            is ShowSupportedPrivileges ->
                Planner {
                    supported(
                        plan.returnColumns(),
                        plan.yields(),
                        plan.returns(),
                        it.runtimeContext().cypherVersion(),
                    )
                }
            else -> null
        }

    private fun source(
        plan: Option<PrivilegePlan>,
        context: AdministrationCommandRuntimeContext,
    ): ExecutionPlan? = if (plan.isDefined) compile(plan.get(), context) else null

    /**
     * A privilege of a role, of a user's or auth rule's ([holder]) for scopes of users and auth
     * rules.
     */
    private data class Held(val role: String, val holder: String?)

    /** Rows of the columns SHOW PRIVILEGES shows, in the order of Neo4j's docs. */
    private fun privileges(
        scope: ShowPrivilegeScope,
        columns: ScalaList<LogicalVariable>,
        yields: Option<Yield>,
        returns: Option<Return>,
        source: ExecutionPlan?,
        version: CypherVersion,
    ): ExecutionPlan {
        val holder = holderColumn(scope)
        val fields =
            listOf("access", "action", "resource", "graph", "segment", "role", "immutable") +
                listOfNotNull(holder)
        val order = listOf("role", "segment", "action", "graph", "resource", "access")
        return show(
            "ShowPrivileges",
            fields,
            columns,
            yields,
            returns,
            listOfNotNull(holder) + order,
            source,
            version,
        ) { graph, securityContext, params ->
            holders(scope, graph, securityContext, params).flatMap { held ->
                graph.roles[held.role]?.privileges.orEmpty().mapNotNull { privilege ->
                    PrivilegeText.row(privilege, graph::databaseName)?.let { row ->
                        map(
                            fields,
                            listOf(
                                Values.utf8Value(row.access),
                                Values.utf8Value(row.action),
                                Values.utf8Value(row.resource),
                                Values.utf8Value(row.graph),
                                Values.utf8Value(row.segment),
                                Values.utf8Value(held.role),
                                Values.booleanValue(row.immutable),
                            ) + listOfNotNull(held.holder?.let(Values::utf8Value)),
                        )
                    }
                }
            }
        }
    }

    /**
     * The commands of SHOW PRIVILEGES AS [REVOKE] COMMANDS, each once, with the roles that hold the
     * privilege and, for users and auth rules, the users or rules; for those, with `$role` for the
     * role, as Neo4j's docs show them. Cypher 5 shows the command and whether it is immutable only.
     */
    private fun commands(
        scope: ShowPrivilegeScope,
        revoke: Boolean,
        columns: ScalaList<LogicalVariable>,
        yields: Option<Yield>,
        returns: Option<Return>,
        source: ExecutionPlan?,
        version: CypherVersion,
    ): ExecutionPlan {
        val holder = holderColumn(scope)?.let { "${it}s" }
        val fields =
            listOf("command", "access", "action", "roles", "immutable") + listOfNotNull(holder)
        return show(
            "ShowPrivilegeCommands",
            fields,
            columns,
            yields,
            returns,
            listOf("command"),
            source,
            version,
        ) { graph, securityContext, params ->
            holders(scope, graph, securityContext, params)
                .flatMap { held ->
                    graph.roles[held.role]?.privileges.orEmpty().mapNotNull { privilege ->
                        val command =
                            PrivilegeText.command(
                                privilege,
                                if (holder != null) null else held.role,
                                revoke,
                                graph::databaseName,
                                version,
                            ) ?: return@mapNotNull null
                        command to (privilege to held)
                    }
                }
                .groupBy({ it.first }, { it.second })
                .map { (command, holders) ->
                    val row = PrivilegeText.row(holders.first().first, graph::databaseName)
                    map(
                        fields,
                        listOf(
                            Values.utf8Value(command.command),
                            row?.access?.let(Values::utf8Value) ?: Values.NO_VALUE,
                            row?.action?.let(Values::utf8Value) ?: Values.NO_VALUE,
                            strings(holders.map { it.second.role }),
                            Values.booleanValue(command.immutable),
                        ) +
                            if (holder != null) {
                                listOf(strings(holders.mapNotNull { it.second.holder }))
                            } else {
                                emptyList()
                            },
                    )
                }
        }
    }

    private fun strings(values: List<String>): AnyValue =
        VirtualValues.fromList(values.distinct().sorted().map(Values::utf8Value))

    private fun supported(
        columns: ScalaList<LogicalVariable>,
        yields: Option<Yield>,
        returns: Option<Return>,
        version: CypherVersion,
    ): ExecutionPlan {
        val fields = listOf("action", "qualifier", "target", "scope", "description")
        return show(
            "ShowSupportedPrivileges",
            fields,
            columns,
            yields,
            returns,
            listOf("action", "target"),
            null,
            version,
        ) { _, _, _ ->
            SupportedPrivileges.ALL.map {
                map(
                    fields,
                    listOf(
                        Values.utf8Value(it.action),
                        it.qualifier?.let(Values::utf8Value) ?: Values.NO_VALUE,
                        Values.utf8Value(it.target),
                        it.scope?.let { scope ->
                            VirtualValues.fromList(scope.map(Values::utf8Value))
                        } ?: Values.NO_VALUE,
                        Values.utf8Value(it.description),
                    ),
                )
            }
        }
    }

    /** The column of who holds the privileges of [scope]: users, auth rules, or none. */
    private fun holderColumn(scope: ShowPrivilegeScope): String? =
        when (scope) {
            is ShowUserPrivileges,
            is ShowUsersPrivileges -> "user"
            is ShowAuthRulesPrivileges -> "authRule"
            else -> null
        }

    /**
     * The roles whose privileges [scope] shows: all, the named ones, those of the named users,
     * PUBLIC included, or of the current user, or those the named auth rules give. A suspended user
     * has none. The roles that auth rules give users are not theirs, as SHOW USERS has it.
     */
    private fun holders(
        scope: ShowPrivilegeScope,
        graph: SecurityGraph,
        securityContext: SecurityContext,
        params: MapValue,
    ): List<Held> =
        when (scope) {
            is ShowAllPrivileges -> graph.roles.keys.map { Held(it, null) }
            is ShowRolesPrivileges ->
                names(scope.roles(), params).filter { it in graph.roles }.map { Held(it, null) }
            is ShowUserPrivileges -> {
                val user =
                    if (scope.user().isDefined) names(listOf(scope.user().get()), params).single()
                    else securityContext.subject().executingUser()
                graph.roles(user).map { Held(it, user) }
            }
            is ShowUsersPrivileges ->
                names(scope.users(), params).distinct().flatMap { user ->
                    graph.roles(user).map { Held(it, user) }
                }
            is ShowAuthRulesPrivileges ->
                names(scope.authRules(), params).distinct().flatMap { name ->
                    graph.authRules
                        .filter { it.name == name }
                        .flatMap { rule -> rule.roles.map { Held(it, name) } }
                }
            else -> throw IllegalArgumentException("Unknown scope: $scope")
        }

    private fun names(expressions: ScalaList<Expression>, params: MapValue): List<String> =
        names(CollectionConverters.asJava(expressions), params)

    private fun names(expressions: List<Expression>, params: MapValue): List<String> =
        expressions.map {
            admin.runtimeStringValue(it, params, false)
        }

    /**
     * A SHOW command whose [rows], of [fields], are made when it runs from a read of the system
     * graph, in [order] unless the query orders them. They come in that order to Cypher too, so
     * that rows an ORDER BY of the query finds equal keep it, as in Neo4j's docs.
     */
    private fun show(
        name: String,
        fields: List<String>,
        columns: ScalaList<LogicalVariable>,
        yields: Option<Yield>,
        returns: Option<Return>,
        order: List<String>,
        source: ExecutionPlan?,
        version: CypherVersion,
        rows: (SecurityGraph, SecurityContext, MapValue) -> List<MapValue>,
    ): ExecutionPlan =
        SystemCommandExecutionPlan(
            name,
            engine,
            authorization,
            """
            UNWIND ${'$'}`$ROWS` AS row
            WITH ${fields.joinToString { "row.$it AS $it" }}
            ${AdministrationShowCommandUtils.generateReturnClause(columns, yields, returns, seqOf(order))}
            """
                .trimIndent(),
            VirtualValues.EMPTY_MAP,
            Option.apply(source),
            SystemCommandExecutionPlan.`$lessinit$greater$default$7`(),
            ParameterTransformer.apply(
                Function3<Transaction, SecurityContext, MapValue, MapValue> {
                    tx,
                    securityContext,
                    params ->
                    val graph = SecurityGraph.read(tx, log)
                    val sorted = rows(graph, securityContext, params).sortedWith(ordering(order))
                    VirtualValues.map(
                        arrayOf(ROWS),
                        arrayOf(VirtualValues.list(*sorted.toTypedArray())),
                    )
                }
            ),
            SystemCommandExecutionPlan.`$lessinit$greater$default$9`(),
            version,
        )

    /** Rows by the text of [fields], one after the other; rows without one first. */
    private fun ordering(fields: List<String>): Comparator<MapValue> =
        fields
            .map { field ->
                compareBy<MapValue, String?>(nullsFirst()) {
                    (it.get(field) as? TextValue)?.stringValue()
                }
            }
            .reduce(Comparator<MapValue>::thenComparing)

    private fun map(fields: List<String>, values: List<AnyValue>): MapValue =
        VirtualValues.map(fields.toTypedArray(), values.toTypedArray())

    private fun seqOf(items: List<String>): Seq<String> =
        CollectionConverters.asScala(items).toSeq()

    private companion object {
        /** The parameter of the rows that a command shows. */
        const val ROWS = "__dodeca_rows"
    }
}
