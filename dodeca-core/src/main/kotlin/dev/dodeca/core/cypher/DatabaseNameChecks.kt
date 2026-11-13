package dev.dodeca.core.cypher

import dev.dodeca.core.Gql
import org.neo4j.common.DependencyResolver
import org.neo4j.cypher.internal.`AdministrationCommandRuntime$`
import org.neo4j.cypher.internal.AdministrationCommandRuntimeContext
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.ast.DatabaseName
import org.neo4j.cypher.internal.logical.plans.DatabaseTypeFilter
import org.neo4j.cypher.internal.logical.plans.DoNothingIfDatabaseExists
import org.neo4j.cypher.internal.logical.plans.DoNothingIfDatabaseNotExists
import org.neo4j.cypher.internal.logical.plans.EnsureDatabaseNodeExists
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.procs.ParameterTransformer
import org.neo4j.cypher.internal.procs.QueryHandler
import org.neo4j.cypher.internal.procs.QueryHandlerResult
import org.neo4j.cypher.internal.procs.ThrowException
import org.neo4j.cypher.internal.procs.UpdatingSystemCommandExecutionPlan
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel
import org.neo4j.exceptions.DatabaseAdministrationOnFollowerException
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues
import scala.Function1
import scala.Function2
import scala.Option
import scala.PartialFunction

/**
 * Neo4j's checks whether a database or alias of a name exists, which commands run before they act
 * on it, and which Neo4j Community plans too. These read dotted names as Dodeca does everywhere, as
 * names of their own: Community's would look for a composite database named like the part before
 * the dot, and fail when a remote alias has that name.
 */
internal class DatabaseNameChecks(
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
        planner(plan) ?: throw IllegalArgumentException("Not a database name check: $plan")

    private fun planner(plan: LogicalPlan): Planner? =
        when (plan) {
            // IF NOT EXISTS: the command does nothing if there is one.
            is DoNothingIfDatabaseExists ->
                Planner {
                    check(
                        "DoNothingIfDatabaseExists",
                        plan.name(),
                        filter(plan.databaseTypeFilter()),
                        QueryHandler.ignoreOnResult()
                            .handleError(errors(plan.command(), "create", "database", plan.name())),
                        compile(plan.source(), it),
                    )
                }
            // IF EXISTS: the command does nothing if there is none.
            is DoNothingIfDatabaseNotExists ->
                Planner {
                    check(
                        "DoNothingIfDatabaseNotExists",
                        plan.name(),
                        filter(plan.databaseTypeFilter()),
                        QueryHandler.ignoreNoResult()
                            .handleError(
                                errors(plan.command(), plan.operation(), "database", plan.name())
                            ),
                        compile(plan.source(), it),
                    )
                }
            is EnsureDatabaseNodeExists ->
                Planner {
                    check(
                        "EnsureNodeExists",
                        plan.name(),
                        plan.extraFilter().apply("node"),
                        QueryHandler.handleNoResult(notFound(plan.action(), plan.name()))
                            .handleError(errors(plan.command(), plan.action(), ALIAS, plan.name())),
                        compile(plan.source(), it),
                    )
                }
            else -> null
        }

    /** Runs [handler] on the name node of [name] that [filter], on `node`, keeps; if any. */
    private fun check(
        planName: String,
        name: DatabaseName,
        filter: String,
        handler: QueryHandler,
        source: ExecutionPlan,
    ): ExecutionPlan {
        val fields = admin.getDatabaseNameFields("name", name, false)
        return UpdatingSystemCommandExecutionPlan(
            planName,
            engine,
            authorization,
            """
            MATCH (node:${TopologyGraphDbmsModel.DATABASE_NAME_LABEL.name()} {
                ${TopologyGraphDbmsModel.NAME_PROPERTY}: ${'$'}`${fields.nameKey()}`,
                ${TopologyGraphDbmsModel.NAMESPACE_PROPERTY}: ${'$'}`${fields.namespaceKey()}`})
            $filter
            RETURN node.${TopologyGraphDbmsModel.NAME_PROPERTY} AS name
            """
                .trimIndent(),
            VirtualValues.map(fields.keys(), fields.values()),
            handler,
            Option.apply(source),
            UpdatingSystemCommandExecutionPlan.`$lessinit$greater$default$8`(),
            UpdatingSystemCommandExecutionPlan.`$lessinit$greater$default$9`(),
            ParameterTransformer(
                    ParameterTransformer.`$lessinit$greater$default$1`(),
                    ParameterTransformer.`$lessinit$greater$default$2`(),
                )
                .convert(fields.nameConverter())
                .convert(inRoot(fields.nameKey(), fields.namespaceKey())),
            UpdatingSystemCommandExecutionPlan.`$lessinit$greater$default$11`(),
        )
    }

    /** Which name nodes count, as Neo4j filters them. */
    private fun filter(filter: DatabaseTypeFilter): String =
        when (filter) {
            is DatabaseTypeFilter.`DatabaseOrLocalAlias$` ->
                "WHERE NOT node:${TopologyGraphDbmsModel.REMOTE_DATABASE}"
            is DatabaseTypeFilter.`CompositeDatabase$` ->
                "WHERE EXISTS { (node)-[:${TopologyGraphDbmsModel.TARGETS}]->" +
                    "(:${TopologyGraphDbmsModel.COMPOSITE_DATABASE}) }"
            is DatabaseTypeFilter.`Alias$` ->
                "WHERE NOT node.${TopologyGraphDbmsModel.PRIMARY_PROPERTY}"
            else -> ""
        }

    private fun notFound(action: String, name: DatabaseName) =
        Function1<MapValue, Option<QueryHandlerResult>> { params ->
            Option.apply(
                ThrowException(
                    Gql.refused(
                        "Failed to $action the specified ${ALIAS.lowercase()} " +
                            "'${show(name, params)}': $ALIAS does not exist."
                    )
                )
            )
        }

    /** Neo4j's mapping of errors: a follower's refusal says so; anything else is unexpected. */
    private fun errors(command: String, operation: String, what: String, name: DatabaseName) =
        Function2<Throwable, MapValue, Throwable> { error, params ->
            val failed =
                "Failed to $operation the specified ${what.lowercase()} '${show(name, params)}'"
            if (
                generateSequence(error) { it.cause }
                    .any {
                        it is Status.HasStatus && it.status() == Status.Cluster.NotALeader
                    }
            ) {
                DatabaseAdministrationOnFollowerException.notALeader(command, failed, error)
            } else {
                IllegalStateException("$failed.", error)
            }
        }

    private fun show(name: DatabaseName, params: MapValue): String =
        admin.runtimeStringValue(name, params)

    private companion object {
        val ALIAS: String = TopologyGraphDbmsModel.DATABASE_NAME_LABEL_DESCRIPTION
    }
}
