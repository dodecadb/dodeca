package dev.dodeca.core.cypher

import org.neo4j.common.DependencyResolver
import org.neo4j.cypher.internal.`AdministrationCommandRuntime$`
import org.neo4j.cypher.internal.AdministrationCommandRuntimeContext
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.notification.InternalNotification
import org.neo4j.cypher.internal.procs.NotifyAndContinue
import org.neo4j.cypher.internal.procs.ParameterTransformer
import org.neo4j.cypher.internal.procs.QueryHandler
import org.neo4j.cypher.internal.procs.QueryHandlerResult
import org.neo4j.cypher.internal.procs.UpdatingSystemCommandExecutionPlan
import org.neo4j.exceptions.DatabaseAdministrationOnFollowerException
import org.neo4j.graphdb.Transaction
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.virtual.MapValue
import scala.Function1
import scala.Function2
import scala.Option
import scala.PartialFunction
import scala.Tuple2
import scala.collection.immutable.Seq
import scala.collection.immutable.Set
import scala.collection.immutable.`Set$`
import scala.jdk.javaapi.CollectionConverters
import scala.util.Either

/**
 * Commands on the security model of the system graph that Neo4j Community plans but refuses to run,
 * each a [Planner] of the plans it knows ([planner]), run as Cypher on `system`.
 */
internal abstract class SecurityCommands(
    protected val engine: ExecutionEngine,
    private val dependencies: DependencyResolver,
) : PartialFunction<LogicalPlan, Planner> {
    /** Compiles a nested plan with all commands, set by the runtime that owns these commands. */
    lateinit var compile: (LogicalPlan, AdministrationCommandRuntimeContext) -> ExecutionPlan

    // Resolved on first use, like Community's runtime does.
    protected val authorization by lazy {
        SecurityAuthorizationHandler(
            dependencies.resolveDependency(AbstractSecurityLog::class.java)
        )
    }

    protected val admin = `AdministrationCommandRuntime$`.`MODULE$`

    /** The [Planner] of [plan], or null if these commands do not plan it. */
    protected abstract fun planner(plan: LogicalPlan): Planner?

    override fun isDefinedAt(plan: LogicalPlan): Boolean = planner(plan) != null

    override fun apply(plan: LogicalPlan): Planner =
        planner(plan)
            ?: throw IllegalArgumentException("Not a ${javaClass.simpleName} command: $plan")

    protected fun updating(
        name: String,
        query: String,
        parameters: MapValue,
        handler: QueryHandler,
        source: ExecutionPlan?,
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

    /** Converts the parameters of names, then [check]s them before the command runs. */
    protected fun transformer(
        vararg converters: Function2<Transaction, MapValue, MapValue>,
        check: (MapValue) -> Unit,
    ): ParameterTransformer =
        converters
            .fold(
                ParameterTransformer(
                    ParameterTransformer.`$lessinit$greater$default$1`(),
                    ParameterTransformer.`$lessinit$greater$default$2`(),
                )
            ) { transformer, converter ->
                transformer.convert(converter)
            }
            .validate(
                Function2 { _, params ->
                    check(params)
                    Tuple2(params, `Set$`.`MODULE$`.empty<InternalNotification>())
                }
            )

    protected fun notify(notification: InternalNotification): QueryHandlerResult =
        NotifyAndContinue(Set.Set1(notification))

    /** [error] of [command], which failed to [operation]. */
    protected fun failed(command: String, operation: String, error: Throwable): Throwable =
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

    protected fun hasStatus(error: Throwable, status: Status): Boolean =
        generateSequence(error) { it.cause }.any { it is Status.HasStatus && it.status() == status }

    protected fun MapValue.flag(key: String): Boolean = (get(key) as BooleanValue).booleanValue()

    protected fun show(name: Either<String, Parameter>, params: MapValue): String =
        nameOf(name, params)

    protected fun parameter(key: String) = "${'$'}`$key`"

    /** The parameter that gives [name], if a parameter does. */
    protected fun parameterName(name: Either<String, Parameter>): String? =
        admin.getParameterName(name).getOrElse { null }

    protected fun <T> seqOf(vararg items: T): Seq<T> =
        CollectionConverters.asScala(items.toList()).toSeq()

    protected companion object {
        val IDENTITY = Function1<String, String> { it }
    }
}
