package dev.dodeca.core.cypher

import dev.dodeca.core.graphtype.AlterGraphTypeCommand
import org.neo4j.cypher.internal.`CommunitySchemaCommandRuntime$`
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.SchemaCommandRuntime
import org.neo4j.cypher.internal.logical.plans.AlterCurrentGraphType
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.procs.SchemaExecutionPlan
import org.neo4j.cypher.internal.procs.SchemaExecutionResult
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.values.virtual.MapValue
import scala.Function1
import scala.Function2
import scala.Option
import scala.PartialFunction
import scala.runtime.AbstractPartialFunction

/**
 * Neo4j Community's runtime of schema commands, with ALTER CURRENT GRAPH TYPE, which Community
 * leaves to Enterprise (see [AlterGraphTypeCommand]). Neo4j's compiler asks it which plans are
 * schema commands, which run in schema transactions.
 */
internal object DodecaSchemaCommandRuntime : SchemaCommandRuntime {
    private val commands: PartialFunction<LogicalPlan, Function1<RuntimeContext, ExecutionPlan>> =
        `CommunitySchemaCommandRuntime$`.`MODULE$`
            .logicalToExecutable()
            .orElse<LogicalPlan, Function1<RuntimeContext, ExecutionPlan>>(GraphTypeCommands)

    override fun logicalToExecutable():
        PartialFunction<LogicalPlan, Function1<RuntimeContext, ExecutionPlan>> = commands

    /** Whether [plan] is a schema command that Community has no runtime for. */
    fun handles(plan: LogicalPlan): Boolean = GraphTypeCommands.isDefinedAt(plan)

    private object GraphTypeCommands :
        AbstractPartialFunction<LogicalPlan, Function1<RuntimeContext, ExecutionPlan>>() {
        override fun isDefinedAt(plan: LogicalPlan): Boolean = plan is AlterCurrentGraphType

        override fun <A1 : LogicalPlan, B1> applyOrElse(
            plan: A1,
            default: Function1<A1, B1>,
        ): B1 {
            if (plan !is AlterCurrentGraphType) return default.apply(plan)
            val executable =
                Function1<RuntimeContext, ExecutionPlan> { context ->
                    val command = AlterGraphTypeCommand(plan.graphType(), context.cypherVersion())
                    SchemaExecutionPlan(
                        "AlterCurrentGraphType",
                        Function2<QueryContext, MapValue, SchemaExecutionResult> { ctx, params ->
                            command.run(ctx, params)
                        },
                        Option.empty(),
                    )
                }
            @Suppress("UNCHECKED_CAST")
            return executable as B1
        }
    }
}
