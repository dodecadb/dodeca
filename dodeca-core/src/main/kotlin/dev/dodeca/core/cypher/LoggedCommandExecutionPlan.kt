package dev.dodeca.core.cypher

import dev.dodeca.core.logging.SecurityLog
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.notification.InternalNotification
import org.neo4j.cypher.internal.plandescription.Argument
import org.neo4j.cypher.internal.procs.IgnoredRuntimeResult
import org.neo4j.cypher.internal.procs.SystemUpdateCountingQueryContext
import org.neo4j.cypher.internal.runtime.ExecutionMode
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.kernel.api.query.RuntimeName
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.virtual.MapValue
import scala.collection.immutable.Seq
import scala.collection.immutable.Set
import scala.collection.immutable.`Set$`
import scala.jdk.javaapi.CollectionConverters

/**
 * An administration command that Neo4j's planner asks to log, as its `LogSystemCommand` plan, with
 * the command's text, passwords hidden: [source], then the security log's line once its transaction
 * commits. Like Neo4j's own logging plan, a command that does nothing (IF NOT EXISTS) is not
 * logged.
 */
internal class LoggedCommandExecutionPlan(
    private val source: ExecutionPlan,
    private val command: String,
    private val log: SecurityLog,
) : ExecutionPlan() {
    override fun run(
        queryContext: QueryContext,
        executionMode: ExecutionMode,
        params: MapValue,
        prePopulateResults: Boolean,
        input: InputDataStream,
        subscriber: QuerySubscriber,
    ): RuntimeResult {
        // Neo4j's way to the kernel's context, as its system commands find it.
        val transactional =
            SystemUpdateCountingQueryContext.from(queryContext).kernelTransactionalContext()
        val result =
            source.run(queryContext, executionMode, params, prePopulateResults, input, subscriber)
        if (result !is IgnoredRuntimeResult) {
            log.command(transactional.kernelTransaction(), transactional.securityContext(), command)
        }
        return result
    }

    override fun runtimeName(): RuntimeName = RuntimeName.SYSTEM

    override fun metadata(): Seq<Argument> =
        CollectionConverters.asScala(emptyList<Argument>()).toSeq()

    override fun notifications(): Set<InternalNotification> = `Set$`.`MODULE$`.empty()
}
