package org.dozendb.core.cypher

import java.time.Duration
import java.util.UUID
import org.dozendb.core.edition.DatabaseProgress
import org.dozendb.core.edition.ServerProgress
import org.dozendb.core.edition.WantedDatabase
import org.neo4j.cypher.internal.`AdministrationCommandRuntime$`
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.ast.DatabaseName
import org.neo4j.cypher.internal.procs.AdministrationChainedExecutionPlan
import org.neo4j.cypher.internal.procs.SystemCommandExecutionPlan
import org.neo4j.cypher.internal.procs.SystemUpdateCountingQueryContext
import org.neo4j.cypher.internal.runtime.ExecutionMode
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel
import org.neo4j.graphdb.Transaction
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues
import scala.Option
import scala.collection.immutable.Set

/**
 * WAIT: commits the transaction in which [source] changed the system graph, as Neo4j Enterprise
 * does, then waits until every server hosting databases did what the system graph now asks of the
 * database [databaseName], failed to, or [timeout] passed. Returns a row per server with its
 * `address`, `state` and `message`, and `success`, whether every server caught up.
 */
internal class WaitExecutionPlan(
    source: ExecutionPlan,
    private val databaseName: DatabaseName,
    private val timeout: Duration,
    private val progress: () -> DatabaseProgress,
    private val engine: ExecutionEngine,
    private val authorization: SecurityAuthorizationHandler,
) : AdministrationChainedExecutionPlan(Option.apply(source)) {
    override fun runSpecific(
        ctx: SystemUpdateCountingQueryContext,
        executionMode: ExecutionMode,
        params: MapValue,
        prePopulateResults: Boolean,
        subscriber: QuerySubscriber,
        previousNotifications: Set<InternalNotification>,
    ): RuntimeResult {
        val transaction = ctx.kernelTransactionalContext()
        // Later commands of the same transaction run in a new one, as in Neo4j Enterprise.
        transaction.commitAndRestartTx()
        val name =
            NormalizedDatabaseName(
                    `AdministrationCommandRuntime$`.`MODULE$`.runtimeStringValue(
                        databaseName,
                        params,
                    )
                )
                .name()
        val servers =
            progress().await(wanted(transaction.transaction(), name), timeout) {
                // Throws once the client stops waiting; the command itself was committed.
                transaction.kernelTransaction().assertOpen()
            }
        val success = servers.all { it.state == ServerProgress.State.CaughtUp }
        val rows =
            VirtualValues.list(
                *servers
                    .map { server ->
                        VirtualValues.map(
                            arrayOf("address", "state", "message", "success"),
                            arrayOf<AnyValue>(
                                Values.utf8Value(server.address),
                                Values.utf8Value(server.state.name),
                                Values.utf8Value(server.message),
                                Values.booleanValue(success),
                            ),
                        )
                    }
                    .toTypedArray()
            )
        return SystemCommandExecutionPlan(
                "WaitForCompletion",
                engine,
                authorization,
                "UNWIND ${'$'}`$ROWS` AS row " +
                    "RETURN row.address AS address, row.state AS state, " +
                    "row.message AS message, row.success AS success",
                VirtualValues.map(arrayOf(ROWS), arrayOf<AnyValue>(rows)),
                Option.empty(),
                SystemCommandExecutionPlan.`apply$default$7`(),
                SystemCommandExecutionPlan.`apply$default$8`(),
                SystemCommandExecutionPlan.`apply$default$9`(),
            )
            .runSpecific(
                ctx,
                executionMode,
                params,
                prePopulateResults,
                subscriber,
                previousNotifications,
            )
    }

    /** What the committed system graph asks of the database [name]. */
    private fun wanted(transaction: Transaction, name: String): WantedDatabase {
        val database =
            transaction.findNode(
                TopologyGraphDbmsModel.DATABASE_LABEL,
                TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY,
                name,
            ) ?: return WantedDatabase(name, null, false)
        return WantedDatabase(
            name,
            UUID.fromString(
                database.getProperty(TopologyGraphDbmsModel.DATABASE_UUID_PROPERTY) as String
            ),
            database.getProperty(TopologyGraphDbmsModel.DATABASE_STATUS_PROPERTY) ==
                TopologyGraphDbmsModel.DatabaseStatus.ONLINE.statusName(),
        )
    }

    private companion object {
        /** The parameter holding the rows, named so as not to clash with the user's. */
        const val ROWS = "__internal_waitRows"
    }
}
