package dev.dodeca.core.cypher

import dev.dodeca.core.edition.DatabaseProgress
import dev.dodeca.core.edition.ServerProgress
import dev.dodeca.core.edition.WantedDatabase
import dev.dodeca.core.servers.ServerRecords
import java.time.Duration
import java.util.UUID
import org.neo4j.cypher.internal.`AdministrationCommandRuntime$`
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.ast.DatabaseName
import org.neo4j.cypher.internal.notification.InternalNotification
import org.neo4j.cypher.internal.notification.WaitServerCatchingUp
import org.neo4j.cypher.internal.notification.WaitServerCaughtUp
import org.neo4j.cypher.internal.notification.WaitServerFailed
import org.neo4j.cypher.internal.notification.WaitServerUnavailable
import org.neo4j.cypher.internal.procs.AdministrationChainedExecutionPlan
import org.neo4j.cypher.internal.procs.SystemCommandExecutionPlan
import org.neo4j.cypher.internal.procs.SystemUpdateCountingQueryContext
import org.neo4j.cypher.internal.runtime.ExecutionMode
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel
import org.neo4j.graphdb.Direction
import org.neo4j.graphdb.Transaction
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues
import scala.Option
import scala.collection.immutable.Set

/**
 * WAIT: commits the transaction in which [source] changed the system graph, as Neo4j Enterprise
 * does, then waits until every server hosting databases did what the system graph now asks of the
 * database [databaseName], failed to, or [timeout] passed. Returns a row per server with its
 * `address`, `state` and `message`, and `success`, whether every server caught up; from Cypher 25
 * on ([version]) also a notification per server, as Enterprise's Cypher 25. A local alias's name
 * stands for its database, as it was before [source] ran, which may drop the alias.
 */
internal class WaitExecutionPlan(
    source: ExecutionPlan,
    private val databaseName: DatabaseName,
    private val timeout: Duration,
    private val progress: () -> DatabaseProgress,
    private val engine: ExecutionEngine,
    private val authorization: SecurityAuthorizationHandler,
    private val version: CypherVersion,
) : AdministrationChainedExecutionPlan(Option.apply(source)) {
    override fun run(
        ctx: QueryContext,
        executionMode: ExecutionMode,
        params: MapValue,
        prePopulateResults: Boolean,
        ignore: InputDataStream,
        subscriber: QuerySubscriber,
    ): RuntimeResult {
        val name =
            NormalizedDatabaseName(
                    `AdministrationCommandRuntime$`.`MODULE$`.runtimeStringValue(
                        databaseName,
                        params,
                    )
                )
                .name()
        val transaction = createContext(ctx).kernelTransactionalContext().transaction()
        return super.run(
            ctx,
            executionMode,
            params.updatedWith(TARGET, Values.utf8Value(target(transaction, name))),
            prePopulateResults,
            ignore,
            subscriber,
        )
    }

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
        val name = (params.get(TARGET) as TextValue).stringValue()
        val servers =
            progress().await(wanted(transaction.transaction(), name), timeout) {
                // Throws once the client stops waiting; the command itself was committed.
                transaction.kernelTransaction().assertOpen()
            }
        val success = servers.all { it.state == ServerProgress.State.CaughtUp }
        val notifications =
            if (version == CypherVersion.Cypher5) previousNotifications
            else {
                val names =
                    ServerRecords.records(transaction.transaction()).mapValues { it.value.name }
                notifications(servers, names).fold(previousNotifications) { all, notification ->
                    all.incl(notification)
                }
            }
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
                SystemCommandExecutionPlan.`$lessinit$greater$default$7`(),
                SystemCommandExecutionPlan.`$lessinit$greater$default$8`(),
                SystemCommandExecutionPlan.`$lessinit$greater$default$9`(),
                version,
            )
            .runSpecific(
                ctx,
                executionMode,
                params,
                prePopulateResults,
                subscriber,
                notifications,
            )
    }

    /** The database of the local alias [name], or [name] itself. */
    private fun target(transaction: Transaction, name: String): String =
        transaction
            .findNodes(
                TopologyGraphDbmsModel.DATABASE_NAME_LABEL,
                TopologyGraphDbmsModel.NAME_PROPERTY,
                name,
            )
            .use { nodes ->
                nodes
                    .asSequence()
                    .filter {
                        it.getProperty(TopologyGraphDbmsModel.NAMESPACE_PROPERTY, null) ==
                            TopologyGraphDbmsModel.DEFAULT_NAMESPACE &&
                            it.getProperty(TopologyGraphDbmsModel.PRIMARY_PROPERTY, true) == false
                    }
                    .mapNotNull {
                        it.getSingleRelationship(
                                TopologyGraphDbmsModel.TARGETS_RELATIONSHIP,
                                Direction.OUTGOING,
                            )
                            ?.endNode
                            ?.getProperty(TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY) as String?
                    }
                    .firstOrNull()
            } ?: name

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

    internal companion object {
        /**
         * The notifications of Neo4j Enterprise's Cypher 25 for [servers], each named as [names]
         * says, or by its id: caught up, failed, still catching up when the time limit passed, or
         * not available, as a server that could not be asked.
         */
        fun notifications(
            servers: List<ServerProgress>,
            names: Map<UUID, String>,
        ): List<InternalNotification> = servers.map { server ->
            val name = names[server.server] ?: server.server.toString()
            when (server.state) {
                ServerProgress.State.CaughtUp -> WaitServerCaughtUp(name, server.address)
                ServerProgress.State.Failed ->
                    WaitServerFailed(name, server.address, server.message)
                ServerProgress.State.TimedOut ->
                    if (server.available) WaitServerCatchingUp(name, server.address)
                    else WaitServerUnavailable(name)
            }
        }

        /** The parameter holding the rows, named so as not to clash with the user's. */
        private const val ROWS = "__internal_waitRows"

        /** The parameter holding the name of the database waited for. */
        private const val TARGET = "__dodeca_waitTarget"
    }
}
