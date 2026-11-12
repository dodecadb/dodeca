package org.dozendb.core.remote

import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.ExecutionException
import java.util.function.Consumer
import org.neo4j.bolt.connection.AccessMode
import org.neo4j.bolt.connection.BasicResponseHandler
import org.neo4j.bolt.connection.BoltConnection
import org.neo4j.bolt.connection.NotificationClassification
import org.neo4j.bolt.connection.NotificationConfig
import org.neo4j.bolt.connection.NotificationSeverity
import org.neo4j.bolt.connection.TransactionType
import org.neo4j.bolt.connection.exception.BoltFailureException
import org.neo4j.bolt.connection.message.Message
import org.neo4j.bolt.connection.message.Messages
import org.neo4j.bolt.connection.values.Value
import org.neo4j.cypher.internal.DefaultQueryLanguageScope
import org.neo4j.cypher.internal.frontend.notification.NotificationWrapping
import org.neo4j.cypher.internal.notification.InternalNotification
import org.neo4j.fabric.bookmark.RemoteBookmark
import org.neo4j.fabric.bookmark.TransactionBookmarkManager
import org.neo4j.fabric.executor.Location
import org.neo4j.fabric.executor.QueryStatementLifecycles
import org.neo4j.graphdb.ExecutionPlanDescription
import org.neo4j.graphdb.GqlStatusObject
import org.neo4j.graphdb.Notification
import org.neo4j.graphdb.QueryExecutionType
import org.neo4j.graphdb.TransactionTerminatedException
import org.neo4j.graphdb.TransactionTerminatedHelper
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.query.ExtendedQueryStatistics
import org.neo4j.kernel.impl.query.NotificationConfiguration
import org.neo4j.kernel.impl.query.QueryExecution
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.memory.HeapHighWaterMarkTracker
import org.neo4j.router.query.Query
import org.neo4j.router.transaction.DatabaseTransaction
import org.neo4j.router.transaction.TransactionInfo
import org.neo4j.values.AnyValue
import scala.Option

/**
 * A transaction of the database at [location] that runs on another server over Bolt, acting as
 * [impersonated] if not null. Its connection, logged in as the transaction's user there, comes from
 * [connect] with its first query, which BEGIN goes with; an auto-commit transaction runs its one
 * query as an auto-commit transaction there, as `CALL { } IN TRANSACTIONS` needs. Results stream as
 * the client demands them, a batch at a time. Results, notifications and plans reach the client as
 * the server sent them; [failures] says what the client learns of a failure. [terminate] may come
 * from any thread.
 *
 * Neo4j's query router drives it from one thread at a time, but for [terminate] and [abandon].
 */
class RemoteTransaction(
    private val location: Location.Remote,
    private val info: TransactionInfo,
    private val connect: () -> CompletionStage<BoltConnection>,
    private val impersonated: String?,
    private val bookmarks: Set<String>,
    private val bookmarkManager: TransactionBookmarkManager,
    private val terminationCallback: Consumer<Status>,
    private val failures: RemoteFailures,
    private val ended: (RemoteTransaction) -> Unit,
) : DatabaseTransaction {
    private val implicit = info.type() == KernelTransaction.Type.IMPLICIT
    private val notificationConfig = notificationConfig(info)
    private val accessMode =
        if (info.accessMode() == org.neo4j.boltmessages.AccessMode.READ) {
            AccessMode.READ
        } else {
            AccessMode.WRITE
        }
    /** The default Cypher version of the transaction's queries, which the router sets. */
    private val languageScope = DefaultQueryLanguageScope.create()
    @Volatile private var connection: BoltConnection? = null
    private var begun = false
    private var lastBookmark: String? = null

    /** The server reported a failure: it rolled the transaction back. */
    private var failed = false
    @Volatile private var open = true
    @Volatile private var committing = false
    @Volatile private var terminated: Status? = null
    @Volatile private var inFlight: CompletableFuture<BasicResponseHandler.Summaries>? = null

    /** The database, by its name on the server. */
    private val database: String = location.databaseName

    override fun location(): Location = location

    override fun isOpen(): Boolean = open

    override fun defaultQueryLanguageScope(): DefaultQueryLanguageScope = languageScope

    override fun executeQuery(
        query: Query,
        querySubscriber: QuerySubscriber,
        statementLifecycle: QueryStatementLifecycles.StatementLifecycle,
        routerNotifications: Set<InternalNotification>,
    ): QueryExecution {
        check(open) { "the transaction is closed" }
        check(!failed) { "the transaction failed on the server" }
        statementLifecycle.startExecution(true)
        // The server plans and runs the query: here, it is running from now on.
        statementLifecycle.monitoredQuery.onCompilationCompleted(null, null, null, 0)
        statementLifecycle.monitoredQuery.onExecutionStarted(HeapHighWaterMarkTracker.NONE) {
            ExtendedQueryStatistics.EMPTY
        }
        val parameters = HashMap<String, Value>()
        query.parameters().foreach<RuntimeException> { key, value ->
            parameters[key] = BoltValue(value)
        }
        val messages = mutableListOf<Message>()
        val timeout = info.txTimeout()?.takeIf { !it.isZero && !it.isNegative }
        if (implicit) {
            messages +=
                Messages.run(
                    database,
                    accessMode,
                    impersonated,
                    bookmarks,
                    query.text(),
                    parameters,
                    timeout,
                    metadata(),
                    notificationConfig,
                )
        } else {
            if (!begun) {
                messages +=
                    Messages.beginTransaction(
                        database,
                        accessMode,
                        impersonated,
                        bookmarks,
                        TransactionType.DEFAULT,
                        timeout,
                        metadata(),
                        notificationConfig,
                    )
                begun = true
            }
            messages += Messages.run(query.text(), parameters)
        }
        // The first batch comes with the query, as drivers ask for it.
        messages += Messages.pull(-1, FETCH_SIZE)
        // The router ends the statement if this fails.
        val summaries = exchange(messages)
        return RemoteExecution(
            summaries.runSummary().queryId(),
            summaries.runSummary().keys(),
            summaries.valuesList(),
            summaries.pullSummary(),
            querySubscriber,
            statementLifecycle,
            routerNotifications,
        )
    }

    override fun commit() {
        terminated?.let { throw terminatedException(it) }
        try {
            if (!implicit && begun) {
                committing = true
                lastBookmark =
                    exchange(listOf(Messages.commit())).commitSummary().bookmark().orElse(null)
            }
            lastBookmark?.let {
                bookmarkManager.remoteTransactionCommitted(location, RemoteBookmark(it))
            }
        } finally {
            end()
        }
    }

    override fun rollback() {
        try {
            if (!implicit && begun && !failed && terminated == null) {
                // The server rolls back without it too, once the connection is reset.
                runCatching { exchange(listOf(Messages.rollback())) }
            }
        } finally {
            end()
        }
    }

    override fun close() {
        // Returned to the pool, the connection is reset: the server rolls back what is open.
        end()
    }

    override fun terminate(reason: Status) {
        synchronized(this) {
            if (!open || terminated != null) return
            terminated = reason
        }
        inFlight?.completeExceptionally(terminatedException(reason))
        val connection = connection ?: return
        // A RESET terminates the transaction on the server; closing the connection makes sure it
        // does, also if the RESET goes unanswered.
        connection.writeAndFlush(BasicResponseHandler(), Messages.reset(), null).whenComplete { _, _
            ->
            connection.forceClose("the remote transaction was terminated")
        }
    }

    /**
     * Gives up on the server, which can no longer serve the transaction: a commit it did not answer
     * yet may or may not have been applied; anything else was not, and the router terminates the
     * transaction for [reason], which the client may retry elsewhere.
     */
    fun abandon(why: String, reason: Status) {
        if (!open || terminated != null) return
        if (committing) {
            inFlight?.completeExceptionally(
                TransactionFailure(failures.outcomeUnknown(database, IllegalStateException(why)))
            )
            connection?.forceClose(why)
        } else {
            terminationCallback.accept(reason)
        }
    }

    private fun end() {
        synchronized(this) {
            if (!open) return
            open = false
        }
        ended(this)
        val connection = connection ?: return
        this.connection = null
        if (terminated == null) connection.close()
    }

    private fun connection(): BoltConnection =
        connection
            ?: try {
                connect().toCompletableFuture().get().also { connection = it }
            } catch (e: ExecutionException) {
                throw failure(e.cause ?: e, sent = false)
            }

    /** Sends [messages] and waits for their responses; [terminate] ends the wait. */
    private fun exchange(messages: List<Message>): BasicResponseHandler.Summaries {
        terminated?.let { throw terminatedException(it) }
        val connection = connection()
        val handler = BasicResponseHandler()
        val response = CompletableFuture<BasicResponseHandler.Summaries>()
        // Whether the server may have read the messages, which it did not if they were not sent.
        var sent = false
        inFlight = response
        connection
            .writeAndFlush(handler, messages, null)
            .thenCompose {
                sent = true
                handler.summaries()
            }
            .whenComplete { summaries, error ->
                if (error == null) response.complete(summaries)
                else response.completeExceptionally(error)
            }
        terminated?.let { response.completeExceptionally(terminatedException(it)) }
        try {
            return response.get()
        } catch (e: ExecutionException) {
            throw failure(e.cause ?: e, sent)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            terminate(Status.Transaction.Terminated)
            throw terminatedException(Status.Transaction.Terminated)
        } finally {
            inFlight = null
        }
    }

    /**
     * What the client learns of [error], which a response or the connection gave, after the
     * messages were [sent] or not.
     */
    private fun failure(error: Throwable, sent: Boolean): RuntimeException {
        val cause = (error as? java.util.concurrent.CompletionException)?.cause ?: error
        return when {
            cause is TransactionTerminatedException -> cause
            cause is TransactionFailure -> cause.failure
            cause is BoltFailureException -> {
                failed = true
                failures.answered(database, cause)
            }
            committing && sent -> failures.outcomeUnknown(database, cause)
            else -> failures.unreachable(database, cause)
        }
    }

    private fun metadata(): Map<String, Value> =
        info.txMetadata().mapValues { (_, value) ->
            BoltValue(value as? AnyValue ?: ValueUtils.of(value))
        }

    /**
     * The results of a query, which arrive in batches of [FETCH_SIZE] as the client demands them:
     * [first] came with the query. [lifecycle] ends with the results, for the query log.
     */
    private inner class RemoteExecution(
        private val queryId: Long,
        private val keys: List<String>,
        first: List<List<Value>>,
        firstSummary: org.neo4j.bolt.connection.summary.PullSummary?,
        private val subscriber: QuerySubscriber,
        private val lifecycle: QueryStatementLifecycles.StatementLifecycle,
        private val routerNotifications: Set<InternalNotification>,
    ) : QueryExecution {
        private val buffer = ArrayDeque(first)
        private var hasMore = firstSummary?.hasMore() ?: true
        private var summary: RemoteSummary? =
            firstSummary?.takeUnless { it.hasMore() }?.let { RemoteSummary(it.metadata()) }
        private var demand = 0L
        private var started = false
        private var ended = false

        override fun request(numberOfRecords: Long) {
            demand =
                if (Long.MAX_VALUE - demand < numberOfRecords) Long.MAX_VALUE
                else demand + numberOfRecords
        }

        override fun await(): Boolean {
            if (ended) return false
            try {
                if (!started) {
                    started = true
                    subscriber.onResult(keys.size)
                }
                while (demand > 0) {
                    val record = buffer.removeFirstOrNull()
                    if (record == null) {
                        if (!hasMore) break
                        pull()
                        continue
                    }
                    subscriber.onRecord()
                    record.forEachIndexed { i, value ->
                        subscriber.onField(i, BoltValues.anyValue(value))
                    }
                    subscriber.onRecordCompleted()
                    demand--
                }
                if (buffer.isEmpty() && !hasMore) {
                    ended = true
                    val summary = checkNotNull(summary) { "the server sent no summary" }
                    if (implicit) lastBookmark = summary.bookmark
                    subscriber.onResultCompleted(summary.statistics)
                    lifecycle.endSuccess()
                }
            } catch (e: Exception) {
                ended = true
                buffer.clear()
                hasMore = false
                QuerySubscriber.safelyOnError(subscriber, e)
                lifecycle.endFailure(e)
            }
            return !ended
        }

        private fun pull() {
            val summaries = exchange(listOf(Messages.pull(queryId, FETCH_SIZE)))
            buffer.addAll(summaries.valuesList())
            hasMore = summaries.pullSummary().hasMore()
            if (!hasMore) summary = RemoteSummary(summaries.pullSummary().metadata())
        }

        override fun cancel() {
            if (ended) return
            ended = true
            buffer.clear()
            if (hasMore && open && !failed && terminated == null) {
                hasMore = false
                runCatching { exchange(listOf(Messages.discard(queryId, -1))) }
            }
            lifecycle.endSuccess()
        }

        override fun executionMetadataAvailable(): Boolean = summary != null

        override fun executionType(): QueryExecutionType =
            summary?.executionType
                ?: QueryExecutionType.query(QueryExecutionType.QueryType.READ_WRITE)

        override fun executionPlanDescription(): ExecutionPlanDescription? = summary?.plan

        @Suppress("DEPRECATION") // Neo4j 5's notifications, for clients of Bolt before 5.6.
        override fun getNotifications(): Iterable<Notification> =
            summary?.notifications.orEmpty() + routerNotifications()

        override fun getGqlStatusObjects(): Iterable<GqlStatusObject> =
            summary?.statuses.orEmpty() + routerNotifications()

        override fun fieldNames(): Array<String> = keys.toTypedArray()

        /** What the router noticed in the query here, as for local queries. */
        private fun routerNotifications() = routerNotifications.map {
            NotificationWrapping.asKernelNotificationJava(Option.empty(), it)
        }
    }

    /** A failure decided here, for a response that did not come. */
    private class TransactionFailure(val failure: RuntimeException) : RuntimeException(failure)

    companion object {
        /** The records fetched at a time, as drivers do by default. */
        const val FETCH_SIZE = 1000L

        /** As Neo4j's own transactions report terminations. */
        fun terminatedException(reason: Status): TransactionTerminatedException =
            TransactionTerminatedHelper.transactionTerminated(reason)

        /** The client's notification filters, which the server applies. */
        private fun notificationConfig(info: TransactionInfo): NotificationConfig {
            val filters = info.queryExecutionConfiguration()?.notificationFilters()
            if (filters == null || filters == NotificationConfiguration.DEFAULT_FILTER) {
                return NotificationConfig.defaultConfig()
            }
            val severity =
                when (filters.severityLevel()) {
                    NotificationConfiguration.Severity.INFORMATION ->
                        NotificationSeverity.INFORMATION
                    NotificationConfiguration.Severity.WARNING -> NotificationSeverity.WARNING
                    NotificationConfiguration.Severity.NONE -> NotificationSeverity.OFF
                }
            val disabled =
                filters
                    .disabledCategories()
                    .map {
                        NotificationClassification(NotificationClassification.Type.valueOf(it.name))
                    }
                    .toSet()
            return NotificationConfig(severity, disabled)
        }
    }
}

/** What the client of a [RemoteTransaction] learns when it fails. */
interface RemoteFailures {
    /** The server of [database] answered with [failure], also when it refused the login. */
    fun answered(database: String, failure: BoltFailureException): RuntimeException

    /** The server of [database] could not be reached, or the connection failed. */
    fun unreachable(database: String, cause: Throwable): RuntimeException

    /** The server of [database] did not answer a commit, which may or may not be applied. */
    fun outcomeUnknown(database: String, cause: Throwable): RuntimeException
}
