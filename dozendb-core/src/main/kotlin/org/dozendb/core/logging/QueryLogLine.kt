package org.dozendb.core.logging

import org.neo4j.configuration.GraphDatabaseSettings.AnnotationDataFormat
import org.neo4j.gqlstatus.ErrorGqlStatusObject
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.query.QuerySnapshot
import org.neo4j.logging.log4j.Neo4jMapMessage
import org.neo4j.values.virtual.MapValue

/** What the query log's settings ask of a line, read when it is written. */
internal data class QueryLogOptions(
    val verbose: Boolean,
    val parameters: Boolean,
    val maxParameterLength: Int,
    val obfuscateLiterals: Boolean,
    val obfuscateErrors: Boolean,
    val plan: Boolean,
    val annotationFormat: AnnotationDataFormat,
)

/** Why a query failed. */
internal class QueryFailure(
    val reason: String?,
    val status: Status?,
    val gql: ErrorGqlStatusObject?,
) {
    /**
     * The reason on one line, passed through [scrub]; with [obfuscate], only the status codes,
     * which hold no data.
     */
    fun text(obfuscate: Boolean, scrub: (String) -> String = { it }): String {
        val codes = listOfNotNull(status?.code()?.serialize(), gql?.gqlStatus()?.let { "($it)" })
        if (obfuscate || reason.isNullOrBlank()) return codes.joinToString(" ").ifEmpty { "failed" }
        return NEWLINES.replace(scrub(reason), " ")
    }

    companion object {
        private val NEWLINES = Regex("\\R+")

        /**
         * Why [failure] happened: its message, and the most specific statuses in its causes, as
         * Neo4j wraps the errors of embedded queries in exceptions with a generic GQL status.
         */
        fun of(failure: Throwable): QueryFailure {
            val chain = generateSequence(failure) { it.cause }.take(10).toList()
            val gql = chain.filterIsInstance<ErrorGqlStatusObject>()
            return QueryFailure(
                failure.message,
                chain.filterIsInstance<Status.HasStatus>().firstOrNull()?.status(),
                gql.firstOrNull { it.gqlStatus() != ErrorGqlStatusObject.DEFAULT_STATUS_CODE }
                    ?: gql.firstOrNull(),
            )
        }
    }
}

/**
 * A line of the query log about a query: its text form follows the example in Neo4j's operations
 * manual, its map the JSON fields Neo4j documents (written by Community's
 * `QueryLogJsonLayout.json`). The format DozenDB defines where Neo4j does not document it is in
 * `doc/COMPATIBILITY.md`.
 */
internal class QueryLogLine
private constructor(
    private val event: Event,
    private val snapshot: QuerySnapshot?,
    private val options: QueryLogOptions,
    private val failure: QueryFailure?,
    private val raw: RawQuery?,
) : Neo4jMapMessage(24) {
    enum class Event(val field: String, val prefix: String) {
        START("start", "Query started: "),
        RAW_START("start", "Query started (raw): "),
        SUCCESS("success", ""),
        FAIL("fail", ""),
    }

    /** What is known of a query before it is parsed, for raw start lines. */
    class RawQuery(
        val id: Long,
        val source: String,
        val database: String,
        val executingUser: String,
        val authenticatedUser: String,
        val text: String,
        val parameters: MapValue,
    )

    private val id: Long = raw?.id ?: snapshot!!.internalQueryId()
    private val text: String?
    private val parameters: String?
    private val annotation: Map<String, Any?>
    private val plan: String?

    init {
        val shown = shownQuery()
        text = shown?.first
        parameters =
            shown
                ?.second
                ?.takeIf { options.parameters }
                ?.let {
                    CypherText.parameters(it, options.maxParameterLength)
                }
        annotation = snapshot?.transactionAnnotationData().orEmpty()
        plan =
            if (options.plan && event != Event.START && event != Event.RAW_START) {
                snapshot?.queryPlanSupplier()?.get()?.toString()
            } else {
                null
            }
        fields()
    }

    /**
     * The text and parameters the log shows. Neo4j obfuscates the text of the queries it parses; a
     * query it could not parse has its string literals masked and its parameters left out, or, with
     * `obfuscate_literals`, nothing shown, as Neo4j documents.
     */
    private fun shownQuery(): Pair<String, MapValue?>? {
        if (raw != null) return raw.text to raw.parameters
        val snapshot = snapshot!!
        val obfuscated = snapshot.obfuscatedQueryText().orElse(null)
        if (obfuscated != null) {
            return obfuscated to snapshot.obfuscatedQueryParameters().orElse(MapValue.EMPTY)
        }
        if (options.obfuscateLiterals) return null
        return QueryText.maskLiterals(snapshot.rawQueryText()) to null
    }

    /** [message] with what it quotes of the query as the log shows it. */
    private fun scrub(message: String): String {
        val rawText = raw?.text ?: snapshot!!.rawQueryText()
        return QueryText.scrub(message, rawText, text ?: QueryText.MASK)
    }

    private fun fields() {
        with("type", "query")
        with("event", event.field)
        if (options.verbose) with("id", id)
        if (snapshot != null) {
            with("elapsedTimeMs", snapshot.elapsedTimeMicros() / 1000)
            with("planning", snapshot.compilationTimeMicros() / 1000)
            snapshot.cpuTimeMicros().ifPresent { with("cpu", it / 1000) }
            with("waiting", snapshot.waitTimeMicros() / 1000)
            with("allocatedBytes", snapshot.allocatedBytes())
            with("pageHits", snapshot.pageHits())
            with("pageFaults", snapshot.pageFaults())
            with("transactionId", snapshot.transactionSequenceNumber())
            with("runtime", snapshot.runtime() ?: "null")
            val hash = if (event == Event.START) 0 else snapshot.executionPlanCacheKeyHash()
            with("executionPlanCacheKeyHash", String.format("%08x", hash))
        }
        with("source", source())
        with("database", database())
        with("executingUser", executingUser())
        with("authenticatedUser", authenticatedUser())
        text?.let { with("query", it) }
        parameters?.let { with("queryParameters", it) }
        with("annotationData", annotationField())
        if (failure != null) {
            with("failureReason", failure.text(options.obfuscateErrors, ::scrub))
            failure.gql?.let { with("errorInfo", errorInfo(it)) }
        }
        plan?.let { with("queryPlan", it) }
    }

    // The documented source format, which Neo4j's security log writes too.
    @Suppress("DEPRECATION")
    private fun source(): String =
        raw?.source ?: snapshot!!.clientConnection().asConnectionDetails()

    private fun database(): String =
        raw?.database ?: snapshot!!.databaseId().map { it.name() }.orElse(NO_DATABASE)

    private fun executingUser(): String = raw?.executingUser ?: snapshot!!.executingUsername()

    private fun authenticatedUser(): String =
        raw?.authenticatedUser ?: snapshot!!.authenticatedUsername()

    private fun annotationField(): Any =
        when (options.annotationFormat) {
            AnnotationDataFormat.CYPHER -> CypherText.map(annotation)
            AnnotationDataFormat.JSON -> annotation
            AnnotationDataFormat.FLAT_JSON -> flatten(annotation)
        }

    private fun errorInfo(gql: ErrorGqlStatusObject): Map<String, Any> = buildMap {
        put("GQLSTATUS", gql.gqlStatus())
        val description = scrub(gql.statusDescription())
        if (!options.obfuscateErrors) {
            put("statusDescription", description)
        } else {
            // The condition, before the message that may hold data.
            description
                .indexOf(". ")
                .takeIf { it > 0 }
                ?.let {
                    put("statusDescription", description.substring(0, it))
                }
        }
        val record = gql.diagnosticRecord()
        record["_classification"]?.let { put("classification", it.toString()) }
        record["_position"]?.let { put("position", it) }
        gql.cause().ifPresent { put("cause", errorInfo(it)) }
    }

    override fun formatAsString(sb: StringBuilder) {
        sb.append(event.prefix)
        if (options.verbose) sb.append("id:").append(id).append(" - ")
        if (snapshot != null) {
            sb.append("transaction id:").append(snapshot.transactionSequenceNumber()).append(" - ")
            sb.append(snapshot.elapsedTimeMicros() / 1000).append(" ms: (planning: ")
            sb.append(snapshot.compilationTimeMicros() / 1000)
            snapshot.cpuTimeMicros().ifPresent { sb.append(", cpu: ").append(it / 1000) }
            sb.append(", waiting: ").append(snapshot.waitTimeMicros() / 1000).append(") - ")
            sb.append(snapshot.allocatedBytes()).append(" B - ")
            sb.append(snapshot.pageHits()).append(" page hits, ")
            sb.append(snapshot.pageFaults()).append(" page faults - ")
        }
        sb.append(source()).append('\t').append(database()).append(" - ")
        val executing = executingUser()
        val authenticated = authenticatedUser()
        if (authenticated.isNotEmpty() && authenticated != executing) {
            sb.append(authenticated).append(':')
        }
        sb.append(executing).append(" - ")
        sb.append(text ?: NOT_PARSED)
        parameters?.let { sb.append(" - ").append(it) }
        sb.append(" - runtime=").append(snapshot?.runtime() ?: "null")
        sb.append(" - ").append(CypherText.map(annotation))
        failure?.let { sb.append(" - ").append(it.text(options.obfuscateErrors, ::scrub)) }
        plan?.let { sb.append(System.lineSeparator()).append(it) }
    }

    companion object {
        /** The database of a query not routed to one, as Neo4j documents. */
        const val NO_DATABASE = "<none>"

        /** The text of a query that could not be parsed and might hold secrets. */
        const val NOT_PARSED = "<not parsed, left out>"

        fun started(snapshot: QuerySnapshot, options: QueryLogOptions) =
            QueryLogLine(Event.START, snapshot, options, null, null)

        fun rawStarted(query: RawQuery, options: QueryLogOptions) =
            QueryLogLine(Event.RAW_START, null, options, null, query)

        fun ended(snapshot: QuerySnapshot, options: QueryLogOptions, failure: QueryFailure?) =
            QueryLogLine(
                if (failure == null) Event.SUCCESS else Event.FAIL,
                snapshot,
                options,
                failure,
                null,
            )

        /** [map] with nested maps' keys joined by dots, as `FLAT_JSON` asks. */
        fun flatten(map: Map<String, Any?>, prefix: String = ""): Map<String, Any?> = buildMap {
            for ((key, value) in map) {
                if (value is Map<*, *>) {
                    @Suppress("UNCHECKED_CAST")
                    putAll(flatten(value as Map<String, Any?>, "$prefix$key."))
                } else {
                    put("$prefix$key", value)
                }
            }
        }
    }
}
