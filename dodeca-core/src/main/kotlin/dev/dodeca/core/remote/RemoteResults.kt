// Neo4j 5's Notification, which Bolt before 5.5 still sends.
@file:Suppress("DEPRECATION", "OVERRIDE_DEPRECATION")

package dev.dodeca.core.remote

import java.util.Optional
import org.neo4j.bolt.connection.exception.BoltFailureException
import org.neo4j.bolt.connection.exception.BoltGqlErrorException
import org.neo4j.bolt.connection.values.Value
import org.neo4j.bolt.protocol.common.fsm.response.metadata.DefaultMetadataHandler
import org.neo4j.gqlstatus.ErrorGqlStatusObject
import org.neo4j.gqlstatus.GqlRuntimeException
import org.neo4j.gqlstatus.NotificationClassification
import org.neo4j.graphdb.ExecutionPlanDescription
import org.neo4j.graphdb.GqlStatusObject
import org.neo4j.graphdb.InputPosition
import org.neo4j.graphdb.Notification
import org.neo4j.graphdb.NotificationCategory
import org.neo4j.graphdb.QueryExecutionType
import org.neo4j.graphdb.QueryStatistics
import org.neo4j.graphdb.SeverityLevel
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.FloatingPointValue
import org.neo4j.values.storable.IntegralValue
import org.neo4j.values.storable.NoValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.MapValue

/**
 * What another server reports about a query that ran there, in the PULL or DISCARD summary that
 * ends its results, as Neo4j's types: Neo4j's Bolt server reports them to the client again.
 */
internal class RemoteSummary(metadata: Map<String, Value>) {
    private val entries: Map<String, AnyValue> = metadata.mapValues {
        BoltValues.anyValue(it.value)
    }

    /** The bookmark of an auto-commit transaction, which this summary ended. */
    val bookmark: String? = (entries["bookmark"] as? TextValue)?.stringValue()

    val statistics: QueryStatistics = RemoteStatistics(entries["stats"] as? MapValue)

    val plan: ExecutionPlanDescription? =
        ((entries["profile"] ?: entries["plan"]) as? MapValue)?.let { RemotePlan(it) }

    val executionType: QueryExecutionType =
        queryType((entries["type"] as? TextValue)?.stringValue()).let { type ->
            when {
                entries["profile"] is MapValue -> QueryExecutionType.profiled(type)
                entries["plan"] is MapValue -> QueryExecutionType.explained(type)
                else -> QueryExecutionType.query(type)
            }
        }

    /** The GQL statuses of the query, from Bolt 5.5 on; notifications are some of them. */
    val statuses: List<RemoteStatus> =
        (entries["statuses"] as? ListValue)
            ?.mapNotNull { (it as? MapValue)?.let(RemoteStatus::of) }
            .orEmpty()

    val notifications: List<Notification> = statuses.filterIsInstance<Notification>()

    private companion object {
        fun queryType(code: String?): QueryExecutionType.QueryType =
            when (code) {
                "r" -> QueryExecutionType.QueryType.READ_ONLY
                "w" -> QueryExecutionType.QueryType.WRITE
                "s" -> QueryExecutionType.QueryType.SCHEMA_WRITE
                else -> QueryExecutionType.QueryType.READ_WRITE
            }
    }
}

/** The statistics in a summary's `stats`, which lists only counts that are not zero. */
internal class RemoteStatistics(stats: MapValue?) : QueryStatistics {
    private val counts: Map<String, Int> = buildMap {
        stats?.foreach<RuntimeException> { key, value ->
            if (value is IntegralValue) put(key, value.longValue().toInt())
        }
    }
    private val updates = (stats?.get("contains-updates") as? BooleanValue)?.booleanValue() == true
    private val systemUpdates =
        (stats?.get("contains-system-updates") as? BooleanValue)?.booleanValue() == true

    private fun count(key: String) = counts[key] ?: 0

    override fun getNodesCreated() = count("nodes-created")

    override fun getNodesDeleted() = count("nodes-deleted")

    override fun getRelationshipsCreated() = count("relationships-created")

    override fun getRelationshipsDeleted() = count("relationships-deleted")

    override fun getPropertiesSet() = count("properties-set")

    override fun getLabelsAdded() = count("labels-added")

    override fun getLabelsRemoved() = count("labels-removed")

    override fun getIndexesAdded() = count("indexes-added")

    override fun getIndexesRemoved() = count("indexes-removed")

    override fun getConstraintsAdded() = count("constraints-added")

    override fun getConstraintsRemoved() = count("constraints-removed")

    override fun getSystemUpdates() = count("system-updates")

    override fun containsUpdates() = updates

    override fun containsSystemUpdates() = systemUpdates
}

/** A plan or profile in a summary, as Neo4j's Bolt server writes them. */
internal class RemotePlan(private val plan: MapValue) : ExecutionPlanDescription {
    override fun getName(): String = (plan.get("operatorType") as TextValue).stringValue()

    override fun getChildren(): List<ExecutionPlanDescription> =
        (plan.get("children") as? ListValue)?.map { RemotePlan(it as MapValue) }.orEmpty()

    override fun getArguments(): Map<String, Any?> =
        (plan.get("args") as? MapValue)?.let(::javaMap).orEmpty()

    override fun getIdentifiers(): Set<String> =
        (plan.get("identifiers") as? ListValue)
            ?.map { (it as TextValue).stringValue() }
            ?.toCollection(LinkedHashSet())
            .orEmpty()

    override fun hasProfilerStatistics(): Boolean =
        listOf("dbHits", "rows", "time", "pageCacheHits").any { plan.containsKey(it) }

    override fun getProfilerStatistics(): ExecutionPlanDescription.ProfilerStatistics =
        object : ExecutionPlanDescription.ProfilerStatistics {
            fun number(key: String): Long? = (plan.get(key) as? IntegralValue)?.longValue()

            override fun hasRows() = number("rows") != null

            override fun getRows() = number("rows") ?: 0

            override fun hasDbHits() = number("dbHits") != null

            override fun getDbHits() = number("dbHits") ?: 0

            override fun hasPageCacheStats() = number("pageCacheHits") != null

            override fun getPageCacheHits() = number("pageCacheHits") ?: 0

            override fun getPageCacheMisses() = number("pageCacheMisses") ?: 0

            override fun getPageCacheHitRatio() =
                (plan.get("pageCacheHitRatio") as? FloatingPointValue)?.doubleValue()
                    ?: super.getPageCacheHitRatio()

            override fun hasTime() = number("time") != null

            override fun getTime() = number("time") ?: 0
        }
}

/**
 * A GQL status in a summary's `statuses`. Neo4j's Bolt server leaves out the entries of the
 * diagnostic record that have their default values; [diagnosticRecord] has them again.
 */
internal open class RemoteStatus(status: MapValue) : GqlStatusObject {
    private val gqlStatus = (status.get("gql_status") as TextValue).stringValue()
    private val description = (status.get("status_description") as? TextValue)?.stringValue() ?: ""
    private val record: Map<String, Any?> =
        DEFAULT_DIAGNOSTIC_RECORD +
            ((status.get("diagnostic_record") as? MapValue)?.let(::javaMap) ?: emptyMap())

    override fun gqlStatus(): String = gqlStatus

    override fun statusDescription(): String = description

    override fun diagnosticRecord(): Map<String, Any?> = record

    override fun getSeverity(): SeverityLevel = runCatching {
        SeverityLevel.valueOf(record["_severity"] as String)
    }
        .getOrDefault(SeverityLevel.UNKNOWN)

    override fun getPosition(): InputPosition {
        val position = record["_position"] as? Map<*, *> ?: return InputPosition.empty
        fun at(key: String) = (position[key] as? Long)?.toInt() ?: -1
        val offset = at("offset")
        return if (offset < 0) InputPosition.empty
        else InputPosition(offset, at("line"), at("column"))
    }

    override fun getClassification(): NotificationClassification = runCatching {
        NotificationClassification.valueOf(record["_classification"] as String)
    }
        .getOrDefault(NotificationClassification.UNKNOWN)

    companion object {
        /** A status, or a notification if it has the code of one. */
        fun of(status: MapValue): RemoteStatus =
            if (status.get("neo4j_code") is TextValue) RemoteNotification(status)
            else RemoteStatus(status)

        /** The entries Neo4j's Bolt server leaves out when they have these values. */
        val DEFAULT_DIAGNOSTIC_RECORD: Map<String, Any?> =
            DefaultMetadataHandler.DEFAULT_DIAGNOSTIC_RECORD
    }
}

/** A notification: a GQL status that also has the code, title and description of Neo4j 5's. */
internal class RemoteNotification(status: MapValue) : RemoteStatus(status), Notification {
    private val code = (status.get("neo4j_code") as TextValue).stringValue()
    private val title = (status.get("title") as? TextValue)?.stringValue() ?: ""
    private val description = (status.get("description") as? TextValue)?.stringValue() ?: ""

    override fun getCode(): String = code

    override fun getTitle(): String = title

    override fun getDescription(): String = description

    override fun getCategory(): NotificationCategory = runCatching {
        NotificationCategory.valueOf(classification.name)
    }
        .getOrDefault(NotificationCategory.UNKNOWN)
}

/**
 * A failure another server reported, or one decided here for it, with its Neo4j status and GQL
 * status, which Neo4j's Bolt server reports to the client as it would its own.
 */
class RemoteFailure(
    private val status: Status,
    message: String,
    gql: ErrorGqlStatusObject,
    cause: Throwable? = null,
) : GqlRuntimeException(gql, message, cause), Status.HasStatus {
    override fun status(): Status = status

    companion object {
        private val STATUSES: Map<String, Status> by lazy {
            Status.Code.all().associateBy { it.code().serialize() }
        }

        /** The Neo4j status of [code]; one this Neo4j version does not know is an unknown error. */
        fun status(code: String): Status = STATUSES[code] ?: Status.General.UnknownError

        /** The failure that [failure], a server's response, reports. */
        fun of(failure: BoltFailureException): RemoteFailure =
            RemoteFailure(
                status(failure.code()),
                failure.message ?: failure.code(),
                RemoteGqlError.of(failure),
                failure,
            )
    }
}

/** The GQL status of a server's failure, and of its causes. */
private class RemoteGqlError(
    private val gqlStatus: String,
    private val description: String,
    private val record: Map<String, Any?>,
    private val message: String,
    private val cause: ErrorGqlStatusObject?,
) : ErrorGqlStatusObject {
    override fun gqlStatus(): String = gqlStatus

    override fun statusDescription(): String = description

    override fun diagnosticRecord(): Map<String, Any?> = record

    override fun cause(): Optional<ErrorGqlStatusObject> = Optional.ofNullable(cause)

    override fun gqlStatusObject(): ErrorGqlStatusObject = this

    override fun getMessage(): String = message

    override fun legacyMessage(): String = message

    companion object {
        fun of(error: BoltGqlErrorException): RemoteGqlError =
            RemoteGqlError(
                error.gqlStatus(),
                error.statusDescription(),
                error.diagnosticRecord().mapValues { javaValue(BoltValues.anyValue(it.value)) },
                error.message ?: "",
                error.gqlCause().map { of(it) }.orElse(null),
            )
    }
}

/** [value] as the Java values that Neo4j's plans and diagnostic records hold. */
private fun javaValue(value: AnyValue): Any? =
    when (value) {
        is NoValue -> null
        is TextValue -> value.stringValue()
        is IntegralValue -> value.longValue()
        is FloatingPointValue -> value.doubleValue()
        is BooleanValue -> value.booleanValue()
        is ListValue -> value.map { javaValue(it) }
        is MapValue -> javaMap(value)
        else -> value.toString()
    }

private fun javaMap(map: MapValue): Map<String, Any?> = buildMap {
    map.foreach<RuntimeException> { key, entry -> put(key, javaValue(entry)) }
}
