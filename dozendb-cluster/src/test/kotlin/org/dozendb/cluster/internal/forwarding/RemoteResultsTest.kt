@file:Suppress("DEPRECATION") // Neo4j 5's Notification.

package org.dozendb.cluster.internal.forwarding

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.neo4j.bolt.connection.exception.BoltFailureException
import org.neo4j.bolt.connection.exception.BoltGqlErrorException
import org.neo4j.bolt.connection.values.Value
import org.neo4j.bolt.protocol.common.fsm.response.metadata.DefaultMetadataHandler
import org.neo4j.bolt.protocol.common.message.Error
import org.neo4j.graphdb.InputPosition
import org.neo4j.graphdb.NotificationCategory
import org.neo4j.graphdb.QueryExecutionType
import org.neo4j.graphdb.SeverityLevel
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.AnyValue

/**
 * What another member reports about a query or a failure, which Neo4j's Bolt server here must
 * report to the client as that member's did.
 */
class RemoteResultsTest {
    private fun value(any: Any?): Value = BoltValues.value(ValueUtils.of(any))

    private fun metadata(vararg entries: Pair<String, Any?>): Map<String, Value> =
        entries.associate { (key, entry) ->
            key to value(entry)
        }

    /** What Neo4j's Bolt server writes for [write], by key. */
    private fun written(write: (DefaultMetadataHandler, (String, AnyValue) -> Unit) -> Unit) =
        buildMap<String, AnyValue> {
            write(DefaultMetadataHandler.getInstance()) { key, value -> put(key, value) }
        }

    private val stats =
        mapOf("contains-updates" to true, "nodes-created" to 2L, "properties-set" to 3L)
    private val unknownLabel =
        mapOf(
            "gql_status" to "01N50",
            "status_description" to "warn: label does not exist. The label `Film` does not exist.",
            "neo4j_code" to "Neo.ClientNotification.Statement.UnknownLabelWarning",
            "title" to "The provided label is not in the database.",
            "description" to "One of the labels in your query is not available in the database",
            "diagnostic_record" to
                mapOf(
                    "_classification" to "UNRECOGNIZED",
                    "_severity" to "WARNING",
                    "_position" to mapOf("offset" to 9L, "line" to 1L, "column" to 10L),
                ),
        )
    private val success =
        mapOf("gql_status" to "00000", "status_description" to "note: successful completion")
    private val profile =
        mapOf(
            "operatorType" to "ProduceResults@neo4j",
            "args" to mapOf("Details" to "n", "EstimatedRows" to 1.0, "Id" to 0L),
            "identifiers" to listOf("n"),
            "dbHits" to 0L,
            "rows" to 1L,
            "time" to 12L,
            "pageCacheHits" to 2L,
            "pageCacheMisses" to 1L,
            "pageCacheHitRatio" to 2.0 / 3,
            "children" to
                listOf(
                    mapOf(
                        "operatorType" to "AllNodesScan@neo4j",
                        "args" to mapOf("Details" to "n"),
                        "identifiers" to listOf("n"),
                        "dbHits" to 3L,
                        "rows" to 1L,
                    )
                ),
        )

    @Test
    fun `a summary reports what the member's server wrote`() {
        val summary =
            RemoteSummary(
                metadata(
                    "type" to "rw",
                    "stats" to stats,
                    "bookmark" to "FB:abc",
                    "statuses" to listOf(success, unknownLabel),
                    "profile" to profile,
                    "db" to "neo4j",
                )
            )

        assertThat(summary.bookmark).isEqualTo("FB:abc")
        assertThat(summary.executionType)
            .isEqualTo(QueryExecutionType.profiled(QueryExecutionType.QueryType.READ_WRITE))
        assertThat(summary.statistics.nodesCreated).isEqualTo(2)
        assertThat(summary.statistics.containsUpdates()).isTrue()
        val notification = summary.notifications.single()
        assertThat(notification.code).isEqualTo(unknownLabel["neo4j_code"])
        assertThat(notification.severity).isEqualTo(SeverityLevel.WARNING)
        assertThat(notification.category).isEqualTo(NotificationCategory.UNRECOGNIZED)
        assertThat(notification.position).isEqualTo(InputPosition(9, 1, 10))
        // Written again by Neo4j's Bolt server, they are what the member's wrote.
        assertThat(written { handler, put -> handler.onQueryStatistics(put, summary.statistics) })
            .isEqualTo(mapOf("stats" to ValueUtils.of(stats)))
        assertThat(
                written { handler, put ->
                    handler.onNotifications(put, summary.notifications, summary.statuses)
                }
            )
            .isEqualTo(mapOf("statuses" to ValueUtils.of(listOf(success, unknownLabel))))
        assertThat(written { handler, put -> handler.onExecutionPlan(put, summary.plan) })
            .isEqualTo(mapOf("profile" to ValueUtils.of(profile)))
        assertThat(written { handler, put -> handler.onExecutionType(put, summary.executionType) })
            .isEqualTo(mapOf("type" to ValueUtils.of("rw")))
    }

    @Test
    fun `a summary without statistics or statuses reports none`() {
        val summary = RemoteSummary(metadata("type" to "r", "plan" to profile - "dbHits"))

        assertThat(summary.executionType)
            .isEqualTo(QueryExecutionType.explained(QueryExecutionType.QueryType.READ_ONLY))
        assertThat(summary.statistics.containsUpdates()).isFalse()
        assertThat(summary.statistics.containsSystemUpdates()).isFalse()
        assertThat(summary.statuses).isEmpty()
        assertThat(summary.bookmark).isNull()
    }

    @Test
    fun `system updates are reported as such`() {
        val summary =
            RemoteSummary(
                metadata(
                    "type" to "s",
                    "stats" to mapOf("contains-system-updates" to true, "system-updates" to 1L),
                )
            )

        assertThat(written { handler, put -> handler.onQueryStatistics(put, summary.statistics) })
            .isEqualTo(
                mapOf(
                    "stats" to
                        ValueUtils.of(
                            mapOf("contains-system-updates" to true, "system-updates" to 1L)
                        )
                )
            )
    }

    @Test
    fun `a failure is reported to the client as the member reported it`() {
        val cause =
            BoltGqlErrorException(
                "Node(0) already exists with label `Film` and property `title` = 'Alien'",
                "22N41",
                "error: data exception - unique constraint violation",
                mapOf("_classification" to value("CLIENT_ERROR")),
                null,
            )
        val failure =
            BoltFailureException(
                "Neo.ClientError.Schema.ConstraintValidationFailed",
                "Node(0) already exists with label `Film` and property `title` = 'Alien'",
                "22N69",
                "error: data exception - constraint verification failed",
                mapOf(
                    "_classification" to value("CLIENT_ERROR"),
                    "OPERATION" to value(""),
                    "OPERATION_CODE" to value("0"),
                    "CURRENT_SCHEMA" to value("/"),
                ),
                cause,
            )

        val reported = Error.from(RemoteFailure.of(failure)).asBoltMessage().metadata()

        assertThat(reported.status()).isEqualTo(Status.Schema.ConstraintValidationFailed)
        assertThat(reported.message()).isEqualTo(failure.message)
        assertThat(reported.gqlStatus()).isEqualTo("22N69")
        assertThat(reported.description()).isEqualTo(failure.statusDescription())
        assertThat(reported.diagnosticRecord())
            .containsEntry("_classification", "CLIENT_ERROR")
            .containsEntry("OPERATION_CODE", "0")
        assertThat(reported.cause().gqlStatus()).isEqualTo("22N41")
        assertThat(reported.cause().message()).isEqualTo(cause.message)
        assertThat(reported.cause().cause()).isNull()
    }

    @Test
    fun `codes of every status are known`() {
        for (status in Status.Code.all()) {
            assertThat(RemoteFailure.status(status.code().serialize())).isEqualTo(status)
        }
        assertThat(RemoteFailure.status("Neo.ClientError.Made.Up"))
            .isEqualTo(Status.General.UnknownError)
    }
}
