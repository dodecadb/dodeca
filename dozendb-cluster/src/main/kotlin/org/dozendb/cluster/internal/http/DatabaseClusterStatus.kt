package org.dozendb.cluster.internal.http

import java.util.UUID

/**
 * A database on this member, as the cluster status endpoints of the HTTP server report it, in Neo4j
 * Enterprise's fields.
 *
 * @property core whether this member hosts it as a primary, not as a secondary.
 * @property running whether it runs here, and so serves reads.
 * @property writable whether this member leads it and takes its writes.
 * @property lastAppliedRaftIndex the Raft index of the last entry it applied: for a secondary, that
 *   of its last transaction.
 * @property participatingInRaftGroup whether this member votes in its group and knows the leader.
 * @property votingMembers the ids of the members that vote in its group, as this member knows them.
 * @property healthy whether it did not fail here.
 * @property memberId this member's id.
 * @property leader the id of its leader, as this member knows it; null if unknown.
 */
internal data class DatabaseClusterStatus(
    val name: String,
    val id: UUID,
    val core: Boolean,
    val running: Boolean,
    val writable: Boolean,
    val lastAppliedRaftIndex: Long,
    val participatingInRaftGroup: Boolean,
    val votingMembers: List<String>,
    val healthy: Boolean,
    val memberId: String,
    val leader: String?,
) {
    /** Neo4j Enterprise's JSON of `/db/<database>/cluster/status`; the leader only if known. */
    fun json(): String = Json.obj(fields())

    /**
     * An element of Neo4j Enterprise's `/dbms/cluster/status`. Its documentation names the health
     * field `healthy` in its example of this endpoint, and `isHealthy` elsewhere: both are given.
     */
    fun dbmsJson(): String =
        Json.obj(
            listOf(
                "databaseName" to Json.string(name),
                "databaseUuid" to Json.string(id.toString()),
                "databaseStatus" to Json.obj(fields() + ("healthy" to healthy.toString())),
            )
        )

    private fun fields(): List<Pair<String, String>> =
        listOfNotNull(
            "core" to core.toString(),
            "lastAppliedRaftIndex" to lastAppliedRaftIndex.toString(),
            "participatingInRaftGroup" to participatingInRaftGroup.toString(),
            "votingMembers" to Json.array(votingMembers.map(Json::string)),
            "isHealthy" to healthy.toString(),
            "memberId" to Json.string(memberId),
            leader?.let { "leader" to Json.string(it) },
        )
}

/** The databases on this member, as the cluster status endpoints report them. */
internal fun interface ClusterStatuses {
    fun statuses(): List<DatabaseClusterStatus>
}

/** Just enough JSON for the cluster status endpoints. */
internal object Json {
    fun obj(fields: List<Pair<String, String>>): String =
        fields.joinToString(",", "{", "}") { (name, value) -> "${string(name)}:$value" }

    fun array(values: List<String>): String = values.joinToString(",", "[", "]")

    fun string(text: String): String = buildString {
        append('"')
        for (c in text) {
            when {
                c == '"' -> append("\\\"")
                c == '\\' -> append("\\\\")
                c < ' ' -> append("\\u%04x".format(c.code))
                else -> append(c)
            }
        }
        append('"')
    }
}
