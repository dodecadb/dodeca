package org.dozendb.cluster.internal.topology

import java.util.Optional
import org.dozendb.consensus.MemberId
import org.dozendb.core.edition.DatabaseState
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.dbms.database.DatabaseDetails
import org.neo4j.dbms.identity.ServerId
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DatabaseAccess

/**
 * What [member] has of a database, as this member knows it.
 *
 * @property answering whether the member answered this member's last probe; this member always
 *   does.
 * @property state the database's state on the member, as it reported it; null if it did not.
 * @property readOnly whether the database is read-only on the member by its configuration.
 */
internal data class Copy(
    val member: MemberId,
    val answering: Boolean,
    val boltAddress: SocketAddress?,
    val state: DatabaseState?,
    val readOnly: Boolean,
    val lastCommittedTxId: Long?,
)

/** The rows of SHOW DATABASES for a database of a cluster, as Neo4j Enterprise shows them. */
internal object DatabaseRows {
    /**
     * A row for each of [copies]: the member's Bolt address, access and status, and `writer` true
     * only for the [leader], if the database is online and writable there. A member that does not
     * answer has the role and status `unknown`. With [txInfo], each row has the copy's last
     * transaction id and how far it is behind the newest copy's. The values that belong to the
     * database itself, such as its type and store, are those of [own], Neo4j Community's row for
     * this member.
     */
    fun rows(
        own: DatabaseDetails,
        copies: Collection<Copy>,
        leader: MemberId?,
        txInfo: Boolean,
    ): List<DatabaseDetails> {
        val online = copies.count {
            it.answering && it.state?.status == DatabaseState.Status.ONLINE
        }
        val newest = copies.mapNotNull { it.lastCommittedTxId }.maxOrNull()
        return copies.map { copy ->
            val status =
                when {
                    !copy.answering -> DatabaseDetails.STATUS_UNKNOWN
                    copy.state?.status == DatabaseState.Status.ONLINE -> ONLINE
                    else -> OFFLINE
                }
            val message =
                when {
                    !copy.answering -> DatabaseDetails.STATUS_MESSAGE_UNKNOWN
                    copy.state?.status == DatabaseState.Status.FAILED ->
                        copy.state.failure.orEmpty()
                    else -> ""
                }
            val lastCommitted = copy.lastCommittedTxId.takeIf { txInfo }
            DatabaseDetails(
                Optional.of(ServerId(copy.member.uuid)),
                if (copy.readOnly) DatabaseAccess.READ_ONLY else DatabaseAccess.READ_WRITE,
                Optional.ofNullable(copy.boltAddress),
                Optional.of(if (copy.answering) DatabaseDetails.ROLE_PRIMARY else UNKNOWN_ROLE),
                copy.member == leader && status == ONLINE && !copy.readOnly,
                status,
                message,
                Optional.ofNullable(lastCommitted),
                if (txInfo) Optional.ofNullable(lastCommitted?.let { it - newest!! })
                else Optional.of(0L),
                own.namedDatabaseId(),
                own.type(),
                own.options(),
                own.storeId(),
                own.externalStoreId(),
                online,
                0,
            )
        }
    }

    private const val ONLINE = "online"
    private const val OFFLINE = "offline"
    private const val UNKNOWN_ROLE = "unknown"
}
