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
     * answer has the role and status `unknown`, and no address, as in Neo4j Enterprise, and one
     * whose copy runs before its Raft group does, as a new database's, `starting`. Where the
     * database is placed on servers, [placed], the status says how its copies move, with Neo4j
     * Enterprise's names: `store copying` on a server that copies the database in, `initial` on one
     * it is placed on that has not created it yet, and `deallocating` on one it moves off, which
     * still runs it, and may still lead it until it hands the leadership over. With [txInfo], each
     * row of a member that answers has the copy's last transaction id and how far it is behind the
     * newest copy's. The values that belong to the database itself, such as its type and store, are
     * those of [own], Neo4j Community's row for this member.
     */
    fun rows(
        own: DatabaseDetails,
        copies: Collection<Copy>,
        leader: MemberId?,
        txInfo: Boolean,
        placed: Set<MemberId> = emptySet(),
    ): List<DatabaseDetails> {
        val online = copies.count {
            it.answering && it.state?.status == DatabaseState.Status.ONLINE
        }
        // What a member that does not answer reported last is not its copy now.
        val newest = copies.filter { it.answering }.mapNotNull { it.lastCommittedTxId }.maxOrNull()
        return copies.map { copy ->
            val running = copy.answering && copy.state?.status == DatabaseState.Status.ONLINE
            val status =
                when {
                    !copy.answering -> DatabaseDetails.STATUS_UNKNOWN
                    copy.state?.status == DatabaseState.Status.COPYING -> STORE_COPYING
                    copy.state?.status == DatabaseState.Status.STARTING -> STARTING
                    running && placed.isNotEmpty() && copy.member !in placed -> DEALLOCATING
                    running -> ONLINE
                    copy.state == null && copy.member in placed -> INITIAL
                    else -> OFFLINE
                }
            val message =
                when {
                    !copy.answering -> DatabaseDetails.STATUS_MESSAGE_UNKNOWN
                    copy.state?.status == DatabaseState.Status.FAILED ->
                        copy.state.failure.orEmpty()
                    else -> ""
                }
            val lastCommitted = copy.lastCommittedTxId.takeIf { txInfo && copy.answering }
            DatabaseDetails(
                Optional.of(ServerId(copy.member.uuid)),
                if (copy.readOnly) DatabaseAccess.READ_ONLY else DatabaseAccess.READ_WRITE,
                Optional.ofNullable(copy.boltAddress.takeIf { copy.answering }),
                Optional.of(if (copy.answering) DatabaseDetails.ROLE_PRIMARY else UNKNOWN_ROLE),
                copy.member == leader && running && !copy.readOnly,
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
    private const val STARTING = "starting"
    private const val OFFLINE = "offline"
    private const val STORE_COPYING = "store copying"
    private const val INITIAL = "initial"
    private const val DEALLOCATING = "deallocating"
    private const val UNKNOWN_ROLE = "unknown"
}
