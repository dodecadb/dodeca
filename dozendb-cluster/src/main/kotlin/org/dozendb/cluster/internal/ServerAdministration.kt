package org.dozendb.cluster.internal

import java.util.UUID
import org.dozendb.core.servers.DatabasePlacement
import org.dozendb.core.servers.Move
import org.dozendb.core.servers.ServerLifecycle
import org.dozendb.core.servers.ServerRecords
import org.dozendb.core.servers.Servers
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Transaction

/**
 * Changes the servers as the server procedures ask, `dbms.cluster.cordonServer` and the like (see
 * [ServerLifecycle]), each in a system transaction on the `system` leader, under the records' lock.
 *
 * @property reading runs a block while the system graph may be read, as catch-up may replace it.
 */
class ServerAdministration(
    private val system: () -> GraphDatabaseService,
    private val reading: (() -> Unit) -> Unit,
    private val leading: () -> Boolean,
    private val servers: Servers,
    private val placement: DatabasePlacement,
) {
    /** A copy moved, or to be moved, with the names its servers have. */
    data class NamedMove(val move: Move, val fromName: String, val toName: String)

    /** Cordons the server [name], by its name or its id. */
    fun cordon(name: String) =
        changing("Could not cordon server '$name'") { tx, failed ->
            ServerLifecycle.cordon(tx, name, servers, failed)
        }

    /** Enables the cordoned server [name] again. */
    fun uncordon(name: String) =
        changing("Could not uncordon server '$name'") { tx, failed ->
            ServerLifecycle.uncordon(tx, name, servers, failed)
        }

    /**
     * Moves copies off the servers [names], which stay as they are: of [database] only, or of
     * [limit] of their databases (see [ServerLifecycle.deallocateCopies]); returns the moves, which
     * a [dryRun] only shows.
     */
    fun deallocate(
        names: List<String>,
        database: String?,
        limit: Int?,
        dryRun: Boolean,
    ): List<NamedMove> {
        val failed =
            "Could not deallocate databases from server${if (names.size == 1) "" else "s"} " +
                names.joinToString { "'$it'" }
        var moves = emptyList<NamedMove>()
        changing(failed, dryRun) { tx, _ ->
            val made =
                ServerLifecycle.deallocateCopies(
                    tx,
                    names,
                    servers,
                    placement,
                    database,
                    limit,
                    dryRun,
                    failed,
                )
            moves = named(tx, made)
        }
        return moves
    }

    /** [moves], with the names their servers have in [tx]. */
    private fun named(tx: Transaction, moves: List<Move>): List<NamedMove> {
        val records = ServerRecords.records(tx)
        val name = { id: UUID -> records[id]?.name ?: id.toString() }
        return moves.map { NamedMove(it, name(it.from), name(it.to)) }
    }

    /**
     * Runs [change] in a system transaction under the records' lock, refused as [failed] says on a
     * member that does not lead `system`; a [dryRun] runs anywhere, and changes nothing.
     */
    private fun changing(
        failed: String,
        dryRun: Boolean = false,
        change: (Transaction, String) -> Unit,
    ) {
        check(dryRun || leading()) {
            "$failed: this member does not lead the system database; run it on the one that does."
        }
        reading {
            system().beginTx().use { tx ->
                if (dryRun) {
                    change(tx, failed)
                } else {
                    ServerRecords.lock(tx)
                    change(tx, failed)
                    tx.commit()
                }
            }
        }
    }
}
