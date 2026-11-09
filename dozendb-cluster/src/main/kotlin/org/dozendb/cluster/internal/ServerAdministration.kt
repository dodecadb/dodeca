package org.dozendb.cluster.internal

import java.util.UUID
import org.dozendb.core.servers.DatabasePlacement
import org.dozendb.core.servers.Move
import org.dozendb.core.servers.Placements
import org.dozendb.core.servers.ServerLifecycle
import org.dozendb.core.servers.ServerRecords
import org.dozendb.core.servers.Servers
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Transaction

/**
 * Changes the servers as the server procedures ask, `dbms.cluster.cordonServer` and the like (see
 * [ServerLifecycle]), each in a system transaction on the `system` leader, under the records' lock.
 *
 * @property reading runs a block while the system graph may be read, as catch-up may replace it.
 * @property defaults the default numbers of copies the configuration sets, while none is recorded.
 * @property autoEnable whether servers that join are enabled at once, as the configuration sets it,
 *   while that is not recorded.
 */
@Suppress("DEPRECATION") // Neo4j 5.26 has no GQL status for most refusals of server commands.
class ServerAdministration(
    private val system: () -> GraphDatabaseService,
    private val reading: (() -> Unit) -> Unit,
    private val leading: () -> Boolean,
    private val servers: Servers,
    private val placement: DatabasePlacement,
    private val defaults: Placements.Defaults,
    private val autoEnable: Boolean,
) {
    /** A copy moved, or to be moved, with the names its servers have. */
    data class NamedMove(val move: Move, val fromName: String, val toName: String)

    /**
     * The settings of the system graph that place databases, as `dbms.showTopologyGraphConfig`
     * shows them.
     *
     * @property defaultDatabase the name of the default database, if there is one.
     * @property autoEnable whether servers that join are enabled at once.
     */
    data class TopologyGraphConfig(
        val defaults: Placements.Defaults,
        val defaultDatabase: String?,
        val autoEnable: Boolean,
    )

    /** The settings of the system graph that place databases, as this member reads them. */
    fun topologyGraphConfig(): TopologyGraphConfig {
        var config: TopologyGraphConfig? = null
        reading {
            system().beginTx().use { tx ->
                config =
                    TopologyGraphConfig(
                        Placements.defaults(tx, defaults),
                        tx.findNode(
                                TopologyGraphDbmsModel.DATABASE_LABEL,
                                TopologyGraphDbmsModel.DATABASE_DEFAULT_PROPERTY,
                                true,
                            )
                            ?.getProperty(TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY, null)
                            as? String,
                        ServerRecords.autoEnable(tx, autoEnable),
                    )
            }
        }
        return checkNotNull(config)
    }

    /**
     * Sets the number of [primaries] and [secondaries] that a new database has when CREATE DATABASE
     * gives no topology, as `dbms.setDefaultAllocationNumbers` does: 1 to 11 primaries, and 0 to 20
     * secondaries.
     */
    fun setDefaults(primaries: Long, secondaries: Long) {
        val failed = "Could not set the default numbers of copies"
        if (primaries !in 1..MAXIMUM_PRIMARIES) {
            throw InvalidArgumentException(
                "$failed: the number of primaries is $primaries, not 1 to $MAXIMUM_PRIMARIES."
            )
        }
        if (secondaries !in 0..MAXIMUM_SECONDARIES) {
            throw InvalidArgumentException(
                "$failed: the number of secondaries is $secondaries, not 0 to $MAXIMUM_SECONDARIES."
            )
        }
        changing(failed) { tx, _ ->
            Placements.setDefaults(tx, Placements.Defaults(primaries.toInt(), secondaries.toInt()))
        }
    }

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

    /**
     * Moves copies to even out how many each server hosts: of [database] only, or at most [limit]
     * of them (see [ServerLifecycle.reallocate]); returns the moves, which a [dryRun] only shows.
     */
    fun reallocate(database: String?, limit: Int?, dryRun: Boolean): List<NamedMove> {
        val failed = "Could not reallocate databases"
        var moves = emptyList<NamedMove>()
        changing(failed, dryRun) { tx, _ ->
            val made = ServerLifecycle.reallocate(tx, placement, database, limit, dryRun, failed)
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

    private companion object {
        /** The most primaries a database can have, as in Neo4j Enterprise. */
        const val MAXIMUM_PRIMARIES = 11L
        const val MAXIMUM_SECONDARIES = 20L
    }
}
