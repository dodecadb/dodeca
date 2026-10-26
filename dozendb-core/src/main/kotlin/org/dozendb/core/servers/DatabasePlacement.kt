package org.dozendb.core.servers

import java.util.UUID
import org.neo4j.graphdb.Transaction

/** The servers a database is placed on, and the number of primaries it requests. */
data class Placement(val hosts: Set<UUID>, val primaries: Int)

/**
 * Where the copies of databases go, as CREATE DATABASE and ALTER DATABASE ask, in their system
 * transaction: in a cluster, on servers the [Allocator] chooses; without one, on this server alone.
 */
interface DatabasePlacement {
    /**
     * Places the new database [name] on [primaries] servers, or the default number of them,
     * [seedServer] among them if given.
     *
     * @throws AllocationException if the servers cannot host them.
     */
    fun place(tx: Transaction, name: String, primaries: Int?, seedServer: UUID?): Placement

    /**
     * Changes the number of primaries of the database [name] to [primaries].
     *
     * @throws AllocationException if the servers cannot host them.
     */
    fun resize(tx: Transaction, name: String, primaries: Int): Placement

    /**
     * Moves the copies that their servers may no longer host, as after ALTER SERVER changed their
     * options, to servers that may; returns the moves.
     *
     * @throws AllocationException if a copy has nowhere to go; then nothing moves.
     */
    fun conform(tx: Transaction): List<Move>
}

/** A server without clustering hosts every database alone: one primary each, on no record. */
object SingleServerPlacement : DatabasePlacement {
    override fun place(tx: Transaction, name: String, primaries: Int?, seedServer: UUID?) =
        resize(tx, name, primaries ?: 1)

    override fun resize(tx: Transaction, name: String, primaries: Int): Placement {
        if (primaries != 1) {
            throw AllocationException(
                "'$name' needs $primaries servers that can host it as a primary, and this server " +
                    "runs without clustering"
            )
        }
        return Placement(emptySet(), 1)
    }

    /** Moves nothing: refuses options that would leave a database without this server. */
    override fun conform(tx: Transaction): List<Move> {
        val record = ServerRecords.records(tx).values.singleOrNull() ?: return emptyList()
        val databases = Placements.placed(tx).map { it.copy(hosts = setOf(record.id)) }
        return Allocator(listOf(Host(record)), databases).conform()
    }
}
