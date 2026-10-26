package org.dozendb.cluster.internal

import java.util.UUID
import org.dozendb.core.servers.AllocationException
import org.dozendb.core.servers.Allocator
import org.dozendb.core.servers.DatabasePlacement
import org.dozendb.core.servers.Move
import org.dozendb.core.servers.Placement
import org.dozendb.core.servers.Placements
import org.dozendb.core.servers.ServerRecords
import org.neo4j.graphdb.Transaction

/**
 * Places databases on the cluster's enabled servers (see `org.dozendb.core.servers.Allocator`), in
 * the system transaction of the command on the `system` leader, under the records' lock, so that
 * placements change one command at a time. Servers that do not answer this member are chosen last:
 * it asks them for each command, as SHOW SERVERS does, so that a server that stopped a moment ago
 * counts as unavailable. While the servers the cluster started with are not recorded yet, it
 * records them first.
 *
 * A database moves off the member that creates its store, its creator, only once a server runs its
 * group: the change then records the store created, so that the creator copies it like any other
 * server if it is placed on it again, and does not create it anew (see `DatabaseReconciler`).
 *
 * @property refresh asks the servers now, waiting a moment at most.
 * @property available whether a server answered this member when last asked.
 * @property recordInitial records the initial servers in a transaction, if they are not yet.
 * @property defaults the default numbers of copies the configuration sets, while none is recorded.
 * @property holders the servers that vote in a database's group, as far as known, which a database
 *   not placed yet stays on first.
 */
internal class ClusterPlacement(
    private val refresh: () -> Unit,
    private val available: (UUID) -> Boolean,
    private val recordInitial: (Transaction) -> Unit,
    private val defaults: Placements.Defaults,
    private val holders: (String) -> List<UUID>,
) : DatabasePlacement {
    override fun place(
        tx: Transaction,
        name: String,
        primaries: Int?,
        seedServer: UUID?,
    ): Placement {
        prepare(tx)
        val count = primaries ?: Placements.defaults(tx, defaults).primaries
        return Placement(
            Placements.allocator(tx, available).place(name, count, first = seedServer),
            count,
        )
    }

    override fun resize(tx: Transaction, name: String, primaries: Int): Placement {
        prepare(tx)
        val allocator = Placements.allocator(tx, available)
        val hosts =
            if (allocator.hosts(name).isEmpty()) {
                allocator.place(name, primaries, prefer = holders(name))
            } else {
                allocator.resize(name, primaries)
            }
        leaving(tx, name, hosts)
        return Placement(hosts, primaries)
    }

    override fun conform(tx: Transaction): List<Move> = moving(tx, dryRun = false) { it.conform() }

    override fun deallocate(
        tx: Transaction,
        from: Set<UUID>,
        database: String?,
        limit: Int?,
        dryRun: Boolean,
    ): List<Move> = moving(tx, dryRun) { it.deallocate(from, database, limit) }

    override fun reallocate(
        tx: Transaction,
        database: String?,
        limit: Int?,
        dryRun: Boolean,
    ): List<Move> = moving(tx, dryRun) { it.reallocate(database, limit) }

    /**
     * The [moves] an allocator of the current placements makes, each letting its database leave its
     * creator (see [leaving]); records them unless [dryRun].
     */
    private fun moving(
        tx: Transaction,
        dryRun: Boolean,
        moves: (Allocator) -> List<Move>,
    ): List<Move> {
        // A dry run writes nothing, as on a member that does not lead system.
        if (dryRun) refresh() else prepare(tx)
        val allocator = Placements.allocator(tx, available)
        val made = moves(allocator)
        for (name in made.map { it.database }.distinct()) {
            leaving(tx, name, allocator.hosts(name), record = !dryRun)
        }
        if (!dryRun) Placements.move(tx, allocator, made)
        return made
    }

    /**
     * Lets the database [name] be placed on [hosts]: if they leave out its creator, only once a
     * server runs its group, which records its store created if [record].
     *
     * @throws AllocationException if its store may not be created yet.
     */
    private fun leaving(tx: Transaction, name: String, hosts: Set<UUID>, record: Boolean = true) {
        val node = Placements.database(tx, name) ?: return
        val creator = Neo4jSystemGraph.creator(node) ?: return
        if (creator.uuid in hosts || Neo4jSystemGraph.created(node)) return
        if (holders(name).isEmpty()) {
            throw AllocationException(
                "'$name' cannot move off server $creator yet, which creates its store: try again " +
                    "once the database runs"
            )
        }
        if (record) Neo4jSystemGraph.markCreated(node)
    }

    private fun prepare(tx: Transaction) {
        ServerRecords.lock(tx)
        recordInitial(tx)
        refresh()
    }
}
