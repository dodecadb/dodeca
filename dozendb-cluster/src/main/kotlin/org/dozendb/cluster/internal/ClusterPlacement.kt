package org.dozendb.cluster.internal

import java.util.UUID
import org.dozendb.core.servers.DatabasePlacement
import org.dozendb.core.servers.Placement
import org.dozendb.core.servers.Placements
import org.dozendb.core.servers.ServerRecords
import org.neo4j.graphdb.Transaction

/**
 * Places databases on the cluster's enabled servers (see `org.dozendb.core.servers.Allocator`), in
 * the system transaction of the command on the `system` leader, under the records' lock, so that
 * placements change one command at a time. Servers that do not answer this member are chosen last.
 * While the servers the cluster started with are not recorded yet, it records them first.
 *
 * @property available whether a server answers this member.
 * @property recordInitial records the initial servers in a transaction, if they are not yet.
 * @property defaults the default numbers of copies the configuration sets, while none is recorded.
 * @property holders the servers that vote in a database's group, as far as known, which a database
 *   not placed yet stays on first.
 */
internal class ClusterPlacement(
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
        return Placement(hosts, primaries)
    }

    private fun prepare(tx: Transaction) {
        ServerRecords.lock(tx)
        recordInitial(tx)
    }
}
