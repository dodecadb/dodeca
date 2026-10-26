package org.dozendb.cluster.internal

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
) {
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
     * Runs [change] in a system transaction under the records' lock, refused as [failed] says on a
     * member that does not lead `system`.
     */
    private fun changing(failed: String, change: (Transaction, String) -> Unit) {
        check(leading()) {
            "$failed: this member does not lead the system database; run it on the one that does."
        }
        reading {
            system().beginTx().use { tx ->
                ServerRecords.lock(tx)
                change(tx, failed)
                tx.commit()
            }
        }
    }
}
