package org.dozendb.core.edition

import java.util.UUID
import org.dozendb.core.servers.ServerLifecycle
import org.dozendb.core.servers.Servers
import org.neo4j.graphdb.GraphDatabaseService

/**
 * `dbms.unquarantineDatabase`: lifts the quarantine of a database's copy on the server a call
 * names, by its name or its id, as Neo4j Enterprise does from any server: this one through [local],
 * another server of the cluster through [remote].
 *
 * @property system the `system` database, whose records name the servers.
 * @property self this server's id.
 */
class Unquarantines(
    private val system: () -> GraphDatabaseService,
    private val servers: Servers,
    private val self: UUID,
    private val local: DatabaseQuarantine,
    private val remote: Remote?,
) {
    /** Lifts quarantines on the other servers of the cluster. */
    fun interface Remote {
        /**
         * Lifts the quarantine of the copy of [database] on [server] with [operation], as [user]
         * asks.
         *
         * @throws RuntimeException saying why, if it could not.
         */
        fun lift(server: UUID, database: String, operation: UnquarantineOperation, user: String)
    }

    /**
     * Lifts the quarantine of the copy of [database] on the server [server], by its name or id,
     * with the operation [operation], as [user] asks.
     *
     * @throws RuntimeException saying why, if it could not.
     */
    fun lift(server: String, database: String, operation: String, user: String) {
        val asked = UnquarantineOperation.of(operation)
        val id =
            system().beginTx().use { tx ->
                ServerLifecycle.target(tx, server, servers, "Cannot find server '$server'").id
            }
        if (id == self) {
            local.lift(database, asked, user)
        } else {
            checkNotNull(remote) { "Server '$server' is not this server." }
                .lift(id, database, asked, user)
        }
    }
}
