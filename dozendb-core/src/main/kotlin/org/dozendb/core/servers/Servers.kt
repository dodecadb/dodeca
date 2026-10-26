package org.dozendb.core.servers

import java.util.UUID
import org.neo4j.graphdb.Transaction

/**
 * The servers that the server commands act on beyond their records: the members of a cluster, or
 * this server alone without clustering.
 */
interface Servers {
    /** The ids of the servers, Free or enabled. */
    fun ids(): Set<UUID>

    /**
     * The options server [id] starts with, from its `initial.server.*` settings; null while they
     * are not known, as while it does not answer.
     */
    fun initialOptions(id: UUID): ServerOptions?

    /** Where the cluster reaches server [id], which a dropped server keeps; null if nowhere. */
    fun address(id: UUID): String?

    /**
     * The databases, `system` aside, whose copies server [id] still holds as far as known, by the
     * names the system graph [tx] reads has: in a cluster, those whose groups it votes in.
     */
    fun holding(tx: Transaction, id: UUID): Set<String>
}
