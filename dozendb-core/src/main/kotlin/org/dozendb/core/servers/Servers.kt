package org.dozendb.core.servers

import java.util.UUID

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
}
