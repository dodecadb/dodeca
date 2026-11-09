package org.dozendb.cluster

import org.dozendb.consensus.HostPort
import org.dozendb.consensus.Member
import org.dozendb.consensus.MemberId

/**
 * How this member hosts `system` when it is a system secondary, which pulls `system` from the
 * servers that host it instead of voting in its group, and how the cluster admits one.
 */
interface SystemHosting {
    /**
     * Whether this member hosts `system` as a secondary, as decided before Neo4j creates it: with
     * no Raft state of `system`, if `server.cluster.system_database_mode` is SECONDARY or its store
     * is a secondary's already.
     */
    val pulls: Boolean

    /** Whether this member's store of `system` is a secondary's, which it pulls. */
    fun pulled(): Boolean

    /** Copies `system` from the member at [source] as a secondary's store, in place of any here. */
    fun copyPulled(source: HostPort)

    /** Whether the system graph here records this member dropped; false while it cannot tell. */
    fun dropped(): Boolean

    /**
     * Records [member] a system secondary, which admits it to the cluster, on the leader of
     * `system`.
     *
     * @throws MembershipChangeException if this member does not lead `system`, or recording failed.
     */
    fun admit(member: Member)

    /**
     * Deletes the record of the system secondary [member], as when it is dropped, on the leader of
     * `system`.
     *
     * @throws MembershipChangeException if this member does not lead `system`, or deleting failed.
     */
    fun forget(member: MemberId)
}
