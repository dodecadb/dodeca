package dev.dodeca.cluster

import dev.dodeca.consensus.HostPort
import dev.dodeca.consensus.Member
import dev.dodeca.consensus.MemberId

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

    /**
     * Whether the system graph here records this member dropped.
     *
     * @throws Exception while the system graph here cannot be read.
     */
    fun dropped(): Boolean

    /**
     * Records [member] a system secondary, which admits it to the cluster, on the leader of
     * `system`.
     *
     * @throws MembershipChangeException if this member does not lead `system`, or recording failed.
     */
    fun admit(member: Member)

    /**
     * Deletes the record of the system secondary [member], as when it is dropped or once it turned
     * primary, on the leader of `system`.
     *
     * @throws MembershipChangeException if this member does not lead `system`, or deleting failed.
     */
    fun forget(member: MemberId)

    /**
     * Asks the system secondary [member] to turn primary, in its record, on the leader of `system`.
     *
     * @throws MembershipChangeException if this member does not lead `system`, or recording failed.
     */
    fun promote(member: MemberId)

    /**
     * Whether the system graph here asks this member, a system secondary, to turn primary.
     *
     * @throws Exception while the system graph here cannot be read.
     */
    fun promoted(): Boolean

    /**
     * Turns `system` here, whose group no longer counts this member in, into a secondary from its
     * own store, which pulls on from there.
     */
    fun turnSecondary()

    /**
     * Turns `system` here from a secondary into a primary from its own store, seeded as the leader
     * of `system` tells, from a copy if the leader's log no longer holds what the store needs: it
     * runs the group and waits to be added. Returns false to be tried again, as while no leader is
     * known.
     */
    fun turnPrimary(): Boolean
}
