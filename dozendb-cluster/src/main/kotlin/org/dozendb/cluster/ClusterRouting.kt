package org.dozendb.cluster

import org.dozendb.consensus.GroupStatus
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.Member
import org.dozendb.consensus.MemberId

/** The Bolt addresses a driver uses for one replicated database. */
data class RoutingEndpoints(
    val routers: List<HostPort>,
    val writers: List<HostPort>,
    val readers: List<HostPort>,
)

/** Routing tables of replicated databases, from the Raft view of this member. */
object ClusterRouting {
    /**
     * Writers: the leader. Readers: the followers, plus the leader if [readsOnLeader], or when no
     * follower can be listed. Routers: every member. Members whose Bolt address is not known yet
     * are left out; during an election there is no writer, so drivers retry until there is one.
     */
    fun endpoints(
        status: GroupStatus,
        members: List<Member>,
        boltAddress: (MemberId) -> HostPort?,
        readsOnLeader: Boolean,
    ): RoutingEndpoints {
        val ordered = members.sortedBy { it.id }
        val leader = status.leader?.takeIf { id -> ordered.any { it.id == id } }
        val leaderAddress = leader?.let(boltAddress)
        val followers = ordered.filter { it.id != leader }.mapNotNull { boltAddress(it.id) }
        val readers =
            when {
                leaderAddress == null -> followers
                readsOnLeader || followers.isEmpty() -> followers + leaderAddress
                else -> followers
            }
        return RoutingEndpoints(
            routers = ordered.mapNotNull { boltAddress(it.id) },
            writers = listOfNotNull(leaderAddress),
            readers = readers,
        )
    }
}
