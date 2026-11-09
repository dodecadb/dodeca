package org.dozendb.cluster

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
     * Writers: the [leader]. Readers: the followers, unless only [secondaries] are to read
     * ([readsOnPrimaries]) and one can, the secondaries, and the leader if [readsOnLeader], or when
     * no other member can be listed. Routers: every member and secondary. Members whose Bolt
     * address is not known yet are left out; during an election there is no writer, so drivers
     * retry until there is one. Members whose copy of the database [failed], such as by a panic,
     * neither write nor read it, but still route: their routing tables send drivers to the others.
     */
    fun endpoints(
        leader: MemberId?,
        members: List<Member>,
        boltAddress: (MemberId) -> HostPort?,
        readsOnLeader: Boolean,
        failed: Set<MemberId>,
        readOnly: Set<MemberId> = emptySet(),
        secondaries: List<MemberId> = emptyList(),
        readsOnPrimaries: Boolean = true,
    ): RoutingEndpoints {
        val ordered = members.sortedBy { it.id }
        val serving = ordered.filter { it.id !in failed }
        val writer = leader?.takeIf { id -> serving.any { it.id == id } }
        val leaderAddress = writer?.let(boltAddress)
        val followers = serving.filter { it.id != writer }.mapNotNull { boltAddress(it.id) }
        val replicas = secondaries.sorted().filter { it !in failed }.mapNotNull(boltAddress)
        val nonWriters =
            (if (readsOnPrimaries || replicas.isEmpty()) followers else emptyList()) + replicas
        val readers =
            when {
                leaderAddress == null -> nonWriters
                readsOnLeader || nonWriters.isEmpty() -> nonWriters + leaderAddress
                else -> nonWriters
            }
        return RoutingEndpoints(
            routers =
                (ordered.map { it.id } + secondaries.sorted()).distinct().mapNotNull(boltAddress),
            // A leader where the database is read-only takes no writes; it hands its leadership
            // over to a member where it is writable, if there is one.
            writers = listOfNotNull(leaderAddress.takeIf { writer !in readOnly }),
            readers = readers,
        )
    }
}
