package dev.dodeca.cluster

import dev.dodeca.consensus.HostPort
import dev.dodeca.consensus.Member
import dev.dodeca.consensus.MemberId
import dev.dodeca.core.servers.ServerRules
import kotlin.random.Random

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
     * ([readsOnPrimaries]) and one can, the secondaries, and the leader if [readsOnWriters], or
     * when no other member can be listed. Routers: every member and secondary. Members whose Bolt
     * address is not known yet, and members and secondaries that are not [available], are left out;
     * during an election there is no writer, so drivers retry until there is one. Members whose
     * copy of the database [failed], such as by a panic, neither write nor read it, but still
     * route: their routing tables send drivers to the others.
     *
     * The routing [policy] a driver named chooses among the readers and among the routers by the
     * servers' [tags]; the writer is the leader whatever it says. A policy that passes no reader
     * leaves the table without one; one that passes no router leaves the leader as the router, so
     * that drivers can still ask for a table, or while there is no leader, the routers it was
     * given. With [random], readers and routers are listed in a random order, else in the order of
     * their ids.
     */
    fun endpoints(
        leader: MemberId?,
        members: List<Member>,
        boltAddress: (MemberId) -> HostPort?,
        readsOnWriters: Boolean,
        failed: Set<MemberId>,
        readOnly: Set<MemberId> = emptySet(),
        secondaries: List<MemberId> = emptyList(),
        readsOnPrimaries: Boolean = true,
        available: (MemberId) -> Boolean = { true },
        policy: ServerRules = ServerRules.ALL,
        tags: (MemberId) -> Set<String> = { emptySet() },
        random: Random? = null,
    ): RoutingEndpoints {
        val ordered = members.map { it.id }.sorted()
        val addresses = (ordered + secondaries).distinct().associateWith(boltAddress)
        fun listable(member: MemberId) = addresses[member] != null && available(member)
        val serving = ordered.filter { it !in failed }
        val writer = leader?.takeIf { it in serving && addresses[it] != null }
        val followers = serving.filter { it != writer && listable(it) }
        val replicas = secondaries.sorted().filter { it !in failed && listable(it) }
        val nonWriters =
            (if (readsOnPrimaries || replicas.isEmpty()) followers else emptyList()) + replicas
        val readers =
            when {
                writer == null -> nonWriters
                readsOnWriters || nonWriters.isEmpty() -> nonWriters + writer
                else -> nonWriters
            }
        val routers = (ordered + secondaries.sorted()).distinct().filter(::listable)
        fun List<MemberId>.listed() =
            (if (random == null) this else shuffled(random)).map { addresses.getValue(it)!! }
        return RoutingEndpoints(
            routers =
                policy
                    .select(routers, tags)
                    .ifEmpty {
                        leader?.takeIf { addresses[it] != null }?.let(::listOf) ?: routers
                    }
                    .listed(),
            // A leader where the database is read-only takes no writes; it hands its leadership
            // over to a member where it is writable, if there is one.
            writers = listOfNotNull(writer?.takeIf { it !in readOnly }?.let(addresses::getValue)),
            readers = policy.select(readers, tags).listed(),
        )
    }
}
