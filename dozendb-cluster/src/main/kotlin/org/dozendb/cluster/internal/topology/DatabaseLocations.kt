package org.dozendb.cluster.internal.topology

import org.dozendb.cluster.GroupViews
import org.dozendb.cluster.MemberDirectory
import org.dozendb.consensus.ConsensusLog
import org.dozendb.consensus.ConsensusUnavailableException
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.Member
import org.dozendb.consensus.MemberId

/**
 * Which members vote in a database's group and which one leads it, as this member knows it: its own
 * view where it votes in the group, else what the members that vote in it last reported (see
 * [MemberDirectory]), so that a member that does not host a database can still send clients to
 * those that do.
 */
internal class DatabaseLocations(
    private val consensus: ConsensusLog,
    private val directory: MemberDirectory,
) {
    /**
     * @property voters the voting members of the group.
     * @property leader the member that leads it, if one is known.
     * @property local whether this member votes in the group, so that this is its own view.
     * @property settled whether no change of the voting members is under way, as far as known.
     */
    data class Location(
        val voters: List<Member>,
        val leader: MemberId?,
        val local: Boolean,
        val settled: Boolean,
    )

    /** Where [database] is, or null while no member is known to vote in its group. */
    fun of(database: DatabaseRef): Location? {
        val local =
            try {
                consensus.membership(database)
            } catch (_: ConsensusUnavailableException) {
                null
            }
        if (local != null && !local.removed && local.members.any { it.id == consensus.self.id }) {
            val leader =
                try {
                    consensus.status(database).leader
                } catch (_: ConsensusUnavailableException) {
                    null
                }
            return Location(local.members, leader, local = true, settled = local.settled)
        }
        val views = GroupViews.reportedByVoters(database.id, directory.groups())
        // The leader with the highest term: one that lost the leadership may not have said so yet.
        val leader =
            views.keys
                .mapNotNull { member ->
                    directory.leading(member)[database.id]?.let { member to it }
                }
                .maxByOrNull { it.second }
                ?.first
        if (views.isEmpty() || leader == null) directory.refreshSoon()
        // While its members change, members report different voters: the leader knows best.
        val agreed = GroupViews.agreed(views.values)
        val voters = agreed ?: leader?.let(views::get) ?: return null
        return Location(
            voters,
            leader?.takeIf { id -> voters.any { it.id == id } },
            local = false,
            settled = agreed != null,
        )
    }
}
