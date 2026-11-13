package dev.dodeca.cluster

import dev.dodeca.consensus.Member
import dev.dodeca.consensus.MemberId
import java.util.UUID

/**
 * The voting members of a group as members that run it report them (see
 * `dev.dodeca.consensus.PeerInfo.groups`), for members that do not run it.
 */
object GroupViews {
    /**
     * The views of the voting members of group [id] that its voters report in [reports], the groups
     * each member runs. A member waiting to be added, or one removed, knows other members than the
     * voters, so only the views that count their reporter in are kept.
     */
    fun reportedByVoters(
        id: UUID,
        reports: Map<MemberId, Map<UUID, List<Member>>>,
    ): Map<MemberId, List<Member>> =
        reports
            .mapNotNull { (member, groups) ->
                groups[id]
                    ?.takeIf { voters -> voters.any { it.id == member } }
                    ?.let { member to it }
            }
            .toMap()

    /** The voting members that all [views] agree on, or null if they disagree or there are none. */
    fun agreed(views: Collection<List<Member>>): List<Member>? =
        views.firstOrNull()?.takeIf { first ->
            views.all { view -> view.map { it.id }.toSet() == first.map { it.id }.toSet() }
        }
}
