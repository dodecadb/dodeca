package dev.dodeca.cluster.internal.topology

import dev.dodeca.cluster.GroupViews
import dev.dodeca.cluster.MemberDirectory
import dev.dodeca.consensus.ConsensusLog
import dev.dodeca.consensus.ConsensusUnavailableException
import dev.dodeca.consensus.DatabaseRef
import dev.dodeca.consensus.Member
import dev.dodeca.consensus.MemberId
import dev.dodeca.core.edition.DatabaseState
import dev.dodeca.core.servers.CopyMode
import java.util.UUID
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel
import org.neo4j.graphdb.Transaction

/**
 * Which members vote in a database's group and which one leads it, as this member knows it: its own
 * view where it votes in the group, else what the members that vote in it last reported (see
 * [MemberDirectory]), so that a member that does not host a database can still send clients to
 * those that do. Also which members run its secondaries.
 *
 * @property placedSecondaries the servers the system graph places a database's secondaries on.
 */
internal class DatabaseLocations(
    private val consensus: ConsensusLog,
    private val directory: MemberDirectory,
    private val placedSecondaries: (DatabaseRef) -> Set<MemberId> = { emptySet() },
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

    /**
     * The databases of the system graph that [tx] reads, `system` aside, whose groups [member]
     * votes in, as far as known: a member that does not answer counts as it last reported.
     */
    fun votedIn(tx: Transaction, member: MemberId): Set<String> =
        tx.findNodes(TopologyGraphDbmsModel.DATABASE_LABEL).use { nodes ->
            nodes
                .asSequence()
                .mapNotNull { node ->
                    val name =
                        node.getProperty(TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY, null)
                            as? String
                    val id =
                        (node.getProperty(TopologyGraphDbmsModel.DATABASE_UUID_PROPERTY, null)
                                as? String)
                            ?.let { runCatching { UUID.fromString(it) }.getOrNull() }
                    if (name == null || id == null || name == SYSTEM_DATABASE_NAME) null
                    else DatabaseRef(name, id)
                }
                .filter { database -> of(database)?.voters.orEmpty().any { it.id == member } }
                .mapTo(HashSet()) { it.name }
        }

    /**
     * The members that run a secondary of [database] and pull, as they last reported, where the
     * system graph places one: a server that it no longer places one on drops it, maybe before it
     * reports so. A paused secondary serves no data it stopped updating.
     */
    fun secondaries(database: DatabaseRef): List<MemberId> =
        placedSecondaries(database).sorted().filter { member ->
            directory.databaseStates(member).orEmpty().any {
                it.id == database.id &&
                    it.mode == CopyMode.SECONDARY &&
                    it.status == DatabaseState.Status.ONLINE &&
                    !it.paused
            }
        }

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
