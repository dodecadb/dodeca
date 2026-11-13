package dev.dodeca.cluster.internal

import dev.dodeca.cluster.DatabaseGroups
import dev.dodeca.cluster.GroupViews
import dev.dodeca.cluster.internal.replication.LocalDatabases
import dev.dodeca.cluster.internal.replication.ModeSwitch
import dev.dodeca.consensus.ConsensusLog
import dev.dodeca.consensus.ConsensusUnavailableException
import dev.dodeca.consensus.DatabaseRef
import dev.dodeca.consensus.Member
import dev.dodeca.core.servers.CopyMode
import java.time.Duration
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.logging.InternalLog

/**
 * Replaces the cluster state of a copy in quarantine on this member, for the operations
 * `replaceStateKeepStore` and `replaceStateReplaceStore` of `dbms.unquarantineDatabase`, so that
 * the copy joins its database as a new member once the quarantine is lifted, as Neo4j Enterprise
 * documents them.
 *
 * A primary leaves its database's Raft group, which a majority of the group's voters must let it
 * do, and joins it again as a member that the group adds: from a new copy of the database, or from
 * its own store, which seeds its new state of the group with the log entry of its last transaction,
 * as a secondary that turns primary does, as long as the group's leader still holds that entry; it
 * is copied otherwise. It first deletes its own state of the group, so that it no longer runs the
 * group, and only then asks the other voters to remove it: one that ran the group meanwhile could
 * be added again with its old state, which it would then delete as a voter, forgetting its votes.
 * Until the group removed it, it is a voter that does not answer; a copy in quarantine does not
 * start, nor does its group run. If the removal fails, the copy stays in quarantine, without its
 * old state: the operation can be run again.
 *
 * A secondary votes in no group: it is copied again, or kept as it is.
 *
 * @property members the members of the cluster, which tell the group's voters where this member
 *   does not run the group.
 * @property timeout how long the group may take to remove this member.
 */
internal class CopyReplacement(
    private val consensus: ConsensusLog,
    private val groups: DatabaseGroups,
    private val databases: LocalDatabases,
    private val members: () -> Collection<Member>,
    private val timeout: Duration,
    private val log: InternalLog,
) {
    /**
     * Replaces the cluster state of the copy of [id] here, from its store if [keepStore], else from
     * a new copy.
     *
     * @throws IllegalStateException saying why, if it cannot.
     */
    fun replace(id: NamedDatabaseId, keepStore: Boolean) {
        val state = databases.registered()[id]
        if (state?.mode == CopyMode.SECONDARY) {
            if (!keepStore) {
                log.info("Deleting the secondary %s here, in quarantine: it is copied again", id)
                databases.drop(id)
            }
            return
        }
        val ref = groups.of(id)
        val self = consensus.self.id
        val voters =
            checkNotNull(voters(ref)) {
                "Database '${id.name()}': the members of its Raft group cannot be told, as none " +
                    "that votes in it answers."
            }
        val voting = voters.any { it.id == self }
        check(!voting || voters.any { it.id != self }) {
            "Database '${id.name()}': the copy here is its only primary, which has no group to " +
                "join as a new member. Lift its quarantine with keepStateKeepStore, or recreate " +
                "the database."
        }
        if (!keepStore || !rejoinFromStore(id)) {
            if (state != null) {
                log.info(
                    "Deleting %s here, in quarantine, and its Raft group: it joins the group as " +
                        "a new member, from a new copy",
                    id,
                )
                databases.drop(id)
            } else if (ref.id in consensus.localGroups()) {
                consensus.removeGroup(ref)
            }
        }
        if (!voting) return
        try {
            runBlocking { consensus.removeMember(ref, self, timeout, voters) }
        } catch (e: ConsensusUnavailableException) {
            throw IllegalStateException(
                "Database '${id.name()}': its Raft group did not remove the copy here, which " +
                    "joins it as a new member: ${e.message}. The copy here is deleted and stays " +
                    "in quarantine; run the operation again once a majority of the database's " +
                    "primaries answers.",
                e,
            )
        }
        log.info("The Raft group of %s removed this member, which joins it as a new member", id)
    }

    /**
     * Replaces the Raft state of the copy of [id] by a seed from its store; false if its group's
     * log moved on, so that it needs a new copy.
     */
    private fun rejoinFromStore(id: NamedDatabaseId): Boolean =
        when (databases.rejoinFromStore(id)) {
            ModeSwitch.SWITCHED -> {
                log.info(
                    "Deleted the Raft group of %s here, in quarantine: it joins the group as a " +
                        "new member, from its store",
                    id,
                )
                true
            }
            ModeSwitch.NEEDS_COPY -> {
                log.info(
                    "The Raft group of %s no longer holds the transactions after the store here: " +
                        "the copy joins as a new member from a new copy instead",
                    id,
                )
                false
            }
            ModeSwitch.LATER ->
                throw IllegalStateException(
                    "Database '${id.name()}': its group's leader cannot tell the term of the last " +
                        "transaction of the copy here now. Run the operation again once the group " +
                        "has a leader."
                )
        }

    /**
     * The voting members of [ref]: this member's view where it votes in the group, else the view
     * that the members voting in it report, if they agree; null if none answers or they disagree.
     */
    private fun voters(ref: DatabaseRef): List<Member>? {
        val local =
            try {
                consensus.membership(ref)
            } catch (_: ConsensusUnavailableException) {
                null
            }
        if (local != null && !local.removed && local.members.any { it.id == consensus.self.id }) {
            return local.members
        }
        val reports =
            runBlocking(Dispatchers.IO) {
                members()
                    .filter { it.id != consensus.self.id }
                    .map { member ->
                        async {
                            try {
                                consensus
                                    .probe(member.raftAddress)
                                    .takeIf { it.member.id == member.id }
                                    ?.let { member.id to it.groups }
                            } catch (_: ConsensusUnavailableException) {
                                null
                            }
                        }
                    }
                    .awaitAll()
                    .filterNotNull()
                    .toMap()
            }
        return GroupViews.agreed(GroupViews.reportedByVoters(ref.id, reports).values)
    }
}
