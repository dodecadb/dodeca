package org.dozendb.cluster.core

import org.dozendb.cluster.api.ConsensusLog
import org.dozendb.cluster.api.DatabaseRef
import org.dozendb.cluster.api.GroupStatus
import org.dozendb.cluster.api.Member
import org.dozendb.cluster.api.Role
import org.neo4j.logging.Log

/** A voting member as seen from this member. */
data class MemberView(val member: Member, val isLeader: Boolean, val isSelf: Boolean)

/**
 * Runs this member's part of the cluster: starts consensus, forms or rejoins the `system` group,
 * and reports status. It knows nothing about Neo4j internals.
 */
class ClusterService(
    private val consensus: ConsensusLog,
    val systemDatabase: DatabaseRef,
    private val formation: ClusterFormation,
    private val userLog: Log,
) {
    private var statusListener: AutoCloseable? = null

    /** Starts consensus and blocks until this member takes part in the `system` group. */
    fun start() {
        consensus.start()
        val bootstrap =
            if (systemDatabase.id in consensus.localGroups()) {
                userLog.info("Rejoining the cluster from local Raft state")
                emptyList()
            } else {
                formation.awaitInitialMembers(systemDatabase.id)
            }
        statusListener = consensus.addStatusListener(systemDatabase, RoleChangeLogger(userLog))
        consensus.startGroup(systemDatabase, NoOpStateMachine(), bootstrap)
    }

    fun stop() {
        statusListener?.close()
        statusListener = null
        consensus.close()
    }

    val self: Member
        get() = consensus.self

    /** The local status of every group this member runs. */
    fun status(): List<GroupStatus> = listOf(consensus.status(systemDatabase))

    /** The voting members of the `system` group. */
    fun members(): List<MemberView> {
        val leader = consensus.status(systemDatabase).leader
        return consensus.members(systemDatabase).map {
            MemberView(it, isLeader = it.id == leader, isSelf = it.id == consensus.self.id)
        }
    }

    private class RoleChangeLogger(private val log: Log) : (GroupStatus) -> Unit {
        private var last: Pair<Role, Any?>? = null

        @Synchronized
        override fun invoke(status: GroupStatus) {
            val current = status.role to status.leader
            if (current == last) return
            last = current
            log.info(
                "Raft group %s: this member is %s, leader %s, term %d",
                status.database.name,
                status.role.displayName,
                status.leader ?: "unknown",
                status.term,
            )
        }
    }
}

/** The name of a role in logs and procedure output. */
val Role.displayName: String
    get() =
        when (this) {
            Role.Leader -> "LEADER"
            Role.Follower -> "FOLLOWER"
            Role.Candidate -> "CANDIDATE"
            Role.Inactive -> "INACTIVE"
        }
