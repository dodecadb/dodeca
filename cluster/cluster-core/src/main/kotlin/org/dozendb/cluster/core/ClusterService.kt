package org.dozendb.cluster.core

import org.dozendb.cluster.api.ConsensusLog
import org.dozendb.cluster.api.ConsensusUnavailableException
import org.dozendb.cluster.api.DatabaseRef
import org.dozendb.cluster.api.GroupStatus
import org.dozendb.cluster.api.Member
import org.dozendb.cluster.api.Role
import org.neo4j.logging.Log

/** A voting member as seen from this member. */
data class MemberView(val member: Member, val isLeader: Boolean, val isSelf: Boolean)

/**
 * Runs this member's part of the cluster: starts consensus, forms or rejoins the `system` group,
 * makes sure this member starts from the cluster's databases, and reports status. It knows nothing
 * about Neo4j internals.
 *
 * @property replicatedDatabases the replicated databases running on this member.
 */
class ClusterService(
    private val consensus: ConsensusLog,
    val systemDatabase: DatabaseRef,
    private val formation: ClusterFormation,
    private val stores: LocalStores,
    private val userLog: Log,
    private val replicatedDatabases: () -> List<DatabaseRef> = { emptyList() },
) {
    private var statusListener: AutoCloseable? = null

    /**
     * Starts consensus and blocks until this member takes part in the `system` group, with
     * databases copied from another member if it has none. Runs before any database starts.
     */
    fun start() {
        consensus.start()
        val joined =
            if (systemDatabase.id in consensus.localGroups()) {
                userLog.info("Rejoining the cluster from local Raft state")
                null
            } else {
                formation.awaitFormation(systemDatabase.id)
            }
        if (!stores.existed) {
            val source = joined?.storeSource
            if (source != null && source.id == consensus.self.id) {
                userLog.info("This member creates the databases of the new cluster")
            } else {
                copyStores(source)
            }
        }
        val listener = RoleChangeLogger(userLog)
        statusListener = consensus.addStatusListener(systemDatabase, listener)
        consensus.startGroup(systemDatabase, NoOpStateMachine(), joined?.members ?: emptyList())
    }

    private fun copyStores(source: Member?) {
        while (true) {
            val address = formation.awaitStoreSource(source)
            userLog.info("Copying the databases from %s", address)
            try {
                stores.copyFrom(address)
                return
            } catch (e: ConsensusUnavailableException) {
                userLog.warn(
                    "Copying the databases from %s failed, retrying: %s",
                    address,
                    e.message,
                )
            } catch (e: java.io.IOException) {
                userLog.warn(
                    "Copying the databases from %s failed, retrying: %s",
                    address,
                    e.message,
                )
            }
            formation.pause()
        }
    }

    fun stop() {
        statusListener?.close()
        statusListener = null
        consensus.close()
    }

    val self: Member
        get() = consensus.self

    /** The local status of every group this member runs. */
    fun status(): List<GroupStatus> =
        (listOf(systemDatabase) + replicatedDatabases()).map(consensus::status)

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
