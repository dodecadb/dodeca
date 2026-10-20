package org.dozendb.cluster.core

import java.time.Duration
import kotlinx.coroutines.runBlocking
import org.dozendb.cluster.api.ConsensusLog
import org.dozendb.cluster.api.ConsensusUnavailableException
import org.dozendb.cluster.api.DatabaseRef
import org.dozendb.cluster.api.GroupStatus
import org.dozendb.cluster.api.HostPort
import org.dozendb.cluster.api.Member
import org.dozendb.cluster.api.MemberId
import org.dozendb.cluster.api.PeerInfo
import org.dozendb.cluster.api.Role
import org.dozendb.cluster.api.Snapshot
import org.neo4j.logging.Log

/**
 * A voting member as seen from this member; [boltAddress] is null while it is not known.
 *
 * @property databases the databases whose groups the member votes in, as this member knows them.
 */
data class MemberView(
    val member: Member,
    val isLeader: Boolean,
    val isSelf: Boolean,
    val boltAddress: HostPort? = null,
    val databases: List<String> = emptyList(),
)

/** A change of the cluster's members that cannot be made; the message tells the operator why. */
class MembershipChangeException(message: String, cause: Throwable? = null) :
    RuntimeException(message, cause)

/**
 * Runs this member's part of the cluster: starts consensus, forms, joins or rejoins the `system`
 * group, makes sure this member starts from the cluster's databases, follows and changes the
 * cluster's members, and reports status. It knows nothing about Neo4j internals.
 *
 * The members of the `system` group are the members of the cluster. A member that starts without
 * Raft state and finds the cluster formed without it joins it: it copies the databases it needs to
 * start, runs the `system` group without voting in it, and waits until a member of the cluster adds
 * it with [addMember]. A member removed with [removeMember] stops its databases and takes no part
 * in the cluster any more; to be added again it needs its data directory deleted, after which it
 * joins as a new member.
 *
 * @property metadata what this member reports when probed, compared with a member being added.
 * @property replicatedDatabases the replicated databases running on this member.
 * @property directory the members' Bolt addresses, if this member serves Bolt.
 */
class ClusterService(
    private val consensus: ConsensusLog,
    val systemDatabase: DatabaseRef,
    private val formation: ClusterFormation,
    private val stores: LocalStores,
    private val membershipStore: MembershipStore,
    private val userLog: Log,
    private val metadata: () -> Map<String, String> = { emptyMap() },
    private val replicatedDatabases: () -> List<DatabaseRef> = { emptyList() },
    val directory: MemberDirectory? = null,
) {
    private var statusListener: AutoCloseable? = null

    @Volatile private var record: MembershipRecord? = null

    /**
     * The members this member formed the cluster with, if it is one of them: the initial members of
     * the groups of `system` and the default database. Null for a member that joined later.
     */
    val foundingMembers: List<Member>?
        get() = record?.formation?.takeIf { members -> members.any { it.id == self.id } }

    /** This member's place in the cluster. */
    val state: MemberState
        get() = record?.state ?: MemberState.JOINING

    /** Runs once this member learns that it was removed from the cluster, on an internal thread. */
    @Volatile var onRemoved: () -> Unit = {}

    /**
     * Starts consensus and blocks until this member takes part in the `system` group, with the
     * databases it lacks copied from another member. Runs before any database starts.
     */
    fun start() {
        consensus.start()
        record = membershipStore.read()
        val joined =
            if (systemDatabase.id in consensus.localGroups()) {
                userLog.info("Rejoining the cluster from local Raft state")
                null
            } else {
                formation.awaitFormation(systemDatabase.id).also { found ->
                    remember(
                        if (found.members.any { it.id == self.id }) {
                            MembershipRecord(MemberState.MEMBER, found.members)
                        } else {
                            MembershipRecord(MemberState.JOINING)
                        }
                    )
                }
            }
        val missing = stores.missing()
        if (missing.isNotEmpty()) {
            val source = joined?.storeSource
            if (source != null && source.id == consensus.self.id) {
                userLog.info("This member creates the databases of the new cluster")
            } else {
                copyStores(source, missing)
            }
        }
        val roleLogger = RoleChangeLogger(userLog)
        statusListener =
            consensus.addStatusListener(systemDatabase) {
                roleLogger(it)
                followMembership()
            }
        // The system database attaches its state machine when it starts. A joining member's
        // bootstrap members leave it out: it votes once it is added.
        consensus.joinGroup(systemDatabase, joined?.members ?: emptyList())
        followMembership()
        if (state == MemberState.REMOVED) userLog.error(REMOVED_MESSAGE)
        if (state == MemberState.JOINING) {
            userLog.info(
                "This member is not part of the cluster yet. To add it, run " +
                    "CALL dozendb.cluster.addMember('%s') on a member of the cluster.",
                self.raftAddress,
            )
        }
        directory?.start()
    }

    /**
     * Records that this member became a voting member, once the change of members settled, and that
     * it was removed: Ratis stopped its group, or the settled members leave it out.
     */
    @Synchronized
    private fun followMembership() {
        val membership =
            try {
                consensus.membership(systemDatabase)
            } catch (_: Exception) {
                // Not running yet, or stopping.
                return
            }
        val included = membership.members.any { it.id == self.id }
        if (state == MemberState.JOINING && membership.settled && included) {
            remember(MembershipRecord(MemberState.MEMBER, record?.formation.orEmpty()))
            userLog.info(
                "This member is now one of the cluster's members; it joins the group of each " +
                    "database once it has copied it"
            )
        }
        if (
            state == MemberState.MEMBER && (membership.removed || (membership.settled && !included))
        ) {
            remember(MembershipRecord(MemberState.REMOVED, record?.formation.orEmpty()))
            userLog.error(REMOVED_MESSAGE)
            onRemoved()
        }
    }

    private fun remember(update: MembershipRecord) {
        membershipStore.write(update)
        record = update
    }

    private fun copyStores(source: Member?, databases: List<String>) {
        while (true) {
            val address = formation.awaitStoreSource(source)
            userLog.info("Copying databases %s from %s", databases.joinToString(), address)
            try {
                stores.copyFrom(address, databases)
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
        directory?.stop()
        statusListener?.close()
        statusListener = null
        consensus.close()
    }

    val self: Member
        get() = consensus.self

    /** The local status of every group this member runs. */
    fun status(): List<GroupStatus> =
        (listOf(systemDatabase) + replicatedDatabases()).distinct().map(consensus::status)

    /** The voting members of the group of [database], as this member knows them. */
    fun groupMembers(database: DatabaseRef): List<Member> = consensus.members(database)

    /** The latest snapshot of [database] on this member: its log may lack entries up to it. */
    fun latestSnapshot(database: DatabaseRef): Snapshot? = consensus.latestSnapshot(database)

    /** The voting members of the `system` group, the members of the cluster. */
    fun members(): List<MemberView> {
        val leader = consensus.status(systemDatabase).leader
        val groups =
            replicatedDatabases()
                .filter { it.id != systemDatabase.id }
                .associateWith { database ->
                    try {
                        consensus.members(database).map { it.id }
                    } catch (_: ConsensusUnavailableException) {
                        emptyList()
                    }
                }
        return consensus.members(systemDatabase).map { member ->
            MemberView(
                member,
                isLeader = member.id == leader,
                isSelf = member.id == consensus.self.id,
                boltAddress = directory?.boltAddress(member.id),
                databases =
                    listOf(systemDatabase.name) +
                        groups.filterValues { member.id in it }.keys.map { it.name }.sorted(),
            )
        }
    }

    /**
     * Adds the member at [address] to the cluster, which first brings it up to date with the
     * `system` group, and returns it. It must wait to join this cluster: started with initial
     * members of this cluster, without Raft state of its own, and on the same Neo4j version.
     * Returns once it votes in the `system` group, at once if it is a member already. It joins the
     * group of each database once it has copied it.
     *
     * @throws MembershipChangeException if the member cannot be added.
     */
    fun addMember(address: HostPort): Member {
        requireMember()
        val peer =
            try {
                runBlocking { consensus.probe(address) }
            } catch (e: ConsensusUnavailableException) {
                throw MembershipChangeException(
                    "No DozenDB member answers at $address: ${e.message}",
                    e,
                )
            }
        val candidate = peer.member
        if (consensus.members(systemDatabase).any { it.id == candidate.id }) return candidate
        checkCanJoin(address, peer)
        userLog.info("Adding member %s to the cluster", candidate)
        try {
            runBlocking { consensus.addMember(systemDatabase, candidate, CHANGE_TIMEOUT) }
        } catch (e: ConsensusUnavailableException) {
            throw MembershipChangeException("Adding member $candidate failed: ${e.message}", e)
        }
        userLog.info("Added member %s to the cluster", candidate)
        return candidate
    }

    /**
     * Removes the member [id] from the cluster: from the `system` group, which it then stops
     * running. Returns once the change is committed; the group of each database follows, and the
     * removed member stops its databases.
     *
     * @throws MembershipChangeException if [id] is not a member, is the only one, or the change
     *   failed.
     */
    fun removeMember(id: MemberId) {
        requireMember()
        val members = consensus.members(systemDatabase)
        val member =
            members.firstOrNull { it.id == id }
                ?: throw MembershipChangeException("$id is not a member of the cluster.")
        if (members.size == 1) {
            throw MembershipChangeException(
                "$member is the only member of the cluster and cannot be removed."
            )
        }
        userLog.info("Removing member %s from the cluster", member)
        try {
            runBlocking { consensus.removeMember(systemDatabase, id, CHANGE_TIMEOUT) }
        } catch (e: ConsensusUnavailableException) {
            throw MembershipChangeException("Removing member $member failed: ${e.message}", e)
        }
        userLog.info("Removed member %s from the cluster", member)
    }

    private fun requireMember() {
        if (state != MemberState.MEMBER) {
            throw MembershipChangeException(
                "This member is not one of the cluster's members; change members on one that is."
            )
        }
    }

    private fun checkCanJoin(address: HostPort, peer: PeerInfo) {
        val own = metadata()
        val candidate = "Member ${peer.member.id} at $address"
        val version = peer.metadata[DozenDbMetadata.NEO4J_VERSION]
        if (version != own[DozenDbMetadata.NEO4J_VERSION]) {
            throw MembershipChangeException(
                "$candidate runs Neo4j $version, but this cluster runs Neo4j " +
                    "${own[DozenDbMetadata.NEO4J_VERSION]}. All members must run the same version."
            )
        }
        when (peer.metadata[DozenDbMetadata.MEMBER_STATE]) {
            MemberState.JOINING.name -> {}
            MemberState.REMOVED.name ->
                throw MembershipChangeException(
                    "$candidate was removed from the cluster and cannot be added again. Delete " +
                        "its data directory and start it again: it then joins as a new member."
                )
            else -> throw MembershipChangeException("$candidate is a member of another cluster.")
        }
        val clusterId = peer.metadata[DozenDbMetadata.CLUSTER_ID]
        if (clusterId == null || systemDatabase.id !in peer.groups) {
            throw MembershipChangeException(
                "$candidate is still starting; add it once it has started."
            )
        }
        if (clusterId != own[DozenDbMetadata.CLUSTER_ID]) {
            throw MembershipChangeException(
                "$candidate waits to join another cluster: its databases come from there. Check " +
                    "its ${DozenDbMetadata.INITIAL_MEMBERS_SETTING}, delete its data directory " +
                    "and start it again."
            )
        }
    }

    private companion object {
        /** How long a change of members may take: bringing a new member up to date included. */
        val CHANGE_TIMEOUT: Duration = Duration.ofMinutes(10)

        const val REMOVED_MESSAGE =
            "This member was removed from the cluster: it takes no part in it any more and its " +
                "databases are stopped. Shut it down. To add it again, delete its data directory " +
                "first, then start it: it joins as a new member."
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
