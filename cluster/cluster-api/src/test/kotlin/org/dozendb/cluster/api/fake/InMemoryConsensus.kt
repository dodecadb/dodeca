package org.dozendb.cluster.api.fake

import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import org.dozendb.cluster.api.CommitResult
import org.dozendb.cluster.api.ConsensusLog
import org.dozendb.cluster.api.ConsensusUnavailableException
import org.dozendb.cluster.api.DatabaseRef
import org.dozendb.cluster.api.GroupStatus
import org.dozendb.cluster.api.HostPort
import org.dozendb.cluster.api.LogEntry
import org.dozendb.cluster.api.Member
import org.dozendb.cluster.api.MemberId
import org.dozendb.cluster.api.PeerInfo
import org.dozendb.cluster.api.ReplicatedStateMachine
import org.dozendb.cluster.api.Role

/**
 * A deterministic, single-JVM stand-in for a cluster of [ConsensusLog] members.
 *
 * There are no elections: tests choose the leader with [electLeader]. An entry submitted on the
 * leader commits immediately when a majority of the group's members are running, and is applied
 * synchronously, in order, by every running member. Starting a group replays the whole log, so
 * state machines must skip entries they have already applied, as with a real implementation. Faults
 * are injected with [crash] and [failNextSubmit].
 *
 * All operations are serialised on one lock. State machines must not call back into the fake from
 * [ReplicatedStateMachine.apply].
 */
class InMemoryConsensus {
    private val lock = Any()
    private val members = mutableMapOf<MemberId, InMemoryConsensusLog>()
    private val groups = mutableMapOf<UUID, GroupState>()
    private var nextPort = 7700

    private class GroupState(val database: DatabaseRef, var members: List<Member>) {
        var term = 0L
        var leader: MemberId? = null
        val log = mutableListOf<LogEntry>()
        var injectedFailure: CommitResult? = null
    }

    fun newMember(
        id: MemberId = MemberId(UUID.randomUUID()),
        address: HostPort? = null,
        metadata: Map<String, String> = emptyMap(),
    ): InMemoryConsensusLog =
        synchronized(lock) {
            check(id !in members) { "member $id already exists" }
            val raftAddress = address ?: HostPort("member-${members.size + 1}", nextPort++)
            InMemoryConsensusLog(Member(id, raftAddress), metadata).also { members[id] = it }
        }

    /** Makes [leader] the leader of [database] in a new term, as if it had won an election. */
    fun electLeader(database: DatabaseRef, leader: MemberId) {
        val notifications =
            synchronized(lock) {
                val group = group(database)
                check(group.members.any { it.id == leader }) { "$leader is not a member" }
                check(members.getValue(leader).isRunning(database)) { "$leader is not running" }
                group.term++
                group.leader = leader
                statusNotifications(database)
            }
        notifications.forEach { it() }
    }

    /** Stops [member] abruptly: all its groups become inactive and it can no longer be probed. */
    fun crash(member: MemberId) {
        val notifications =
            synchronized(lock) {
                val log = members.getValue(member)
                val affected = log.runningGroups()
                log.crashLocked()
                affected.flatMap { database ->
                    val group = group(database)
                    if (group.leader == member) group.leader = null
                    statusNotifications(database)
                }
            }
        notifications.forEach { it() }
    }

    /** Makes the next [ConsensusLog.submit] to [database] return [result] without appending. */
    fun failNextSubmit(database: DatabaseRef, result: CommitResult) {
        require(result !is CommitResult.Committed) { "inject a failure, not a success" }
        synchronized(lock) { group(database).injectedFailure = result }
    }

    /** The committed entries of [database]. */
    fun committedEntries(database: DatabaseRef): List<LogEntry> =
        synchronized(lock) { group(database).log.toList() }

    private fun group(database: DatabaseRef): GroupState =
        groups[database.id] ?: throw IllegalStateException("group $database does not exist")

    private fun majorityRunning(group: GroupState): Boolean {
        val running = group.members.count { members[it.id]?.isRunning(group.database) == true }
        return running > group.members.size / 2
    }

    private fun deliverAll(group: GroupState) {
        group.members
            .mapNotNull { members[it.id] }
            .forEach { it.deliver(group.database, group.log) }
    }

    private fun statusNotifications(database: DatabaseRef): List<() -> Unit> =
        members.values.flatMap { it.statusNotifications(database) }

    /** One member of an [InMemoryConsensus]. */
    inner class InMemoryConsensusLog(
        override val self: Member,
        private val metadata: Map<String, String>,
    ) : ConsensusLog {
        private var transportUp = false
        private val localState = mutableSetOf<UUID>()
        private val running = mutableMapOf<UUID, ReplicatedStateMachine>()
        private val delivered = mutableMapOf<UUID, Long>()
        private val listeners = mutableMapOf<UUID, CopyOnWriteArrayList<(GroupStatus) -> Unit>>()

        internal fun isRunning(database: DatabaseRef): Boolean = database.id in running

        internal fun runningGroups(): List<DatabaseRef> =
            running.keys.map { groups.getValue(it).database }

        internal fun crashLocked() {
            transportUp = false
            running.clear()
            delivered.clear()
        }

        internal fun deliver(database: DatabaseRef, log: List<LogEntry>) {
            val stateMachine = running[database.id] ?: return
            var index = delivered.getOrDefault(database.id, 0L)
            while (index < log.size) {
                val entry = log[index.toInt()]
                if (entry.index > stateMachine.lastAppliedIndex()) stateMachine.apply(entry)
                index++
            }
            delivered[database.id] = index
        }

        internal fun statusNotifications(database: DatabaseRef): List<() -> Unit> {
            val registered = listeners[database.id] ?: return emptyList()
            val status = statusLocked(database)
            return registered.map { listener -> { listener(status) } }
        }

        override fun start() {
            synchronized(lock) { transportUp = true }
        }

        override suspend fun probe(address: HostPort): PeerInfo =
            synchronized(lock) {
                val target =
                    members.values.firstOrNull { it.self.raftAddress == address && it.transportUp }
                        ?: throw ConsensusUnavailableException("no member reachable at $address")
                PeerInfo(
                    target.self,
                    target.metadata,
                    target.localState.associateWith { groups.getValue(it).members },
                )
            }

        override fun localGroups(): Set<UUID> = synchronized(lock) { localState.toSet() }

        override fun startGroup(
            database: DatabaseRef,
            stateMachine: ReplicatedStateMachine,
            bootstrapMembers: List<Member>,
        ) {
            synchronized(lock) {
                check(transportUp) { "start() the member before starting groups" }
                check(database.id !in running) { "group $database is already running" }
                val group =
                    groups.getOrPut(database.id) {
                        require(bootstrapMembers.isNotEmpty()) { "no bootstrap members" }
                        GroupState(database, bootstrapMembers.toList())
                    }
                if (database.id !in localState) {
                    check(bootstrapMembers == group.members) {
                        "conflicting bootstrap configuration for $database: " +
                            "stored=${group.members}, supplied=$bootstrapMembers"
                    }
                    localState += database.id
                }
                running[database.id] = stateMachine
                delivered[database.id] = 0L
                deliver(database, group.log)
            }
        }

        override fun stopGroup(database: DatabaseRef) {
            val notifications =
                synchronized(lock) {
                    running.remove(database.id) ?: return
                    delivered.remove(database.id)
                    val group = group(database)
                    if (group.leader == self.id) group.leader = null
                    statusNotifications(database)
                }
            notifications.forEach { it() }
        }

        override suspend fun submit(database: DatabaseRef, payload: ByteArray): CommitResult =
            synchronized(lock) {
                val group = group(database)
                if (!isRunning(database) || group.leader != self.id) {
                    return CommitResult.NotLeader(group.leader)
                }
                group.injectedFailure?.let {
                    group.injectedFailure = null
                    return it
                }
                if (!majorityRunning(group)) {
                    return CommitResult.OutcomeUnknown(
                        ConsensusUnavailableException("no majority of $database is running")
                    )
                }
                val entry = LogEntry(group.log.size + 1L, group.term, payload.copyOf())
                group.log += entry
                deliverAll(group)
                CommitResult.Committed(entry.index, entry.term)
            }

        override suspend fun readIndex(database: DatabaseRef): Long =
            synchronized(lock) {
                val group = group(database)
                if (!isRunning(database) || group.leader == null || !majorityRunning(group)) {
                    throw ConsensusUnavailableException("no leader for $database")
                }
                group.log.size.toLong()
            }

        override fun status(database: DatabaseRef): GroupStatus =
            synchronized(lock) { statusLocked(database) }

        private fun statusLocked(database: DatabaseRef): GroupStatus {
            val group = groups[database.id]
            val stateMachine = running[database.id]
            if (group == null || stateMachine == null) {
                return GroupStatus(database, Role.Inactive, null, group?.term ?: 0, -1, -1)
            }
            val role = if (group.leader == self.id) Role.Leader else Role.Follower
            return GroupStatus(
                database,
                role,
                group.leader,
                group.term,
                group.log.size.toLong(),
                stateMachine.lastAppliedIndex(),
            )
        }

        override fun members(database: DatabaseRef): List<Member> =
            synchronized(lock) { group(database).members.toList() }

        override fun addStatusListener(
            database: DatabaseRef,
            listener: (GroupStatus) -> Unit,
        ): AutoCloseable {
            synchronized(lock) {
                listeners.getOrPut(database.id) { CopyOnWriteArrayList() } += listener
            }
            return AutoCloseable { synchronized(lock) { listeners[database.id]?.remove(listener) } }
        }

        override suspend fun addMember(database: DatabaseRef, member: Member) {
            changeMembers(database) { current ->
                require(current.none { it.id == member.id }) { "${member.id} is already a member" }
                current + member
            }
        }

        override suspend fun removeMember(database: DatabaseRef, id: MemberId) {
            changeMembers(database) { current ->
                require(current.any { it.id == id }) { "$id is not a member" }
                current.filterNot { it.id == id }
            }
        }

        private fun changeMembers(database: DatabaseRef, change: (List<Member>) -> List<Member>) {
            synchronized(lock) {
                val group = group(database)
                if (group.leader == null || !majorityRunning(group)) {
                    throw ConsensusUnavailableException("no leader for $database")
                }
                group.members = change(group.members)
            }
        }

        override fun close() {
            val database = synchronized(lock) { runningGroups() }
            database.forEach { stopGroup(it) }
            synchronized(lock) { transportUp = false }
        }
    }
}
