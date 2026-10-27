package org.dozendb.consensus.fake

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import kotlinx.coroutines.delay
import org.dozendb.consensus.BulkSource
import org.dozendb.consensus.CommitResult
import org.dozendb.consensus.ConsensusLog
import org.dozendb.consensus.ConsensusUnavailableException
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.GroupMembership
import org.dozendb.consensus.GroupProgress
import org.dozendb.consensus.GroupStatus
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.LeaderPlacement
import org.dozendb.consensus.LogEntry
import org.dozendb.consensus.LogPosition
import org.dozendb.consensus.Member
import org.dozendb.consensus.MemberId
import org.dozendb.consensus.PeerInfo
import org.dozendb.consensus.ReplicatedStateMachine
import org.dozendb.consensus.Role
import org.dozendb.consensus.Snapshot

/**
 * A deterministic, single-JVM stand-in for a cluster of [ConsensusLog] members.
 *
 * There are no elections: tests choose the leader with [electLeader]. An entry submitted on the
 * leader commits immediately when a majority of the group's members are running, and is applied
 * synchronously, in order, by every running member. Membership changes commit at once; a member
 * that runs a group without being one of its members receives nothing until it is added, and a
 * removed member stops running the group. Starting a group replays the whole log, so state machines
 * must skip entries they have already applied, as with a real implementation. Faults are injected
 * with [crash] and [failNextSubmit]. Bulk transfers run synchronously on the calling thread.
 *
 * Logs are compacted only by [compact]. A member that then needs discarded entries, beyond its
 * installed snapshot, gets [ReplicatedStateMachine.onSnapshotNeeded] instead of entries. State
 * machines attached beyond what their member applied are not checked with
 * [ReplicatedStateMachine.isLastApplied].
 *
 * All operations are serialised on one lock. State machines must not call back into the fake from
 * [ReplicatedStateMachine.apply] or [ReplicatedStateMachine.onSnapshotNeeded].
 */
class InMemoryConsensus {
    private val lock = Any()
    private val members = mutableMapOf<MemberId, InMemoryConsensusLog>()
    private val groups = mutableMapOf<UUID, GroupState>()
    private var nextPort = 7700

    /** The [ConsensusLog.maxEntrySize] of every member. */
    @Volatile var maxEntrySize: Int = 16 shl 20

    /** Stands in for the state machine of a member that joined a group without one. */
    private object Unattached : ReplicatedStateMachine {
        override fun apply(entry: LogEntry): ByteArray = error("no state machine is attached")

        override fun lastAppliedIndex(): Long = -1

        override fun durableIndex(): Long = -1

        override fun onSnapshotNeeded(leader: Member?, minimumIndex: Long) {}

        override fun isLastApplied(entry: LogEntry): Boolean = true

        override fun onForeignState(reason: String) {}
    }

    internal class GroupState(val database: DatabaseRef, val initialMembers: List<Member>) {
        var members: List<Member> = initialMembers
        var term = 0L
        var leader: MemberId? = null
        val log = mutableListOf<LogEntry>()
        /** Entries up to this index are discarded: no member can receive them any more. */
        var compactedUpTo = 0L
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

    /** The committed entries of [database], including discarded ones. */
    fun committedEntries(database: DatabaseRef): List<LogEntry> =
        synchronized(lock) { group(database).log.toList() }

    /** Discards the entries of [database] up to [index], as a snapshot on every member would. */
    fun compact(database: DatabaseRef, index: Long) {
        synchronized(lock) {
            val group = group(database)
            require(index <= group.log.size) { "only ${group.log.size} entries are committed" }
            group.compactedUpTo = maxOf(group.compactedUpTo, index)
        }
    }

    private fun group(database: DatabaseRef): GroupState =
        groups[database.id] ?: throw IllegalStateException("group $database does not exist")

    private fun majorityRunning(group: GroupState): Boolean {
        val running = group.members.count { members[it.id]?.isRunning(group.database) == true }
        return running > group.members.size / 2
    }

    /** Delivers the log to every running member; returns [responder]'s response to the tail. */
    private fun deliverAll(group: GroupState, responder: MemberId): ByteArray {
        var response = ByteArray(0)
        group.members
            .mapNotNull { members[it.id] }
            .forEach { member ->
                val answer = member.deliver(group)
                if (answer != null && member.self.id == responder) response = answer
            }
        return response
    }

    private fun statusNotifications(database: DatabaseRef): List<() -> Unit> =
        members.values.flatMap { it.statusNotifications(database) }

    /** One member of an [InMemoryConsensus]. */
    inner class InMemoryConsensusLog(
        override val self: Member,
        @Volatile var metadata: Map<String, String>,
    ) : ConsensusLog {
        /** How long this member takes to answer a probe. */
        @Volatile var probeDelay: Duration = Duration.ZERO

        /** What this member does when probed, before it answers. */
        @Volatile var onProbe: suspend () -> Unit = {}

        private val bulkSources = ConcurrentHashMap<String, BulkSource>()
        private var transportUp = false
        private val localState = mutableSetOf<UUID>()
        private val running = mutableMapOf<UUID, ReplicatedStateMachine>()
        /** The index of the last entry delivered (or skipped) since the group started. */
        private val delivered = mutableMapOf<UUID, Long>()
        private val snapshots = mutableMapOf<UUID, Snapshot>()
        private val snapshotRequested = mutableSetOf<UUID>()
        private val listeners = mutableMapOf<UUID, CopyOnWriteArrayList<(GroupStatus) -> Unit>>()
        private val removedFrom = mutableSetOf<UUID>()
        private val passingConfigurations = mutableMapOf<UUID, List<Member>>()
        private val transfers = mutableMapOf<UUID, Long>()

        @Volatile
        override var leaderPlacement: LeaderPlacement = LeaderPlacement.CONFIGURATION_ORDER

        internal fun isRunning(database: DatabaseRef): Boolean = database.id in running

        /**
         * Shows this member the settled configuration [members] of [database] and notifies its
         * status listeners, then restores the group's own configuration, as Ratis briefly shows an
         * older configuration to a follower that installs a snapshot.
         */
        fun showPassingConfiguration(database: DatabaseRef, members: List<Member>) {
            val notifications =
                synchronized(lock) {
                    passingConfigurations[database.id] = members
                    statusNotifications(database)
                }
            try {
                notifications.forEach { it() }
            } finally {
                synchronized(lock) { passingConfigurations.remove(database.id) }
            }
        }

        internal fun runningGroups(): List<DatabaseRef> =
            running.keys.map { groups.getValue(it).database }

        internal fun crashLocked() {
            transportUp = false
            running.clear()
            delivered.clear()
        }

        /** Applies undelivered entries; returns the response to the last one applied, if any. */
        internal fun deliver(group: GroupState): ByteArray? {
            val id = group.database.id
            val stateMachine = running[id] ?: return null
            if (stateMachine === Unattached || group.members.none { it.id == self.id }) return null
            var index = maxOf(delivered.getOrDefault(id, 0L), snapshots[id]?.position?.index ?: 0L)
            if (index < group.compactedUpTo) {
                // A member that applied the discarded entries before has them in its own log.
                if (stateMachine.lastAppliedIndex() < group.compactedUpTo) {
                    if (snapshotRequested.add(id)) {
                        val leader = group.members.firstOrNull { it.id == group.leader }
                        stateMachine.onSnapshotNeeded(leader, group.compactedUpTo)
                    }
                    return null
                }
                index = group.compactedUpTo
            }
            var response: ByteArray? = null
            while (index < group.log.size) {
                val entry = group.log[index.toInt()]
                if (entry.index > stateMachine.lastAppliedIndex()) {
                    response = stateMachine.apply(entry)
                }
                index++
            }
            delivered[id] = index
            return response
        }

        internal fun statusNotifications(database: DatabaseRef): List<() -> Unit> {
            val registered = listeners[database.id] ?: return emptyList()
            val status = statusLocked(database)
            return registered.map { listener -> { listener(status) } }
        }

        override fun start() {
            synchronized(lock) { transportUp = true }
        }

        override suspend fun probe(address: HostPort): PeerInfo {
            val probed =
                synchronized(lock) {
                    members.values.firstOrNull { it.self.raftAddress == address }
                }
            probed?.onProbe?.invoke()
            val delay = probed?.probeDelay
            if (delay != null && !delay.isZero) delay(delay.toMillis())
            return synchronized(lock) {
                val target =
                    members.values.firstOrNull { it.self.raftAddress == address && it.transportUp }
                        ?: throw ConsensusUnavailableException("no member reachable at $address")
                PeerInfo(
                    target.self,
                    target.metadata,
                    target.localState.associateWith { groups.getValue(it).members },
                )
            }
        }

        override fun localGroups(): Set<UUID> = synchronized(lock) { localState.toSet() }

        override fun startGroup(
            database: DatabaseRef,
            stateMachine: ReplicatedStateMachine,
            bootstrapMembers: List<Member>,
        ) {
            synchronized(lock) {
                check(running[database.id].let { it == null || it === Unattached }) {
                    "group $database is already running"
                }
                val group = register(database, bootstrapMembers)
                running[database.id] = stateMachine
                delivered[database.id] = 0L
                snapshotRequested -= database.id
                deliver(group)
            }
        }

        /**
         * Takes part in the group, applying nothing until [startGroup] attaches a state machine.
         */
        override fun joinGroup(database: DatabaseRef, bootstrapMembers: List<Member>) {
            synchronized(lock) {
                register(database, bootstrapMembers)
                running.putIfAbsent(database.id, Unattached)
            }
        }

        private fun register(database: DatabaseRef, bootstrapMembers: List<Member>): GroupState {
            check(transportUp) { "start() the member before starting groups" }
            val group =
                groups.getOrPut(database.id) {
                    require(bootstrapMembers.isNotEmpty()) { "no bootstrap members" }
                    GroupState(database, bootstrapMembers.toList())
                }
            if (database.id !in localState) {
                check(
                    bootstrapMembers.none { it.id == self.id } ||
                        bootstrapMembers == group.initialMembers
                ) {
                    "conflicting bootstrap configuration for $database: " +
                        "initial=${group.initialMembers}, supplied=$bootstrapMembers"
                }
                localState += database.id
                removedFrom -= database.id
            }
            return group
        }

        override fun removeGroup(database: DatabaseRef) {
            val notifications =
                synchronized(lock) {
                    running.remove(database.id)
                    delivered.remove(database.id)
                    snapshotRequested -= database.id
                    localState -= database.id
                    removedFrom -= database.id
                    val group = groups[database.id] ?: return
                    if (group.leader == self.id) group.leader = null
                    statusNotifications(database)
                }
            notifications.forEach { it() }
        }

        override fun detachGroup(database: DatabaseRef, handOverLeadership: Boolean) {
            val notifications =
                synchronized(lock) {
                    running.remove(database.id) ?: return
                    delivered.remove(database.id)
                    val group = group(database)
                    if (handOverLeadership && group.leader == self.id) group.leader = null
                    statusNotifications(database)
                }
            notifications.forEach { it() }
        }

        /**
         * Makes [to] the leader in a new term at once, if this member leads and [to] runs the
         * group; [timeout] is not needed.
         */
        override suspend fun transferLeadership(
            database: DatabaseRef,
            to: MemberId,
            reason: String,
            timeout: Duration,
        ): Boolean {
            val notifications =
                synchronized(lock) {
                    val group = groups[database.id] ?: return false
                    val target = members[to]
                    if (
                        group.leader != self.id ||
                            !isRunning(database) ||
                            group.members.none { it.id == to } ||
                            target == null ||
                            !target.isRunning(database)
                    ) {
                        return false
                    }
                    group.term++
                    group.leader = to
                    transfers[database.id] = (transfers[database.id] ?: 0) + 1
                    this@InMemoryConsensus.statusNotifications(database)
                }
            notifications.forEach { it() }
            return true
        }

        override fun currentSnapshot(database: DatabaseRef): Snapshot? =
            synchronized(lock) {
                val stateMachine = running[database.id] ?: return null
                val group = group(database)
                val last =
                    maxOf(
                        delivered.getOrDefault(database.id, 0L),
                        snapshots[database.id]?.position?.index ?: 0L,
                    )
                val position =
                    if (last == 0L) LogPosition(0, 0)
                    else LogPosition(last, group.log[(last - 1).toInt()].term)
                Snapshot(position, minOf(stateMachine.lastAppliedIndex(), position.index))
            }

        override fun latestSnapshot(database: DatabaseRef): Snapshot? =
            synchronized(lock) { snapshots[database.id] }

        /** Not needed: this fake never asks for a copy of entries a state machine holds. */
        override fun seed(database: DatabaseRef, snapshot: Snapshot) {}

        override fun installSnapshot(database: DatabaseRef, snapshot: Snapshot): Boolean =
            synchronized(lock) {
                check(database.id !in running) { "detach the state machine of $database first" }
                check(database.id in localState) { "group $database has no local state" }
                val latest = snapshots[database.id]
                require(latest == null || snapshot.position.index >= latest.position.index) {
                    "snapshot $snapshot is behind the latest snapshot $latest"
                }
                val behind = database.id in snapshotRequested
                if (behind && snapshot.position.index >= group(database).compactedUpTo) {
                    snapshots[database.id] = snapshot
                    true
                } else {
                    false
                }
            }

        override val maxEntrySize: Int
            get() = this@InMemoryConsensus.maxEntrySize

        override suspend fun submit(database: DatabaseRef, payload: ByteArray): CommitResult {
            require(payload.size <= maxEntrySize) {
                "entry of ${payload.size} bytes exceeds the maximum of $maxEntrySize"
            }
            return synchronized(lock) {
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
                val response = deliverAll(group, self.id)
                CommitResult.Committed(entry.index, entry.term, response)
            }
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
            if (group == null || stateMachine == null || group.members.none { it.id == self.id }) {
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

        override fun progress(database: DatabaseRef): GroupProgress? =
            synchronized(lock) {
                val status = statusLocked(database)
                if (status.role == Role.Inactive) return@synchronized null
                val group = group(database)
                val leading = status.role == Role.Leader
                GroupProgress(
                    appendIndex = group.log.size.toLong(),
                    commitIndex = status.commitIndex,
                    appliedIndex = status.appliedIndex,
                    pruneIndex = 1,
                    term = status.term,
                    leader = leading,
                    sinceLeaderContactMillis = if (status.leader == null) null else 0,
                    followerMatchIndexes =
                        if (leading) {
                            group.members
                                .filter { it.id != self.id }
                                .associate { it.id to group.log.size.toLong() }
                        } else {
                            null
                        },
                    followerContactMillis =
                        if (leading) {
                            group.members
                                .filter { it.id != self.id }
                                .associate {
                                    it.id to
                                        if (members.getValue(it.id).isRunning(database)) 0L
                                        else Long.MAX_VALUE
                                }
                        } else {
                            null
                        },
                    snapshotsTaken = 0,
                    leaderChanges = 0,
                    leadershipTransfers = transfers[database.id] ?: 0,
                )
            }

        override fun membership(database: DatabaseRef): GroupMembership =
            synchronized(lock) {
                if (database.id !in localState) {
                    throw ConsensusUnavailableException("group $database is not running here")
                }
                GroupMembership(
                    passingConfigurations[database.id] ?: group(database).members.toList(),
                    settled = true,
                    removed = database.id in removedFrom,
                )
            }

        override fun addStatusListener(
            database: DatabaseRef,
            listener: (GroupStatus) -> Unit,
        ): AutoCloseable {
            synchronized(lock) {
                listeners.getOrPut(database.id) { CopyOnWriteArrayList() } += listener
            }
            return AutoCloseable { synchronized(lock) { listeners[database.id]?.remove(listener) } }
        }

        // Every member sees every group here, so the members given are not needed.
        override suspend fun addMember(
            database: DatabaseRef,
            member: Member,
            timeout: Duration,
            members: List<Member>,
        ) {
            val notifications =
                synchronized(lock) {
                    val group = group(database)
                    if (group.members.any { it.id == member.id }) return
                    requireLeader(group)
                    val target = this@InMemoryConsensus.members[member.id]
                    if (
                        target == null || !target.transportUp || database.id !in target.localState
                    ) {
                        throw ConsensusUnavailableException("$member does not run $database")
                    }
                    group.members = group.members + member
                    deliverAll(group, self.id)
                    this@InMemoryConsensus.statusNotifications(database)
                }
            notifications.forEach { it() }
        }

        override suspend fun removeMember(
            database: DatabaseRef,
            id: MemberId,
            timeout: Duration,
            members: List<Member>,
        ) {
            val notifications =
                synchronized(lock) {
                    val group = group(database)
                    if (group.members.none { it.id == id }) return
                    requireLeader(group)
                    group.members = group.members.filterNot { it.id == id }
                    if (group.leader == id) group.leader = null
                    this@InMemoryConsensus.members[id]?.let { removed ->
                        removed.running.remove(database.id)
                        removed.delivered.remove(database.id)
                        removed.removedFrom += database.id
                    }
                    this@InMemoryConsensus.statusNotifications(database)
                }
            notifications.forEach { it() }
        }

        private fun requireLeader(group: GroupState) {
            if (group.leader == null || !majorityRunning(group)) {
                throw ConsensusUnavailableException("no leader for ${group.database}")
            }
        }

        override fun registerBulkSource(name: String, source: BulkSource) {
            bulkSources[name] = source
        }

        override fun <T> fetchBulk(
            address: HostPort,
            name: String,
            request: ByteArray,
            consume: (InputStream) -> T,
        ): T {
            val target =
                synchronized(lock) {
                    members.values.firstOrNull { it.self.raftAddress == address && it.transportUp }
                } ?: throw ConsensusUnavailableException("no member reachable at $address")
            val source =
                target.bulkSources[name]
                    ?: throw ConsensusUnavailableException("$address does not serve $name")
            val buffer = ByteArrayOutputStream()
            try {
                source.serve(request, buffer)
            } catch (e: Exception) {
                throw ConsensusUnavailableException("$address failed to serve $name", e)
            }
            return ByteArrayInputStream(buffer.toByteArray()).use(consume)
        }

        override fun close() {
            val database = synchronized(lock) { runningGroups() }
            database.forEach { detachGroup(it) }
            synchronized(lock) { transportUp = false }
        }
    }
}
