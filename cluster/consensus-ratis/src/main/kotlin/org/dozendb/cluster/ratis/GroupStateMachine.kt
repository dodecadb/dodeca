package org.dozendb.cluster.ratis

import java.nio.ByteBuffer
import java.nio.file.Path
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto
import org.apache.ratis.proto.RaftProtos.RoleInfoProto
import org.apache.ratis.protocol.ClientId
import org.apache.ratis.protocol.Message
import org.apache.ratis.protocol.RaftGroupId
import org.apache.ratis.protocol.RaftGroupMemberId
import org.apache.ratis.protocol.RaftPeerId
import org.apache.ratis.server.RaftServer
import org.apache.ratis.server.protocol.TermIndex
import org.apache.ratis.server.raftlog.RaftLog
import org.apache.ratis.server.storage.FileInfo
import org.apache.ratis.server.storage.RaftStorage
import org.apache.ratis.statemachine.SnapshotInfo
import org.apache.ratis.statemachine.SnapshotRetentionPolicy
import org.apache.ratis.statemachine.StateMachineStorage
import org.apache.ratis.statemachine.TransactionContext
import org.apache.ratis.statemachine.impl.BaseStateMachine
import org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations
import org.apache.ratis.util.LifeCycle
import org.dozendb.cluster.api.DatabaseRef
import org.dozendb.cluster.api.HostPort
import org.dozendb.cluster.api.LogEntry
import org.dozendb.cluster.api.LogPosition
import org.dozendb.cluster.api.Member
import org.dozendb.cluster.api.MemberId
import org.dozendb.cluster.api.ReplicatedStateMachine
import org.dozendb.cluster.api.Snapshot
import org.slf4j.LoggerFactory

/**
 * Adapts a [ReplicatedStateMachine] to Ratis, for as long as the group runs on this member.
 *
 * The state machine is attached with [attach] and detached with [detach], for example while its
 * database is stopped; the group keeps running meanwhile, and applying waits. Ratis creates the
 * adapter as soon as it finds a group, which on restart happens before the database integration
 * attaches its state machine. A state machine older than the group's snapshot is never applied to;
 * it is asked to replace its state with a copy instead. Exceptions from
 * [ReplicatedStateMachine.apply] propagate, which makes Ratis close the group instead of skipping
 * the entry.
 *
 * Snapshots are [SnapshotRecord]s: the state lives in the database, so a snapshot only records up
 * to where the state machine holds every entry durably. A snapshot is taken at the second-to-last
 * durable index observed by [snapshotDue], so the log keeps at least the entries since then and a
 * member that was down briefly catches up from it. A member behind the start of the leader's log is
 * asked for a copy ([ReplicatedStateMachine.onSnapshotNeeded]); [install] then completes Ratis'
 * snapshot installation, which skips the group's log to the copy.
 */
internal class GroupStateMachine(
    private val groupId: UUID,
    private val localClientId: ClientId,
    private val listener: Listener,
) : BaseStateMachine() {
    interface Listener {
        /** The role, leader or configuration of the group may have changed. */
        fun onGroupChanged(groupId: UUID)

        /** A write submitted by this member with [callId] is about to be appended to the log. */
        fun onAppending(callId: Long)

        /** This member leads the group although its state machine was detached. */
        fun onLeadingDetached(groupId: UUID)
    }

    /** A request of Ratis to install a snapshot, waiting for a copy of the state. */
    private class PendingInstall(
        val future: CompletableFuture<TermIndex>,
        val leader: Member?,
        val minimumIndex: Long,
    )

    private val log = LoggerFactory.getLogger(GroupStateMachine::class.java)

    /**
     * Held while an entry is applied, so that detaching waits for the entry being applied and
     * readers see the state machine and the position together. Only the apply thread holds it for
     * long; Ratis callbacks never take it.
     */
    private val applyLock = ReentrantLock()

    /** Guards the fields below; held briefly. */
    private val stateLock = ReentrantLock()
    private val attachedOrReleased = stateLock.newCondition()
    private var candidate: ReplicatedStateMachine? = null
    private var target: ReplicatedStateMachine? = null
    private var detached = false
    private var released = false
    private var snapshotRequested = false
    private var pendingInstall: PendingInstall? = null
    private var stateMachineDirectory: Path? = null

    // Guarded by this; used by the snapshot poller and the apply thread.
    private var latestDurable = RaftLog.INVALID_LOG_INDEX
    private var snapshotCandidate = RaftLog.INVALID_LOG_INDEX

    @Volatile private var record: SnapshotRecord? = null

    /** Snapshots have no files; Ratis only asks this storage for the latest one. */
    private val storage =
        object : StateMachineStorage {
            override fun init(raftStorage: RaftStorage) {}

            override fun getLatestSnapshot(): SnapshotInfo? = this@GroupStateMachine.latestSnapshot

            override fun format() {}

            override fun cleanupOldSnapshots(snapshotRetentionPolicy: SnapshotRetentionPolicy) {}
        }

    @Volatile
    var database: DatabaseRef = DatabaseRef("unbound", groupId)
        private set

    /** Whether the state machine was detached and not attached again since. */
    val isDetached: Boolean
        get() = stateLock.withLock { detached }

    /** Attaches [stateMachine]; it starts applying once the group's snapshot record is loaded. */
    fun attach(database: DatabaseRef, stateMachine: ReplicatedStateMachine) {
        require(database.id == groupId) { "database $database does not belong to group $groupId" }
        stateLock.withLock {
            val attached = candidate
            if (attached != null) {
                check(attached === stateMachine) {
                    "group $groupId is already attached to another state machine"
                }
                return
            }
            candidate = stateMachine
            detached = false
            this.database = database
        }
        activateIfReady()
    }

    /**
     * Detaches the state machine, after the entry being applied, if any. Applying waits until a
     * state machine is attached again.
     */
    fun detach() {
        stateLock.withLock {
            candidate = null
            target = null
            detached = true
            snapshotRequested = false
        }
        // Wait for the entry being applied to the previous state machine.
        applyLock.withLock {}
    }

    override fun initialize(server: RaftServer, groupId: RaftGroupId, storage: RaftStorage) {
        super.initialize(server, groupId, storage)
        val directory = storage.storageDir.stateMachineDir.toPath()
        var loaded = SnapshotRecord.read(directory)
        if (loaded != null && loaded.installed) {
            // An installed snapshot covers every local entry, but Ratis may not have discarded the
            // log before a restart. It opens the log after this and must not find segments that end
            // before the snapshot.
            SnapshotRecord.discardCoveredSegments(
                storage.storageDir.currentDir.toPath(),
                loaded.snapshot.position.index,
            )
            loaded = loaded.copy(installed = false)
            SnapshotRecord.write(directory, loaded)
        }
        loaded?.let { setLastAppliedTermIndex(termIndex(it.snapshot)) }
        start()
        stateLock.withLock {
            record = loaded
            stateMachineDirectory = directory
        }
        activateIfReady()
    }

    /** Starts applying once attached and initialised, unless the state machine is too old. */
    private fun activateIfReady() {
        val (attached, snapshot) =
            stateLock.withLock {
                if (target != null || stateMachineDirectory == null) return
                val attached = candidate ?: return
                attached to record?.snapshot
            }
        if (snapshot != null && attached.lastAppliedIndex() < snapshot.appliedIndex) {
            log.error(
                "The state of {} is older than the group's snapshot {} (applied index {}); " +
                    "it must be replaced by a copy from another member",
                database,
                snapshot,
                attached.lastAppliedIndex(),
            )
            requestSnapshot(attached, null, snapshot.position.index)
            return
        }
        val pending = stateLock.withLock {
            if (candidate !== attached) return
            target = attached
            attachedOrReleased.signalAll()
            pendingInstall
        }
        pending?.let { requestSnapshot(attached, it.leader, it.minimumIndex) }
    }

    private fun requestSnapshot(
        attached: ReplicatedStateMachine,
        leader: Member?,
        minimumIndex: Long,
    ) {
        stateLock.withLock {
            if (snapshotRequested || candidate !== attached) return
            snapshotRequested = true
        }
        attached.onSnapshotNeeded(leader, minimumIndex)
    }

    /**
     * Ratis found this member behind the start of the leader's log. The returned future completes
     * when [install] records a copy of the state; until then Ratis reports the installation as in
     * progress.
     */
    override fun notifyInstallSnapshotFromLeader(
        roleInfoProto: RoleInfoProto,
        firstTermIndexInLog: TermIndex,
    ): CompletableFuture<TermIndex> {
        val leader = leaderOf(roleInfoProto)
        val minimumIndex = firstTermIndexInLog.index - 1
        log.info(
            "{} is behind the leader's log, which starts at {}; it needs a copy from {}",
            database,
            firstTermIndexInLog,
            leader ?: "another member",
        )
        val pending = PendingInstall(CompletableFuture(), leader, minimumIndex)
        val attached = stateLock.withLock {
            pendingInstall = pending
            target
        }
        attached?.let { requestSnapshot(it, leader, minimumIndex) }
        return pending.future
    }

    /**
     * Records that the detached state machine now holds [snapshot]. If Ratis is waiting for a
     * snapshot that [snapshot] satisfies, it continues the group after it; otherwise the local log
     * holds the entries after the state machine's and nothing changes.
     *
     * @return whether the group continues after [snapshot].
     */
    fun install(snapshot: Snapshot): Boolean {
        val (pending, directory) =
            stateLock.withLock {
                check(candidate == null) { "detach the state machine of $database first" }
                pendingInstall to checkNotNull(stateMachineDirectory) { "$database is not started" }
            }
        val latest = record?.snapshot
        require(latest == null || snapshot.position.index >= latest.position.index) {
            "snapshot $snapshot of $database is behind its latest snapshot $latest"
        }
        if (pending == null) {
            log.info("{} needs no snapshot: its log continues after {}", database, snapshot)
            return false
        }
        if (snapshot.position.index < pending.minimumIndex) {
            log.warn(
                "Snapshot {} of {} is behind the leader's log, which needs {}",
                snapshot,
                database,
                pending.minimumIndex,
            )
            return false
        }
        // Durable first: after a crash the record discards the log it covers.
        val installed = SnapshotRecord(snapshot, installed = true)
        SnapshotRecord.write(directory, installed)
        stateLock.withLock {
            record = installed
            pendingInstall = null
        }
        log.info("Installed snapshot {} of {}", snapshot, database)
        pending.future.complete(termIndex(snapshot))
        return true
    }

    /** Moves the lifecycle from new or paused to running. */
    private fun start() {
        val state = lifeCycle.currentState
        if (state == LifeCycle.State.NEW || state == LifeCycle.State.PAUSED) {
            lifeCycle.transition(LifeCycle.State.STARTING)
            lifeCycle.transition(LifeCycle.State.RUNNING)
        }
    }

    /** Ratis is about to continue the group after an installed snapshot. */
    override fun pause() {
        if (lifeCycle.currentState == LifeCycle.State.RUNNING) {
            lifeCycle.transition(LifeCycle.State.PAUSING)
            lifeCycle.transition(LifeCycle.State.PAUSED)
        }
    }

    /** Ratis continues the group after the installed snapshot, from the paused state. */
    override fun reinitialize() {
        record?.let { applyLock.withLock { setLastAppliedTermIndex(termIndex(it.snapshot)) } }
        start()
    }

    /**
     * Lets the group stop although no state machine applies: Ratis stops a group only after
     * applying every committed entry. Entries skipped this way are not recorded as applied, so they
     * are delivered again when the group starts.
     */
    fun release() {
        stateLock.withLock {
            released = true
            attachedOrReleased.signalAll()
        }
    }

    override fun preAppendTransaction(trx: TransactionContext): TransactionContext {
        val request = trx.clientRequest
        if (request != null && request.clientId == localClientId)
            listener.onAppending(request.callId)
        return trx
    }

    override fun applyTransaction(trx: TransactionContext): CompletableFuture<Message> {
        val entry = trx.logEntry
        while (true) {
            val attached =
                stateLock.withLock {
                    // Ratis does not interrupt this thread when the group stops; release() wakes
                    // it.
                    while (target == null && !released) attachedOrReleased.await()
                    target
                } ?: return CompletableFuture.completedFuture(Message.EMPTY)
            applyLock.withLock {
                // Detached while waiting for the lock: wait for the next state machine.
                if (stateLock.withLock { target } !== attached) return@withLock
                val response =
                    if (entry.index > attached.lastAppliedIndex()) {
                        attached.apply(
                            LogEntry(
                                entry.index,
                                entry.term,
                                entry.stateMachineLogEntry.logData.toByteArray(),
                            )
                        )
                    } else {
                        ByteArray(0)
                    }
                advance(entry.term, entry.index)
                return CompletableFuture.completedFuture(replyMessage(entry.term, response))
            }
        }
    }

    /** Ratis applied an entry that does not reach the state machine, such as a configuration. */
    override fun notifyTermIndexUpdated(term: Long, index: Long) {
        applyLock.withLock { advance(term, index) }
    }

    private fun advance(term: Long, index: Long) {
        val previousTerm = lastAppliedTermIndex.term
        updateLastAppliedTermIndex(term, index)
        // Applying the first entry of a term can make the leader writable.
        if (term != previousTerm) listener.onGroupChanged(groupId)
    }

    /** The term of the last entry applied by this member. */
    fun appliedTerm(): Long = lastAppliedTermIndex.term

    /** The local state as a snapshot, or null unless a state machine is attached and applied. */
    fun currentSnapshot(): Snapshot? = applyLock.withLock {
        val attached = stateLock.withLock { target } ?: return null
        val applied = lastAppliedTermIndex
        if (applied.index < 0) return null
        Snapshot(
            LogPosition(applied.index, applied.term),
            minOf(attached.lastAppliedIndex(), applied.index),
        )
    }

    /** The latest snapshot, or null if there is none. */
    fun latestSnapshot(): Snapshot? = record?.snapshot

    /**
     * Observes the state machine's durable index. Returns true if a snapshot would move forward:
     * the second-to-last durable index observed is beyond the latest snapshot.
     */
    fun snapshotDue(): Boolean {
        val attached = stateLock.withLock { target } ?: return false
        val durable = minOf(attached.durableIndex(), lastAppliedTermIndex.index)
        synchronized(this) {
            if (durable > latestDurable) {
                snapshotCandidate = latestDurable
                latestDurable = durable
            }
            return snapshotCandidate >
                (record?.snapshot?.position?.index ?: RaftLog.INVALID_LOG_INDEX)
        }
    }

    /** Runs on the apply thread, between entries, when Ratis is asked for a snapshot. */
    override fun takeSnapshot(): Long {
        val candidateIndex = synchronized(this) { snapshotCandidate }
        val directory = stateLock.withLock { stateMachineDirectory }
        val currentIndex = record?.snapshot?.position?.index ?: RaftLog.INVALID_LOG_INDEX
        val applied = lastAppliedTermIndex
        val index = minOf(candidateIndex, applied.index)
        if (directory == null || index <= currentIndex) return currentIndex
        val term = if (index == applied.index) applied.term else termOf(index)
        if (term == null) {
            log.warn("No term known for entry {} of {}; snapshot skipped", index, database)
            return currentIndex
        }
        val written = SnapshotRecord(Snapshot(LogPosition(index, term), index), installed = false)
        SnapshotRecord.write(directory, written)
        record = written
        return index
    }

    private fun termOf(index: Long): Long? =
        try {
            server.get().getDivision(RaftGroupId.valueOf(groupId)).raftLog.getTermIndex(index)?.term
        } catch (e: Exception) {
            log.warn("Could not read the term of entry {} of {}", index, database, e)
            null
        }

    override fun getLatestSnapshot(): SnapshotInfo? = record?.let {
        RecordSnapshotInfo(it.snapshot)
    }

    override fun getStateMachineStorage(): StateMachineStorage = storage

    /** A snapshot without files: members install copies of the database instead. */
    private class RecordSnapshotInfo(snapshot: Snapshot) : SnapshotInfo {
        private val termIndex = termIndex(snapshot)

        override fun getTermIndex(): TermIndex = termIndex

        override fun getFiles(): List<FileInfo> = emptyList()

        override fun toString(): String = "DozenDB snapshot $termIndex"
    }

    /** Answers read-index requests with the applied index, after Ratis confirmed leadership. */
    override fun query(request: Message): CompletableFuture<Message> =
        CompletableFuture.completedFuture(longMessage(lastAppliedTermIndex.index))

    override fun notifyLeaderChanged(groupMemberId: RaftGroupMemberId, newLeaderId: RaftPeerId?) {
        listener.onGroupChanged(groupId)
        val leadingDetached = newLeaderId == groupMemberId.peerId && stateLock.withLock { detached }
        if (leadingDetached) listener.onLeadingDetached(groupId)
    }

    override fun notifyLeaderReady() {
        listener.onGroupChanged(groupId)
    }

    override fun notifyConfigurationChanged(
        term: Long,
        index: Long,
        newRaftConfiguration: RaftConfigurationProto,
    ) {
        listener.onGroupChanged(groupId)
    }

    override fun close() {
        release()
        super.close()
        listener.onGroupChanged(groupId)
    }

    override fun toString(): String = "GroupStateMachine($database)"

    companion object {
        fun termIndex(snapshot: Snapshot): TermIndex =
            TermIndex.valueOf(snapshot.position.term, snapshot.position.index)

        fun longMessage(value: Long): Message =
            Message.valueOf(
                UnsafeByteOperations.unsafeWrap(
                    ByteBuffer.allocate(Long.SIZE_BYTES).putLong(value).flip()
                )
            )

        fun decodeLong(message: Message): Long = message.content.asReadOnlyByteBuffer().getLong()

        /** A write reply: the entry's term followed by the state machine's response. */
        fun replyMessage(term: Long, response: ByteArray): Message =
            Message.valueOf(
                UnsafeByteOperations.unsafeWrap(
                    ByteBuffer.allocate(Long.SIZE_BYTES + response.size)
                        .putLong(term)
                        .put(response)
                        .flip()
                )
            )

        /** Splits a write reply into the entry's term and the state machine's response. */
        fun decodeReply(message: Message): Pair<Long, ByteArray> {
            val buffer = message.content.asReadOnlyByteBuffer()
            val term = buffer.getLong()
            val response = ByteArray(buffer.remaining()).also { buffer.get(it) }
            return term to response
        }

        /** The leader named in a follower's role information, if any. */
        fun leaderOf(roleInfo: RoleInfoProto): Member? {
            if (!roleInfo.hasFollowerInfo()) return null
            val peer = roleInfo.followerInfo.leaderInfo.id
            return runCatching {
                Member(
                    MemberId.parse(RaftPeerId.valueOf(peer.id).toString()),
                    HostPort.parse(peer.address),
                )
            }
                .getOrNull()
        }
    }
}
