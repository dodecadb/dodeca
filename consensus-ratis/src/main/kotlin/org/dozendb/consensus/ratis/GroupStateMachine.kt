package org.dozendb.consensus.ratis

import java.nio.ByteBuffer
import java.nio.file.Path
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.first
import org.apache.ratis.proto.RaftProtos.LogEntryProto
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
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.LogEntry
import org.dozendb.consensus.LogPosition
import org.dozendb.consensus.Member
import org.dozendb.consensus.MemberId
import org.dozendb.consensus.ReplicatedStateMachine
import org.dozendb.consensus.Snapshot
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
 * snapshot installation, which skips the group's log to the copy. Nothing is applied while the copy
 * is awaited: Ratis meanwhile truncates the member's log and rewrites its configuration file, which
 * applying a configuration entry would write at the same time.
 *
 * A member being added copies its databases before it runs their groups, and records each copy's
 * position as a seed ([seed]). When the leader then asks for a copy the seed covers, the seed is
 * installed instead. A seed never becomes the group's starting snapshot: Ratis learns the group's
 * configuration with the leader's request, which it would otherwise miss.
 *
 * A state machine attached with a higher applied index than the group delivered to state machines
 * on this member or holds a copy up to, as one whose state was put in place by hand, may hold
 * another group's state. Before any later entry is applied to it, the group's entry at its applied
 * index must be the one it applied last ([ReplicatedStateMachine.isLastApplied]). Otherwise it is
 * refused ([ReplicatedStateMachine.onForeignState]) and treated as detached with leadership
 * handover. A state at or below that index is not checked; one beyond the end of the group's log is
 * checked once the log reaches its index.
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
    /** Signalled when the apply thread may go on: attached, a copy installed, or released. */
    private val mayApply = stateLock.newCondition()
    private var candidate: ReplicatedStateMachine? = null
    private var target: ReplicatedStateMachine? = null
    private var detached = false
    private var handOverWhileDetached = false
    private var released = false
    private var snapshotRequested = false
    private var pendingInstall: PendingInstall? = null
    private var stateMachineDirectory: Path? = null
    /** A copy of the state made before the group ran here; kept in its file once it runs. */
    private var seed: Snapshot? = null
    /** The index whose entry the attached state machine must have applied last; null if checked. */
    private var unverified: Long? = null
    /** The last entry delivered to state machines on this member; guarded by [applyLock]. */
    private var lastStateEntry = RaftLog.INVALID_LOG_INDEX
    /** The index of the last entry applied, as [lastAppliedTermIndex], for [awaitApplied]. */
    private val appliedIndex = MutableStateFlow(RaftLog.INVALID_LOG_INDEX)

    // Guarded by this; used by the snapshot poller and the apply thread.
    private var latestDurable = RaftLog.INVALID_LOG_INDEX
    private var snapshotCandidate = RaftLog.INVALID_LOG_INDEX

    @Volatile private var record: SnapshotRecord? = null

    /**
     * Whether Ratis shut the group down because this member is not one of its members: its election
     * found it left out of the configuration, or the leader answered that it was removed.
     */
    @Volatile
    var shutDownAsNonMember: Boolean = false
        private set

    /** Whether an entry failed to apply here, after which Ratis closes the group. */
    @Volatile
    var applyFailed: Boolean = false
        private set

    /**
     * Whether Ratis closed the group on this member, or is about to because an entry failed to
     * apply. A leader keeps trying such a member as one that is down; see
     * [RefuseUnavailableGroups].
     */
    @Volatile
    var closed: Boolean = false
        private set

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

    /**
     * Whether this member hands leadership over while it leads: its state machine was detached with
     * leadership handover and not attached again since.
     */
    val handsOverLeadership: Boolean
        get() = stateLock.withLock { detached && handOverWhileDetached }

    /** Whether this member waits for a copy of the state, which the leader asked it to install. */
    val awaitingCopy: Boolean
        get() = stateLock.withLock { pendingInstall != null }

    /** Names the group's database before a state machine is attached. */
    fun bind(database: DatabaseRef) {
        require(database.id == groupId) { "database $database does not belong to group $groupId" }
        stateLock.withLock { this.database = database }
    }

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
     * state machine is attached again. [handOverLeadership] tells whether this member hands
     * leadership over whenever it leads meanwhile.
     */
    fun detach(handOverLeadership: Boolean) {
        stateLock.withLock {
            candidate = null
            target = null
            detached = true
            handOverWhileDetached = handOverLeadership
            snapshotRequested = false
            unverified = null
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
        loaded?.let { appliedUpTo(it.snapshot) }
        start()
        val storedSeed = SnapshotRecord.read(directory, SnapshotRecord.SEED_FILE_NAME)?.snapshot
        val keptSeed = stateLock.withLock {
            record = loaded
            stateMachineDirectory = directory
            seed =
                listOfNotNull(seed, storedSeed)
                    .maxByOrNull { it.position.index }
                    ?.takeIf { beyondRecord(it) }
            seed
        }
        when {
            keptSeed == null && storedSeed != null ->
                SnapshotRecord.delete(directory, SnapshotRecord.SEED_FILE_NAME)
            keptSeed != null && keptSeed != storedSeed -> writeSeed(directory, keptSeed)
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
        // Stable: nothing is applied to it before it becomes the target.
        val applied = attached.lastAppliedIndex()
        if (snapshot != null && applied < snapshot.appliedIndex) {
            log.error(
                "The state of {} is older than the group's snapshot {} (applied index {}); " +
                    "it must be replaced by a copy from another member",
                database,
                snapshot,
                applied,
            )
            requestSnapshot(attached, null, snapshot.position.index)
            return
        }
        var foreign = false
        // Holding applyLock, so that no entry is applied between deciding what to check and
        // starting to apply.
        val pending = applyLock.withLock {
            stateLock.withLock {
                if (candidate !== attached) return
                val reached =
                    maxOf(
                        lastStateEntry,
                        record?.snapshot?.position?.index ?: RaftLog.INVALID_LOG_INDEX,
                        seed?.position?.index ?: RaftLog.INVALID_LOG_INDEX,
                    )
                // Ratis applied the entry there while detached: not one for state machines.
                foreign = applied > reached && applied <= lastAppliedTermIndex.index
                if (!foreign) {
                    target = attached
                    unverified = applied.takeIf { it > reached }
                    mayApply.signalAll()
                }
                pendingInstall
            }
        }
        if (foreign) refuse(attached, applied)
        else pending?.let { requestSnapshot(attached, it.leader, it.minimumIndex) }
    }

    /**
     * Stops applying to [attached], whose state was applied up to [index] in another group, as if
     * it was detached with leadership handover, and tells it why.
     */
    private fun refuse(attached: ReplicatedStateMachine, index: Long) {
        stateLock.withLock {
            if (candidate !== attached) return
            candidate = null
            target = null
            detached = true
            handOverWhileDetached = true
            snapshotRequested = false
            unverified = null
        }
        val reason =
            "it was applied up to Raft index $index, but the group's entry there is a different one"
        log.error(
            "The state of {} holds another Raft group's entries: {}. Nothing more is applied to it.",
            database,
            reason,
        )
        attached.onForeignState(reason)
        listener.onLeadingDetached(groupId)
        listener.onGroupChanged(groupId)
    }

    private fun requestSnapshot(
        attached: ReplicatedStateMachine,
        leader: Member?,
        minimumIndex: Long,
    ) {
        if (installSeed(attached, minimumIndex)) return
        stateLock.withLock {
            if (snapshotRequested || candidate !== attached) return
            snapshotRequested = true
        }
        try {
            attached.onSnapshotNeeded(leader, minimumIndex)
        } catch (e: Exception) {
            stateLock.withLock { snapshotRequested = false }
            throw e
        }
    }

    /**
     * Records that this member holds a copy of the state holding [snapshot], made before the group
     * ran here. Ignored if the group's snapshot or an earlier seed reaches as far.
     */
    fun seed(snapshot: Snapshot) {
        val directory = stateLock.withLock {
            val current = seed
            if (
                !beyondRecord(snapshot) ||
                    (current != null && current.position.index >= snapshot.position.index)
            ) {
                return
            }
            seed = snapshot
            stateMachineDirectory
        }
        log.info("{} holds a copy at {}, made before its group ran here", database, snapshot)
        directory?.let { writeSeed(it, snapshot) }
    }

    /**
     * Completes the pending installation with the seed, if the leader needs no more than the seed
     * covers and [attached] still holds it. Returns whether it did.
     */
    private fun installSeed(attached: ReplicatedStateMachine, minimumIndex: Long): Boolean {
        val (pending, usable, directory) =
            stateLock.withLock { Triple(pendingInstall, seed, stateMachineDirectory) }
        if (pending == null || usable == null || directory == null) return false
        if (usable.position.index < minimumIndex) return false
        if (attached.lastAppliedIndex() < usable.appliedIndex) {
            log.warn(
                "{} no longer holds its copy at {} (applied index {}); it needs another copy",
                database,
                usable,
                attached.lastAppliedIndex(),
            )
            return false
        }
        // Durable first, as for any installed snapshot. The local log ends before the seed.
        val installed = SnapshotRecord(usable, installed = true)
        SnapshotRecord.write(directory, installed)
        stateLock.withLock {
            if (pendingInstall !== pending) return false
            record = installed
            pendingInstall = null
            seed = null
            mayApply.signalAll()
        }
        SnapshotRecord.delete(directory, SnapshotRecord.SEED_FILE_NAME)
        log.info(
            "Installed snapshot {} of {} from the copy it made before joining",
            usable,
            database,
        )
        pending.future.complete(termIndex(usable))
        return true
    }

    /** Whether [snapshot] reaches beyond the group's snapshot. Called holding [stateLock]. */
    private fun beyondRecord(snapshot: Snapshot): Boolean =
        snapshot.position.index > (record?.snapshot?.position?.index ?: RaftLog.INVALID_LOG_INDEX)

    private fun writeSeed(directory: Path, snapshot: Snapshot) {
        SnapshotRecord.write(
            directory,
            SnapshotRecord(snapshot, installed = false),
            SnapshotRecord.SEED_FILE_NAME,
        )
    }

    /** Forgets the seed once the group's snapshot reaches as far. */
    private fun dropCoveredSeed() {
        val directory =
            stateLock.withLock {
                val current = seed ?: return
                if (beyondRecord(current)) return
                seed = null
                stateMachineDirectory
            } ?: return
        SnapshotRecord.delete(directory, SnapshotRecord.SEED_FILE_NAME)
    }

    /**
     * Ratis found this member behind the start of the leader's log, or is adding it to the group.
     * The returned future completes when [install] records a copy of the state; until then Ratis
     * reports the installation as in progress. A member being added is asked even if the leader's
     * log continues its own; it then needs no copy and the future completes at once, empty. The
     * leader's log continues this member's only if this member holds the leader's first entry, or
     * if both logs start empty: otherwise the leader cannot name the entry before the ones it would
     * send, and would only ask again.
     *
     * Never throws: Ratis would then report the installation as in progress for good. A failure
     * fails the returned future instead, after [RETRY_DELAY_MILLIS], and the leader asks again.
     */
    override fun notifyInstallSnapshotFromLeader(
        roleInfoProto: RoleInfoProto,
        firstTermIndexInLog: TermIndex,
    ): CompletableFuture<TermIndex> {
        val pending =
            PendingInstall(
                CompletableFuture(),
                leaderOf(roleInfoProto),
                firstTermIndexInLog.index - 1,
            )
        return try {
            awaitCopy(pending, firstTermIndexInLog)
        } catch (e: Exception) {
            stateLock.withLock {
                if (pendingInstall === pending) {
                    pendingInstall = null
                    mayApply.signalAll()
                }
            }
            log.error("Preparing a copy of {} failed; the leader will ask again", database, e)
            // Ratis expects the future to complete after this returns.
            CompletableFuture<TermIndex>().also {
                CompletableFuture.delayedExecutor(RETRY_DELAY_MILLIS, TimeUnit.MILLISECONDS)
                    .execute { it.completeExceptionally(e) }
            }
        }
    }

    /** Returns the future of [pending], or a completed one if this member needs no copy. */
    private fun awaitCopy(
        pending: PendingInstall,
        firstTermIndexInLog: TermIndex,
    ): CompletableFuture<TermIndex> {
        val raftLog = server.get().getDivision(RaftGroupId.valueOf(groupId)).raftLog
        val logEnd = raftLog.nextIndex - 1
        val continues =
            raftLog.getTermIndex(firstTermIndexInLog.index) == firstTermIndexInLog ||
                (logEnd == RaftLog.INVALID_LOG_INDEX &&
                    firstTermIndexInLog.index == RaftLog.LEAST_VALID_LOG_INDEX)
        if (continues) {
            log.info(
                "{} needs no copy: the leader's log, which starts at {}, continues this member's, " +
                    "which ends at {}",
                database,
                firstTermIndexInLog,
                logEnd,
            )
            return CompletableFuture.completedFuture(null)
        }
        log.info(
            "{} is behind the leader's log, which starts at {}; it needs a copy from {}",
            database,
            firstTermIndexInLog,
            pending.leader ?: "another member",
        )
        val attached = stateLock.withLock {
            pendingInstall = pending
            target
        }
        attached?.let { requestSnapshot(it, pending.leader, pending.minimumIndex) }
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
            mayApply.signalAll()
        }
        dropCoveredSeed()
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
        record?.let { applyLock.withLock { appliedUpTo(it.snapshot) } }
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
            mayApply.signalAll()
        }
    }

    override fun preAppendTransaction(trx: TransactionContext): TransactionContext {
        val request = trx.clientRequest
        if (request != null && request.clientId == localClientId)
            listener.onAppending(request.callId)
        return trx
    }

    override fun applyTransaction(trx: TransactionContext): CompletableFuture<Message> =
        try {
            applyEntry(trx.logEntry)
        } catch (e: Throwable) {
            // Ratis closes the group once this returns.
            applyFailed = true
            closed = true
            throw e
        }

    private fun applyEntry(entry: LogEntryProto): CompletableFuture<Message> {
        while (true) {
            val attached =
                stateLock.withLock {
                    // Ratis does not interrupt this thread when the group stops; release() wakes
                    // it.
                    while ((target == null || pendingInstall != null) && !released) {
                        mayApply.await()
                    }
                    target
                } ?: return CompletableFuture.completedFuture(Message.EMPTY)
            applyLock.withLock {
                val (current, unverified) = stateLock.withLock { target to unverified }
                // Detached while waiting for the lock: wait for the next state machine.
                if (current !== attached) return@withLock
                fun logEntry() =
                    LogEntry(
                        entry.index,
                        entry.term,
                        entry.stateMachineLogEntry.logData.toByteArray(),
                    )
                if (entry.index == unverified) {
                    if (!attached.isLastApplied(logEntry())) {
                        refuse(attached, entry.index)
                        // Applied by the next state machine attached.
                        return@withLock
                    }
                    stateLock.withLock { this.unverified = null }
                }
                val response =
                    if (entry.index > attached.lastAppliedIndex()) {
                        attached.apply(logEntry())
                    } else {
                        ByteArray(0)
                    }
                lastStateEntry = entry.index
                advance(entry.term, entry.index)
                return CompletableFuture.completedFuture(replyMessage(entry.term, response))
            }
        }
    }

    /**
     * Ratis applied an entry that does not reach the state machine, such as a configuration. Waits
     * while a copy is awaited, so that Ratis applies no later entry meanwhile.
     */
    override fun notifyTermIndexUpdated(term: Long, index: Long) {
        stateLock.withLock {
            while (pendingInstall != null && !released) mayApply.await()
        }
        applyLock.withLock {
            // Not an entry that the state machine can have applied last.
            stateLock.withLock { target?.takeIf { unverified == index } }?.let { refuse(it, index) }
            advance(term, index)
        }
    }

    /** Takes [snapshot]'s entry as the last applied, as when the group continues from it. */
    private fun appliedUpTo(snapshot: Snapshot) {
        setLastAppliedTermIndex(termIndex(snapshot))
        appliedIndex.value = snapshot.position.index
    }

    private fun advance(term: Long, index: Long) {
        val previousTerm = lastAppliedTermIndex.term
        updateLastAppliedTermIndex(term, index)
        appliedIndex.value = index
        // Applying the first entry of a term can make the leader writable.
        if (term != previousTerm) listener.onGroupChanged(groupId)
    }

    /** The term of the last entry applied by this member. */
    fun appliedTerm(): Long = lastAppliedTermIndex.term

    /** Suspends until this member applied the entry at [index], or installed a snapshot past it. */
    suspend fun awaitApplied(index: Long) {
        appliedIndex.first { it >= index }
    }

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
     * the second-to-last durable index observed is beyond the latest snapshot. Never while a copy
     * is awaited, as Ratis then refuses snapshots; the copy's snapshot is beyond any state held
     * before.
     */
    fun snapshotDue(): Boolean {
        val attached =
            stateLock.withLock { target.takeIf { pendingInstall == null } } ?: return false
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

    private val snapshotCount = AtomicLong()
    private val leaderChangeCount = AtomicLong()
    private val transferCount = AtomicLong()

    /** Snapshots taken since this group started on this member. */
    val snapshotsTaken: Long
        get() = snapshotCount.get()

    /** Leaders this member learned of since this group started on it. */
    val leaderChanges: Long
        get() = leaderChangeCount.get()

    /** Leaderships this member transferred to another member since this group started on it. */
    val leadershipTransfers: Long
        get() = transferCount.get()

    fun countTransfer() {
        transferCount.incrementAndGet()
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
        stateLock.withLock { record = written }
        snapshotCount.incrementAndGet()
        dropCoveredSeed()
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
        if (newLeaderId != null) leaderChangeCount.incrementAndGet()
        listener.onGroupChanged(groupId)
        if (newLeaderId == groupMemberId.peerId && handsOverLeadership) {
            listener.onLeadingDetached(groupId)
        }
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

    override fun notifyServerShutdown(roleInfo: RoleInfoProto, allServer: Boolean) {
        if (!allServer) {
            log.info(
                "Raft group {} stopped on this member, which is not one of its members",
                database,
            )
            shutDownAsNonMember = true
            listener.onGroupChanged(groupId)
        }
    }

    override fun close() {
        closed = true
        release()
        super.close()
        listener.onGroupChanged(groupId)
    }

    override fun toString(): String = "GroupStateMachine($database)"

    companion object {
        const val RETRY_DELAY_MILLIS = 1000L

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
