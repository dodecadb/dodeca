package dev.dodeca.cluster.internal.replication

import dev.dodeca.cluster.DatabaseGroups
import dev.dodeca.cluster.internal.ClusterMetrics
import dev.dodeca.cluster.internal.of
import dev.dodeca.cluster.internal.reads.ReadConsistencyException
import dev.dodeca.consensus.CommitResult
import dev.dodeca.consensus.ConsensusLog
import dev.dodeca.consensus.ConsensusUnavailableException
import dev.dodeca.consensus.DatabaseRef
import dev.dodeca.consensus.GroupStatus
import dev.dodeca.consensus.Member
import dev.dodeca.consensus.MemberId
import dev.dodeca.consensus.Role
import dev.dodeca.core.edition.HostedDatabase
import java.time.Duration
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlinx.coroutines.runBlocking
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.HostedOnMode
import org.neo4j.internal.id.IdGeneratorFactory
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException
import org.neo4j.kernel.KernelVersionProvider
import org.neo4j.kernel.api.KernelTransactionHandle
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.database.Database
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.impl.api.KernelTransactions
import org.neo4j.kernel.impl.api.LeaseService
import org.neo4j.kernel.impl.api.TransactionCommitProcess
import org.neo4j.kernel.impl.api.TransactionalProcessFactory
import org.neo4j.logging.InternalLog
import org.neo4j.storageengine.api.CommandBatch

/**
 * One replicated database on this member: commits its transactions through Raft, runs its group
 * while the database runs, and turns Raft leadership into write leases and ID allocation.
 *
 * ID generators start with free-ID reuse disabled. When this member becomes writable leader it gets
 * a lease at once, but reuses free IDs only after every transaction that was open at that moment
 * has closed: such a transaction may hold IDs from an earlier term, and its rollback can mark free
 * an ID that another leader used since. Until then IDs come from the high ID, which is always safe.
 *
 * @property initialMembers the initial members of the database's group, which this member creates
 *   with them if it has no local state for it.
 * @property mustDetach whether this member brought the database's store to the cluster it formed
 *   and has yet to detach it from the Raft group it may come from; [detached] records that it has.
 * @property adopt runs once such a store is detached, before the group starts and before other
 *   members copy the store, and may commit to the database locally.
 * @property onSnapshotNeeded called when the group's log no longer holds entries this member needs.
 * @property notLeaderMessage why this member refuses writes while another member, or none, leads.
 * @property counts where its replications are counted, for metrics.
 * @property afterGroupChange runs after the status of the database's group changed here: its role,
 *   leader or members, or Ratis stopped it.
 * @property groups the groups of the databases: the database's [ref] names its group.
 */
internal class ReplicatedDatabase(
    val namedDatabaseId: NamedDatabaseId,
    private val consensus: ConsensusLog,
    private val initialMembers: () -> List<Member>,
    private val mustDetach: () -> Boolean,
    private val detached: () -> Unit,
    private val adopt: () -> Unit,
    storeExists: Boolean,
    private val scheduler: ScheduledExecutorService,
    private val log: InternalLog,
    private val afterApply: () -> Unit,
    private val onSnapshotNeeded: (ReplicatedDatabase, Member?, Long) -> Unit,
    private val notLeaderMessage: (MemberId?) -> String,
    private val counts: ClusterMetrics.Counters = ClusterMetrics.Counters(),
    private val afterGroupChange: () -> Unit = {},
    private val groups: DatabaseGroups = DatabaseGroups(),
) : HostedDatabase {
    /** The database's Raft group. */
    val ref: DatabaseRef
        get() = groups.of(namedDatabaseId)

    override val hostedOnMode: HostedOnMode = HostedOnMode.RAFT

    /**
     * True from creating a new store until the group starts. Neo4j then commits one local
     * transaction that initialises the store; every other member copies the result.
     */
    @Volatile
    var bootstrapping: Boolean = !storeExists
        private set

    /** True while [adopt] runs. */
    @Volatile private var adopting: Boolean = false

    /**
     * Whether transactions commit locally, without a lease, before the group starts: while Neo4j
     * initialises a new store, and while [adopt] runs.
     */
    val commitsLocally: Boolean
        get() = bootstrapping || adopting

    override val leaseService =
        ClusterLeaseService({ commitsLocally }) { NotLeaderException(refusal(leader())) }

    /** The leader of the database as this member knows it, if any. */
    private fun leader(): MemberId? =
        try {
            consensus.status(ref).leader
        } catch (_: ConsensusUnavailableException) {
            null
        }

    /**
     * True while the database's transactions go through Raft and its committed entries are applied,
     * from [start] to [stop]. Only then does the Raft log make its commits durable.
     */
    @Volatile
    var replicating: Boolean = false
        private set

    /** The local commit process that applies committed entries, set when the database starts. */
    @Volatile var applyProcess: TransactionCommitProcess? = null

    /**
     * Whether this member hands leadership over when the database stops. Not when it stops on every
     * member, as when it is taken offline or dropped: nobody could take over.
     */
    @Volatile var handOverOnStop: Boolean = true

    /** Whether this member is the leader and can commit transactions. */
    val writable: Boolean
        get() = synchronized(lock) { writableTerm != NOT_WRITABLE }

    private val lock = Any()
    private lateinit var database: Database
    private var kernelTransactions: KernelTransactions? = null
    private var statusListener: AutoCloseable? = null
    private var writableTerm = NOT_WRITABLE
    private var reuseCheck: ScheduledFuture<*>? = null

    override fun commitProcessFactory(
        local: TransactionalProcessFactory
    ): TransactionalProcessFactory = RaftCommitProcessFactory(this, local)

    override fun attach(database: Database) {
        this.database = database
    }

    /** Joins the database's Raft group. Called once the database has started. */
    fun start() {
        synchronized(lock) {
            check(statusListener == null) { "${ref.name} is already replicating" }
            kernelTransactions =
                database.dependencyResolver.resolveDependency(KernelTransactions::class.java)
            val stateMachine =
                DatabaseStateMachine(
                    database,
                    checkNotNull(applyProcess) { "no commit process" },
                    afterApply,
                ) { leader, minimumIndex ->
                    onSnapshotNeeded(this, leader, minimumIndex)
                }
            if (mustDetach()) {
                // Before the group applies anything and before other members copy the store.
                stateMachine.detachFromPreviousGroup()?.let { previous ->
                    log.info(
                        "Database %s holds transactions replicated in another Raft group, up to " +
                            "index %d there; its new group starts from it",
                        ref.name,
                        previous,
                    )
                }
                adopting = true
                try {
                    adopt()
                } finally {
                    adopting = false
                }
                detached()
            }
            val bootstrap = if (ref.id in consensus.localGroups()) emptyList() else initialMembers()
            // From now on every transaction goes through Raft.
            bootstrapping = false
            val listener = consensus.addStatusListener(ref, ::onStatus)
            statusListener = listener
            replicating = true
            try {
                consensus.startGroup(ref, stateMachine, bootstrap)
            } catch (e: Exception) {
                replicating = false
                listener.close()
                statusListener = null
                throw e
            }
            log.info(
                "Replicating database %s (%s), applied up to Raft index %d",
                ref.name,
                ref.id,
                stateMachine.lastAppliedIndex(),
            )
        }
    }

    /**
     * Joins the group of a database whose store was just created, once Neo4j has initialised it.
     * Neo4j does that after announcing the start, in a transaction that commits locally, while it
     * holds the database's monitor; holding it here also keeps the database from stopping
     * meanwhile.
     */
    fun startAfterBootstrap() {
        synchronized(database) {
            if (database.isStarted) start()
        }
    }

    /**
     * Stops applying the group's entries, before the database stops or after it panicked. The group
     * keeps running on this member, which hands leadership over unless [handOverOnStop] is off. A
     * leader first grants no lease, so that new commits fail as on a member that does not lead, and
     * lets the commits under way finish, for [STOP_DRAIN] at most: once detached, it would no
     * longer apply their entries, and could not tell their outcome.
     */
    fun stop() {
        val leading =
            synchronized(lock) {
                if (statusListener == null) return
                val leading = writableTerm != NOT_WRITABLE
                handingOver = true
                leaveWritableTerm()
                leading
            }
        if (leading && !awaitNoCommits(STOP_DRAIN)) {
            log.warn(
                "Stopping %s while commits are still under way after %s: their clients may not " +
                    "learn whether they committed",
                ref.name,
                STOP_DRAIN,
            )
        }
        synchronized(lock) {
            handingOver = false
            val listener = statusListener ?: return
            replicating = false
            listener.close()
            statusListener = null
            leaveWritableTerm()
            kernelTransactions = null
            consensus.detachGroup(ref, handOverOnStop)
            log.info("Stopped replicating database %s", ref.name)
        }
    }

    fun isRunning(): Boolean = synchronized(lock) { statusListener != null }

    /**
     * Whether this member is handing its leadership of the database over to another member; it
     * grants no lease meanwhile.
     */
    @Volatile
    var handingOver: Boolean = false
        private set

    /** Held to count the commits under way in [replicate]. */
    private val commits = ReentrantLock()
    private val noCommits = commits.newCondition()
    private var committing = 0

    /**
     * Hands the leadership of this database over to [to] without failing commits under way: this
     * member grants no lease from now on, so that new commits fail as on a member that does not
     * lead, and clients retry them; once the commits under way have finished, it transfers the
     * leadership (see [ConsensusLog.transferLeadership]). [timeout] bounds each of the two waits.
     * If the transfer fails while this member still leads, it leads on.
     *
     * @return whether [to] leads afterwards.
     */
    fun transferLeadership(to: MemberId, reason: String, timeout: Duration): Boolean {
        synchronized(lock) {
            if (statusListener == null || writableTerm == NOT_WRITABLE || handingOver) return false
            handingOver = true
            leaveWritableTerm()
        }
        try {
            if (!awaitNoCommits(timeout)) {
                log.info(
                    "Leadership of %s stays on this member: its commits under way did not " +
                        "finish within %s",
                    ref.name,
                    timeout,
                )
                return false
            }
            return runBlocking { consensus.transferLeadership(ref, to, reason, timeout) }
        } finally {
            synchronized(lock) {
                handingOver = false
                // Leads on if the transfer failed.
                if (statusListener != null) onStatus(consensus.status(ref))
            }
        }
    }

    private fun awaitNoCommits(timeout: Duration): Boolean = commits.withLock {
        var left = timeout.toNanos()
        while (committing > 0) {
            if (left <= 0) return false
            left = noCommits.awaitNanos(left)
        }
        true
    }

    /**
     * Returns once this member has applied every entry the group had committed when it was called,
     * as confirmed by the leader (a Raft ReadIndex).
     */
    fun awaitLinearizable() {
        try {
            runBlocking { consensus.readIndex(ref) }
        } catch (e: ConsensusUnavailableException) {
            throw ReadConsistencyException(
                Status.Cluster.ReplicationFailure,
                "A linearizable read of ${ref.name} could not be confirmed with the leader; retry later.",
                e,
            )
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            throw ReadConsistencyException(
                Status.Cluster.ReplicationFailure,
                "Interrupted while confirming a linearizable read of ${ref.name}",
                e,
            )
        }
    }

    /**
     * Replicates a status check of `dbms.cluster.statusCheck` ([TransactionEnvelope.statusCheck]),
     * which every member applies without committing anything. Only on the leader.
     */
    suspend fun replicateStatusCheck(): CommitResult =
        consensus.submit(
            ref,
            TransactionEnvelope.statusCheck(
                database.dependencyResolver
                    .resolveDependency(KernelVersionProvider::class.java)
                    .kernelVersion()
            ),
        )

    /** Replicates a kernel transaction and returns the append index of its local apply. */
    fun replicate(batch: CommandBatch): Long {
        // Counted before the lease is checked: a transfer that revokes the lease, then finds no
        // commit under way, knows that every later one finds the lease revoked.
        commits.withLock { committing++ }
        try {
            return replicateUnderLease(batch)
        } finally {
            commits.withLock { if (--committing == 0) noCommits.signalAll() }
        }
    }

    private fun replicateUnderLease(batch: CommandBatch): Long {
        counts.replicationNew.increment()
        val started = System.nanoTime()
        val lease = leaseService.currentLease
        if (lease == LeaseService.NO_LEASE) throw failed(notLeader(consensus.status(ref).leader))
        if (batch.leaseId != lease) throw failed(leaseExpired(batch.leaseId))
        val payload = TransactionEnvelope.encode(batch)
        if (payload.size > consensus.maxEntrySize) {
            throw failed(
                CommitFailure.COMMIT_FAILED.exception(
                    "The transaction is too large to replicate: ${payload.size} bytes of changes, " +
                        "the maximum is ${consensus.maxEntrySize}. Split it into smaller " +
                        "transactions."
                )
            )
        }
        val result =
            try {
                runBlocking { consensus.submit(ref, payload) }
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                throw unknown(outcomeUnknown(e))
            }
        return when (result) {
            is CommitResult.Committed ->
                when (val outcome = ApplyOutcome.decode(result.response)) {
                    is ApplyOutcome.Applied -> {
                        counts.replicationSuccess.increment()
                        counts.replicationMillis.update(
                            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)
                        )
                        outcome.appendIndex
                    }
                    is ApplyOutcome.LeaseExpired -> throw failed(leaseExpired(outcome.leaseId))
                    null ->
                        throw unknown(
                            outcomeUnknown(
                                IllegalStateException("entry ${result.index} had no outcome")
                            )
                        )
                }
            is CommitResult.NotLeader -> throw failed(notLeader(result.leaderHint))
            is CommitResult.OutcomeUnknown -> throw unknown(outcomeUnknown(result.cause))
        }
    }

    /** [failure], counted as a transaction refused before or instead of replicating. */
    private fun failed(failure: TransactionFailureException) = failure.also {
        counts.replicationFail.increment()
    }

    /** [failure], counted as a transaction whose outcome is unknown. */
    private fun unknown(failure: TransactionFailureException) = failure.also {
        counts.replicationMaybe.increment()
    }

    private fun onStatus(status: GroupStatus) {
        synchronized(lock) {
            if (statusListener == null) return
            val term = if (status.role == Role.Leader && !handingOver) status.term else NOT_WRITABLE
            if (term != writableTerm) {
                leaveWritableTerm()
                if (term != NOT_WRITABLE) enterWritableTerm(term)
            }
        }
        afterGroupChange()
    }

    private fun enterWritableTerm(term: Long) {
        writableTerm = term
        leaseService.grant(term)
        val open = checkNotNull(kernelTransactions).activeTransactions()
        log.info(
            "This member is the writable leader of %s in term %d; free-ID reuse waits for %d open transactions",
            ref.name,
            term,
            open.size,
        )
        reuseCheck =
            scheduler.scheduleWithFixedDelay(
                { enableIdReuse(term, open) },
                0,
                REUSE_CHECK_MILLIS,
                TimeUnit.MILLISECONDS,
            )
    }

    private fun enableIdReuse(term: Long, open: Set<KernelTransactionHandle>) {
        synchronized(lock) {
            if (writableTerm != term || open.any { it.isOpen }) return
            // Cancelled first: an exception would otherwise end the schedule without a trace.
            reuseCheck?.cancel(false)
            reuseCheck = null
            if (clearIdCaches(reuse = true)) {
                log.info("Free-ID reuse enabled for %s in term %d", ref.name, term)
            }
        }
    }

    private fun leaveWritableTerm() {
        // Revoke first, so no transaction gets a lease while the ID caches are being dropped.
        leaseService.revoke()
        reuseCheck?.cancel(false)
        reuseCheck = null
        if (writableTerm != NOT_WRITABLE) {
            // Cached IDs may be used by the next leader; drop them.
            clearIdCaches(reuse = false)
            log.info(
                "This member is no longer the writable leader of %s (term %d)",
                ref.name,
                writableTerm,
            )
        }
        writableTerm = NOT_WRITABLE
    }

    /** Returns false, after logging why, if the caches could not be cleared; reuse stays off. */
    private fun clearIdCaches(reuse: Boolean): Boolean =
        try {
            val ids = database.dependencyResolver.resolveDependency(IdGeneratorFactory::class.java)
            database.cursorContextFactory.create(ID_CACHE_TAG).use { ids.clearCache(reuse, it) }
            true
        } catch (e: Exception) {
            log.error("Clearing the ID caches of ${ref.name} failed; free IDs are not reused", e)
            false
        }

    private fun notLeader(leader: MemberId?): TransactionFailureException =
        CommitFailure.NOT_LEADER.exception(refusal(leader))

    /** Why this member refuses a write, given the leader it knows of. */
    private fun refusal(leader: MemberId?): String =
        if (handingOver) {
            "This member is handing the leadership of database '${ref.name}' over to another " +
                "member. Retry: writes succeed once it has taken over."
        } else {
            notLeaderMessage(leader)
        }

    private fun leaseExpired(leaseId: Int): TransactionFailureException =
        CommitFailure.LEASE_EXPIRED.exception(
            "The transaction ran under the lease of Raft term $leaseId of database ${ref.name}, which is " +
                "no longer valid."
        )

    private fun outcomeUnknown(cause: Throwable): TransactionFailureException =
        CommitFailure.COMMIT_FAILED.exception(
            "Replicating the transaction on database ${ref.name} failed and its outcome is unknown: it " +
                "may or may not be committed. Check the data before retrying.",
            cause,
        )

    private companion object {
        val STOP_DRAIN: Duration = Duration.ofSeconds(10)
        const val NOT_WRITABLE = -1L
        const val REUSE_CHECK_MILLIS = 200L
        const val ID_CACHE_TAG = "dodecaIdCache"
    }
}
