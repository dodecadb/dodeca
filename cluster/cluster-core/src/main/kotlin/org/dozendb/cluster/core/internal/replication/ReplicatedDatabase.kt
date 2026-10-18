package org.dozendb.cluster.core.internal.replication

import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.runBlocking
import org.dozendb.cluster.api.CommitResult
import org.dozendb.cluster.api.ConsensusLog
import org.dozendb.cluster.api.ConsensusUnavailableException
import org.dozendb.cluster.api.DatabaseRef
import org.dozendb.cluster.api.GroupStatus
import org.dozendb.cluster.api.Member
import org.dozendb.cluster.api.MemberId
import org.dozendb.cluster.api.Role
import org.dozendb.cluster.core.internal.reads.ReadConsistencyException
import org.neo4j.internal.id.IdGeneratorFactory
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException
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
 * @property onSnapshotNeeded called when the group's log no longer holds entries this member needs.
 */
internal class ReplicatedDatabase(
    val namedDatabaseId: NamedDatabaseId,
    private val consensus: ConsensusLog,
    private val systemDatabase: DatabaseRef,
    private val idGeneratorFactory: IdGeneratorFactory,
    storeExists: Boolean,
    private val scheduler: ScheduledExecutorService,
    private val log: InternalLog,
    private val onSnapshotNeeded: (ReplicatedDatabase, Member?, Long) -> Unit,
) {
    val ref = DatabaseRef(namedDatabaseId.name(), namedDatabaseId.databaseId().uuid())

    /**
     * True from creating a new store until the group starts. Neo4j then commits one local
     * transaction that initialises the store; every other member copies the result.
     */
    @Volatile
    var bootstrapping: Boolean = !storeExists
        private set

    val leaseService = ClusterLeaseService { bootstrapping }

    /** The local commit process that applies committed entries, set when the database starts. */
    @Volatile var applyProcess: TransactionCommitProcess? = null

    private val lock = Any()
    private lateinit var database: Database
    private var kernelTransactions: KernelTransactions? = null
    private var statusListener: AutoCloseable? = null
    private var writableTerm = NOT_WRITABLE
    private var reuseCheck: ScheduledFuture<*>? = null

    fun commitProcessFactory(local: TransactionalProcessFactory): TransactionalProcessFactory =
        RaftCommitProcessFactory(this, local)

    fun attach(database: Database) {
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
                ) { leader, minimumIndex ->
                    onSnapshotNeeded(this, leader, minimumIndex)
                }
            val bootstrap =
                if (ref.id in consensus.localGroups()) emptyList()
                else consensus.members(systemDatabase)
            // From now on every transaction goes through Raft.
            bootstrapping = false
            val listener = consensus.addStatusListener(ref, ::onStatus)
            statusListener = listener
            try {
                consensus.startGroup(ref, stateMachine, bootstrap)
            } catch (e: Exception) {
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
     * keeps running on this member, which no longer leads it.
     */
    fun stop() {
        synchronized(lock) {
            val listener = statusListener ?: return
            listener.close()
            statusListener = null
            leaveWritableTerm()
            kernelTransactions = null
            consensus.detachGroup(ref)
            log.info("Stopped replicating database %s", ref.name)
        }
    }

    fun isRunning(): Boolean = synchronized(lock) { statusListener != null }

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

    /** Replicates a kernel transaction and returns the append index of its local apply. */
    fun replicate(batch: CommandBatch): Long {
        val lease = leaseService.currentLease
        if (lease == LeaseService.NO_LEASE) throw notLeader(consensus.status(ref).leader)
        if (batch.leaseId != lease) throw leaseExpired(batch.leaseId)
        val payload = TransactionEnvelope.encode(batch)
        if (payload.size > consensus.maxEntrySize) {
            throw CommitFailure.COMMIT_FAILED.exception(
                "The transaction is too large to replicate: ${payload.size} bytes of changes, the " +
                    "maximum is ${consensus.maxEntrySize}. Split it into smaller transactions."
            )
        }
        val result =
            try {
                runBlocking { consensus.submit(ref, payload) }
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                throw outcomeUnknown(e)
            }
        return when (result) {
            is CommitResult.Committed ->
                when (val outcome = ApplyOutcome.decode(result.response)) {
                    is ApplyOutcome.Applied -> outcome.appendIndex
                    is ApplyOutcome.LeaseExpired -> throw leaseExpired(outcome.leaseId)
                    null ->
                        throw outcomeUnknown(
                            IllegalStateException("entry ${result.index} had no outcome")
                        )
                }
            is CommitResult.NotLeader -> throw notLeader(result.leaderHint)
            is CommitResult.OutcomeUnknown -> throw outcomeUnknown(result.cause)
        }
    }

    private fun onStatus(status: GroupStatus) {
        synchronized(lock) {
            if (statusListener == null) return
            val term = if (status.role == Role.Leader) status.term else NOT_WRITABLE
            if (term == writableTerm) return
            leaveWritableTerm()
            if (term != NOT_WRITABLE) enterWritableTerm(term)
        }
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
            database.cursorContextFactory.create(ID_CACHE_TAG).use {
                idGeneratorFactory.clearCache(reuse, it)
            }
            true
        } catch (e: Exception) {
            log.error("Clearing the ID caches of ${ref.name} failed; free IDs are not reused", e)
            false
        }

    private fun notLeader(leader: MemberId?): TransactionFailureException {
        val address = leader?.let { id ->
            consensus.members(ref).firstOrNull { it.id == id }?.raftAddress
        }
        val known =
            if (leader == null) "No leader is known right now."
            else "The leader is member $leader ($address)."
        return CommitFailure.NOT_LEADER.exception(
            "This member is not the leader of database ${ref.name}. $known"
        )
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
        const val NOT_WRITABLE = -1L
        const val REUSE_CHECK_MILLIS = 200L
        const val ID_CACHE_TAG = "dozendbIdCache"
    }
}
