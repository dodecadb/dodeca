package org.dozendb.core.monitoring

import java.io.IOException
import java.time.ZoneId
import java.util.stream.Stream
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.dbms.database.DatabaseContext
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.internal.kernel.api.security.AdminActionOnResource
import org.neo4j.internal.kernel.api.security.PrivilegeAction
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.internal.kernel.api.security.UserSegment
import org.neo4j.io.ByteUnit
import org.neo4j.kernel.api.KernelTransactionHandle
import org.neo4j.kernel.api.procedure.SystemProcedure
import org.neo4j.kernel.database.DatabaseReferenceRepository
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.kernel.impl.api.KernelTransactions
import org.neo4j.kernel.impl.locking.LockManager
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.memory.MemoryPools
import org.neo4j.memory.ScopedMemoryPool
import org.neo4j.procedure.Admin
import org.neo4j.procedure.Context
import org.neo4j.procedure.Description
import org.neo4j.procedure.Mode
import org.neo4j.procedure.Name
import org.neo4j.procedure.Procedure
import org.neo4j.procedure.builtin.ProceduresTimeFormatHelper.formatInterval
import org.neo4j.procedure.builtin.ProceduresTimeFormatHelper.formatTime
import org.neo4j.procedure.builtin.QueryId
import org.neo4j.procedure.builtin.TransactionId
import org.neo4j.scheduler.JobScheduler

/** Neo4j Enterprise's procedures for looking into a running server, as its manual lists them. */
class MonitoringProcedures {
    @JvmField @Context var graph: GraphDatabaseAPI? = null

    @JvmField @Context var securityContext: SecurityContext? = null

    @SystemProcedure
    @Description(
        "Starts a checkpoint of the database and waits for it, or for the one under way, to finish."
    )
    @Procedure(name = "db.checkpoint", mode = Mode.DBMS)
    fun checkpoint(): Stream<CheckpointResult> {
        val checkPointer =
            checkNotNull(graph).dependencyResolver.resolveDependency(CheckPointer::class.java)
        val result =
            try {
                checkPointer.tryCheckPoint(SimpleTriggerInfo("Call to db.checkpoint() procedure"))
                CheckpointResult(true, "Checkpoint completed.")
            } catch (e: IOException) {
                CheckpointResult(false, "Checkpoint failed: ${e.message}")
            }
        return Stream.of(result)
    }

    /** A row of `db.checkpoint`. */
    class CheckpointResult(@JvmField val success: Boolean, @JvmField val message: String)

    @Admin
    @SystemProcedure
    @Description("Lists the jobs of the server's internal job scheduler that it keeps track of.")
    @Procedure(name = "dbms.scheduler.jobs", mode = Mode.DBMS)
    fun schedulerJobs(): Stream<JobRow> {
        val zone = timezone()
        return scheduler().monitoredJobs.stream().map { job ->
            JobRow(
                jobId = jobId(job.id),
                group = job.group.groupName(),
                submitted = formatTime(job.submitted, zone),
                database = job.targetDatabaseName.orEmpty(),
                submitter = job.submitter?.describe().orEmpty(),
                description = job.description.orEmpty(),
                type = job.type.name,
                scheduledAt = job.nextDeadline?.let { formatTime(it, zone) }.orEmpty(),
                period = job.period?.let { formatInterval(it.toMillis()) }.orEmpty(),
                state = job.state.name,
                currentStateDescription = job.currentStateDescription.orEmpty(),
            )
        }
    }

    /** A row of `dbms.scheduler.jobs`. */
    class JobRow(
        @JvmField val jobId: String,
        @JvmField val group: String,
        @JvmField val submitted: String,
        @JvmField val database: String,
        @JvmField val submitter: String,
        @JvmField val description: String,
        @JvmField val type: String,
        @JvmField val scheduledAt: String,
        @JvmField val period: String,
        @JvmField val state: String,
        @JvmField val currentStateDescription: String,
    )

    @Admin
    @SystemProcedure
    @Description(
        "Lists the runs of jobs of the server's internal job scheduler that failed, the latest ones."
    )
    @Procedure(name = "dbms.scheduler.failedJobs", mode = Mode.DBMS)
    fun schedulerFailedJobs(): Stream<FailedJobRow> {
        val zone = timezone()
        return scheduler().failedJobRuns.stream().map { run ->
            FailedJobRow(
                jobId = jobId(run.jobId),
                group = run.group.groupName(),
                database = run.targetDatabaseName.orEmpty(),
                submitter = run.submitter?.describe().orEmpty(),
                description = run.description.orEmpty(),
                type = run.jobType.name,
                submitted = formatTime(run.submitted, zone),
                executionStart = formatTime(run.executionStart, zone),
                failureTime = formatTime(run.failureTime, zone),
                failureDescription = run.failureDescription.orEmpty(),
            )
        }
    }

    /** A row of `dbms.scheduler.failedJobs`. */
    class FailedJobRow(
        @JvmField val jobId: String,
        @JvmField val group: String,
        @JvmField val database: String,
        @JvmField val submitter: String,
        @JvmField val description: String,
        @JvmField val type: String,
        @JvmField val submitted: String,
        @JvmField val executionStart: String,
        @JvmField val failureTime: String,
        @JvmField val failureDescription: String,
    )

    @Admin
    @SystemProcedure
    @Description(
        "Lists the groups of the server's internal job scheduler that run jobs, with their threads."
    )
    @Procedure(name = "dbms.scheduler.groups", mode = Mode.DBMS)
    fun schedulerGroups(): Stream<GroupRow> =
        scheduler().activeGroups().map { GroupRow(it.group().groupName(), it.threads().toLong()) }

    /** A row of `dbms.scheduler.groups`. */
    class GroupRow(@JvmField val group: String, @JvmField val threads: Long)

    @SystemProcedure
    @Description(
        "Lists the server's memory pools, and the pools of the databases the user may see in " +
            "them."
    )
    @Procedure(name = "dbms.listPools", mode = Mode.DBMS)
    fun listPools(): Stream<PoolRow> {
        val dependencies = checkNotNull(graph).dependencyResolver
        val pools = dependencies.resolveDependency(MemoryPools::class.java)
        val references = dependencies.resolveDependency(DatabaseReferenceRepository::class.java)
        val access = checkNotNull(securityContext).databaseAccessMode()
        val visible = { name: String ->
            references
                .getByAlias(NormalizedDatabaseName(name))
                .map { access.canSeeDatabase(it) }
                .orElse(false)
        }
        return pools.pools.stream().flatMap { global ->
            Stream.concat(
                Stream.of(PoolRow.of(global, "")),
                global.databasePools
                    .filter { visible(it.databaseName()) }
                    .map { PoolRow.of(it, it.databaseName()) }
                    .stream(),
            )
        }
    }

    /** A row of `dbms.listPools`: sizes readable and in bytes, as Enterprise's are, as text. */
    class PoolRow(
        @JvmField val pool: String,
        @JvmField val databaseName: String,
        @JvmField val heapMemoryUsed: String,
        @JvmField val heapMemoryUsedBytes: String,
        @JvmField val nativeMemoryUsed: String,
        @JvmField val nativeMemoryUsedBytes: String,
        @JvmField val freeMemory: String,
        @JvmField val freeMemoryBytes: String,
        @JvmField val totalPoolMemory: String,
        @JvmField val totalPoolMemoryBytes: String,
    ) {
        companion object {
            fun of(pool: ScopedMemoryPool, database: String): PoolRow {
                val unlimited = pool.totalSize() == Long.MAX_VALUE
                return PoolRow(
                    pool = pool.group().getName(),
                    databaseName = database,
                    heapMemoryUsed = ByteUnit.bytesToString(pool.usedHeap()),
                    heapMemoryUsedBytes = pool.usedHeap().toString(),
                    nativeMemoryUsed = ByteUnit.bytesToString(pool.usedNative()),
                    nativeMemoryUsedBytes = pool.usedNative().toString(),
                    freeMemory = if (unlimited) UNLIMITED else ByteUnit.bytesToString(pool.free()),
                    freeMemoryBytes = if (unlimited) UNLIMITED else pool.free().toString(),
                    totalPoolMemory =
                        if (unlimited) UNLIMITED else ByteUnit.bytesToString(pool.totalSize()),
                    totalPoolMemoryBytes =
                        if (unlimited) UNLIMITED else pool.totalSize().toString(),
                )
            }

            /** What a pool without a limit has free, and in total. */
            const val UNLIMITED = "Unlimited"
        }
    }

    @Admin
    @SystemProcedure
    @Description("Lists the locks held in the database, with the transactions that hold them.")
    @Procedure(name = "db.listLocks", mode = Mode.DBMS)
    fun listLocks(): Stream<LockRow> {
        val database = checkNotNull(graph)
        val locks = mutableListOf<LockRow>()
        database.dependencyResolver.resolveDependency(LockManager::class.java).accept {
            mode,
            type,
            transaction,
            resource,
            _,
            _,
            _ ->
            locks +=
                LockRow(
                    mode.name,
                    type.name,
                    resource,
                    TransactionId.formatTransactionId(database.databaseName(), transaction),
                )
        }
        return locks.stream()
    }

    /** A row of `db.listLocks`: the transaction as SHOW TRANSACTIONS names it. */
    class LockRow(
        @JvmField val mode: String,
        @JvmField val resourceType: String,
        @JvmField val resourceId: Long,
        @JvmField val transactionId: String,
    )

    @SystemProcedure
    @Description(
        "Lists the locks granted to the transaction running the query with the given id, if the " +
            "user may see the transaction."
    )
    @Procedure(name = "dbms.listActiveLocks", mode = Mode.DBMS)
    fun listActiveLocks(@Name("queryId") queryId: String): Stream<ActiveLockRow> {
        val id = QueryId.parse(queryId)
        val contexts =
            checkNotNull(graph)
                .dependencyResolver
                .resolveDependency(DatabaseContextProvider::class.java)
                .registeredDatabases()
                .values
        val running =
            contexts
                .asSequence()
                .filter { it.database().isStarted }
                .flatMap { context ->
                    val dependencies = context.dependencies()
                    if (dependencies.containsDependency(KernelTransactions::class.java)) {
                        val transactions =
                            dependencies.resolveDependency(KernelTransactions::class.java)
                        transactions.executingTransactions().map { context.databaseName() to it }
                    } else {
                        emptyList()
                    }
                }
        val (database, handle) =
            running.firstOrNull { (_, handle) ->
                handle.executingQuery().map { it.internalQueryId() == id }.orElse(false)
            } ?: return Stream.empty()
        if (!visible(database, handle)) return Stream.empty()
        return handle.activeLocks().stream().map {
            ActiveLockRow(it.lockType().name, it.resourceType().name, it.resourceId())
        }
    }

    /** A row of `dbms.listActiveLocks`. */
    class ActiveLockRow(
        @JvmField val mode: String,
        @JvmField val resourceType: String,
        @JvmField val resourceId: Long,
    )

    /**
     * Whether the user may see [handle], a transaction in [database], as SHOW TRANSACTIONS does.
     */
    private fun visible(database: String, handle: KernelTransactionHandle): Boolean {
        val security = checkNotNull(securityContext)
        val user = handle.subject().executingUser()
        if (security.subject().hasUsername(user)) return true
        return security
            .allowsAdminAction(
                AdminActionOnResource(
                    PrivilegeAction.SHOW_TRANSACTION,
                    AdminActionOnResource.DatabaseScope(database),
                    UserSegment(user),
                )
            )
            .allowsAccess()
    }

    private fun DatabaseContext.databaseName(): String = database().namedDatabaseId.name()

    private fun scheduler(): JobScheduler =
        checkNotNull(graph).dependencyResolver.resolveDependency(JobScheduler::class.java)

    /** The time zone that Neo4j's procedures show times in. */
    private fun timezone(): ZoneId =
        checkNotNull(graph)
            .dependencyResolver
            .resolveDependency(Config::class.java)
            .get(GraphDatabaseSettings.db_temporal_timezone)

    private fun jobId(id: Long): String = "job-$id"
}
