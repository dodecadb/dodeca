package org.dozendb.core.monitoring

import java.io.IOException
import java.time.ZoneId
import java.util.stream.Stream
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.kernel.api.procedure.SystemProcedure
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.procedure.Admin
import org.neo4j.procedure.Context
import org.neo4j.procedure.Description
import org.neo4j.procedure.Mode
import org.neo4j.procedure.Procedure
import org.neo4j.procedure.builtin.ProceduresTimeFormatHelper.formatInterval
import org.neo4j.procedure.builtin.ProceduresTimeFormatHelper.formatTime
import org.neo4j.scheduler.JobScheduler

/** Neo4j Enterprise's procedures for looking into a running server, as its manual lists them. */
class MonitoringProcedures {
    @JvmField @Context var graph: GraphDatabaseAPI? = null

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
