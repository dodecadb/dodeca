package org.dozendb.core.monitoring

import java.nio.file.Path
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import org.assertj.core.api.Assertions.assertThat
import org.dozendb.core.StandaloneServer
import org.dozendb.core.StandaloneServer.Companion.SYSTEM
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.neo4j.common.Subject
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer
import org.neo4j.scheduler.Group
import org.neo4j.scheduler.JobMonitoringParams
import org.neo4j.scheduler.JobScheduler
import org.neo4j.storageengine.api.TransactionIdStore

/** Neo4j Enterprise's procedures for looking into a running server. */
class MonitoringProceduresTest {
    @TempDir lateinit var root: Path
    private lateinit var server: StandaloneServer

    @BeforeEach
    fun start() {
        server = StandaloneServer(root).also { it.start() }
    }

    @AfterEach
    fun stop() {
        server.close()
    }

    @Test
    fun `db checkpoint checkpoints the database it runs on, up to its last transaction`() {
        server.rows("CREATE (:Film)")
        val database = server.database().dependencyResolver
        val last = database.resolveDependency(TransactionIdStore::class.java)
        val checkPointer = database.resolveDependency(CheckPointer::class.java)
        assertThat(checkPointer.latestCheckPointInfo().highestObservedClosedTransactionId().id())
            .isLessThan(last.lastCommittedTransactionId)

        val rows = server.rows("CALL db.checkpoint()")

        assertThat(rows)
            .containsExactly(mapOf("success" to true, "message" to "Checkpoint completed."))
        assertThat(checkPointer.latestCheckPointInfo().highestObservedClosedTransactionId().id())
            .isEqualTo(last.lastCommittedTransactionId)
        assertThat(server.rows("CALL db.checkpoint() YIELD success", SYSTEM))
            .describedAs("on system")
            .containsExactly(mapOf("success" to true))
    }

    private val scheduler: JobScheduler
        get() = server.database().dependencyResolver.resolveDependency(JobScheduler::class.java)

    @Test
    fun `dbms scheduler jobs lists the jobs the scheduler keeps track of`() {
        val job =
            scheduler.schedule(
                Group.TESTING,
                JobMonitoringParams(Subject("alice"), "neo4j", "a job for later"),
                {},
                1,
                TimeUnit.HOURS,
            )
        try {
            val rows =
                server.rows("CALL dbms.scheduler.jobs()", SYSTEM).filter {
                    it["description"] == "a job for later"
                }

            assertThat(rows).hasSize(1)
            val row = rows.single()
            assertThat(
                    row.filterKeys {
                        it in setOf("group", "database", "submitter", "type", "state")
                    }
                )
                .isEqualTo(
                    mapOf(
                        "group" to Group.TESTING.groupName(),
                        "database" to "neo4j",
                        "submitter" to "alice",
                        "type" to "DELAYED",
                        "state" to "SCHEDULED",
                    )
                )
            assertThat(row.keys)
                .containsExactlyInAnyOrder(
                    "jobId",
                    "group",
                    "submitted",
                    "database",
                    "submitter",
                    "description",
                    "type",
                    "scheduledAt",
                    "period",
                    "state",
                    "currentStateDescription",
                )
            assertThat(row.values).allMatch { it is String }
        } finally {
            job.cancel()
        }
    }

    @Test
    fun `dbms scheduler failedJobs lists the jobs that failed`() {
        scheduler
            .schedule(
                Group.TESTING,
                JobMonitoringParams(Subject("alice"), "neo4j", "a job that fails"),
                Runnable { throw IllegalStateException("out of luck") },
            )
            .let { job -> runCatching { job.waitTermination() } }

        val rows =
            server.rows("CALL dbms.scheduler.failedJobs()", SYSTEM).filter {
                it["description"] == "a job that fails"
            }

        assertThat(rows).hasSize(1)
        assertThat(rows.single()["failureDescription"] as String).contains("out of luck")
        assertThat(rows.single()["submitter"]).isEqualTo("alice")
        assertThat(rows.single().keys)
            .containsExactlyInAnyOrder(
                "jobId",
                "group",
                "database",
                "submitter",
                "description",
                "type",
                "submitted",
                "executionStart",
                "failureTime",
                "failureDescription",
            )
    }

    @Test
    fun `dbms scheduler groups lists the groups that run jobs, with their threads`() {
        val running = CountDownLatch(1)
        val release = CountDownLatch(1)
        val job =
            // Not TESTING, whose jobs run on the thread that schedules them.
            scheduler.schedule(Group.FILE_IO_HELPER) {
                running.countDown()
                release.await()
            }
        try {
            running.await()

            val groups = server.rows("CALL dbms.scheduler.groups()", SYSTEM)

            assertThat(groups)
                .contains(mapOf("group" to Group.FILE_IO_HELPER.groupName(), "threads" to 1L))
        } finally {
            release.countDown()
            job.waitTermination()
        }
    }
}
