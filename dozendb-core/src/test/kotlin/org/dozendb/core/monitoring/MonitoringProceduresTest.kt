package org.dozendb.core.monitoring

import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilNotNull
import org.dozendb.core.StandaloneServer
import org.dozendb.core.StandaloneServer.Companion.SYSTEM
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.neo4j.common.Subject
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Transaction
import org.neo4j.kernel.impl.coreapi.InternalTransaction
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

    @Test
    fun `dbms listPools lists the memory pools and the pools of each database`() {
        server.administer("CREATE DATABASE films WAIT")

        val rows = server.rows("CALL dbms.listPools()", SYSTEM)

        val pools = rows.map { it["pool"] to it["databaseName"] }
        assertThat(pools)
            .contains("Transaction" to "", "Transaction" to "neo4j", "Transaction" to "films")
        for (row in rows) {
            assertThat(row.keys)
                .containsExactlyInAnyOrder(
                    "pool",
                    "databaseName",
                    "heapMemoryUsed",
                    "heapMemoryUsedBytes",
                    "nativeMemoryUsed",
                    "nativeMemoryUsedBytes",
                    "freeMemory",
                    "freeMemoryBytes",
                    "totalPoolMemory",
                    "totalPoolMemoryBytes",
                )
            assertThat(row.values).allMatch { it is String }
            assertThat(row["heapMemoryUsedBytes"] as String)
                .describedAs("$row")
                .containsOnlyDigits()
        }
    }

    @Test
    fun `dbms listPools shows a user the pools of the databases the user may see`() {
        server.close()
        server =
            StandaloneServer(
                    root.resolve("secured"),
                    mapOf(
                        GraphDatabaseSettings.auth_enabled to true,
                        BoltConnector.enabled to true,
                    ),
                )
                .also { it.start() }
        server.administer("CREATE DATABASE films WAIT")
        server.administer("CREATE USER alice SET PASSWORD 'alice-secret-1' CHANGE NOT REQUIRED")

        val pools =
            GraphDatabase.driver(
                    "bolt://${server.boltAddress()}",
                    AuthTokens.basic("alice", "alice-secret-1"),
                )
                .use { driver ->
                    driver.session(SessionConfig.forDatabase(SYSTEM)).use { session ->
                        session.run("CALL dbms.listPools()").list {
                            it["pool"].asString() to it["databaseName"].asString()
                        }
                    }
                }

        // The role PUBLIC gives access to the default database only.
        assertThat(pools).contains("Transaction" to "", "Transaction" to "neo4j")
        assertThat(pools.map { it.second }).doesNotContain("films")
    }

    /** The id SHOW TRANSACTIONS gives [tx], an open transaction of the default database. */
    private fun transactionId(tx: Transaction): String =
        "neo4j-transaction-${(tx as InternalTransaction).kernelTransaction().transactionSequenceNumber}"

    @Test
    fun `db listLocks lists the locks held in the database, with their transactions`() {
        val film =
            server.database().executeTransactionally(
                "CREATE (f:Film) SET f.id = id(f) RETURN f.id AS id",
                emptyMap(),
            ) {
                it.next()["id"] as Long
            }
        server.database().beginTx().use { holder ->
            holder.acquireWriteLock(holder.findNode(Label.label("Film"), "id", film))

            val locks = server.rows("CALL db.listLocks()")

            assertThat(locks)
                .contains(
                    mapOf(
                        "mode" to "EXCLUSIVE",
                        "resourceType" to "NODE",
                        "resourceId" to film,
                        "transactionId" to transactionId(holder),
                    )
                )
        }
    }

    /**
     * Runs [check] with the id of a running query, of a transaction that holds the exclusive lock
     * of a movie and waits for that of a film, and the movie's id.
     */
    private fun withWaitingQuery(check: (query: String, movie: Long) -> Unit) {
        val (film, movie) =
            server.database().executeTransactionally(
                "CREATE (f:Film), (m:Movie) SET f.id = id(f) RETURN id(f) AS film, id(m) AS movie",
                emptyMap(),
            ) {
                it.next().let { row -> row["film"] as Long to row["movie"] as Long }
            }
        val executor = Executors.newSingleThreadExecutor()
        server.database().beginTx().use { holder ->
            holder.acquireWriteLock(holder.findNode(Label.label("Film"), "id", film))
            val waiting = executor.submit {
                server.database().beginTx().use { tx ->
                    tx.execute("MATCH (m:Movie) SET m.seen = true").close()
                    tx.execute("MATCH (f:Film) SET f.seen = true").close()
                    tx.commit()
                }
            }
            try {
                val query =
                    await atMost
                        Duration.ofSeconds(30) untilNotNull
                        {
                            server
                                .rows(
                                    "SHOW TRANSACTIONS YIELD currentQueryId, currentQuery " +
                                        "WHERE currentQuery STARTS WITH 'MATCH (f:Film)' " +
                                        "RETURN currentQueryId",
                                    SYSTEM,
                                )
                                .singleOrNull()
                                ?.get("currentQueryId") as String?
                        }
                check(query, movie)
            } finally {
                holder.rollback()
                waiting.get()
                executor.shutdown()
            }
        }
    }

    @Test
    fun `dbms listActiveLocks lists the locks of the transaction running a query`() =
        withWaitingQuery { query, movie ->
            val locks = server.rows("CALL dbms.listActiveLocks('$query')", SYSTEM)

            assertThat(locks)
                .contains(
                    mapOf("mode" to "EXCLUSIVE", "resourceType" to "NODE", "resourceId" to movie)
                )
            assertThat(server.rows("CALL dbms.listActiveLocks('query-999999')", SYSTEM)).isEmpty()
        }

    @Test
    fun `dbms listActiveLocks shows only the transactions a user may see`() {
        server.close()
        server =
            StandaloneServer(
                    root.resolve("secured"),
                    mapOf(
                        GraphDatabaseSettings.auth_enabled to true,
                        BoltConnector.enabled to true,
                    ),
                )
                .also { it.start() }
        server.administer("ALTER USER neo4j SET PASSWORD 'admin-secret-1' CHANGE NOT REQUIRED")
        server.administer("CREATE USER alice SET PASSWORD 'alice-secret-1' CHANGE NOT REQUIRED")
        fun locks(user: String, password: String, query: String): List<Long> =
            GraphDatabase.driver("bolt://${server.boltAddress()}", AuthTokens.basic(user, password))
                .use { driver ->
                    driver.session(SessionConfig.forDatabase(SYSTEM)).use { session ->
                        session.run("CALL dbms.listActiveLocks('$query')").list {
                            it["resourceId"].asLong()
                        }
                    }
                }

        withWaitingQuery { query, movie ->
            // Not alice's own, and PUBLIC may not see other users' transactions.
            assertThat(locks("alice", "alice-secret-1", query)).isEmpty()
            assertThat(locks("neo4j", "admin-secret-1", query)).contains(movie)
        }
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
