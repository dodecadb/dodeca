package org.dozendb.cluster.internal

import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.until
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.graphdb.event.DatabaseEventContext
import org.neo4j.graphdb.event.DatabaseEventListenerAdapter
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo
import org.neo4j.storageengine.api.TransactionIdStore

/**
 * The Raft log of `system` is discarded like that of any database: a member behind it copies
 * `system`, and its start, its reconciler and administration commands keep working meanwhile.
 */
class SystemCatchUpTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null
    private var users = 0

    @AfterEach
    fun stop() {
        harness?.close()
    }

    /**
     * The smallest entries and segments (256 KiB, about 280 administration commands), and frequent
     * snapshots, so that logs are discarded quickly.
     */
    private fun cluster(size: Int = 3): ClusterHarness =
        ClusterHarness(root, size) {
                it.copy(
                    maxEntrySize = 64 shl 10,
                    logSegmentSize = 1,
                    snapshotInterval = Duration.ofMillis(200),
                )
            }
            .also {
                harness = it
                it.start()
            }

    private fun ClusterHarness.administer(command: String) {
        awaitLeader(SYSTEM).database(SYSTEM).executeTransactionally(command)
    }

    private fun ClusterHarness.Member.checkpoint(name: String) {
        database(name)
            .dependencyResolver
            .resolveDependency(CheckPointer::class.java)
            .forceCheckPoint(SimpleTriggerInfo("test"))
    }

    /** Creates users, checkpointing every running member, until the leader discarded [index]. */
    private fun ClusterHarness.discardSystemLogPast(index: Long) {
        await withPollInterval
            Duration.ofMillis(10) atMost
            Duration.ofMinutes(5) untilAsserted
            {
                repeat(100) {
                    administer(
                        "CREATE USER user${users++} SET PASSWORD 'user-secret' CHANGE NOT REQUIRED"
                    )
                }
                running().forEach { it.checkpoint(SYSTEM) }
                assertThat(awaitLeader(SYSTEM).firstLogIndex(SYSTEM_GROUP)).isGreaterThan(index + 1)
            }
    }

    /** Writes and checkpoints the default database until the leader discarded [index]. */
    private fun ClusterHarness.discardDefaultLogPast(index: Long, group: UUID) {
        val blob = "x".repeat(20_000)
        await withPollInterval
            Duration.ofMillis(10) atMost
            Duration.ofMinutes(2) untilAsserted
            {
                awaitLeader()
                    .database()
                    .executeTransactionally(
                        "CREATE (:Blob {data: \$data})",
                        mapOf<String, Any>("data" to blob),
                    )
                running().forEach { it.checkpoint(DEFAULT) }
                assertThat(awaitLeader().firstLogIndex(group)).isGreaterThan(index + 1)
            }
    }

    private fun ClusterHarness.Member.lastTransaction(name: String): Pair<Long, Long> {
        val transaction =
            database(name)
                .dependencyResolver
                .resolveDependency(TransactionIdStore::class.java)
                .lastCommittedTransaction
        return transaction.id() to transaction.consensusIndex()
    }

    private fun ClusterHarness.Member.userCount(): Long =
        database(SYSTEM).executeTransactionally("SHOW USERS YIELD user", emptyMap()) {
            it.stream().count()
        }

    /** Waits until every running member holds the leader's last transaction of [names]. */
    private fun ClusterHarness.awaitSameEverywhere(vararg names: String) {
        // Members replacing a database fail reads of it for a moment.
        await.ignoreExceptions() withPollInterval
            Duration.ofMillis(200) atMost
            Duration.ofMinutes(2) untilAsserted
            {
                for (name in names) {
                    val leader = awaitLeader(name)
                    for (member in running()) {
                        assertThat(member.lastTransaction(name))
                            .describedAs("$name on $member: last transaction id and Raft index")
                            .isEqualTo(leader.lastTransaction(name))
                    }
                }
            }
    }

    private fun ClusterHarness.awaitOnlineEverywhere(name: String) {
        await.ignoreExceptions() withPollInterval
            Duration.ofMillis(200) atMost
            Duration.ofMinutes(2) untilAsserted
            {
                for (member in running()) {
                    assertThat(member.database(name).isAvailable(0)).describedAs("$member").isTrue()
                }
            }
    }

    private fun ClusterHarness.Member.debugLog(): List<String> =
        Files.readAllLines(home.resolve("logs/debug.log"))

    /** The time of a line of Neo4j's debug log. */
    private fun timeOf(line: String): Instant =
        OffsetDateTime.parse(line.substring(0, 28), LOG_TIME).toInstant()

    private fun ClusterHarness.Member.assertCopiedSystemWithoutErrors() {
        val log = debugLog()
        assertThat(log).anyMatch { "Database system now holds Raft snapshot" in it }
        assertThat(log.filter { " ERROR " in it }).describedAs("ERROR lines of $this").isEmpty()
    }

    @Test
    fun `a member down past compaction copies system and its database, and follows what it missed`() {
        val cluster = cluster()
        val leader = cluster.awaitLeader(SYSTEM)
        val behind = cluster.members.first { it != leader && it != cluster.awaitLeader() }
        val defaultGroup = behind.database().databaseId().databaseId().uuid()
        behind.stop()
        cluster.administer("CREATE DATABASE films")
        cluster.discardSystemLogPast(checkNotNull(leader.status(SYSTEM)).commitIndex)
        cluster.discardDefaultLogPast(
            checkNotNull(cluster.awaitLeader().status()).commitIndex,
            defaultGroup,
        )

        behind.start()

        cluster.awaitSameEverywhere(SYSTEM, DEFAULT)
        assertThat(behind.userCount()).isEqualTo(cluster.awaitLeader(SYSTEM).userCount())
        // The cluster is idle: nothing applied after the copy tells the member about films.
        cluster.awaitOnlineEverywhere("films")
        behind.assertCopiedSystemWithoutErrors()
        assertThat(behind.debugLog()).anyMatch { "Database neo4j now holds Raft snapshot" in it }
    }

    @Test
    fun `a member that learns it is behind while it starts copies system while it starts`() {
        val cluster = cluster()
        val leader = cluster.awaitLeader(SYSTEM)
        val behind = cluster.members.first { it != leader }
        behind.stop()
        cluster.discardSystemLogPast(checkNotNull(leader.status(SYSTEM)).commitIndex)
        val held = AtomicBoolean()
        // Holds the first start of the default database, which follows the start of system, until
        // the leader's snapshot notice made system catch up, so that its copy replaces system
        // while the DBMS starts. A Raft group hears from the leader only once its database runs,
        // and the leader retries a member that was down only every few seconds.
        behind.databaseEventListeners =
            listOf(
                object : DatabaseEventListenerAdapter() {
                    override fun databaseStart(eventContext: DatabaseEventContext) {
                        if (
                            eventContext.databaseName == DEFAULT && held.compareAndSet(false, true)
                        ) {
                            await atMost
                                Duration.ofMinutes(1) until
                                {
                                    behind.debugLog().any { CATCH_UP_NOTICE in it }
                                }
                        }
                    }
                }
            )

        behind.start()
        val started = Instant.now()

        val catchUp = behind.debugLog().first { CATCH_UP_NOTICE in it }
        assertThat(timeOf(catchUp)).describedAs("catch-up began while starting").isBefore(started)
        cluster.awaitSameEverywhere(SYSTEM)
        assertThat(behind.database().isAvailable(0)).isTrue()
        behind.assertCopiedSystemWithoutErrors()
    }

    @Test
    fun `databases created and dropped while a member copies system follow on it`() {
        val cluster = cluster()
        val leader = cluster.awaitLeader(SYSTEM)
        val behind = cluster.members.first { it != leader }
        cluster.administer("CREATE DATABASE films")
        cluster.awaitOnlineEverywhere("films")
        behind.stop()
        cluster.discardSystemLogPast(checkNotNull(leader.status(SYSTEM)).commitIndex)

        val starting = thread { behind.start() }
        cluster.administer("CREATE DATABASE series")
        cluster.administer("DROP DATABASE films")
        repeat(50) {
            cluster.administer(
                "CREATE USER during$it SET PASSWORD 'user-secret' CHANGE NOT REQUIRED"
            )
        }
        starting.join()

        cluster.awaitSameEverywhere(SYSTEM)
        cluster.awaitOnlineEverywhere("series")
        await atMost
            Duration.ofMinutes(1) untilAsserted
            {
                assertThat(checkNotNull(behind.dbms).listDatabases()).doesNotContain("films")
            }
        behind.assertCopiedSystemWithoutErrors()
    }

    @Test
    fun `a single member discards its system log and restarts with every user`() {
        val cluster = cluster(1)
        val only = cluster.members.single()
        cluster.discardSystemLogPast(1)
        val count = only.userCount()

        only.stop()
        only.start()

        assertThat(only.userCount()).isEqualTo(count)
        cluster.administer("CREATE USER after SET PASSWORD 'user-secret' CHANGE NOT REQUIRED")
        assertThat(only.userCount()).isEqualTo(count + 1)
    }

    private companion object {
        const val SYSTEM = "system"
        const val DEFAULT = "neo4j"
        const val CATCH_UP_NOTICE = "Database system needs Raft entries"
        val SYSTEM_GROUP: UUID = NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID.databaseId().uuid()
        val LOG_TIME: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSXX")
    }
}
