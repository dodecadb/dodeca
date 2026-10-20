package org.dozendb.cluster.internal

import java.nio.file.Path
import java.time.Duration
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.dozendb.consensus.DatabaseRef
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.io.fs.FileUtils
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo
import org.neo4j.storageengine.api.StorageEngine
import org.neo4j.storageengine.api.StoreId
import org.neo4j.storageengine.api.TransactionIdStore

/** Members that missed entries the log no longer holds, or lost their database, catch up. */
class CatchUpTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    /** Small entries and segments, and frequent snapshots, so that logs are discarded quickly. */
    private fun cluster(): ClusterHarness =
        ClusterHarness(root, 3) {
                it.copy(
                    maxEntrySize = 1 shl 20,
                    logSegmentSize = 1,
                    snapshotInterval = Duration.ofMillis(200),
                )
            }
            .also {
                harness = it
                it.start()
            }

    private val blob = "x".repeat(200_000)

    private fun ClusterHarness.write(cypher: String, parameters: Map<String, Any> = emptyMap()) {
        awaitLeader().database().executeTransactionally(cypher, parameters)
    }

    private fun ClusterHarness.Member.count(label: String): Long =
        database().executeTransactionally("MATCH (n:$label) RETURN count(n) AS c", emptyMap()) {
            it.next()["c"] as Long
        }

    private fun ClusterHarness.Member.lastTransactionId(): Long =
        database()
            .dependencyResolver
            .resolveDependency(TransactionIdStore::class.java)
            .lastCommittedTransactionId

    private fun ClusterHarness.Member.ref(): DatabaseRef {
        val id = database().databaseId()
        return DatabaseRef(id.name(), id.databaseId().uuid())
    }

    private fun ClusterHarness.Member.checkpoint() {
        database()
            .dependencyResolver
            .resolveDependency(CheckPointer::class.java)
            .forceCheckPoint(SimpleTriggerInfo("test"))
    }

    /** Writes and checkpoints on every running member until the leader discarded [index]. */
    private fun ClusterHarness.discardLogUpTo(index: Long, database: UUID) {
        var written = 0
        await withPollInterval
            Duration.ofMillis(300) atMost
            Duration.ofSeconds(60) untilAsserted
            {
                write(
                    "CREATE (:Blob {i: \$i, data: \$data})",
                    mapOf("i" to written++, "data" to blob),
                )
                running().forEach { it.checkpoint() }
                assertThat(awaitLeader().firstLogIndex(database)).isGreaterThan(index + 1)
            }
    }

    /** Waits until [member] holds what the leader holds, with the same transaction ids. */
    private fun ClusterHarness.awaitSameAsLeader(member: ClusterHarness.Member) {
        await withPollInterval
            Duration.ofMillis(200) atMost
            Duration.ofSeconds(60) untilAsserted
            {
                val leader = awaitLeader()
                assertThat(member.count("Blob")).isEqualTo(leader.count("Blob"))
                assertThat(member.count("Marker")).isEqualTo(leader.count("Marker"))
                assertThat(member.lastTransactionId()).isEqualTo(leader.lastTransactionId())
            }
    }

    @Test
    fun `a member behind the leader's log replaces its database with a copy and catches up`() {
        val cluster = cluster()
        val leader = cluster.awaitLeader()
        val behind = cluster.members.first { it != leader }
        cluster.write("CREATE (:Marker {at: 'before'})")
        val database = behind.ref()
        behind.stop()
        val stoppedAt = checkNotNull(leader.status()).commitIndex

        cluster.discardLogUpTo(stoppedAt, database.id)
        behind.start()

        cluster.awaitSameAsLeader(behind)
        assertThat(behind.cluster.latestSnapshot(database)?.position?.index)
            .describedAs("snapshot installed from the copy")
            .isGreaterThan(stoppedAt)
        cluster.write("CREATE (:Marker {at: 'after'})")
        cluster.awaitSameAsLeader(behind)
        assertThat(behind.count("Marker")).isEqualTo(2)
    }

    @Test
    fun `a member whose database was deleted copies it at startup and keeps its system database`() {
        val cluster = cluster()
        val leader = cluster.awaitLeader()
        val wiped = cluster.members.first { it != leader }
        cluster.write("CREATE (:Marker {at: 'before'})")
        val systemStore = wiped.storeId("system")
        wiped.stop()
        cluster.write("CREATE (:Marker {at: 'while stopped'})")

        FileUtils.deleteDirectory(wiped.home.resolve("data/databases/neo4j"))
        FileUtils.deleteDirectory(wiped.home.resolve("data/transactions/neo4j"))
        wiped.start()

        cluster.awaitSameAsLeader(wiped)
        assertThat(wiped.count("Marker")).isEqualTo(2)
        assertThat(wiped.storeId("system"))
            .describedAs("the system database is kept, not copied")
            .isEqualTo(systemStore)
    }

    private fun ClusterHarness.Member.storeId(name: String): StoreId =
        database(name)
            .dependencyResolver
            .resolveDependency(StorageEngine::class.java)
            .retrieveStoreId()

    @Test
    fun `a member that missed no discarded entries catches up from the log`() {
        val cluster = cluster()
        val leader = cluster.awaitLeader()
        val behind = cluster.members.first { it != leader }
        cluster.write("CREATE (:Marker {at: 'before'})")
        val database = behind.ref()
        val snapshotBefore = behind.cluster.latestSnapshot(database)
        behind.stop()
        cluster.write("CREATE (:Marker {at: 'while stopped'})")

        behind.start()

        cluster.awaitSameAsLeader(behind)
        assertThat(behind.cluster.latestSnapshot(database)?.position?.index ?: -1)
            .describedAs("no snapshot installed")
            .isEqualTo(snapshotBefore?.position?.index ?: -1)
    }
}
