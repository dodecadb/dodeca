package dev.dodeca.cluster.internal

import java.nio.file.Path
import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.io.fs.FileUtils
import org.neo4j.storageengine.api.StoreIdProvider

class ClusterStartTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    private fun ClusterHarness.Member.storeId(name: String) =
        database(name).dependencyResolver.resolveDependency(StoreIdProvider::class.java).storeId

    @Test
    fun `members without databases start from copies of one member's databases`() {
        println("Cluster harness in $root")
        val cluster = ClusterHarness(root, 3).also { harness = it }

        cluster.start()

        for (name in listOf("system", ClusterHarness.DEFAULT_DATABASE)) {
            assertThat(cluster.members.map { it.database(name).databaseId() }.distinct()).hasSize(1)
            assertThat(cluster.members.map { it.storeId(name) }.distinct())
                .describedAs(name)
                .hasSize(1)
        }
        val logs = cluster.members.map { it.home.resolve("logs/debug.log").toFile().readText() }
        assertThat(logs.count { "This member creates the databases of the new cluster" in it })
            .isEqualTo(1)
        assertThat(logs.count { "Copied database neo4j from" in it }).isEqualTo(2)
    }

    @Test
    fun `a cluster formed from another cluster's database takes writes at once`() {
        val first = ClusterHarness(root.resolve("first"), 3).also { harness = it }
        first.start()
        val written = first.write(200)
        val source = first.members.first()
        first.close()

        // As with a dump of the database loaded into a new server: its transactions carry the
        // Raft indexes of the first cluster's group.
        val second = ClusterHarness(root.resolve("second"), 3).also { harness = it }
        for (directory in listOf("data/databases/neo4j", "data/transactions/neo4j")) {
            FileUtils.copyDirectory(
                source.home.resolve(directory),
                second.members.first().home.resolve(directory),
            )
        }
        second.start()

        assertThat(second.write(300)).describedAs("writes acknowledged").isEqualTo(300)
        await withPollInterval
            Duration.ofMillis(200) atMost
            ClusterHarness.AWAIT untilAsserted
            {
                assertThat(second.members.map { it.count() }).containsOnly(written + 300L)
            }
        assertThat(second.members.first().home.resolve("logs/debug.log").toFile().readText())
            .contains("Database neo4j holds transactions replicated in another Raft group")
        second.members.forEach { it.stop() }
        second.members.forEach {
            assertThat(it.checkConsistency().isSuccessful).describedAs("$it consistent").isTrue
        }
    }

    /** Writes [count] nodes, one per transaction; returns how many were acknowledged. */
    private fun ClusterHarness.write(count: Int): Int =
        (1..count).count { n ->
            runCatching {
                awaitLeader()
                    .database()
                    .executeTransactionally("CREATE (:Written {n: ${'$'}n})", mapOf("n" to n))
            }
                .isSuccess
        }

    private fun ClusterHarness.Member.count(): Long =
        database().executeTransactionally("MATCH (n:Written) RETURN count(n) AS c", emptyMap()) {
            it.next()["c"] as Long
        }

    @Test
    fun `a restarted member keeps its databases`() {
        val cluster = ClusterHarness(root, 3).also { harness = it }
        cluster.start()
        val member = cluster.members.last()
        val storeId = member.storeId(ClusterHarness.DEFAULT_DATABASE)

        member.stop()
        member.start()

        assertThat(member.storeId(ClusterHarness.DEFAULT_DATABASE)).isEqualTo(storeId)
        assertThat(member.home.resolve("logs/debug.log").toFile().readText())
            .contains("Rejoining the cluster from local Raft state")
    }
}
