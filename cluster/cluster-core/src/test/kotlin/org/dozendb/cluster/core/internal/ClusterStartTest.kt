package org.dozendb.cluster.core.internal

import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
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
