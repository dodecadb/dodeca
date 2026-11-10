package org.dozendb.cluster.internal

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir

/**
 * A member whose `data/server_id` was removed or replaced, as Neo4j Community's `neo4j-admin server
 * unbind` does, while its Raft state stayed: Neo4j gives it a new id, under which its Raft state
 * and databases are not its own.
 */
class LostServerIdTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    private fun ClusterHarness.Member.rows(cypher: String, database: String = SYSTEM) =
        database(database).executeTransactionally(cypher, emptyMap()) { result ->
            result.asSequence().map { it.toMap() }.toList()
        }

    @Test
    fun `refuses to start, keeps the databases, and starts as before with its id back`() {
        val cluster = ClusterHarness(root, 3).also { harness = it }
        cluster.start()
        cluster
            .awaitLeader(SYSTEM)
            .database(SYSTEM)
            .executeTransactionally("CREATE DATABASE $ONLY TOPOLOGY 1 PRIMARY WAIT")
        val host =
            cluster.members[0]
                .rows("SHOW DATABASE $ONLY YIELD serverID")
                .single()["serverID"]
                .let { id -> cluster.members.single { it.serverId == id } }
        host.database(ONLY).executeTransactionally("CREATE (:Precious)")
        val serverId = host.home.resolve("data/server_id")
        val kept = root.resolve("server_id.kept")
        Files.copy(serverId, kept)

        host.stop()
        Files.delete(serverId)
        val refused = catchThrowable { host.start() }

        assertThat(refused).describedAs("start with a new server id").isNotNull
        assertThat(generateSequence(refused) { it.cause }.mapNotNull { it.message }.toList())
            .describedAs("$refused")
            .anySatisfy {
                assertThat(it).contains("data/server_id").contains("neo4j-admin server unbind")
            }
        assertThat(host.home.resolve("data/databases/$ONLY")).isDirectory()

        Files.copy(kept, serverId, StandardCopyOption.REPLACE_EXISTING)
        host.start()
        await atMost
            ClusterHarness.AWAIT untilAsserted
            {
                assertThat(host.rows("MATCH (p:Precious) RETURN count(p) AS c", ONLY))
                    .containsExactly(mapOf("c" to 1L))
            }
    }

    private companion object {
        const val SYSTEM = "system"
        const val ONLY = "only"
    }
}
