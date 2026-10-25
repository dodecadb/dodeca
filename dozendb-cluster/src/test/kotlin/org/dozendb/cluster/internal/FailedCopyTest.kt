package org.dozendb.cluster.internal

import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.dozendb.cluster.internal.ClusterHarness.Companion.AWAIT
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir

/** A member whose copy of a database failed to apply a transaction. */
class FailedCopyTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    private fun ClusterHarness.administer(cypher: String) {
        awaitLeader("system").database("system").executeTransactionally(cypher)
    }

    private fun ClusterHarness.write(
        database: String,
        among: () -> List<ClusterHarness.Member> = ::running,
    ) {
        awaitLeader(database, among).database(database).executeTransactionally("CREATE (:Node)")
    }

    private fun ClusterHarness.Member.count(database: String): Long =
        database(database).executeTransactionally(
            "MATCH (n:Node) RETURN count(n) AS count",
            emptyMap(),
        ) {
            it.next()["count"] as Long
        }

    /** What SHOW DATABASE shows of this member's copy of [database]. */
    private fun ClusterHarness.Member.shown(database: String): Map<String, Any?> =
        database("system").executeTransactionally(
            "SHOW DATABASE $database YIELD serverID, currentStatus, statusMessage " +
                "WHERE serverID = \$self RETURN currentStatus, statusMessage",
            mapOf("self" to cluster.self.id.uuid.toString()),
        ) {
            it.next().toMap()
        }

    @Test
    fun `a copy that failed catches up once its database is started again`() {
        val fileSystems = ConcurrentHashMap<Int, SlowLogFileSystem>()
        val cluster =
            ClusterHarness(root, 3, { fileSystems.getOrPut(it.index) { SlowLogFileSystem() } })
                .also { harness = it }
        cluster.start()
        cluster.administer("CREATE DATABASE films WAIT")
        cluster.write("films")
        val member = cluster.members.first { it != cluster.awaitLeader("films") }
        await atMost AWAIT untilAsserted { assertThat(member.count("films")).isEqualTo(1) }

        // Its disk fails, and with it the next transaction it applies; the others commit it.
        fileSystems.getValue(member.index).failing = "films"
        cluster.write("films")
        await atMost
            AWAIT untilAsserted
            {
                assertThat(member.shown("films")).containsEntry("currentStatus", "offline")
            }

        // Once repaired, the operator starts the database again.
        fileSystems.getValue(member.index).failing = null
        cluster.administer("STOP DATABASE films")
        await atMost
            AWAIT untilAsserted
            {
                assertThat(member.shown("films")).containsEntry("currentStatus", "offline")
                assertThat(cluster.running().map { it.shown("films")["currentStatus"] })
                    .containsOnly("offline")
            }
        cluster.administer("START DATABASE films")
        cluster.write("films") { cluster.running().filter { it != member } }

        await atMost
            AWAIT untilAsserted
            {
                assertThat(member.shown("films")).containsEntry("currentStatus", "online")
                assertThat(member.count("films")).isEqualTo(3)
            }
    }
}
