package org.dozendb.cluster.internal

import java.nio.file.Path
import java.time.Duration
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.dozendb.cluster.MemberMetadata
import org.dozendb.cluster.MemberState
import org.dozendb.core.servers.CopyMode
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir

/**
 * A cluster whose `system` runs in a Raft group with an id of its own, as once a recovery re-formed
 * it: everything works as with the database's id, which Neo4j keeps for `system` throughout.
 */
class SystemGroupTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null
    private val group = UUID.randomUUID()

    @AfterEach
    fun stop() {
        harness?.close()
    }

    /** Runs [query] on the leader of `system` among its primaries, the first three members. */
    private fun ClusterHarness.administer(query: String) {
        awaitLeader(SYSTEM) { members.take(3) }.database(SYSTEM).executeTransactionally(query)
    }

    private fun ClusterHarness.Member.rows(query: String, database: String = SYSTEM) =
        database(database).executeTransactionally(query, emptyMap()) { result ->
            result.asSequence().map { it.toMap() }.toList()
        }

    private fun eventually(assertion: () -> Unit) {
        await withPollInterval
            Duration.ofMillis(100) atMost
            ClusterHarness.AWAIT untilAsserted
            {
                assertion()
            }
    }

    @Test
    fun `system in a group of its own forms, administers, replicates and takes a secondary`() {
        val cluster = ClusterHarness(root, 3, systemGroup = group).also { harness = it }
        cluster.start()

        for (member in cluster.members) {
            assertThat(member.cluster.status().single { it.database.name == SYSTEM }.database.id)
                .isEqualTo(group)
            assertThat(
                    member.rows("CALL dozendb.cluster.status()").single { it["database"] == SYSTEM }
                )
                .containsEntry("groupId", group.toString())
        }
        eventually {
            for (member in cluster.members) {
                for (other in cluster.members) {
                    assertThat(member.cluster.directory?.reported(other.cluster.self.id))
                        .containsEntry(MemberMetadata.SYSTEM_GROUP, group.toString())
                }
            }
        }
        cluster.administer("CREATE DATABASE films WAIT")
        cluster.awaitLeader("films").database("films").executeTransactionally("CREATE (:Film)")
        val shown =
            cluster.members[0].rows(
                "SHOW DATABASE system YIELD serverID, currentStatus, writer, role"
            )
        assertThat(shown.map { it["currentStatus"] }).containsOnly("online")
        assertThat(shown.count { it["writer"] == true }).isEqualTo(1)
        assertThat(
                cluster.members[0].rows("CALL dbms.cluster.statusCheck(['system'])").map {
                    it["memberStatus"]
                }
            )
            .containsOnly("APPLYING")

        // A member that restarts rejoins the group.
        val follower = cluster.members.first { it != cluster.awaitLeader(SYSTEM) }
        follower.stop()
        cluster.administer("CREATE DATABASE books")
        follower.start()
        eventually {
            assertThat(follower.rows("SHOW DATABASE books YIELD name")).isNotEmpty()
        }

        // A system secondary pulls from the group's members, and is copied from them.
        val secondary =
            cluster.newMember().also {
                it.settings = mapOf(ClusterSettings.systemDatabaseMode to CopyMode.SECONDARY)
                it.start()
            }
        assertThat(secondary.cluster.state).isEqualTo(MemberState.JOINING)
        cluster.administer("CREATE DATABASE series")
        eventually {
            assertThat(secondary.rows("SHOW DATABASES YIELD name").map { it["name"] })
                .contains("series")
        }
    }

    private companion object {
        const val SYSTEM = "system"
    }
}
