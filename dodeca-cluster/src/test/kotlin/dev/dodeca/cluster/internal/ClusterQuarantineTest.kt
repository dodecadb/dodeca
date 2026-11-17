package dev.dodeca.cluster.internal

import dev.dodeca.cluster.internal.ClusterHarness.Companion.AWAIT
import dev.dodeca.consensus.Role
import dev.dodeca.core.edition.Quarantines
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.CompletableFuture
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.dbms.api.DatabaseNotFoundException

/**
 * `dbms.quarantineDatabase` in a cluster: the copy on the member it runs on stops there only, and
 * stays stopped, while the others serve the database, until its quarantine is lifted, also with
 * `dbms.unquarantineDatabase` from another member.
 */
class ClusterQuarantineTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    private fun cluster(): ClusterHarness =
        ClusterHarness(root, 3, settings = mapOf(BoltConnector.enabled to true))
            .also { harness = it }
            .apply {
                start()
                administer("CREATE DATABASE films WAIT")
                write("films")
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

    /** Calls `dbms.quarantineDatabase` on this member, on a database it runs. */
    private fun ClusterHarness.Member.quarantine(
        database: String,
        set: Boolean,
        reason: String = "",
        on: String = "neo4j",
    ): Map<String, Any?> =
        database(on).executeTransactionally(
            // Enterprise's Cypher 25 has no dbms.quarantineDatabase.
            "CYPHER 5 CALL dbms.quarantineDatabase(\$database, \$set, \$reason)",
            mapOf("database" to database, "set" to set, "reason" to reason),
        ) {
            it.next().toMap()
        }

    /** Calls `dbms.unquarantineDatabase` on this member, for the copy of [database] on [server]. */
    private fun ClusterHarness.Member.unquarantine(
        server: String,
        database: String,
        operation: String = "keepStateKeepStore",
    ) {
        database("neo4j")
            .executeTransactionally(
                "CALL dbms.unquarantineDatabase(\$server, \$database, \$operation)",
                mapOf("server" to server, "database" to database, "operation" to operation),
            )
    }

    /**
     * Files left in this member's store of [database] and in its Raft group's directory, which tell
     * whether they were replaced.
     */
    private fun ClusterHarness.Member.mark(database: String): Pair<Path, Path> {
        val id = database(database).databaseId().databaseId().uuid()
        val data = home.resolve("data")
        return Files.writeString(data.resolve("databases/$database/marker"), "") to
            Files.writeString(data.resolve("raft/$id/marker"), "")
    }

    /** The rows of SHOW DATABASE [database] on this member, by server id. */
    private fun ClusterHarness.Member.shown(database: String): Map<String, Map<String, Any?>> =
        database("system").executeTransactionally(
            "SHOW DATABASE $database YIELD serverID, currentStatus, statusMessage, role, writer",
            emptyMap(),
        ) { result ->
            result.asSequence().associate { it["serverID"] as String to it.toMap() }
        }

    /** The ports of the Bolt addresses in this member's routing table of [database], by role. */
    private fun ClusterHarness.Member.routing(database: String): Map<String, List<String>> =
        database().executeTransactionally(
            "CALL dbms.routing.getRoutingTable({}, \$database) YIELD servers",
            mapOf("database" to database),
        ) { result ->
            @Suppress("UNCHECKED_CAST")
            (result.next()["servers"] as List<Map<String, Any>>).associate { server ->
                server["role"] as String to
                    (server["addresses"] as List<String>).map { it.substringAfterLast(':') }
            }
        }

    /** Whether this member's copy of [database] runs; its Raft group may run without it. */
    private fun ClusterHarness.Member.runs(database: String): Boolean =
        try {
            checkNotNull(dbms).database(database).isAvailable
        } catch (_: DatabaseNotFoundException) {
            // Not registered here, as while it is copied anew.
            false
        }

    @Test
    fun `a follower's copy in quarantine stops there only, and catches up once lifted`() {
        val cluster = cluster()
        val member = cluster.members.first { it != cluster.awaitLeader("films") }
        val others = { cluster.running().filter { it != member } }

        val result = member.quarantine("films", true, "Inspecting its files")

        assertThat(result).containsEntry("databaseName", "films").containsEntry("quarantined", true)
        val message = result["result"] as String
        assertThat(message).matches("By anonymous at .*Z: Inspecting its files")
        assertThat(member.runs("films")).isFalse()
        await atMost
            AWAIT untilAsserted
            {
                for (asked in cluster.running()) {
                    val rows = asked.shown("films")
                    assertThat(rows.getValue(member.serverId))
                        .describedAs("on $asked")
                        .containsEntry("currentStatus", "quarantined")
                        .containsEntry("statusMessage", message)
                        .containsEntry("role", "unknown")
                        .containsEntry("writer", false)
                    assertThat(others().map { rows.getValue(it.serverId)["currentStatus"] })
                        .containsOnly("online")
                }
            }
        // The others serve it; routing tables send drivers to them only.
        cluster.write("films", others)
        val refused = member.boltAddress().substringAfterLast(':')
        await atMost
            AWAIT untilAsserted
            {
                for (router in cluster.running()) {
                    val table = router.routing("films")
                    assertThat(table["WRITE"].orEmpty() + table["READ"].orEmpty())
                        .describedAs("$router's routing table of films: $table")
                        .doesNotContain(refused)
                }
            }

        // START DATABASE does not start it there; the others follow commands as usual.
        cluster.administer("STOP DATABASE films")
        cluster.administer("START DATABASE films WAIT")
        cluster.write("films", others)
        assertThat(member.runs("films")).isFalse()
        assertThat(member.shown("films").getValue(member.serverId))
            .containsEntry("currentStatus", "quarantined")

        val lifted = member.quarantine("films", false)

        assertThat(lifted).containsEntry("quarantined", false)
        await atMost
            AWAIT untilAsserted
            {
                assertThat(member.shown("films").getValue(member.serverId))
                    .containsEntry("currentStatus", "online")
                assertThat(member.count("films")).isEqualTo(3)
            }
    }

    @Test
    fun `dbms_unquarantineDatabase lifts the quarantine of a copy on another member`() {
        val cluster = cluster()
        val member = cluster.members.first { it != cluster.awaitLeader("films") }
        val caller = cluster.members.first { it != member }
        val others = { cluster.running().filter { it != member } }
        member.quarantine("films", true)
        cluster.write("films", others)

        caller.unquarantine(member.serverId, "films")

        await atMost
            AWAIT untilAsserted
            {
                assertThat(member.shown("films").getValue(member.serverId))
                    .containsEntry("currentStatus", "online")
                assertThat(member.count("films")).isEqualTo(2)
            }
        // By its name; the member's refusals reach the caller.
        cluster.administer("RENAME SERVER '${member.serverId}' TO 'east'")
        member.quarantine("films", true)
        val refused = catchThrowable { caller.unquarantine("east", "neo4j") }
        assertThat(generateSequence(refused) { it.cause }.mapNotNull { it.message }.toList())
            .anySatisfy {
                assertThat(it)
                    .contains("Setting/removing the quarantine marker failed: ")
                    .contains("Database 'neo4j' is not in quarantine on this server.")
            }
        caller.unquarantine("east", "films")
        await atMost
            AWAIT untilAsserted
            {
                assertThat(member.runs("films")).isTrue()
            }
    }

    @Test
    fun `replaceStateReplaceStore joins a primary as a new member of its group, from a new copy`() {
        val (store, raft) = joinAsNew("replaceStateReplaceStore")

        assertThat(store).describedAs("the store was replaced").doesNotExist()
        assertThat(raft).describedAs("the Raft state was replaced").doesNotExist()
    }

    @Test
    fun `replaceStateReplaceStore keeps the copy in quarantine until its group removed it`() {
        val cluster = cluster()
        val leader = cluster.awaitLeader("films")
        val member = cluster.members.first { it != leader }
        val other = cluster.members.first { it != leader && it != member }
        val films = leader.database("films").databaseId().databaseId().uuid()
        member.quarantine("films", true)
        // The leader alone is no majority of the group, which cannot remove the member meanwhile.
        other.stop()
        val lifting = CompletableFuture.runAsync {
            leader.unquarantine(member.serverId, "films", "replaceStateReplaceStore")
        }

        await atMost
            AWAIT untilAsserted
            {
                assertThat(member.logged("Dropped 'DatabaseId{")).isTrue()
            }
        // Deleted, it stays in quarantine, so that it does not run the group with a new state as
        // the voter it was, which forgot its votes.
        assertThat(member.quarantines().of(films)).isNotNull()
        assertThat(member.runs("films")).isFalse()

        other.start()
        lifting.join()
        await atMost
            AWAIT untilAsserted
            {
                assertThat(member.runs("films")).isTrue()
                assertThat(member.count("films")).isEqualTo(1)
                assertThat(cluster.running().any { it.logged("Adding member ${member.serverId}") })
                    .isTrue()
            }
    }

    /** The quarantines of copies on this member, as it keeps them on disk. */
    private fun ClusterHarness.Member.quarantines(): Quarantines =
        Quarantines(home.resolve("data").resolve(Quarantines.FILE_NAME))

    /** Whether this member's debug log says [text], also in a line it is still writing. */
    private fun ClusterHarness.Member.logged(text: String): Boolean =
        text in Files.readString(home.resolve("logs/debug.log"))

    @Test
    fun `replaceStateKeepStore joins a primary as a new member of its group, from its store`() {
        val (store, raft) = joinAsNew("replaceStateKeepStore")

        assertThat(store).describedAs("the store was kept").exists()
        assertThat(raft).describedAs("the Raft state was replaced").doesNotExist()
    }

    /**
     * Puts a follower's copy of films in quarantine, writes to the others, and lifts its quarantine
     * from another member with [operation]; returns the files left in its store and Raft state.
     */
    private fun joinAsNew(operation: String): Pair<Path, Path> {
        val cluster = cluster()
        val member = cluster.members.first { it != cluster.awaitLeader("films") }
        val caller = cluster.members.first { it != member }
        val others = { cluster.running().filter { it != member } }
        member.quarantine("films", true)
        cluster.write("films", others)
        val marks = member.mark("films")

        caller.unquarantine(member.serverId, "films", operation)

        await atMost
            AWAIT untilAsserted
            {
                assertThat(member.runs("films")).isTrue()
                assertThat(member.count("films")).isEqualTo(2)
                assertThat(member.status("films")?.role).isIn(Role.Follower, Role.Leader)
            }
        // The group removed it, and added it as a new member.
        assertThat(cluster.running().flatMap { it.debugEntries() }.map { it.message }).anySatisfy {
            assertThat(it)
                .startsWith("Adding member ${member.serverId}")
                .endsWith("to the group of database films")
        }
        cluster.write("films")
        await atMost AWAIT untilAsserted { assertThat(member.count("films")).isEqualTo(3) }
        return marks
    }

    @Test
    fun `the leader's copy in quarantine hands its leadership over`() {
        val cluster = cluster()
        val leader = cluster.awaitLeader("films")
        val others = { cluster.running().filter { it != leader } }

        leader.quarantine("films", true)

        assertThat(cluster.awaitLeader("films", others)).isNotEqualTo(leader)
        cluster.write("films", others)
        leader.quarantine("films", false)
        await atMost
            AWAIT untilAsserted
            {
                assertThat(leader.runs("films")).isTrue()
                assertThat(leader.count("films")).isEqualTo(2)
            }
    }

    @Test
    fun `a quarantine is kept across a restart, and system's is lifted by one`() {
        val cluster = cluster()
        val member = cluster.members.first { it != cluster.awaitLeader("films") }
        val message = member.quarantine("films", true)["result"]
        member.quarantine("system", true)
        assertThat(member.runs("system")).isFalse()
        // The others administer the cluster.
        cluster.administer("CREATE DATABASE books")

        member.stop()
        member.start()

        await atMost
            AWAIT untilAsserted
            {
                assertThat(member.shown("books").getValue(member.serverId))
                    .containsEntry("currentStatus", "online")
            }
        assertThat(member.shown("films").getValue(member.serverId))
            .containsEntry("currentStatus", "quarantined")
            .containsEntry("statusMessage", message)
        assertThat(member.runs("films")).isFalse()
    }

    @Test
    fun `lifting the quarantine of system brings the member's system graph up to date`() {
        val cluster = cluster()
        val member = cluster.members.first { it != cluster.awaitLeader("system") }
        member.quarantine("system", true)
        cluster.administer("CREATE DATABASE books")

        member.quarantine("system", false)

        await atMost
            AWAIT untilAsserted
            {
                assertThat(member.shown("books").getValue(member.serverId))
                    .containsEntry("currentStatus", "online")
            }
    }
}
