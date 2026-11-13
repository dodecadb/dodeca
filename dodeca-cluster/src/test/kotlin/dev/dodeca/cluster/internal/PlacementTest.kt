package dev.dodeca.cluster.internal

import dev.dodeca.cluster.internal.replication.ReplicationService
import dev.dodeca.core.servers.Placements
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.io.fs.FileSystemAbstraction

/**
 * Databases are placed on as many servers as their topology asks for, as in Neo4j Enterprise, and
 * only those servers host them.
 */
class PlacementTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    private fun cluster(
        defaultPrimaries: Int? = null,
        fileSystem: ((ClusterHarness.Member) -> FileSystemAbstraction)? = null,
    ): ClusterHarness =
        ClusterHarness(
                root,
                3,
                fileSystem,
                settings =
                    defaultPrimaries?.let { mapOf(ClusterSettings.defaultPrimaries to it) }
                        ?: emptyMap(),
            )
            .also {
                harness = it
                it.start()
            }

    private fun ClusterHarness.administer(query: String) {
        awaitLeader(SYSTEM).database(SYSTEM).executeTransactionally(query)
    }

    /** The members whose copy of [database] runs, and which vote in its group. */
    private fun ClusterHarness.hosts(database: String): Set<ClusterHarness.Member> =
        running()
            .filter { member ->
                member.cluster.status().any { it.database.name == database } &&
                    runCatching { member.database(database).isAvailable(0) }.getOrDefault(false)
            }
            .toSet()

    private fun ClusterHarness.voters(database: String): Set<String> =
        running()
            .flatMap { member ->
                runCatching { member.cluster.groupMembers(member.ref(database)) }
                    .getOrDefault(emptyList())
            }
            .map { it.id.toString() }
            .toSet()

    private fun ClusterHarness.Member.ref(database: String) =
        cluster.status().first { it.database.name == database }.database

    private fun ClusterHarness.Member.rows(query: String): List<Map<String, Any?>> =
        database(SYSTEM).executeTransactionally(query, emptyMap()) { result ->
            result.asSequence().map { it.toMap() }.toList()
        }

    /** The members that SET TOPOLOGY [primaries] keeps [database] on, as the allocator decides. */
    private fun ClusterHarness.keptBy(
        database: String,
        primaries: Int,
    ): Set<ClusterHarness.Member> {
        val kept =
            awaitLeader(SYSTEM).database(SYSTEM).beginTx().use { tx ->
                Placements.allocator(tx) { true }.resize(database, primaries)
            }
        return members.filter { it.cluster.self.id.uuid in kept }.toSet()
    }

    /** Makes [member] the leader of [database]. */
    private fun ClusterHarness.lead(database: String, member: ClusterHarness.Member) {
        eventually {
            val leader = awaitLeader(database) { hosts(database).toList() }
            if (leader != member) {
                val replica =
                    leader
                        .database(database)
                        .dependencyResolver
                        .resolveDependency(ReplicationService::class.java)
                        .replica(leader.database(database).databaseId())
                checkNotNull(replica)
                    .transferLeadership(member.cluster.self.id, "a test", Duration.ofSeconds(5))
            }
            assertThat(awaitLeader(database) { hosts(database).toList() }).isEqualTo(member)
        }
    }

    private fun eventually(assertion: () -> Unit) {
        await withPollInterval
            Duration.ofMillis(200) atMost
            ClusterHarness.AWAIT untilAsserted
            {
                assertion()
            }
    }

    @Test
    fun `a database is placed on as many servers as its topology asks for`() {
        val cluster = cluster()

        cluster.administer("CREATE DATABASE films TOPOLOGY 2 PRIMARIES WAIT")

        eventually { assertThat(cluster.hosts("films")).hasSize(2) }
        val leader = cluster.awaitLeader(SYSTEM)
        val rows =
            leader.rows("SHOW DATABASE films YIELD address, requestedPrimariesCount, currentStatus")
        assertThat(rows).hasSize(2)
        assertThat(rows.map { it["requestedPrimariesCount"] }).containsOnly(2L)
        assertThat(rows.map { it["currentStatus"] }).containsOnly("online")
        val requested =
            leader.rows("SHOW SERVERS YIELD requestedHosting").map {
                "films" in it["requestedHosting"] as List<*>
            }
        assertThat(requested.count { it }).isEqualTo(2)
        val outside = (cluster.members - cluster.hosts("films")).single()
        assertThat(outside.cluster.status().map { it.database.name }).doesNotContain("films")
    }

    @Test
    fun `a topology the servers cannot satisfy fails, and changes nothing`() {
        val cluster = cluster()

        val error = catchThrowable {
            cluster.administer("CREATE DATABASE films TOPOLOGY 4 PRIMARIES")
        }

        assertThat(error)
            .hasMessageContaining("Failed to create the specified database 'films'")
            .hasMessageContaining(
                "'films' needs 4 servers that can host it as a primary, and only 3 can"
            )
        assertThat(cluster.awaitLeader(SYSTEM).rows("SHOW DATABASES YIELD name").map { it["name"] })
            .doesNotContain("films")
    }

    @Test
    fun `more and fewer primaries add and remove copies, which keep every write`() {
        val cluster = cluster()
        cluster.administer("CREATE DATABASE films TOPOLOGY 1 PRIMARY WAIT")
        eventually { assertThat(cluster.hosts("films")).hasSize(1) }
        val first = cluster.awaitLeader("films") { cluster.hosts("films").toList() }
        first
            .database("films")
            .executeTransactionally("UNWIND range(1, 100) AS n CREATE (:Film {n: n})")

        cluster.administer("ALTER DATABASE films SET TOPOLOGY 3 PRIMARIES")

        eventually {
            assertThat(cluster.hosts("films")).hasSize(3)
            assertThat(cluster.voters("films")).hasSize(3)
            for (member in cluster.members) {
                assertThat(
                        member.database("films").executeTransactionally(
                            "MATCH (f:Film) RETURN count(f) AS c",
                            emptyMap(),
                        ) {
                            it.next()["c"]
                        }
                    )
                    .isEqualTo(100L)
            }
        }

        cluster.administer("ALTER DATABASE films SET TOPOLOGY 1 PRIMARY")

        eventually {
            assertThat(cluster.hosts("films")).hasSize(1)
            assertThat(cluster.voters("films")).hasSize(1)
        }
        val kept = cluster.hosts("films").single()
        for (member in cluster.members - kept) {
            eventually {
                assertThat(Files.exists(member.home.resolve("data/databases/films"))).isFalse
            }
        }
        assertThat(
                kept.database("films").executeTransactionally(
                    "MATCH (f:Film) RETURN count(f) AS c",
                    emptyMap(),
                ) {
                    it.next()["c"]
                }
            )
            .isEqualTo(100L)
    }

    @Test
    fun `a copy that leads its database hands the leadership over before it leaves`() {
        val cluster = cluster()
        cluster.administer("CREATE DATABASE films TOPOLOGY 2 PRIMARIES WAIT")
        eventually { assertThat(cluster.hosts("films")).hasSize(2) }
        val kept = cluster.keptBy("films", 1).single()
        val leaving = (cluster.hosts("films") - kept).single()
        cluster.lead("films", leaving)

        cluster.administer("ALTER DATABASE films SET TOPOLOGY 1 PRIMARY")

        eventually { assertThat(cluster.voters("films")).containsExactly(kept.serverId) }
        assertThat(cluster.awaitLeader("films") { listOf(kept) }).isEqualTo(kept)
        assertThat(root.resolve("raft.log"))
            .content()
            .contains("to ${kept.cluster.self.id}: the database moves off this member")
        assertThat(cluster.awaitLeader(SYSTEM).home.resolve("logs/debug.log"))
            .content()
            .doesNotContain("still leads database films")
    }

    /** SHOW DATABASE [database]'s status and writer on each server, by server id. */
    private fun ClusterHarness.statuses(database: String): Map<String, Pair<String, Boolean>> =
        awaitLeader(SYSTEM)
            .rows("SHOW DATABASE $database YIELD serverID, currentStatus, writer")
            .associate {
                it["serverID"] as String to
                    (it["currentStatus"] as String to it["writer"] as Boolean)
            }

    private fun ClusterHarness.Member.readOnly(vararg databases: String) {
        database(SYSTEM)
            .dependencyResolver
            .resolveDependency(Config::class.java)
            .setDynamic(GraphDatabaseSettings.read_only_databases, databases.toSet(), "a test")
    }

    @Test
    fun `SHOW DATABASES shows copies that are copied in and those that move off`() {
        val fileSystems = ConcurrentHashMap<Int, SlowLogFileSystem>()
        val cluster =
            cluster(fileSystem = { fileSystems.getOrPut(it.index) { SlowLogFileSystem() } })
        cluster.administer("CREATE DATABASE films TOPOLOGY 1 PRIMARY WAIT")
        eventually { assertThat(cluster.hosts("films")).hasSize(1) }
        val first = cluster.awaitLeader("films") { cluster.hosts("films").toList() }
        first.database("films").executeTransactionally("CREATE (:Film {title: 'Alien'})")
        val holds =
            (cluster.members - first).associateWith { member ->
                SlowLogFileSystem.Hold("films").also {
                    fileSystems.getValue(member.index).holding = it
                }
            }

        cluster.administer("ALTER DATABASE films SET TOPOLOGY 2 PRIMARIES")

        eventually { assertThat(holds.values.filter { it.held.count == 0L }).hasSize(1) }
        val copying = holds.entries.single { it.value.held.count == 0L }.key
        eventually {
            assertThat(cluster.statuses("films"))
                .containsEntry(copying.serverId, "store copying" to false)
                .containsEntry(first.serverId, "online" to true)
        }
        holds.values.forEach { it.release() }
        eventually {
            assertThat(cluster.statuses("films"))
                .containsOnlyKeys(first.serverId, copying.serverId)
                .allSatisfy { _, (status, _) -> assertThat(status).isEqualTo("online") }
        }

        // The copy that stays cannot take the leadership over while the database is read-only on
        // it, so the one that leaves keeps leading it.
        val kept = cluster.keptBy("films", 1).single()
        val leaving = setOf(first, copying).single { it != kept }
        cluster.lead("films", leaving)
        kept.readOnly("films")
        cluster.administer("ALTER DATABASE films SET TOPOLOGY 1 PRIMARY")

        eventually {
            assertThat(cluster.statuses("films"))
                .containsEntry(leaving.serverId, "deallocating" to true)
                .containsEntry(kept.serverId, "online" to false)
        }
        kept.readOnly()
        eventually {
            assertThat(cluster.statuses("films"))
                .containsOnlyKeys(kept.serverId)
                .containsEntry(kept.serverId, "online" to true)
        }
    }

    @Test
    fun `a server that was down while its copy moved off deletes it when it starts again`() {
        val cluster = cluster()
        cluster.administer("CREATE DATABASE films TOPOLOGY 3 PRIMARIES WAIT")
        eventually { assertThat(cluster.voters("films")).hasSize(3) }
        val stopped =
            cluster.members.first {
                it != cluster.awaitLeader(SYSTEM) && it != cluster.awaitLeader("films")
            }
        val stoppedId = stopped.serverId
        stopped.stop()

        // A server that does not answer is the first one fewer copies leave.
        cluster.administer("ALTER DATABASE films SET TOPOLOGY 2 PRIMARIES")
        eventually { assertThat(cluster.voters("films")).hasSize(2).doesNotContain(stoppedId) }
        stopped.start()

        eventually {
            assertThat(stopped.cluster.status().map { it.database.name }).doesNotContain("films")
            assertThat(Files.exists(stopped.home.resolve("data/databases/films"))).isFalse
        }
        // Its group's leader told it that it left, as it asked for votes.
        assertThat(stopped.home.resolve("logs/debug.log"))
            .content()
            .contains("it is placed on other servers, and this member left its group")
        assertThat(cluster.statuses("films")).containsOnlyKeys(cluster.voters("films"))
    }

    @Test
    fun `SET ACCESS READ ONLY refuses writes where the database leads, until READ WRITE`() {
        val cluster = cluster()
        cluster.administer("CREATE DATABASE films TOPOLOGY 2 PRIMARIES WAIT")
        eventually { assertThat(cluster.hosts("films")).hasSize(2) }
        // Led off the system leader, it learns of the change through the log of system.
        cluster.lead("films", cluster.hosts("films").first { it != cluster.awaitLeader(SYSTEM) })
        val write = {
            cluster
                .awaitLeader("films") { cluster.hosts("films").toList() }
                .database("films")
                .executeTransactionally("MERGE (:Probe)")
        }

        cluster.administer("ALTER DATABASE films SET ACCESS READ ONLY")

        eventually { assertThat(catchThrowable { write() }).hasMessageContaining("read-only") }
        cluster.administer("ALTER DATABASE films SET ACCESS READ WRITE")
        eventually { write() }
    }

    @Test
    fun `the default database of a new cluster is placed on the default number of servers`() {
        val cluster = cluster(defaultPrimaries = 2)

        eventually { assertThat(cluster.hosts(NEO4J)).hasSize(2) }
        cluster.administer("CREATE DATABASE films")

        eventually { assertThat(cluster.hosts("films")).hasSize(2) }
    }

    @Test
    fun `the default database takes as many servers as there are, at most its default number`() {
        val cluster = cluster(defaultPrimaries = 5)

        eventually { assertThat(cluster.hosts(NEO4J)).hasSize(3) }
        assertThat(catchThrowable { cluster.administer("CREATE DATABASE films") })
            .hasMessageContaining("'films' needs 5 servers")
    }

    private companion object {
        const val SYSTEM = "system"
        const val NEO4J = "neo4j"
    }
}
