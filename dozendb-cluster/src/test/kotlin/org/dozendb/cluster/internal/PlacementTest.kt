package org.dozendb.cluster.internal

import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
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

    private fun cluster(defaultPrimaries: Int? = null): ClusterHarness =
        ClusterHarness(
                root,
                3,
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
