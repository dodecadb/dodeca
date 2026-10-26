package org.dozendb.cluster.internal

import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.dozendb.core.DozenDbSettings
import org.dozendb.core.servers.Placements
import org.dozendb.core.servers.ServerRecords
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.dbms.systemgraph.InstanceModeConstraint
import org.neo4j.io.fs.FileSystemAbstraction

/**
 * How servers change what they host, as in Neo4j Enterprise: by their options, cordoning,
 * deallocating, reallocating and dropping.
 */
class ServerLifecycleTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    /**
     * A cluster of three, started after [configure] set up its members, whose databases have one
     * copy by default, as in Neo4j Enterprise.
     */
    private fun cluster(
        fileSystem: ((ClusterHarness.Member) -> FileSystemAbstraction)? = null,
        configure: (ClusterHarness) -> Unit = {},
    ): ClusterHarness =
        ClusterHarness(
                root,
                3,
                fileSystem,
                settings = mapOf(ClusterSettings.defaultPrimaries to 1),
            )
            .also {
                harness = it
                configure(it)
                it.start()
            }

    private fun ClusterHarness.administer(query: String) {
        awaitLeader(SYSTEM).database(SYSTEM).executeTransactionally(query)
    }

    private fun ClusterHarness.rows(query: String): List<Map<String, Any?>> =
        awaitLeader(SYSTEM).database(SYSTEM).executeTransactionally(query, emptyMap()) { result ->
            result.asSequence().map { it.toMap() }.toList()
        }

    /** The row of [server] in SHOW SERVERS YIELD *. */
    private fun ClusterHarness.server(server: ClusterHarness.Member): Map<String, Any?> =
        rows("SHOW SERVERS YIELD *").single { it["serverId"] == server.serverId }

    /** The members whose copy of [database] runs, and which vote in its group. */
    private fun ClusterHarness.hosts(database: String): Set<ClusterHarness.Member> =
        running()
            .filter { member ->
                member.cluster.status().any { it.database.name == database } &&
                    runCatching { member.database(database).isAvailable(0) }.getOrDefault(false)
            }
            .toSet()

    private fun ClusterHarness.Member.films(): Long =
        database("films").executeTransactionally(
            "MATCH (f:Film) RETURN count(f) AS c",
            emptyMap(),
        ) {
            it.next()["c"] as Long
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
    fun `options that no longer let a server host a database move its copy to another server`() {
        val cluster = cluster()
        cluster.administer("CREATE DATABASE films TOPOLOGY 2 PRIMARIES WAIT")
        eventually { assertThat(cluster.hosts("films")).hasSize(2) }
        cluster
            .awaitLeader("films") { cluster.hosts("films").toList() }
            .database("films")
            .executeTransactionally("CREATE (:Film {title: 'Alien'})")

        // Each time, the server that hosts no copy is the only one the copy can move to.
        for (options in
            listOf(
                "{deniedDatabases: ['FILMS']}",
                "{modeConstraint: 'SECONDARY'}",
                "{allowedDatabases: ['neo4j']}",
            )) {
            val hosts = cluster.hosts("films")
            val leaving = hosts.first()
            val free = (cluster.members - hosts).single()

            cluster.administer("ALTER SERVER '${leaving.serverId}' SET OPTIONS $options")

            eventually {
                assertThat(cluster.hosts("films"))
                    .describedAs(options)
                    .isEqualTo(hosts - leaving + free)
                assertThat(free.films()).isEqualTo(1)
            }
            cluster.administer("ALTER SERVER '${leaving.serverId}' SET OPTIONS {}")
        }
    }

    /** Denies [database] on [server], as ALTER SERVER does. */
    private fun ClusterHarness.deny(server: ClusterHarness.Member, database: String) {
        administer("ALTER SERVER '${server.serverId}' SET OPTIONS {deniedDatabases: ['$database']}")
    }

    @Test
    fun `a database that moves back to the server that created it is copied there`() {
        val cluster = cluster()
        cluster.administer("CREATE DATABASE films TOPOLOGY 1 PRIMARY WAIT")
        eventually { assertThat(cluster.hosts("films")).hasSize(1) }
        val creator = cluster.awaitLeader("films") { cluster.hosts("films").toList() }
        creator.database("films").executeTransactionally("CREATE (:Film {title: 'Alien'})")

        cluster.deny(creator, "films")
        eventually { assertThat(cluster.hosts("films")).hasSize(1).doesNotContain(creator) }
        val second = cluster.hosts("films").single()
        val third = (cluster.members - creator - second).single()
        cluster.administer("ALTER SERVER '${creator.serverId}' SET OPTIONS {}")
        cluster.deny(third, "films")
        cluster.deny(second, "films")

        eventually {
            assertThat(cluster.hosts("films")).containsExactly(creator)
            assertThat(creator.films()).isEqualTo(1)
        }
    }

    @Test
    fun `a database moves off the server that creates it only once it is created`() {
        val fileSystems = ConcurrentHashMap<Int, SlowLogFileSystem>()
        val cluster = cluster({ fileSystems.getOrPut(it.index) { SlowLogFileSystem() } })
        // The server that CREATE DATABASE places it on, which creates it, once servers are
        // recorded.
        eventually {
            cluster.awaitLeader(SYSTEM).database(SYSTEM).beginTx().use { tx ->
                assertThat(ServerRecords.initialServersEnabled(tx)).isTrue()
            }
        }
        val placed =
            cluster.awaitLeader(SYSTEM).database(SYSTEM).beginTx().use { tx ->
                Placements.allocator(tx) { true }.place("films", 1)
            }
        val creator = cluster.members.single { it.cluster.self.id.uuid in placed }
        val hold = SlowLogFileSystem.Hold("films")
        fileSystems.getValue(creator.index).holding = hold
        cluster.administer("CREATE DATABASE films TOPOLOGY 1 PRIMARY")
        hold.held.await()

        assertThat(catchThrowable { cluster.deny(creator, "films") })
            .hasMessageStartingWith("Could not alter server '${creator.serverId}'")
            .hasMessageContaining("cannot move off server ${creator.cluster.self.id} yet")

        hold.release()
        eventually { assertThat(cluster.hosts("films")).containsExactly(creator) }
        cluster.deny(creator, "films")
        eventually { assertThat(cluster.hosts("films")).hasSize(1).doesNotContain(creator) }
    }

    @Test
    fun `options that leave a copy nowhere to go fail, and change nothing`() {
        val cluster = cluster()
        cluster.administer("CREATE DATABASE films TOPOLOGY 3 PRIMARIES WAIT")
        val server = cluster.members.first()

        val error = catchThrowable {
            cluster.administer(
                "ALTER SERVER '${server.serverId}' SET OPTIONS {deniedDatabases: ['films']}"
            )
        }

        assertThat(error)
            .hasMessageStartingWith("Could not alter server '${server.serverId}'")
            .hasMessageContaining("no other server can host 'films'")
        assertThat(cluster.server(server)["deniedDatabases"]).isEqualTo(emptyList<String>())
        eventually { assertThat(cluster.hosts("films")).hasSize(3) }
    }

    @Test
    fun `the initial servers take the mode constraint and databases they start with`() {
        val cluster = cluster {
            it.members[0].settings =
                mapOf(DozenDbSettings.initialServerDeniedDatabases to listOf("films"))
            it.members[1].settings =
                mapOf(DozenDbSettings.initialServerModeConstraint to InstanceModeConstraint.PRIMARY)
        }
        val (denying, primary, other) = cluster.members

        eventually {
            assertThat(cluster.server(denying)["deniedDatabases"]).isEqualTo(listOf("films"))
            assertThat(cluster.server(primary)["modeConstraint"]).isEqualTo("PRIMARY")
            assertThat(cluster.server(other)["modeConstraint"]).isEqualTo("NONE")
        }
        cluster.administer("CREATE DATABASE films TOPOLOGY 2 PRIMARIES WAIT")

        eventually { assertThat(cluster.hosts("films")).containsExactlyInAnyOrder(primary, other) }
    }

    private companion object {
        const val SYSTEM = "system"
    }
}
