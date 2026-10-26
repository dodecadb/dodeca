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
import org.dozendb.cluster.MemberState
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

    /** Runs [query] on the `system` leader of [among]. */
    private fun ClusterHarness.administer(
        query: String,
        among: () -> List<ClusterHarness.Member> = ::running,
    ) {
        awaitLeader(SYSTEM, among).database(SYSTEM).executeTransactionally(query)
    }

    private fun ClusterHarness.rows(
        query: String,
        among: () -> List<ClusterHarness.Member> = ::running,
    ): List<Map<String, Any?>> =
        awaitLeader(SYSTEM, among).database(SYSTEM).executeTransactionally(query, emptyMap()) {
            result ->
            result.asSequence().map { it.toMap() }.toList()
        }

    /** The row of [server] in SHOW SERVERS YIELD *. */
    private fun ClusterHarness.server(
        server: ClusterHarness.Member,
        among: () -> List<ClusterHarness.Member> = ::running,
    ): Map<String, Any?> =
        rows("SHOW SERVERS YIELD *", among).single { it["serverId"] == server.serverId }

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
    fun `a cordoned server keeps its copies and takes no new ones, until it is enabled again`() {
        val cluster = cluster()
        cluster.administer("CREATE DATABASE films TOPOLOGY 1 PRIMARY WAIT")
        eventually { assertThat(cluster.hosts("films")).hasSize(1) }
        val host = cluster.hosts("films").single()
        val id = host.serverId

        cluster.administer("CALL dbms.cluster.cordonServer('$id')")

        assertThat(cluster.server(host)["state"]).isEqualTo("Cordoned")
        assertThat(
                catchThrowable { cluster.administer("CREATE DATABASE books TOPOLOGY 3 PRIMARIES") }
            )
            .hasMessageContaining(
                "'books' needs 3 servers that can host it as a primary, and only 2 can"
            )
        cluster.administer("ALTER SERVER '$id' SET OPTIONS {tags: ['a']}")
        assertThat(cluster.server(host)["state"]).describedAs("kept").isEqualTo("Cordoned")
        assertThat(cluster.hosts("films")).containsExactly(host)

        cluster.administer("CALL dbms.cluster.uncordonServer('$id')")

        assertThat(cluster.server(host)["state"]).isEqualTo("Enabled")
        cluster.administer("CREATE DATABASE books TOPOLOGY 3 PRIMARIES")
        cluster.administer("CALL dbms.cluster.cordonServer('$id')")
        cluster.administer("ENABLE SERVER '$id'")
        assertThat(cluster.server(host)["state"]).isEqualTo("Enabled")
    }

    @Test
    fun `cordoning a server that is not enabled fails`() {
        val cluster = cluster()

        assertThat(catchThrowable { cluster.administer("CALL dbms.cluster.cordonServer('none')") })
            .hasMessageStartingWith("Could not cordon server 'none'")
            .hasMessageContaining("there is no such server")
    }

    @Test
    fun `DEALLOCATE moves every copy off the servers, which then take no new ones`() {
        val cluster = cluster()
        cluster.administer("CREATE DATABASE films TOPOLOGY 2 PRIMARIES WAIT")
        eventually { assertThat(cluster.hosts("films")).hasSize(2) }
        cluster
            .awaitLeader("films") { cluster.hosts("films").toList() }
            .database("films")
            .executeTransactionally("CREATE (:Film {title: 'Alien'})")
        val leaving = cluster.hosts("films").first()

        cluster.administer("DEALLOCATE DATABASES FROM SERVER '${leaving.serverId}'")

        assertThat(cluster.server(leaving)["state"]).isEqualTo("Deallocating")
        eventually {
            assertThat(cluster.hosts("films")).hasSize(2).doesNotContain(leaving)
            assertThat(cluster.hosts("neo4j")).doesNotContain(leaving)
            for (host in cluster.hosts("films")) assertThat(host.films()).isEqualTo(1)
        }
        assertThat(
                catchThrowable { cluster.administer("CREATE DATABASE books TOPOLOGY 3 PRIMARIES") }
            )
            .hasMessageContaining("only 2 can")
    }

    @Test
    fun `DRYRUN DEALLOCATE shows the moves, and changes nothing`() {
        val cluster = cluster()
        cluster.administer("CREATE DATABASE films TOPOLOGY 2 PRIMARIES WAIT")
        eventually { assertThat(cluster.hosts("films")).hasSize(2) }
        val hosts = cluster.hosts("films")
        val leaving = hosts.first()
        val free = (cluster.members - hosts).single()

        val rows = cluster.rows("DRYRUN DEALLOCATE DATABASES FROM SERVER '${leaving.serverId}'")

        assertThat(rows.filter { it["database"] == "films" })
            .containsExactly(
                mapOf(
                    "database" to "films",
                    "fromServerName" to leaving.serverId,
                    "fromServerId" to leaving.serverId,
                    "toServerName" to free.serverId,
                    "toServerId" to free.serverId,
                    "mode" to "primary",
                )
            )
        assertThat(cluster.server(leaving)["state"]).isEqualTo("Enabled")
        assertThat(cluster.hosts("films")).isEqualTo(hosts)
    }

    @Test
    fun `DEALLOCATE fails, changing nothing, if a copy cannot move or is offline`() {
        val cluster = cluster()
        cluster.administer("CREATE DATABASE films TOPOLOGY 3 PRIMARIES WAIT")
        val server = cluster.members.first()

        assertThat(
                catchThrowable {
                    cluster.administer("DEALLOCATE DATABASES FROM SERVER '${server.serverId}'")
                }
            )
            .hasMessageContaining("no other server can host 'films'")
        assertThat(cluster.server(server)["state"]).isEqualTo("Enabled")

        cluster.administer("ALTER DATABASE films SET TOPOLOGY 1 PRIMARY")
        eventually { assertThat(cluster.hosts("films")).hasSize(1) }
        cluster.administer("STOP DATABASE films WAIT")
        eventually { assertThat(cluster.hosts("films")).isEmpty() }
        val placed = cluster.rows("SHOW DATABASE films YIELD serverID").map { it["serverID"] }
        val host = cluster.members.single { it.serverId in placed }

        assertThat(
                catchThrowable {
                    cluster.administer("DEALLOCATE DATABASES FROM SERVER '${host.serverId}'")
                }
            )
            .hasMessageContaining("'films' is offline")
        assertThat(cluster.server(host)["state"]).isEqualTo("Enabled")
    }

    @Test
    fun `the deallocate procedures move copies, and leave the servers enabled`() {
        val cluster = cluster()
        cluster.administer("CREATE DATABASE films TOPOLOGY 1 PRIMARY WAIT")
        eventually { assertThat(cluster.hosts("films")).hasSize(1) }
        val host = cluster.hosts("films").single()

        val dry =
            cluster.rows(
                "CALL dbms.cluster.deallocateDatabaseFromServer('${host.serverId}', 'films', true)"
            )
        assertThat(dry.map { it["database"] }).containsExactly("films")
        assertThat(cluster.hosts("films")).containsExactly(host)

        val rows =
            cluster.rows(
                "CALL dbms.cluster.deallocateDatabaseFromServer('${host.serverId}', 'films')"
            )

        assertThat(rows.map { it["fromServerId"] }).containsExactly(host.serverId)
        assertThat(cluster.server(host)["state"]).isEqualTo("Enabled")
        eventually { assertThat(cluster.hosts("films")).hasSize(1).doesNotContain(host) }
        val second = cluster.hosts("films").single()
        cluster.rows("CALL dbms.cluster.deallocateNumberOfDatabases('${second.serverId}', 1)")
        eventually { assertThat(cluster.hosts("films")).hasSize(1).doesNotContain(second) }
    }

    /** A member that hosts no database but `system`, once `neo4j` is placed. */
    private fun ClusterHarness.idle(): ClusterHarness.Member {
        var placed = emptyList<Any?>()
        eventually {
            placed = rows("SHOW DATABASE neo4j YIELD serverID").map { it["serverID"] }
            assertThat(placed).hasSize(1)
        }
        return members.first { it.serverId !in placed }
    }

    /**
     * Creates [names] with one copy each while [idle] is cordoned, so that the other servers host
     * them all, and enables [idle] again.
     */
    private fun ClusterHarness.unevenly(idle: ClusterHarness.Member, vararg names: String) {
        administer("CALL dbms.cluster.cordonServer('${idle.serverId}')")
        for (name in names) administer("CREATE DATABASE $name TOPOLOGY 1 PRIMARY WAIT")
        administer("ENABLE SERVER '${idle.serverId}'")
    }

    /** The databases [member] hosts, `system` aside. */
    private fun ClusterHarness.hosted(member: ClusterHarness.Member): List<*> =
        (server(member)["hosting"] as List<*>) - SYSTEM

    @Test
    fun `REALLOCATE moves copies to a server that hosts fewer, and DRYRUN shows the moves`() {
        val cluster = cluster()
        val idle = cluster.idle()
        // With neo4j, 4 copies on one server and 3 on the other: 2 move to the idle server.
        cluster.unevenly(idle, *SIX)

        val dry = cluster.rows("DRYRUN REALLOCATE DATABASES")

        assertThat(dry).hasSize(2).allSatisfy {
            assertThat(it["toServerId"]).isEqualTo(idle.serverId)
            assertThat(it["mode"]).isEqualTo("primary")
        }
        assertThat(cluster.hosted(idle)).isEmpty()

        cluster.administer("REALLOCATE DATABASES")

        eventually {
            assertThat(cluster.hosted(idle))
                .containsExactlyInAnyOrderElementsOf(dry.map { it["database"] })
            val counts = cluster.members.map { cluster.hosted(it).size }
            assertThat(counts.max() - counts.min()).isEqualTo(1)
        }
    }

    @Test
    fun `the reallocate procedures move a number of copies, or those of one database`() {
        val cluster = cluster()
        val idle = cluster.idle()
        cluster.unevenly(idle, *SIX)

        // One of the 2 moves that REALLOCATE makes, leaving 3, 3 and 1 copies.
        val one = cluster.rows("CALL dbms.cluster.reallocateNumberOfDatabases(1)")

        assertThat(one).hasSize(1)
        val moved = one.single()["database"]
        eventually { assertThat(cluster.hosted(idle)).containsExactly(moved) }
        val other = (SIX.toList() - moved).first()
        assertThat(cluster.rows("CALL dbms.cluster.reallocateDatabase('$other', true)"))
            .extracting<Any?> { it["database"] }
            .containsExactly(other)

        val rows = cluster.rows("CALL dbms.cluster.reallocateDatabase('$other')")

        assertThat(rows.map { it["database"] to it["toServerId"] })
            .containsExactly(other to idle.serverId)
        eventually { assertThat(cluster.hosted(idle)).containsExactlyInAnyOrder(moved, other) }
        assertThat(catchThrowable { cluster.rows("CALL dbms.cluster.reallocateDatabase('none')") })
            .hasMessageContaining("Could not reallocate databases: there is no database 'none'")
        assertThat(
                catchThrowable { cluster.rows("CALL dbms.cluster.reallocateNumberOfDatabases(0)") }
            )
            .hasMessageContaining("the number of databases is 0, not 1 or more")
    }

    @Test
    fun `DROP SERVER removes a Deallocated server, which shows Dropped while it answers`() {
        val cluster = cluster()
        val leaving = cluster.members.first { it != cluster.awaitLeader(SYSTEM) }
        val remaining = { cluster.members - leaving }
        cluster.administer("DEALLOCATE DATABASES FROM SERVER '${leaving.serverId}'")
        eventually { assertThat(cluster.server(leaving)["state"]).isEqualTo("Deallocated") }

        cluster.administer("DROP SERVER '${leaving.serverId}'")

        eventually {
            assertThat(leaving.cluster.state).isEqualTo(MemberState.REMOVED)
            assertThat(cluster.server(leaving, remaining)["state"]).isEqualTo("Dropped")
        }
        assertThat(
                catchThrowable {
                    cluster.administer("ENABLE SERVER '${leaving.serverId}'", remaining)
                }
            )
            .hasMessageContaining("it was dropped")
        leaving.stop()
        eventually {
            assertThat(cluster.rows("SHOW SERVERS YIELD serverId", remaining))
                .extracting<Any?> { it["serverId"] }
                .containsExactlyInAnyOrderElementsOf(remaining().map { it.serverId })
        }
    }

    @Test
    fun `DROP SERVER removes a Free server, and refuses one that is not Deallocated`() {
        val cluster = cluster()
        val enabled = cluster.members.first()
        cluster.administer("CALL dbms.cluster.setAutomaticallyEnableFreeServers(false)")

        assertThat(catchThrowable { cluster.administer("DROP SERVER '${enabled.serverId}'") })
            .hasMessageStartingWith("Could not drop server '${enabled.serverId}'")
            .hasMessageContaining("it is Enabled")

        val free = cluster.newMember().apply { start() }
        val remaining = { cluster.members - free }
        cluster.administer(
            "CALL dozendb.cluster.addMember('127.0.0.1:${free.raftPort}')",
            remaining,
        )
        eventually { assertThat(cluster.server(free)["state"]).isEqualTo("Free") }

        cluster.administer("DROP SERVER '${free.serverId}'")

        eventually {
            assertThat(free.cluster.state).isEqualTo(MemberState.REMOVED)
            assertThat(cluster.rows("SHOW SERVERS YIELD serverId, state", remaining))
                .filteredOn { it["serverId"] != free.serverId }
                .extracting<Any?> { it["state"] }
                .containsOnly("Enabled")
        }
    }

    @Test
    fun `DROP SERVER refuses a deallocating server that may hold the only copy of a database`() {
        val cluster = cluster()
        val holder = cluster.members.first { it != cluster.awaitLeader(SYSTEM) }
        val others = cluster.members - holder
        for (other in others) cluster.administer(
            "CALL dbms.cluster.cordonServer('${other.serverId}')"
        )
        cluster.administer("CREATE DATABASE films TOPOLOGY 1 PRIMARY WAIT")
        for (other in others) cluster.administer("ENABLE SERVER '${other.serverId}'")
        // The system leader has heard from it that it leads the group of films.
        eventually {
            assertThat(cluster.rows("SHOW DATABASE films YIELD serverID, writer"))
                .containsExactly(mapOf("serverID" to holder.serverId, "writer" to true))
        }
        val id = holder.serverId
        holder.stop()

        cluster.administer("DEALLOCATE DATABASES FROM SERVER '$id'")

        assertThat(
                cluster.rows("SHOW SERVERS YIELD serverId, state").single { it["serverId"] == id }
            )
            .containsEntry("state", "Deallocating")
        assertThat(catchThrowable { cluster.administer("DROP SERVER '$id'") })
            .hasMessageStartingWith("Could not drop server '$id'")
            .hasMessageContaining("'films'")
            .hasMessageContaining("Deallocated")
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
        val SIX = arrayOf("one", "two", "three", "four", "five", "six")
    }
}
