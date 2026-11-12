package org.dozendb.cluster.internal

import java.nio.file.Path
import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.dozendb.cluster.MemberState
import org.dozendb.consensus.MemberId
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.dbms.api.DatabaseNotFoundException
import org.neo4j.graphdb.config.Setting
import org.neo4j.wal.checkpoint.CheckPointer
import org.neo4j.wal.checkpoint.SimpleTriggerInfo

/** Members added to and removed from a running cluster. */
class MembershipTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    private fun cluster(settings: Map<Setting<*>, Any> = emptyMap()): ClusterHarness =
        ClusterHarness(root, 3, settings = settings).also {
            harness = it
            it.start()
        }

    private fun eventually(assertion: () -> Unit) {
        await withPollInterval
            Duration.ofMillis(200) atMost
            ClusterHarness.AWAIT untilAsserted
            {
                assertion()
            }
    }

    /** The members that vote in `system`, as [observer] knows them. */
    private fun ClusterHarness.voters(
        observer: ClusterHarness.Member
    ): List<ClusterHarness.Member> {
        val ids = observer.cluster.members().map { it.member.id }
        return members.filter { it.running && it.cluster.self.id in ids }
    }

    /** Runs [query] on the system leader and returns its rows. */
    private fun ClusterHarness.administer(
        query: String,
        among: () -> List<ClusterHarness.Member> = { members.take(3) },
    ): List<Map<String, Any?>> =
        awaitLeader(SYSTEM, among).database(SYSTEM).executeTransactionally(query, emptyMap()) {
            it.stream().toList()
        }

    /**
     * Places [databases] on [member] too, with as many primaries as [among] and it, once its server
     * is enabled.
     */
    private fun ClusterHarness.placeOn(
        member: ClusterHarness.Member,
        vararg databases: String,
        among: () -> List<ClusterHarness.Member> = { members.take(3) },
    ) {
        eventually {
            val states =
                administer("SHOW SERVERS YIELD serverId, state", among).associate {
                    it["serverId"] to it["state"]
                }
            assertThat(states[member.serverId]).isEqualTo("Enabled")
        }
        val primaries = among().size + 1
        for (database in databases) {
            administer("ALTER DATABASE $database SET TOPOLOGY $primaries PRIMARIES", among)
        }
    }

    /** Waits until SHOW SERVERS shows the server [id] Deallocated, then drops it. */
    private fun ClusterHarness.dropDeallocated(
        id: String,
        among: () -> List<ClusterHarness.Member> = { members.take(3) },
    ) {
        eventually {
            val states =
                administer("SHOW SERVERS YIELD serverId, state", among).associate {
                    it["serverId"] to it["state"]
                }
            assertThat(states[id]).isEqualTo("Deallocated")
        }
        administer("DROP SERVER '$id'", among)
    }

    /**
     * Drops [server] as an operator does where every server hosts [databases]: cordoned, so that
     * its copies go when they shrink to the other servers, then deallocated and dropped.
     */
    private fun ClusterHarness.drop(server: ClusterHarness.Member, vararg databases: String) {
        administer("CALL dbms.cluster.cordonServer('${server.serverId}')")
        for (database in databases) {
            administer("ALTER DATABASE $database SET TOPOLOGY ${members.size - 1} PRIMARIES")
        }
        administer("DEALLOCATE DATABASES FROM SERVER '${server.serverId}'")
        dropDeallocated(server.serverId)
    }

    /**
     * Writes to [database] and checkpoints every running member until the leader discarded the
     * start of the database's log, so that a member that joins later cannot catch up from it.
     */
    private fun ClusterHarness.discardLogStart(database: String) {
        var written = 0
        val among = { members.take(3) }
        await withPollInterval
            Duration.ofMillis(300) atMost
            ClusterHarness.AWAIT untilAsserted
            {
                val leader = awaitLeader(database, among)
                leader
                    .database(database)
                    .executeTransactionally(
                        "CREATE (:Blob {i: \$i, data: \$data})",
                        mapOf("i" to written++, "data" to "x".repeat(200_000)),
                    )
                running().forEach { it.checkpoint(database) }
                val id = leader.database(database).databaseId().databaseId().uuid()
                assertThat(leader.firstLogIndex(id)).isGreaterThan(1)
            }
    }

    private fun ClusterHarness.Member.checkpoint(database: String) {
        database(database)
            .dependencyResolver
            .resolveDependency(CheckPointer::class.java)
            .forceCheckPoint(SimpleTriggerInfo("test"))
    }

    /** The nodes in [database]; fails as an assertion while the database does not run here. */
    private fun ClusterHarness.Member.count(database: String): Long =
        try {
            database(database).executeTransactionally(
                "MATCH (n) RETURN count(n) AS c",
                emptyMap(),
            ) {
                it.next()["c"] as Long
            }
        } catch (e: DatabaseNotFoundException) {
            throw AssertionError("$database does not run on $this", e)
        }

    /** The databases [member] votes in, as [observer] knows them. */
    private fun ClusterHarness.Member.databasesOf(member: ClusterHarness.Member): List<String> =
        cluster
            .members()
            .firstOrNull { it.member.id == member.cluster.self.id }
            ?.databases
            .orEmpty()

    @Test
    fun `an added member joins the groups of the databases placed on it, copying each once`() {
        val cluster =
            ClusterHarness(root, 3, settings = ClusterHarness.METRICS) {
                    it.copy(
                        maxEntrySize = 1 shl 20,
                        logSegmentSize = 1,
                        snapshotInterval = Duration.ofMillis(200),
                    )
                }
                .also {
                    harness = it
                    it.start()
                }
        cluster.administer("CREATE DATABASE films")
        eventually {
            for (member in cluster.members) assertThat(member.count("films")).isEqualTo(0)
        }
        cluster.discardLogStart("films")
        cluster.discardLogStart(DEFAULT)
        val joining = cluster.newMember().apply { start() }

        cluster.administer("CALL dozendb.cluster.addMember('127.0.0.1:${joining.raftPort}')")
        cluster.placeOn(joining, "films", DEFAULT)
        cluster.administer(
            "CREATE DATABASE series TOPOLOGY 4 PRIMARIES",
            among = { cluster.members },
        )

        eventually {
            for (member in cluster.members) {
                assertThat(member.databasesOf(joining))
                    .describedAs("databases of the new member, seen by $member")
                    .containsExactly(SYSTEM, "films", DEFAULT, "series")
            }
        }
        val leader = cluster.awaitLeader("films")
        leader.database("films").executeTransactionally("CREATE (:Film {title: 'Alien'})")
        eventually {
            for (database in listOf("films", DEFAULT)) {
                assertThat(joining.count(database))
                    .describedAs(database)
                    .isEqualTo(cluster.awaitLeader(database).count(database))
            }
        }
        assertThat(joining.home.resolve("logs/debug.log"))
            .describedAs("no database copied twice")
            .content()
            .doesNotContain("needs Raft entries")
        eventually {
            val copies = joining.samples()
            for (database in listOf(SYSTEM, "films", DEFAULT)) {
                assertThat(
                        copies[
                            "neo4j_database_${database}_dozendb_cluster_store_copy_received_total"]
                    )
                    .describedAs("copies of $database")
                    .isEqualTo(1.0)
            }
            for (member in cluster.members) {
                assertThat(member.samples()["neo4j_dbms_dozendb_cluster_members"])
                    .describedAs("members, as $member knows them")
                    .isEqualTo(4.0)
            }
        }
    }

    /** The ids of the members voting in the group of [name], as this member knows them. */
    private fun ClusterHarness.Member.groupMembers(name: String): List<MemberId> {
        val database = cluster.status().first { it.database.name == name }.database
        return cluster.groupMembers(database).map { it.id }
    }

    private val ClusterHarness.Member.id: MemberId
        get() = cluster.self.id

    /** Waits until every group of [members] lists exactly them, as each of them knows it. */
    private fun awaitGroups(members: List<ClusterHarness.Member>, vararg names: String) {
        eventually {
            for (member in members) {
                for (name in names) {
                    assertThat(member.groupMembers(name))
                        .describedAs("members of $name seen by $member")
                        .containsExactlyInAnyOrderElementsOf(members.map { it.id })
                }
            }
        }
    }

    @Test
    fun `a dropped server leaves every group, stops its databases and cannot be added again`() {
        val cluster = cluster(ClusterHarness.METRICS)
        cluster.administer("CREATE DATABASE films")
        eventually {
            for (member in cluster.members) assertThat(member.count("films")).isEqualTo(0)
        }
        val removed = cluster.members.first { it != cluster.awaitLeader(SYSTEM) }
        val remaining = cluster.members - removed

        cluster.drop(removed, DEFAULT, "films")

        awaitGroups(remaining, SYSTEM, DEFAULT, "films")
        eventually {
            assertThat(removed.cluster.state).isEqualTo(MemberState.REMOVED)
            // Its copies moved off before it was dropped.
            for (name in listOf(DEFAULT, "films")) {
                assertThatThrownBy { removed.database(name) }
                    .describedAs(name)
                    .isInstanceOf(DatabaseNotFoundException::class.java)
            }
            assertThat(removed.samples().keys)
                .describedAs("metrics of the removed member")
                .noneMatch { it.startsWith("neo4j_database_films_") }
                .noneMatch { it.startsWith("neo4j_database_neo4j_") }
                // It left the group of system, which still runs here: the counts of what it
                // replicated stay, its progress in the group goes.
                .noneMatch {
                    it.startsWith("neo4j_database_system_cluster_raft_") && !it.endsWith("_total")
                }
            val leader = cluster.awaitLeader("films", among = { remaining })
            assertThat(leader.samples()["neo4j_database_films_dozendb_cluster_raft_followers"])
                .isEqualTo(1.0)
            for (member in remaining) {
                assertThat(member.samples()["neo4j_dbms_dozendb_cluster_members"]).isEqualTo(2.0)
            }
        }
        cluster
            .awaitLeader("films", among = { remaining })
            .database("films")
            .executeTransactionally("CREATE (:Film {title: 'Alien'})")
        assertThatThrownBy {
                cluster.administer(
                    "CALL dozendb.cluster.addMember('127.0.0.1:${removed.raftPort}')",
                    among = { remaining },
                )
            }
            .hasMessageContaining("was removed from the cluster")
    }

    @Test
    fun `the system leader can be dropped, and the others carry on`() {
        val cluster = cluster()
        val leader = cluster.awaitLeader(SYSTEM)
        val remaining = cluster.members - leader

        cluster.drop(leader, DEFAULT)

        awaitGroups(remaining, SYSTEM, DEFAULT)
        eventually { assertThat(leader.cluster.state).isEqualTo(MemberState.REMOVED) }
        cluster
            .awaitLeader(DEFAULT, among = { remaining })
            .database(DEFAULT)
            .executeTransactionally("CREATE (:Marker)")
    }

    @Test
    fun `a stopped server is replaced, and learns it was dropped when it starts again`() {
        val cluster = cluster()
        val stopped = cluster.members.first { it != cluster.awaitLeader(SYSTEM) }
        val stoppedId = stopped.serverId
        stopped.stop()
        val remaining = cluster.members - stopped
        val replacement = cluster.newMember().apply { start() }
        cluster.administer(
            "CALL dozendb.cluster.addMember('127.0.0.1:${replacement.raftPort}')",
            among = { remaining },
        )
        eventually {
            val states =
                cluster
                    .administer("SHOW SERVERS YIELD serverId, state", among = { remaining })
                    .associate { it["serverId"] to it["state"] }
            assertThat(states[replacement.serverId]).isEqualTo("Enabled")
        }

        // Its copies are made again from the others, on the replacement.
        cluster.administer(
            "DEALLOCATE DATABASES FROM SERVER '$stoppedId'",
            among = { remaining },
        )
        cluster.dropDeallocated(stoppedId, among = { remaining })

        awaitGroups(remaining + replacement, SYSTEM, DEFAULT)
        stopped.start()
        eventually {
            assertThat(stopped.cluster.state).isEqualTo(MemberState.REMOVED)
            assertThat(stopped.database(DEFAULT).isAvailable(0)).isFalse()
        }
        awaitGroups(remaining + replacement, SYSTEM, DEFAULT)
    }

    @Test
    fun `a member started after the cluster formed votes in system once it is added`() {
        val cluster = cluster()
        val joining = cluster.newMember().apply { start() }
        assertThat(joining.cluster.state).isEqualTo(MemberState.JOINING)
        assertThat(cluster.voters(cluster.members[0])).doesNotContain(joining)

        val added =
            cluster.administer("CALL dozendb.cluster.addMember('127.0.0.1:${joining.raftPort}')")

        assertThat(added)
            .containsExactly(
                mapOf(
                    "memberId" to joining.cluster.self.id.toString(),
                    "raftAddress" to "127.0.0.1:${joining.raftPort}",
                )
            )
        eventually {
            assertThat(joining.cluster.state).isEqualTo(MemberState.MEMBER)
            for (member in cluster.members) {
                assertThat(cluster.voters(member)).describedAs("voters seen by $member").hasSize(4)
            }
        }
        cluster.administer(
            "CREATE USER ann SET PASSWORD 'a-long-password' CHANGE NOT REQUIRED",
            among = { cluster.members },
        )
        eventually {
            assertThat(
                    joining.database(SYSTEM).executeTransactionally(
                        "SHOW USERS YIELD user",
                        emptyMap(),
                    ) {
                        it.stream().map { row -> row["user"] }.toList()
                    }
                )
                .contains("ann")
        }
    }

    private companion object {
        const val SYSTEM = "system"
        const val DEFAULT = "neo4j"
    }
}
