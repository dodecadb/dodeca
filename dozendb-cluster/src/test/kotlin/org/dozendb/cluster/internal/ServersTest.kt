package org.dozendb.cluster.internal

import java.nio.file.Path
import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.dozendb.cluster.internal.replication.ReplicatedDatabase
import org.dozendb.cluster.internal.replication.ReplicationService
import org.dozendb.core.DozenDbSettings
import org.dozendb.core.servers.ServerOptions
import org.dozendb.core.servers.ServerRecord
import org.dozendb.core.servers.ServerRecords
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.internal.Version

/** SHOW SERVERS and the server commands, as Neo4j Enterprise has them, on a cluster. */
class ServersTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    /** A cluster of three whose members are tagged with their index and `eu`. */
    private fun tagged(): ClusterHarness =
        ClusterHarness(root, 3).also { cluster ->
            harness = cluster
            cluster.members.forEach {
                it.settings = mapOf(DozenDbSettings.initialServerTags to it.tags)
            }
            cluster.start()
        }

    private val ClusterHarness.Member.tags: List<String>
        get() = listOf("member-$index", "eu")

    /** The server records in this member's system graph, and whether the initial ones are in. */
    private fun ClusterHarness.Member.records(): Pair<Set<ServerRecord>, Boolean> =
        database(SYSTEM).beginTx().use { tx ->
            ServerRecords.records(tx).values.toSet() to ServerRecords.initialServersEnabled(tx)
        }

    /** A cluster of three whose servers that join stay Free until enabled, as by default. */
    private fun cluster(): ClusterHarness =
        ClusterHarness(root, 3, settings = mapOf(ClusterSettings.autoEnableFreeServers to false))
            .also {
                harness = it
                it.start()
            }

    /** Starts a new member, with [tags], and admits it to the cluster. */
    private fun ClusterHarness.admit(tags: List<String> = emptyList()): ClusterHarness.Member =
        newMember().also {
            it.settings = mapOf(DozenDbSettings.initialServerTags to tags)
            it.start()
            administer("CALL dozendb.cluster.addMember('127.0.0.1:${it.raftPort}')")
        }

    /** Runs [query] on the system leader, among the initial members. */
    private fun ClusterHarness.administer(query: String) {
        awaitLeader(SYSTEM) { members.take(3) }.database(SYSTEM).executeTransactionally(query)
    }

    /** The row of [server] in SHOW SERVERS YIELD *, as this member shows it. */
    private fun ClusterHarness.Member.server(server: ClusterHarness.Member): Map<String, Any?> =
        showServers().single { it["serverId"] == server.serverId }

    /** The ids of the members of the Raft group of [database], as this member knows them. */
    private fun ClusterHarness.Member.groupMembers(database: String): List<*>? =
        database(SYSTEM).executeTransactionally(
            "CALL dozendb.cluster.status() YIELD database, members",
            emptyMap(),
        ) { result ->
            result.asSequence().firstOrNull { it["database"] == database }?.get("members")
                as List<*>?
        }

    private fun ClusterHarness.Member.replica(name: String): ReplicatedDatabase {
        val database = database(name)
        return checkNotNull(
            database.dependencyResolver
                .resolveDependency(ReplicationService::class.java)
                .replica(database.databaseId())
        )
    }

    /**
     * Makes [member] the system leader, as elections and balancing can make a Free server, which
     * leads no other database.
     */
    private fun ClusterHarness.leadSystem(member: ClusterHarness.Member) {
        eventually {
            val leader = awaitLeader(SYSTEM)
            if (leader != member) {
                leader
                    .replica(SYSTEM)
                    .transferLeadership(member.cluster.self.id, "a test", Duration.ofSeconds(5))
            }
            assertThat(awaitLeader(SYSTEM)).isEqualTo(member)
        }
    }

    /** The databases whose Raft group runs on this member. */
    private val ClusterHarness.Member.hosted: List<String>
        get() = cluster.status().map { it.database.name }.sorted()

    private fun eventually(assertion: () -> Unit) {
        await withPollInterval
            Duration.ofMillis(200) atMost
            ClusterHarness.AWAIT untilAsserted
            {
                assertion()
            }
    }

    private fun ClusterHarness.Member.showServers(): List<Map<String, Any?>> =
        database(SYSTEM).executeTransactionally("SHOW SERVERS YIELD *", emptyMap()) { result ->
            result.asSequence().map { it.toMap() }.toList()
        }

    @Test
    fun `every member of a new cluster is an enabled server, with the tags it started with`() {
        val cluster = tagged()
        val servers = cluster.running().associateBy { it.serverId }

        for (member in cluster.running()) {
            await atMost
                ClusterHarness.AWAIT untilAsserted
                {
                    val rows = member.showServers()
                    assertThat(rows.map { it["serverId"] })
                        .describedAs("on $member")
                        .containsExactlyInAnyOrderElementsOf(servers.keys)
                    for (row in rows) {
                        val server = servers.getValue(row["serverId"] as String)
                        assertThat(row["name"]).isEqualTo(row["serverId"])
                        assertThat(row["state"]).isEqualTo("Enabled")
                        assertThat(row["health"]).isEqualTo("Available")
                        assertThat(row["tags"]).isEqualTo(server.tags)
                        assertThat(row["modeConstraint"]).isEqualTo("NONE")
                        assertThat(row["allowedDatabases"]).isEqualTo(emptyList<String>())
                        assertThat(row["deniedDatabases"]).isEqualTo(emptyList<String>())
                        assertThat(row["hosting"]).isEqualTo(listOf(SYSTEM, NEO4J))
                        assertThat(row["requestedHosting"]).isEqualTo(listOf(SYSTEM, NEO4J))
                        assertThat(row["version"]).isEqualTo(Version.getNeo4jVersion())
                    }
                    assertThat(member.records())
                        .isEqualTo(
                            servers.values
                                .map {
                                    ServerRecord(
                                        it.cluster.self.id.uuid,
                                        options = ServerOptions(it.tags),
                                    )
                                }
                                .toSet() to true
                        )
                }
        }
    }

    @Test
    fun `a member that stopped is unavailable`() {
        val cluster = tagged()
        val leader = cluster.awaitLeader(SYSTEM)
        val stopped = cluster.running().first { it != leader }
        val stoppedId = stopped.serverId

        stopped.stop()

        await atMost
            ClusterHarness.AWAIT untilAsserted
            {
                val rows = leader.showServers().associateBy { it["serverId"] }
                assertThat(rows.getValue(stoppedId)["health"]).isEqualTo("Unavailable")
                assertThat(rows.getValue(stoppedId)["state"]).isEqualTo("Enabled")
                assertThat(rows.getValue(stoppedId)["hosting"]).isEqualTo(emptyList<String>())
                assertThat(rows.getValue(leader.serverId)["health"]).isEqualTo("Available")
            }
    }

    @Test
    fun `a server that joins is Free and hosts only system, until ENABLE SERVER`() {
        val cluster = cluster()
        val joining = cluster.admit(tags = listOf("new"))
        cluster.administer("CREATE DATABASE films WAIT")
        val leader = cluster.awaitLeader(SYSTEM) { cluster.members.take(3) }
        // It applied the creation of films: its system graph has it.
        eventually {
            assertThat(joining.server(joining)["requestedHosting"]).isEqualTo(emptyList<String>())
        }
        eventually {
            assertThat(
                    joining.database(SYSTEM).executeTransactionally(
                        "SHOW DATABASES YIELD name",
                        emptyMap(),
                    ) { r ->
                        r.asSequence().map { it["name"] }.toSet()
                    }
                )
                .contains("films")
        }

        assertThat(joining.hosted).containsExactly(SYSTEM)
        val free = leader.server(joining)
        assertThat(free["state"]).isEqualTo("Free")
        assertThat(free["hosting"]).isEqualTo(listOf(SYSTEM))
        assertThat(free["requestedHosting"]).isEqualTo(emptyList<String>())
        assertThat(free["tags"]).isEqualTo(emptyList<String>())

        cluster.administer("ENABLE SERVER '${joining.serverId}'")

        // As in Neo4j Enterprise, enabling places no database on it.
        eventually {
            val enabled = leader.server(joining)
            assertThat(enabled["state"]).isEqualTo("Enabled")
            assertThat(enabled["tags"]).isEqualTo(listOf("new"))
            assertThat(enabled["requestedHosting"]).isEqualTo(listOf(SYSTEM))
        }
        assertThat(joining.hosted).containsExactly(SYSTEM)

        cluster.administer("ALTER DATABASE films SET TOPOLOGY 4 PRIMARIES")

        eventually {
            assertThat(joining.hosted).containsExactly("films", SYSTEM)
            val placed = leader.server(joining)
            assertThat(placed["hosting"]).isEqualTo(listOf(SYSTEM, "films"))
            assertThat(placed["requestedHosting"]).isEqualTo(listOf(SYSTEM, "films"))
        }
    }

    @Test
    fun `ENABLE SERVER takes the options given over the initial ones, once`() {
        val cluster = cluster()
        val joining = cluster.admit(tags = listOf("new"))
        val enable = "ENABLE SERVER '${joining.serverId}' OPTIONS {tags: ['given']}"

        cluster.administer(enable)
        cluster.administer(enable)
        val again = catchThrowable {
            cluster.administer("ENABLE SERVER '${joining.serverId}' OPTIONS {tags: ['other']}")
        }

        assertThat(again)
            .hasMessageContaining("is enabled already")
            .hasMessageContaining("ALTER SERVER")
        val leader = cluster.awaitLeader(SYSTEM) { cluster.members.take(3) }
        eventually { assertThat(leader.server(joining)["tags"]).isEqualTo(listOf("given")) }
        assertThat(catchThrowable { cluster.administer("ENABLE SERVER 'nobody'") })
            .hasMessageContaining("nobody")
    }

    @Test
    fun `a database created while a Free server leads system starts on the enabled servers`() {
        val cluster = cluster()
        val enabled = cluster.members.take(3)
        val free = cluster.admit()
        eventually { assertThat(free.server(free)["state"]).isEqualTo("Free") }

        cluster.leadSystem(free)
        free.database(SYSTEM).executeTransactionally("CREATE DATABASE films")

        eventually {
            for (member in enabled) {
                assertThat(member.hosted).describedAs("on $member").contains("films")
                assertThat(member.groupMembers("films"))
                    .describedAs("on $member")
                    .containsExactlyInAnyOrderElementsOf(enabled.map { it.serverId })
            }
        }
        assertThat(free.hosted).containsExactly(SYSTEM)
    }

    @Test
    fun `a Free server that leads system changes the members of every database group`() {
        val cluster = cluster()
        cluster.administer("CREATE DATABASE films WAIT")
        val enabled = cluster.members.take(3)
        val free = cluster.admit()
        val joining = cluster.admit()
        eventually {
            assertThat(free.server(free)["state"]).isEqualTo("Free")
            assertThat(free.server(joining)["state"]).isEqualTo("Free")
        }
        cluster.leadSystem(free)

        val system = free.database(SYSTEM)
        system.executeTransactionally("ENABLE SERVER '${joining.serverId}'")
        system.executeTransactionally("ALTER DATABASE films SET TOPOLOGY 4 PRIMARIES")
        system.executeTransactionally("ALTER DATABASE $NEO4J SET TOPOLOGY 4 PRIMARIES")
        val voters = enabled + joining
        eventually {
            for (member in voters) {
                for (database in listOf("films", NEO4J)) {
                    assertThat(member.groupMembers(database))
                        .describedAs("$database on $member")
                        .containsExactlyInAnyOrderElementsOf(voters.map { it.serverId })
                }
            }
        }
        // A cordoned server's copies go first when the databases shrink.
        val removed = enabled.last()
        system.executeTransactionally("CALL dbms.cluster.cordonServer('${removed.serverId}')")
        system.executeTransactionally("ALTER DATABASE films SET TOPOLOGY 3 PRIMARIES")
        system.executeTransactionally("ALTER DATABASE $NEO4J SET TOPOLOGY 3 PRIMARIES")
        val remaining = voters - removed
        eventually {
            for (member in remaining) {
                for (database in listOf("films", NEO4J)) {
                    assertThat(member.groupMembers(database))
                        .describedAs("$database on $member")
                        .containsExactlyInAnyOrderElementsOf(remaining.map { it.serverId })
                }
            }
        }

        // The Free server made the changes: it led system throughout.
        assertThat(cluster.awaitLeader(SYSTEM)).isEqualTo(free)
        assertThat(free.hosted).containsExactly(SYSTEM)
    }

    @Test
    fun `ALTER SERVER replaces every option, and refuses options that are not valid`() {
        val cluster = tagged()
        val leader = cluster.awaitLeader(SYSTEM)
        val server = cluster.running().first()
        val id = server.serverId
        cluster.administer(
            "ALTER SERVER '$id' SET OPTIONS {tags: ['a', 'b'], modeConstraint: 'PRIMARY'}"
        )
        eventually {
            assertThat(leader.server(server)["tags"]).isEqualTo(listOf("a", "b"))
            assertThat(leader.server(server)["modeConstraint"]).isEqualTo("PRIMARY")
        }

        cluster.administer("ALTER SERVER '$id' SET OPTIONS {tags: ['c']}")

        eventually {
            assertThat(leader.server(server)["tags"]).isEqualTo(listOf("c"))
            assertThat(leader.server(server)["modeConstraint"]).isEqualTo("NONE")
        }
        for (options in
            listOf(
                "{modeConstraint: 'TERTIARY'}",
                "{allowedDatabases: ['neo4j'], deniedDatabases: ['films']}",
                "{tags: ['c', 'c']}",
            )) {
            assertThat(
                    catchThrowable { cluster.administer("ALTER SERVER '$id' SET OPTIONS $options") }
                )
                .describedAs(options)
                .hasMessageStartingWith("Could not alter server '$id'")
        }
        assertThat(leader.server(server)["tags"]).isEqualTo(listOf("c"))
    }

    @Test
    fun `ALTER SERVER refuses a Free server, which ENABLE SERVER enables`() {
        val cluster = cluster()
        val joining = cluster.admit()

        assertThat(
                catchThrowable {
                    cluster.administer(
                        "ALTER SERVER '${joining.serverId}' SET OPTIONS {tags: ['a']}"
                    )
                }
            )
            .hasMessageContaining("is Free")
            .hasMessageContaining("ENABLE SERVER")
    }

    @Test
    fun `a renamed server goes by its new name, which no other server may take`() {
        val cluster = tagged()
        val leader = cluster.awaitLeader(SYSTEM)
        val (first, second) = cluster.running().take(2)

        cluster.administer("RENAME SERVER '${first.serverId}' TO 'one'")
        eventually { assertThat(leader.server(first)["name"]).isEqualTo("one") }
        cluster.administer("ALTER SERVER 'one' SET OPTIONS {tags: ['renamed']}")
        eventually { assertThat(leader.server(first)["tags"]).isEqualTo(listOf("renamed")) }

        for (name in listOf("one", first.serverId, "")) {
            assertThat(
                    catchThrowable {
                        cluster.administer("RENAME SERVER '${second.serverId}' TO '$name'")
                    }
                )
                .describedAs(name)
                .hasMessageStartingWith("Could not rename server '${second.serverId}'")
        }
        cluster.administer("RENAME SERVER 'one' TO '${first.serverId}'")
        eventually { assertThat(leader.server(first)["name"]).isEqualTo(first.serverId) }
    }

    @Test
    fun `server commands that change servers run on the system leader`() {
        val cluster = cluster()
        val joining = cluster.admit()
        val leader = cluster.awaitLeader(SYSTEM) { cluster.members.take(3) }
        val follower = cluster.members.take(3).first { it != leader }

        val refused = catchThrowable {
            follower.database(SYSTEM).executeTransactionally("ENABLE SERVER '${joining.serverId}'")
        }

        assertThat((refused as QueryExecutionException).statusCode)
            .isEqualTo(Status.Cluster.NotALeader.code().serialize())
        assertThat(leader.server(joining)["state"]).isEqualTo("Free")
    }

    @Test
    fun `with servers enabled at once, one that joins can host databases at once`() {
        val cluster =
            ClusterHarness(root, 3, settings = mapOf(ClusterSettings.autoEnableFreeServers to true))
                .also {
                    harness = it
                    it.start()
                }

        val joining = cluster.admit(tags = listOf("new"))

        eventually {
            assertThat(joining.server(joining)["state"]).isEqualTo("Enabled")
            assertThat(joining.server(joining)["tags"]).isEqualTo(listOf("new"))
        }
        cluster.administer("ALTER DATABASE $NEO4J SET TOPOLOGY 4 PRIMARIES")
        eventually { assertThat(joining.hosted).containsExactly(NEO4J, SYSTEM) }
    }

    @Test
    fun `enabling servers at once enables those that are Free`() {
        val cluster = cluster()
        val joining = cluster.admit()
        eventually { assertThat(joining.server(joining)["state"]).isEqualTo("Free") }

        cluster.administer("CALL dbms.cluster.setAutomaticallyEnableFreeServers(true)")

        eventually { assertThat(joining.server(joining)["state"]).isEqualTo("Enabled") }
    }

    private companion object {
        const val SYSTEM = "system"
        const val NEO4J = "neo4j"
    }
}
