package dev.dodeca.cluster.internal

import dev.dodeca.cluster.MemberState
import dev.dodeca.consensus.Role
import dev.dodeca.core.servers.CopyMode
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.driver.AccessMode
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig
import org.neo4j.graphdb.config.Setting
import org.neo4j.kernel.database.NamedDatabaseId

/**
 * A server with `server.cluster.system_database_mode=SECONDARY` pulls `system` instead of voting in
 * its group: it joins a formed cluster, is admitted by the system leader, and is a server like any
 * other, Free until enabled.
 */
class SystemSecondariesTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    /** Three primaries of `system`, whose servers that join stay Free until enabled. */
    private fun cluster(settings: Map<Setting<*>, Any> = emptyMap()): ClusterHarness =
        ClusterHarness(
                root,
                3,
                settings = mapOf(ClusterSettings.autoEnableFreeServers to false) + settings,
            )
            .also {
                harness = it
                it.start()
            }

    /** Starts a server that hosts `system` as a secondary; it waits to be admitted. */
    private fun ClusterHarness.startSecondary(): ClusterHarness.Member =
        newMember().also {
            it.settings = mapOf(ClusterSettings.systemDatabaseMode to CopyMode.SECONDARY)
            it.start()
        }

    private val ClusterHarness.primaries: List<ClusterHarness.Member>
        get() = members.take(3)

    private fun ClusterHarness.systemLeader(): ClusterHarness.Member =
        awaitLeader(SYSTEM) { primaries }

    private fun ClusterHarness.administer(query: String) {
        systemLeader().database(SYSTEM).executeTransactionally(query)
    }

    private fun ClusterHarness.admit(member: ClusterHarness.Member) =
        administer("CALL dodeca.cluster.addMember('127.0.0.1:${member.raftPort}')")

    /** The row of [server] in SHOW SERVERS YIELD *, as this member shows it. */
    private fun ClusterHarness.Member.server(server: ClusterHarness.Member): Map<String, Any?> =
        database(SYSTEM).executeTransactionally("SHOW SERVERS YIELD *", emptyMap()) { result ->
            result.asSequence().map { it.toMap() }.single { it["serverId"] == server.serverId }
        }

    /** The names of the databases this member's system graph has. */
    private fun ClusterHarness.Member.databaseNames(): Set<Any?> =
        database(SYSTEM).executeTransactionally("SHOW DATABASES YIELD name", emptyMap()) { r ->
            r.asSequence().map { it["name"] }.toSet()
        }

    /** The roles of the rows of SHOW DATABASES for [name], as this member shows them, sorted. */
    private fun ClusterHarness.Member.roles(name: String): List<Any?> =
        database(SYSTEM).executeTransactionally("SHOW DATABASE $name YIELD role", emptyMap()) { r ->
            r.asSequence().map { it["role"] as String }.sorted().toList()
        }

    private val ClusterHarness.Member.marked: String
        get() =
            home.resolve("data/raft/secondaries").let {
                if (Files.exists(it)) Files.readString(it) else ""
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
    fun `a system secondary pulls system, and is a Free server once the system leader admits it`() {
        val cluster = cluster()
        val secondary = cluster.startSecondary()

        // It pulls system before it is admitted, and votes in no group.
        assertThat(secondary.cluster.state).isEqualTo(MemberState.JOINING)
        assertThat(secondary.marked).contains(SYSTEM_ID)
        cluster.administer("CREATE DATABASE films")
        eventually { assertThat(secondary.databaseNames()).contains("films") }
        assertThat(secondary.status(SYSTEM)?.role).isEqualTo(Role.Inactive)

        cluster.admit(secondary)

        eventually { assertThat(secondary.cluster.state).isEqualTo(MemberState.MEMBER) }
        val leader = cluster.systemLeader()
        eventually {
            val free = leader.server(secondary)
            assertThat(free["state"]).isEqualTo("Free")
            assertThat(free["hosting"]).isEqualTo(listOf(SYSTEM))
        }
        eventually {
            assertThat(leader.roles(SYSTEM))
                .containsExactly("primary", "primary", "primary", "secondary")
        }
        assertThat(leader.cluster.groupMembers(leader.cluster.systemDatabase)).hasSize(3)
        // Admitting it again changes nothing.
        cluster.admit(secondary)
    }

    @Test
    fun `only the system leader admits a system secondary`() {
        val cluster = cluster()
        val secondary = cluster.startSecondary()
        val follower = cluster.primaries.first { it != cluster.systemLeader() }

        assertThatThrownBy {
                follower
                    .database(SYSTEM)
                    .executeTransactionally(
                        "CALL dodeca.cluster.addMember('127.0.0.1:${secondary.raftPort}')"
                    )
            }
            .hasMessageContaining("which the leader of the system database admits")

        assertThat(secondary.cluster.state).isEqualTo(MemberState.JOINING)
    }

    @Test
    fun `an enabled system secondary hosts databases, primaries among them`() {
        val cluster = cluster()
        val secondary = cluster.startSecondary()
        cluster.admit(secondary)
        eventually { assertThat(secondary.cluster.state).isEqualTo(MemberState.MEMBER) }
        cluster.administer("ENABLE SERVER '${secondary.serverId}'")

        cluster.administer("CREATE DATABASE films TOPOLOGY 4 PRIMARIES")

        eventually { assertThat(secondary.status("films")?.role).isIn(Role.Leader, Role.Follower) }
        cluster.awaitLeader("films").database("films").executeTransactionally("CREATE (:Film)")
        eventually {
            assertThat(
                    secondary.database("films").executeTransactionally(
                        "MATCH (f:Film) RETURN count(f) AS c",
                        emptyMap(),
                    ) {
                        it.next()["c"]
                    }
                )
                .isEqualTo(1L)
        }
    }

    @Test
    fun `administration commands through a system secondary run on the system leader, and it reads them at once`() {
        val cluster =
            cluster(
                mapOf(
                    BoltConnector.enabled to true,
                    // Routing tables name only the server a driver connects to.
                    GraphDatabaseSettings.routing_default_router to
                        GraphDatabaseSettings.RoutingMode.SERVER,
                    // Reads that wait for a bookmark do not wait for the next pull.
                    ClusterSettings.pullInterval to Duration.ofMinutes(10),
                )
            )
        // Admitted or not: it would see its admission only at its next pull.
        val secondary = cluster.startSecondary()

        GraphDatabase.driver("neo4j://${secondary.boltAddress()}", AuthTokens.none()).use { driver
            ->
            val bookmarks =
                driver.session(SessionConfig.forDatabase(SYSTEM)).use { session ->
                    session.run("CREATE DATABASE books").consume()
                    session.lastBookmarks()
                }
            driver
                .session(
                    SessionConfig.builder()
                        .withDatabase(SYSTEM)
                        .withBookmarks(bookmarks)
                        .withDefaultAccessMode(AccessMode.READ)
                        .build()
                )
                .use { session ->
                    assertThat(
                            session
                                .run(
                                    "SHOW DATABASES YIELD name WHERE name = 'books' RETURN DISTINCT name"
                                )
                                .list()
                        )
                        .hasSize(1)
                }
        }
        assertThat(cluster.systemLeader().databaseNames()).contains("books")
    }

    @Test
    fun `a system secondary that restarts pulls on from its own store`() {
        val cluster = cluster()
        val secondary = cluster.startSecondary()
        cluster.admit(secondary)
        eventually { assertThat(secondary.cluster.state).isEqualTo(MemberState.MEMBER) }
        secondary.stop()

        cluster.administer("CREATE DATABASE films")
        secondary.start()

        eventually { assertThat(secondary.databaseNames()).contains("films") }
        assertThat(secondary.cluster.state).isEqualTo(MemberState.MEMBER)
        assertThat(secondary.status(SYSTEM)?.role).isEqualTo(Role.Inactive)
    }

    @Test
    fun `a dropped system secondary learns it from the system graph, and cannot be added again`() {
        val cluster = cluster()
        val secondary = cluster.startSecondary()
        cluster.admit(secondary)
        eventually { assertThat(secondary.cluster.state).isEqualTo(MemberState.MEMBER) }

        cluster.administer("DROP SERVER '${secondary.serverId}'")

        eventually { assertThat(secondary.cluster.state).isEqualTo(MemberState.REMOVED) }
        val leader = cluster.systemLeader()
        eventually {
            assertThat(
                    leader.database(SYSTEM).beginTx().use { tx -> SystemSecondaryRecords.all(tx) }
                )
                .isEmpty()
            assertThat(leader.server(secondary)["state"]).isEqualTo("Dropped")
        }
        assertThatThrownBy { cluster.admit(secondary) }.hasMessageContaining("was removed")
    }

    @Test
    fun `the cluster carries on without a system secondary that stopped`() {
        val cluster = cluster()
        val (stopped, running) = List(2) { cluster.startSecondary() }
        listOf(stopped, running).forEach { cluster.admit(it) }
        eventually {
            assertThat(listOf(stopped, running).map { it.cluster.state })
                .containsOnly(MemberState.MEMBER)
        }

        stopped.stop()
        cluster.administer("CREATE USER dave SET PASSWORD 'dave-secret-1'")
        cluster.administer("CREATE DATABASE films WAIT")

        eventually {
            assertThat(running.databaseNames()).contains("films")
            assertThat(
                    running.database(SYSTEM).executeTransactionally(
                        "SHOW USERS YIELD user WHERE user = 'dave' RETURN user",
                        emptyMap(),
                    ) {
                        it.stream().count()
                    }
                )
                .isEqualTo(1L)
        }
    }

    @Test
    fun `a primary restarted as a system secondary turns secondary in place once admitted again`() {
        val cluster = cluster()
        val turning = cluster.newMember().also { it.start() }
        cluster.admit(turning)
        val leader = cluster.systemLeader()
        eventually { assertThat(voters(leader)).contains(turning.serverId) }
        turning.stop()
        turning.settings = mapOf(ClusterSettings.systemDatabaseMode to CopyMode.SECONDARY)
        turning.start()

        // It votes on until it is admitted again.
        eventually { assertThat(turning.status(SYSTEM)?.role).isIn(Role.Follower, Role.Leader) }
        assertThat(turning.cluster.pullsSystem).isFalse()
        cluster.admit(turning)

        eventually {
            assertThat(turning.cluster.pullsSystem).isTrue()
            assertThat(voters(cluster.systemLeader())).doesNotContain(turning.serverId)
            assertThat(cluster.systemLeader().roles(SYSTEM))
                .containsExactly("primary", "primary", "primary", "secondary")
        }
        assertThat(turning.cluster.state).isEqualTo(MemberState.MEMBER)
        cluster.administer("CREATE DATABASE films")
        eventually { assertThat(turning.databaseNames()).contains("films") }
        assertThat(turning.marked).contains(SYSTEM_ID)
    }

    @Test
    fun `a system secondary restarted as a primary turns primary in place once admitted again`() {
        val cluster = cluster()
        val turning = cluster.startSecondary()
        cluster.admit(turning)
        eventually { assertThat(turning.cluster.state).isEqualTo(MemberState.MEMBER) }
        cluster.administer("CREATE DATABASE films")
        eventually { assertThat(turning.databaseNames()).contains("films") }
        turning.stop()
        turning.settings = mapOf(ClusterSettings.systemDatabaseMode to CopyMode.PRIMARY)
        turning.start()

        // It pulls on until it is admitted again.
        assertThat(turning.cluster.pullsSystem).isTrue()
        cluster.administer("CREATE DATABASE books")
        eventually { assertThat(turning.databaseNames()).contains("books") }
        cluster.admit(turning)

        assertThat(voters(cluster.systemLeader())).contains(turning.serverId)
        eventually {
            assertThat(turning.status(SYSTEM)?.role).isIn(Role.Follower, Role.Leader)
            assertThat(
                    cluster.systemLeader().database(SYSTEM).beginTx().use { tx ->
                        SystemSecondaryRecords.all(tx)
                    }
                )
                .isEmpty()
        }
        assertThat(turning.cluster.pullsSystem).isFalse()
        assertThat(turning.cluster.state).isEqualTo(MemberState.MEMBER)
        assertThat(turning.marked).doesNotContain(SYSTEM_ID)
        cluster.administer("CREATE DATABASE music")
        eventually { assertThat(turning.databaseNames()).contains("music") }
    }

    /** The ids of the voting members of `system`, as [member] knows them. */
    private fun voters(member: ClusterHarness.Member): List<String> =
        member.cluster.groupMembers(member.cluster.systemDatabase).map { it.id.uuid.toString() }

    private companion object {
        const val SYSTEM = "system"
        val SYSTEM_ID = NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID.databaseId().uuid().toString()
    }
}
