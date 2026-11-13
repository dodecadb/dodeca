package dev.dodeca.cluster.internal

import dev.dodeca.cluster.DatabaseGroups
import dev.dodeca.cluster.MemberState
import dev.dodeca.cluster.MembershipFile
import dev.dodeca.cluster.internal.ClusterHarness.Companion.AWAIT
import dev.dodeca.cluster.internal.ClusterHarness.Companion.DEFAULT_DATABASE
import dev.dodeca.cluster.internal.admin.unbindSystemDb
import dev.dodeca.core.DodecaSettings
import dev.dodeca.core.servers.CopyMode
import dev.dodeca.core.servers.ServerRecords
import java.nio.file.Path
import java.time.Duration
import kotlin.concurrent.thread
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.until
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.helpers.SocketAddress

/**
 * Recovering `system` after it lost its majority, as Neo4j Enterprise's disaster recovery guide
 * does: the servers left are unbound from `system` with `neo4j-admin dbms unbind-system-db` and
 * re-form it, with new servers if need be; the lost servers stay servers, are refused when they
 * come back with their old state, and rejoin under their old ids once unbound too.
 */
class SystemRecoveryTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    @Test
    fun `two of five servers and a new one re-form system, and lost servers are refused until they rebind`() {
        val cluster = ClusterHarness(root, 5).also { harness = it }
        cluster.start()
        cluster.administer("CREATE USER alice SET PASSWORD 'alice-secret' CHANGE NOT REQUIRED")
        cluster.awaitLeader().database().executeTransactionally("CREATE (:Film {title: 'Heat'})")
        val lost = cluster.members.take(3)
        val left = cluster.members.drop(3)
        // Before the disaster, the servers left hold the user and the film.
        cluster.awaitEverywhere(left, users = listOf("alice"), films = listOf("Heat"))
        val lostIds = lost.map { it.serverId }
        val neo4j = cluster.members[0].database().databaseId().databaseId().uuid()
        cluster.members.forEach { it.stop() }
        for (member in left) {
            val result = unbindSystemDb(member.home)
            assertThat(result.exitCode).describedAs("$result").isZero
        }
        val new = cluster.newMember()
        val reforming = left + new
        reforming.forEach { it.initialMembers(reforming) }

        cluster.start(reforming)

        val leader = cluster.awaitLeader(SYSTEM, among = { reforming })
        val group = leader.cluster.systemDatabase.id
        assertThat(group).isNotEqualTo(DatabaseGroups.SYSTEM_DATABASE)
        for (member in reforming) {
            assertThat(member.cluster.systemDatabase.id).describedAs("$member").isEqualTo(group)
            assertThat(member.cluster.groupMembers(member.cluster.systemDatabase).map { it.id })
                .describedAs("$member")
                .containsExactlyInAnyOrderElementsOf(reforming.map { it.cluster.self.id })
            val record = member.record()
            assertThat(record.systemGroup).describedAs("$member").isEqualTo(group)
            assertThat(record.systemUnbound).describedAs("$member").isFalse()
            assertThat(record.state).describedAs("$member").isEqualTo(MemberState.MEMBER)
        }
        cluster.administer("CREATE USER bob SET PASSWORD 'bob-secret' CHANGE NOT REQUIRED")
        cluster.awaitEverywhere(reforming, users = listOf("alice", "bob"))
        // The servers left out stay servers, with the databases placed on them.
        leader.database(SYSTEM).beginTx().use { tx ->
            assertThat(ServerRecords.records(tx).keys.map { it.toString() }).containsAll(lostIds)
            assertThat(ServerRecords.lost(tx).map { it.toString() })
                .containsExactlyInAnyOrderElementsOf(lostIds)
        }
        left.forEach { assertThat(it.films()).describedAs("$it").containsExactly("Heat") }

        // Back with their old state, the lost servers are refused: they would run the old group.
        for (member in lost) {
            val refusal = catchThrowable { member.start() }
            assertThat(refusal).describedAs("$member").isNotNull
            assertThat(generateSequence(refusal) { it.cause }.last())
                .describedAs("$member")
                .hasMessageContaining("recovered its system database without this server")
            assertThat(member.running).isFalse()
        }
        cluster.administer("CREATE USER carol SET PASSWORD 'carol-secret' CHANGE NOT REQUIRED")

        // Unbound too, a lost server rebinds: added under its old id, it keeps its copies.
        val rebinding = lost[2]
        assertThat(unbindSystemDb(rebinding.home).exitCode).isZero
        rebinding.initialMembers(listOf(leader))
        val starting = thread(name = "start-$rebinding") { rebinding.start() }
        var refusal: Throwable? = null
        val added = runCatching {
            await atMost
                AWAIT until
                {
                    runCatching {
                        cluster.administer(
                            "CALL dodeca.cluster.addMember('127.0.0.1:${rebinding.raftPort}')",
                            among = reforming,
                        )
                    }
                        .onFailure { refusal = it }
                        .isSuccess
                }
        }
        assertThat(added.isSuccess).describedAs("added; last refusal: $refusal").isTrue
        starting.join(ClusterHarness.START_TIMEOUT.toMillis())
        assertThat(rebinding.running).isTrue()
        assertThat(rebinding.serverId).isEqualTo(lostIds[2])
        assertThat(rebinding.cluster.systemDatabase.id).isEqualTo(group)
        cluster.awaitEverywhere(reforming + rebinding, users = listOf("alice", "bob", "carol"))
        await atMost
            AWAIT untilAsserted
            {
                cluster
                    .awaitLeader(SYSTEM) { reforming }
                    .database(SYSTEM)
                    .beginTx()
                    .use { tx ->
                        assertThat(ServerRecords.lost(tx).map { it.toString() })
                            .containsExactlyInAnyOrderElementsOf(lostIds.take(2))
                    }
            }
        // With it, three of the default database's five primaries run again: it has a leader.
        val primaries = left + rebinding
        cluster
            .awaitLeader(DEFAULT_DATABASE, among = { primaries })
            .database()
            .executeTransactionally("CREATE (:Film {title: 'Ronin'})")
        cluster.awaitEverywhere(primaries, films = listOf("Heat", "Ronin"))
        // The lost servers keep their places in its group until DEALLOCATE moves it off them.
        for (member in primaries) {
            assertThat(member.cluster.groupMembers(member.cluster.groups.of("neo4j", neo4j)))
                .describedAs("$member")
                .extracting<String> { it.id.uuid.toString() }
                .containsExactlyInAnyOrderElementsOf(lostIds + left.map { it.serverId })
        }
    }

    @Test
    fun `a system secondary unbound with the others pulls from the re-formed group`() {
        val cluster = ClusterHarness(root, 3).also { harness = it }
        cluster.start()
        val secondary =
            cluster.newMember().also {
                it.settings = mapOf(ClusterSettings.systemDatabaseMode to CopyMode.SECONDARY)
                it.start()
            }
        cluster.administer(
            "CREATE USER alice SET PASSWORD 'alice-secret' CHANGE NOT REQUIRED",
            among = cluster.members.take(3),
        )
        cluster.awaitEverywhere(cluster.members, users = listOf("alice"))
        cluster.members.forEach { it.stop() }
        val left = cluster.members[2]
        for (member in listOf(left, secondary)) {
            val result = unbindSystemDb(member.home)
            assertThat(result.exitCode).describedAs("$result").isZero
            member.initialMembers(listOf(left))
        }

        cluster.start(listOf(left, secondary))

        val group = left.cluster.systemDatabase.id
        assertThat(group).isNotEqualTo(DatabaseGroups.SYSTEM_DATABASE)
        assertThat(secondary.cluster.systemDatabase.id).isEqualTo(group)
        assertThat(secondary.cluster.pullsSystem).isTrue()
        assertThat(secondary.record().systemGroup).isEqualTo(group)
        assertThat(secondary.record().systemUnbound).isFalse()
        cluster.administer(
            "CREATE USER bob SET PASSWORD 'bob-secret' CHANGE NOT REQUIRED",
            among = listOf(left),
        )
        cluster.awaitEverywhere(listOf(left, secondary), users = listOf("alice", "bob"))
    }

    @Test
    fun `lost servers are listed, cordoned, and dropped once their databases moved off them`() {
        val cluster =
            ClusterHarness(root, 5, settings = mapOf(ClusterSettings.defaultPrimaries to 3)).also {
                harness = it
            }
        // films goes to servers 2 to 4, and the default database not to server 2.
        cluster.members.take(2).forEach { it.deny(FILMS) }
        cluster.members[2].deny(DEFAULT_DATABASE)
        cluster.start()
        cluster.administer("CREATE DATABASE $FILMS TOPOLOGY 3 PRIMARIES WAIT")
        cluster
            .awaitLeader(FILMS) { cluster.members.drop(2) }
            .database(FILMS)
            .executeTransactionally("CREATE (:Film {title: 'Heat'})")
        val lost = cluster.members.take(3)
        val left = cluster.members.drop(3)
        cluster.awaitEverywhere(left, films = listOf("Heat"), database = FILMS)
        val lostIds = lost.map { it.serverId }
        cluster.members.forEach { it.stop() }
        left.forEach { assertThat(unbindSystemDb(it.home).exitCode).isZero }
        val new = cluster.newMember()
        val servers = left + new
        servers.forEach { it.initialMembers(servers) }
        cluster.start(servers)
        val leader = cluster.awaitLeader(SYSTEM, among = { servers })

        // Lost servers stay servers: Unavailable, as recorded, to host what is placed on them.
        val rows = leader.showServers()
        for (id in lostIds) {
            val row = rows.single { it["serverId"] == id }
            assertThat(row["health"]).describedAs(id).isEqualTo("Unavailable")
            assertThat(row["state"]).describedAs(id).isEqualTo("Enabled")
            assertThat(row["hosting"]).describedAs(id).isEqualTo(emptyList<String>())
        }
        assertThat(rows.single { it["serverId"] == lostIds[2] }["requestedHosting"])
            .isEqualTo(listOf(SYSTEM, FILMS))
        for (id in lostIds) cluster.administer(
            "CALL dbms.cluster.cordonServer('$id')",
            among = servers,
        )
        assertThat(leader.showServers().filter { it["serverId"] in lostIds })
            .hasSize(3)
            .allSatisfy { assertThat(it["state"]).isEqualTo("Cordoned") }

        // films keeps its majority: its copy on the lost server moves to the new one.
        cluster.administer(
            "DEALLOCATE DATABASES FROM SERVER '${lostIds[2]}'",
            among = servers,
        )
        await atMost
            AWAIT untilAsserted
            {
                assertThat(leader.server(lostIds[2])["state"]).isEqualTo("Deallocated")
            }
        cluster.awaitEverywhere(listOf(new), films = listOf("Heat"), database = FILMS)
        cluster.administer("DROP SERVER '${lostIds[2]}'", among = servers)
        await atMost
            AWAIT untilAsserted
            {
                assertThat(leader.showServers().map { it["serverId"] })
                    .doesNotContain(lostIds[2])
                    .contains(lostIds[0], lostIds[1])
            }
        leader.database(SYSTEM).beginTx().use { tx ->
            assertThat(ServerRecords.lost(tx).map { it.toString() })
                .containsExactlyInAnyOrder(lostIds[0], lostIds[1])
        }
    }

    private fun ClusterHarness.Member.deny(database: String) {
        settings = settings + mapOf(DodecaSettings.initialServerDeniedDatabases to listOf(database))
    }

    private fun ClusterHarness.Member.showServers(): List<Map<String, Any?>> =
        database(SYSTEM).executeTransactionally("SHOW SERVERS YIELD *", emptyMap()) { result ->
            result.asSequence().map { it.toMap() }.toList()
        }

    private fun ClusterHarness.Member.server(id: String): Map<String, Any?> =
        showServers().single { it["serverId"] == id }

    private fun ClusterHarness.Member.initialMembers(members: List<ClusterHarness.Member>) {
        settings =
            settings +
                mapOf(
                    ClusterSettings.initialMembers to
                        members.map { SocketAddress("127.0.0.1", it.raftPort) }
                )
    }

    private fun ClusterHarness.Member.record() =
        checkNotNull(MembershipFile(home.resolve("data/raft/${MembershipFile.FILE_NAME}")).read())

    /** Runs [command] on the leader of `system` among its voters, [among]. */
    private fun ClusterHarness.administer(
        command: String,
        among: List<ClusterHarness.Member> = running(),
    ) {
        awaitLeader(SYSTEM) { among }.database(SYSTEM).executeTransactionally(command)
    }

    private fun ClusterHarness.Member.users(): List<String> =
        database(SYSTEM).executeTransactionally("SHOW USERS YIELD user", emptyMap()) {
            it.stream().map { row -> row["user"] as String }.toList()
        }

    private fun ClusterHarness.Member.films(database: String = DEFAULT_DATABASE): List<String> =
        database(database).executeTransactionally(
            "MATCH (f:Film) RETURN f.title AS title ORDER BY title",
            emptyMap(),
        ) {
            it.stream().map { row -> row["title"] as String }.toList()
        }

    /** Waits until each of [members] holds [users] and [films], as replicated to it. */
    private fun ClusterHarness.awaitEverywhere(
        members: List<ClusterHarness.Member>,
        users: List<String> = emptyList(),
        films: List<String>? = null,
        database: String = DEFAULT_DATABASE,
    ) {
        await withPollInterval
            Duration.ofMillis(200) atMost
            AWAIT untilAsserted
            {
                for (member in members) {
                    assertThat(member.users()).describedAs("$member").containsAll(users)
                    films?.let {
                        assertThat(member.films(database)).describedAs("$member").isEqualTo(it)
                    }
                }
            }
    }

    private companion object {
        const val SYSTEM = "system"
        const val FILMS = "films"
    }
}
