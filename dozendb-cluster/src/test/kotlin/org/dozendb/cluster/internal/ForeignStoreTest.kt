package org.dozendb.cluster.internal

import java.nio.file.Path
import java.time.Duration
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
import org.neo4j.io.fs.FileUtils
import org.neo4j.storageengine.api.TransactionIdStore

/** A member whose database files were replaced by hand with those of another database. */
class ForeignStoreTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    private fun ClusterHarness.administer(cypher: String) {
        awaitLeader("system").database("system").executeTransactionally(cypher)
    }

    private fun ClusterHarness.write(
        database: String,
        times: Int,
        among: () -> List<ClusterHarness.Member> = ::running,
    ) {
        repeat(times) {
            awaitLeader(database, among).database(database).executeTransactionally("CREATE (:Node)")
        }
    }

    private fun ClusterHarness.Member.count(database: String): Long =
        database(database).executeTransactionally(
            "MATCH (n:Node) RETURN count(n) AS count",
            emptyMap(),
        ) {
            it.next()["count"] as Long
        }

    private fun ClusterHarness.Member.raftIndex(database: String): Long =
        database(database)
            .dependencyResolver
            .resolveDependency(TransactionIdStore::class.java)
            .lastCommittedTransaction
            .consensusIndex()

    /** What SHOW DATABASE shows of this member's copy of [database]. */
    private fun ClusterHarness.Member.shown(database: String): Map<String, Any?> =
        database("system").executeTransactionally(
            "SHOW DATABASE $database YIELD serverID, currentStatus, statusMessage " +
                "WHERE serverID = \$self RETURN currentStatus, statusMessage",
            mapOf("self" to cluster.self.id.uuid.toString()),
        ) {
            it.next().toMap()
        }

    /** The ports of the Bolt addresses in [member]'s routing table for [database], by role. */
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

    private fun ClusterHarness.awaitSame(database: String) {
        await atMost
            AWAIT untilAsserted
            {
                val counts = running().map { it.count(database) }
                assertThat(counts.distinct()).describedAs("$counts").hasSize(1)
            }
    }

    @Test
    fun `a member holding another database's store puts it in quarantine, and copies it once deleted`() {
        val cluster =
            ClusterHarness(root, 3, settings = mapOf(BoltConnector.enabled to true)).also {
                harness = it
            }
        cluster.start()
        cluster.administer("CREATE DATABASE films WAIT")
        cluster.administer("CREATE DATABASE books WAIT")
        cluster.write("films", 3)
        cluster.write("books", 30)
        cluster.awaitSame("films")
        cluster.awaitSame("books")
        val member = cluster.members.first { it != cluster.awaitLeader("films") }
        val foreignIndex = member.raftIndex("books")
        assertThat(foreignIndex).isGreaterThan(member.raftIndex("films"))
        member.stop()

        // As Neo4j names it, symbolic links resolved.
        val data = member.home.resolve("data").toRealPath()
        for (directory in listOf("databases", "transactions")) {
            FileUtils.deleteDirectory(data.resolve("$directory/films"))
            FileUtils.copyDirectory(
                data.resolve("$directory/books"),
                data.resolve("$directory/films"),
            )
        }
        member.start()
        // Without the member, which stops replicating films once it notices.
        val others = { cluster.running().filter { it != member } }
        // Until the group of films reaches the index of the books store.
        while (cluster.awaitLeader("films", others).raftIndex("films") <= foreignIndex) {
            cluster.write("films", 1, others)
        }

        val message =
            "The store of database `films` holds transactions that its Raft group does not " +
                "have: it was applied up to Raft index $foreignIndex, but the group's entry " +
                "there is a different one. Its files were probably put in place by hand, from " +
                "another cluster or database. To copy the database from the other members " +
                "instead, stop this server, delete ${data.resolve("databases/films")} and " +
                "${data.resolve("transactions/films")}, and start it again."
        await atMost
            AWAIT untilAsserted
            {
                val shown = member.shown("films")
                assertThat(shown).containsEntry("currentStatus", "quarantined")
                assertThat(shown["statusMessage"] as String)
                    .startsWith("By the server at ")
                    .contains("Database 'films' failed while running: $message")
            }
        // Drivers are sent to the others for films, whichever member they ask; it still routes.
        val refused = member.boltAddress().substringAfterLast(':')
        val serving = others().map { it.boltAddress().substringAfterLast(':') }
        await atMost
            AWAIT untilAsserted
            {
                for (router in cluster.running()) {
                    val table = router.routing("films")
                    assertThat(table["WRITE"].orEmpty() + table["READ"].orEmpty())
                        .describedAs("$router's routing table for films: $table")
                        .doesNotContain(refused)
                        .containsAll(serving)
                    assertThat(table["ROUTE"]).describedAs("$router").contains(refused)
                }
            }
        assertThat(catchThrowable { member.count("films") }).isNotNull()
        // The others keep serving it.
        cluster.write("films", 1, others)
        val expected = cluster.awaitLeader("films", others).count("films")

        member.stop()
        FileUtils.deleteDirectory(data.resolve("databases/films"))
        FileUtils.deleteDirectory(data.resolve("transactions/films"))
        member.start()
        // Copied again, it waits for its quarantine to be lifted.
        member.database().executeTransactionally("CALL dbms.quarantineDatabase('films', false)")

        await atMost
            AWAIT untilAsserted
            {
                assertThat(member.shown("films")["currentStatus"]).isEqualTo("online")
                assertThat(member.count("films")).isEqualTo(expected)
            }
        assertThat(member.count("books")).isEqualTo(30)
    }

    private companion object {
        val AWAIT: Duration = Duration.ofSeconds(60)
    }
}
