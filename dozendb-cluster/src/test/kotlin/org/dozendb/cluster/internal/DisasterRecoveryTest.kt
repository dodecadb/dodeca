package org.dozendb.cluster.internal

import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.PrintStream
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.dozendb.backup.cli.BackupCommand
import org.dozendb.backup.cli.RestoreCommand
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.cli.ExecutionContext
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import org.neo4j.storageengine.api.TransactionIdStore
import picocli.CommandLine

/** A cluster lost, restored from its backups on one server, which forms a new cluster. */
class DisasterRecoveryTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private val clusters = mutableListOf<ClusterHarness>()

    @AfterEach
    fun stop() {
        clusters.forEach { it.close() }
    }

    private fun cluster(name: String, defaultPrimaries: Int? = null): ClusterHarness =
        ClusterHarness(
                root.resolve(name),
                3,
                settings =
                    defaultPrimaries?.let { mapOf(ClusterSettings.defaultPrimaries to it) }
                        ?: emptyMap(),
            )
            .also { clusters += it }

    /** Runs a `neo4j-admin` command for the server in [home] and returns what it printed. */
    private fun admin(
        home: Path,
        command: (ExecutionContext) -> Any,
        vararg arguments: String,
    ): String {
        val out = ByteArrayOutputStream()
        val err = ByteArrayOutputStream()
        val context =
            ExecutionContext(
                home,
                home.resolve("conf"),
                PrintStream(out, true),
                PrintStream(err, true),
                InputStream.nullInputStream(),
                DefaultFileSystemAbstraction(),
            )
        val exitCode = CommandLine(command(context)).execute(*arguments)
        assertThat(exitCode).describedAs("$out\n$err").isZero
        return out.toString()
    }

    /** Backs up every database of [cluster], each from a member that hosts it; returns where. */
    private fun backUp(cluster: ClusterHarness): Path {
        val backups = Files.createDirectories(root.resolve("backups"))
        admin(
            Files.createDirectories(root.resolve("client")),
            ::BackupCommand,
            "--from=${cluster.members.joinToString(",") { "127.0.0.1:${it.backupPort()}" }}",
            "--to-path=$backups",
            "--type=FULL",
            "*",
        )
        return backups
    }

    private fun ClusterHarness.administer(cypher: String): List<Map<String, Any?>> =
        awaitLeader("system").database("system").executeTransactionally(cypher, emptyMap()) {
            it.asSequence().map { row -> row.toMap() }.toList()
        }

    /** Runs [cypher] on the leader of [database], among the members that run it. */
    private fun ClusterHarness.write(database: String, cypher: String) {
        awaitLeader(database) { running().filter { it.status(database) != null } }
            .database(database)
            .executeTransactionally(cypher)
    }

    private fun ClusterHarness.Member.count(database: String, label: String): Long =
        database(database).executeTransactionally(
            "MATCH (n:$label) RETURN count(n) AS count",
            emptyMap(),
        ) {
            it.next()["count"] as Long
        }

    private fun ClusterHarness.Member.lastTransactionId(database: String): Long =
        database(database)
            .dependencyResolver
            .resolveDependency(TransactionIdStore::class.java)
            .lastCommittedTransactionId

    private fun ClusterHarness.Member.rows(cypher: String): List<Map<String, Any?>> =
        database("system").executeTransactionally(cypher, emptyMap()) {
            it.asSequence().map { row -> row.toMap() }.toList()
        }

    @Test
    fun `every database restored on one server reaches every member of the cluster it forms`() {
        val lost = cluster("lost")
        lost.start()
        lost.administer("CREATE DATABASE films WAIT")
        lost.administer("CREATE DATABASE books WAIT")
        lost.administer("CREATE USER alice SET PASSWORD 'a long password' CHANGE NOT REQUIRED")
        lost.write("neo4j", "UNWIND range(1, 100) AS i CREATE (:Thing {i: i})")
        lost.write("films", "UNWIND range(1, 200) AS i CREATE (:Film {i: i})")
        lost.write("books", "UNWIND range(1, 300) AS i CREATE (:Book {i: i})")
        val backups = backUp(lost)
        lost.members.forEach { it.stop() }

        val recovered = cluster("recovered")
        val founder = recovered.members.first()
        admin(founder.home, ::RestoreCommand, "--from-path=$backups")
        recovered.start()

        val expected =
            mapOf(
                "neo4j" to ("Thing" to 100L),
                "films" to ("Film" to 200L),
                "books" to ("Book" to 300L),
            )
        await atMost
            AWAIT untilAsserted
            {
                for (member in recovered.members) {
                    val self = member.cluster.self.id.uuid.toString()
                    val online =
                        member
                            .rows("SHOW DATABASES YIELD name, serverID, currentStatus")
                            .filter { it["serverID"] == self }
                            .associate { it["name"] to it["currentStatus"] }
                    assertThat(online)
                        .describedAs("$member")
                        .containsEntry("films", "online")
                        .containsEntry("books", "online")
                    for ((database, nodes) in expected) {
                        assertThat(member.count(database, nodes.first))
                            .describedAs("$database on $member")
                            .isEqualTo(nodes.second)
                    }
                }
            }
        for (member in recovered.members) {
            assertThat(member.rows("SHOW USERS YIELD user").map { it["user"] })
                .describedAs("$member")
                .contains("alice")
        }
        // Writable, and the writes reach every member, with the same transactions.
        for ((database, nodes) in expected) {
            recovered.write(database, "CREATE (:${nodes.first} {i: 0})")
        }
        await atMost
            AWAIT untilAsserted
            {
                for ((database, nodes) in expected) {
                    val transactions = recovered.members.map { it.lastTransactionId(database) }
                    assertThat(transactions.distinct())
                        .describedAs("$database: $transactions")
                        .hasSize(1)
                    for (member in recovered.members) {
                        assertThat(member.count(database, nodes.first)).isEqualTo(nodes.second + 1)
                    }
                }
            }
        recovered.members.forEach { it.stop() }
        for (member in recovered.members) {
            for (database in expected.keys + "system") {
                assertThat(member.checkConsistency(database).isSuccessful)
                    .describedAs("$database on $member consistent")
                    .isTrue
            }
        }
    }

    @Test
    fun `a restored database keeps its number of primaries, or takes the default if larger`() {
        // One primary by default, as in Neo4j Enterprise.
        val lost = cluster("lost", defaultPrimaries = 1)
        lost.start()
        // Placed once the cluster recorded its servers.
        await atMost
            AWAIT untilAsserted
            {
                assertThat(lost.administer("SHOW DATABASE neo4j YIELD currentStatus"))
                    .containsExactly(mapOf("currentStatus" to "online"))
            }
        lost.administer("CREATE DATABASE films TOPOLOGY 3 PRIMARIES WAIT")
        lost.administer("CREATE DATABASE books WAIT")
        lost.administer("CALL dbms.setDefaultAllocationNumbers(2, 0)")
        // Their groups serve once they took a write, which WAIT does not wait for.
        for (database in listOf("neo4j", "films", "books")) lost.write(database, "CREATE ()")
        val backups = backUp(lost)
        lost.members.forEach { it.stop() }

        val recovered = cluster("recovered", defaultPrimaries = 1)
        admin(recovered.members.first().home, ::RestoreCommand, "--from-path=$backups")
        recovered.start()

        // The default database and books had one primary, fewer than the restored default.
        val expected = mapOf("films" to 3L, "books" to 2L, "neo4j" to 2L)
        await atMost
            AWAIT untilAsserted
            {
                val rows =
                    recovered.administer(
                        "SHOW DATABASES YIELD name, currentStatus, requestedPrimariesCount"
                    )
                for ((database, primaries) in expected) {
                    val copies = rows.filter { it["name"] == database }
                    assertThat(copies.map { it["requestedPrimariesCount"] })
                        .describedAs(database)
                        .containsOnly(primaries)
                    assertThat(copies.map { it["currentStatus"] })
                        .describedAs(database)
                        .hasSize(primaries.toInt())
                        .containsOnly("online")
                }
            }
    }

    private companion object {
        val AWAIT: Duration = Duration.ofSeconds(90)
    }
}
