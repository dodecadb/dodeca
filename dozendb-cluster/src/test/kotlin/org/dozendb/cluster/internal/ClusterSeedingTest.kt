package org.dozendb.cluster.internal

import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.PrintStream
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
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
import org.neo4j.dbms.identity.ServerIdentity
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import picocli.CommandLine

/** CREATE DATABASE ... OPTIONS {seedURI | existingDataSeedServer | storeFormat} in a cluster. */
class ClusterSeedingTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    private fun cluster(): ClusterHarness =
        ClusterHarness(root, 3).also {
            harness = it
            it.start()
        }

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

    /** A full backup of `neo4j` with [nodes] `Seed` nodes, from a member of [cluster]. */
    private fun seed(cluster: ClusterHarness, nodes: Int): Path {
        cluster
            .awaitLeader()
            .database()
            .executeTransactionally("UNWIND range(1, $nodes) AS i CREATE (:Seed {i: i})")
        val backups = Files.createDirectories(root.resolve("backups"))
        val printed =
            admin(
                Files.createDirectories(root.resolve("client")),
                ::BackupCommand,
                "--from=127.0.0.1:${cluster.members.first().backupPort()}",
                "--to-path=$backups",
                "--type=FULL",
            )
        return Path.of(checkNotNull(Regex("to (\\S+\\.backup):").find(printed)).groupValues[1])
    }

    private fun ClusterHarness.administer(cypher: String): List<Map<String, Any?>> =
        awaitLeader("system").database("system").executeTransactionally(cypher, emptyMap()) {
            it.asSequence().map { row -> row.toMap() }.toList()
        }

    private fun ClusterHarness.Member.count(database: String, label: String): Long =
        database(database).executeTransactionally(
            "MATCH (n:$label) RETURN count(n) AS count",
            emptyMap(),
        ) {
            it.next()["count"] as Long
        }

    /** Every member holds [count] `Seed` nodes and takes new writes, which reach every member. */
    private fun ClusterHarness.assertSeeded(database: String, count: Long) {
        for (member in members) {
            assertThat(member.count(database, "Seed")).describedAs("$member").isEqualTo(count)
        }
        repeat(20) {
            awaitLeader(database).database(database).executeTransactionally("CREATE (:Later)")
        }
        await atMost
            AWAIT untilAsserted
            {
                for (member in members) {
                    assertThat(member.count(database, "Later")).describedAs("$member").isEqualTo(20)
                }
            }
        members.forEach { it.stop() }
        for (member in members) {
            assertThat(member.checkConsistency(database).isSuccessful)
                .describedAs("$member consistent")
                .isTrue
        }
    }

    @Test
    fun `one member creates a database from its seed, and the others copy it`() {
        val cluster = cluster()
        val seed = seed(cluster, 500)

        val waited =
            cluster.administer(
                "CREATE DATABASE films OPTIONS {existingData: 'use', seedURI: '${seed.toUri()}'} WAIT"
            )

        assertThat(waited.map { it["success"] }).describedAs("$waited").containsOnly(true)
        // A backup of a clustered database starts a new group: its writes are taken at once.
        cluster.assertSeeded("films", 500)
    }

    @Test
    fun `a member that restored a database creates it, and the others copy it`() {
        val cluster = cluster()
        val seed = seed(cluster, 300)
        val seeder = cluster.members.last()
        Files.createDirectories(seeder.home.resolve("conf"))
        Files.writeString(
            seeder.home.resolve("conf/neo4j.conf"),
            "dbms.dozendb.cluster.enabled=true\n",
        )
        val printed = admin(seeder.home, ::RestoreCommand, "--from-path=$seed", "films")
        val create =
            checkNotNull(Regex("CREATE DATABASE films OPTIONS \\{[^}]+\\}").find(printed)) {
                printed
            }

        val waited = cluster.administer("${create.value} WAIT")

        assertThat(waited.map { it["success"] }).describedAs("$waited").containsOnly(true)
        cluster.assertSeeded("films", 300)
    }

    @Test
    fun `a designated seeder that restored nothing fails the database, visibly`() {
        val cluster = cluster()
        val seeder = cluster.members.last()
        val id = seederId(seeder)

        val waited =
            cluster.administer(
                "CREATE DATABASE films OPTIONS {existingData: 'use', existingDataSeedServer: '$id'} " +
                    "WAIT 30 SECONDS"
            )

        assertThat(waited.map { it["success"] }).contains(false)
        assertThat(waited.map { it["message"] as String }).anyMatch {
            it.contains(
                "Unable to create `films` from the store restored on this server: there is " +
                    "none. Restore it here with 'neo4j-admin database restore' first"
            )
        }
    }

    @Test
    fun `the member that creates a new database creates its store in the format asked for`() {
        val cluster = cluster()

        val waited =
            cluster.administer("CREATE DATABASE films OPTIONS {storeFormat: 'standard'} WAIT")

        assertThat(waited.map { it["success"] }).describedAs("$waited").containsOnly(true)
        for (member in cluster.members) {
            val stores =
                member.database("system").executeTransactionally(
                    "SHOW DATABASE films YIELD store",
                    emptyMap(),
                ) {
                    it.asSequence().mapNotNull { row -> row["store"] as String? }.toList()
                }
            assertThat(stores).describedAs("$member").isNotEmpty.allMatch {
                it.contains("standard")
            }
        }
    }

    @Test
    fun `takes the seeding server by its deprecated option name too, with a warning`() {
        val cluster = cluster()
        val id = seederId(cluster.members.last())

        val warnings =
            cluster.awaitLeader("system").database("system").executeTransactionally(
                "CREATE DATABASE films OPTIONS {existingData: 'use', " +
                    "existingDataSeedInstance: '$id'}",
                emptyMap(),
            ) { result ->
                result.forEachRemaining {}
                result.gqlStatusObjects
                    .filter { it.gqlStatus().startsWith("01") }
                    .map { it.gqlStatus() to it.statusDescription() }
            }

        assertThat(warnings)
            .singleElement()
            .satisfies({
                assertThat(it.first).isEqualTo("01N01")
                assertThat(it.second)
                    .contains(
                        "existingDataSeedInstance is deprecated. It is replaced by existingDataSeedServer."
                    )
            })
        // The server named restored nothing to create the database from, which it says.
        await atMost
            AWAIT untilAsserted
            {
                assertThat(
                        cluster.administer("SHOW DATABASE films YIELD statusMessage").map {
                            it["statusMessage"] as String
                        }
                    )
                    .anyMatch {
                        it.contains("from the store restored on this server: there is none")
                    }
            }
    }

    @Test
    fun `refuses seeds it cannot use, when the database is created`() {
        val cluster = cluster()
        val failed = "Failed to create the specified database 'films'"
        val stranger = "6c0a1b1e-0000-4000-8000-000000000000"

        assertThat(
                catchThrowable {
                    cluster.administer(
                        "CREATE DATABASE films OPTIONS {existingDataSeedServer: '$stranger'}"
                    )
                }
            )
            .hasMessage(
                "$failed: server $stranger is not an enabled server of this cluster: SHOW " +
                    "SERVERS lists the servers."
            )
        assertThat(
                catchThrowable {
                    cluster.administer(
                        "CREATE DATABASE films OPTIONS {existingDataSeedInstance: '$stranger'}"
                    )
                }
            )
            .hasMessage(
                "$failed: server $stranger is not an enabled server of this cluster: SHOW " +
                    "SERVERS lists the servers."
            )
        assertThat(
                catchThrowable {
                    cluster.administer("CREATE DATABASE films OPTIONS {existingData: 'use'}")
                }
            )
            .hasMessage(
                "$failed: with clustering, existingData 'use' needs existingDataSeedServer, the " +
                    "server where 'neo4j-admin database restore' prepared the database, or a " +
                    "seedURI."
            )
    }

    private fun seederId(member: ClusterHarness.Member): String =
        member
            .database("system")
            .dependencyResolver
            .resolveDependency(ServerIdentity::class.java)
            .serverId()
            .uuid()
            .toString()

    private companion object {
        val AWAIT: Duration = Duration.ofSeconds(60)
    }
}
