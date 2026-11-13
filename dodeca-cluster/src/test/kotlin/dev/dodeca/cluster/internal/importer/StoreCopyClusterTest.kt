package dev.dodeca.cluster.internal.importer

import dev.dodeca.cluster.internal.ClusterHarness
import dev.dodeca.core.DodecaSettings
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
import org.awaitility.kotlin.withPollInterval
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.cli.AdminTool
import org.neo4j.cli.ExecutionContext
import org.neo4j.dbms.systemgraph.InstanceModeConstraint
import org.neo4j.graphdb.config.Setting
import org.neo4j.io.fs.DefaultFileSystemAbstraction

/**
 * `neo4j-admin database copy` on a server of a cluster: a copy is a store of its own, which the
 * cluster takes as a seed, not as the state of the Raft group of the database it replaces.
 */
class StoreCopyClusterTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    /** [size] members; those from [secondariesFrom] on host only secondaries. */
    private fun cluster(size: Int = 3, secondariesFrom: Int = size): ClusterHarness =
        ClusterHarness(root, size).also {
            harness = it
            for (member in it.members) {
                if (member.index >= secondariesFrom) {
                    member.settings =
                        mapOf<Setting<*>, Any>(
                            DodecaSettings.initialServerModeConstraint to
                                InstanceModeConstraint.SECONDARY
                        )
                }
                // neo4j-admin reads the server's configuration from its conf directory.
                Files.createDirectories(member.home.resolve("conf"))
                Files.writeString(
                    member.home.resolve("conf/neo4j.conf"),
                    "dbms.dodeca.cluster.enabled=true\n",
                )
            }
            it.start()
        }

    private fun ClusterHarness.administer(cypher: String): List<Map<String, Any?>> =
        awaitLeader(SYSTEM).database(SYSTEM).executeTransactionally(cypher, emptyMap()) {
            it.asSequence().map { row -> row.toMap() }.toList()
        }

    private fun ClusterHarness.write(database: String, cypher: String) {
        awaitLeader(database) { members.filter { it.running && it.status(database) != null } }
            .database(database)
            .executeTransactionally(cypher)
    }

    private fun ClusterHarness.Member.count(database: String, label: String): Long =
        database(database).executeTransactionally(
            "MATCH (n:$label) RETURN count(n) AS c",
            emptyMap(),
        ) {
            it.next()["c"] as Long
        }

    private fun ClusterHarness.status(member: ClusterHarness.Member): Map<String, Any?> =
        administer(
                "SHOW DATABASE $FILMS YIELD serverID, currentStatus, statusMessage " +
                    "WHERE serverID = '${member.serverId}'"
            )
            .single()

    private fun ClusterHarness.hostOf(role: String): ClusterHarness.Member {
        val id =
            administer("SHOW DATABASE $FILMS YIELD serverID, role WHERE role = '$role'")
                .single()["serverID"]
        return members.single { it.serverId == id }
    }

    private fun eventually(assertion: () -> Unit) {
        // The database is missing on a server for a moment while it is recreated.
        await.ignoreExceptions() withPollInterval
            Duration.ofMillis(200) atMost
            ClusterHarness.AWAIT untilAsserted
            {
                assertion()
            }
    }

    private class Result(val exitCode: Int, val out: String, val err: String) {
        override fun toString() = "exit code $exitCode\n$out\n$err"
    }

    /** Copies films to [to] on [member], leaving out its Temp nodes. */
    private fun copy(member: ClusterHarness.Member, to: String): Result {
        val out = ByteArrayOutputStream()
        val err = ByteArrayOutputStream()
        val context =
            ExecutionContext(
                member.home,
                member.home.resolve("conf"),
                PrintStream(out, true),
                PrintStream(err, true),
                InputStream.nullInputStream(),
                DefaultFileSystemAbstraction(),
            )
        val exitCode =
            AdminTool.execute(
                context,
                "database",
                "copy",
                "--to-path-schema=$root",
                "--ignore-nodes-with-labels=Temp",
                FILMS,
                to,
            )
        return Result(exitCode, out.toString(), err.toString()).also {
            assertThat(it.exitCode).describedAs(it.toString()).isZero
        }
    }

    private fun ClusterHarness.Member.marker(name: String): Path =
        home.resolve("data/databases/.$name.copied")

    /** Films with two persons and a Temp node on every server hosting it, stopped. */
    private fun ClusterHarness.films(create: String, hosts: List<ClusterHarness.Member>) {
        administer(create)
        write(FILMS, "CREATE (:Person {name: 'a'}), (:Person {name: 'b'}), (:Temp)")
        eventually { hosts.forEach { assertThat(it.count(FILMS, "Person")).isEqualTo(2) } }
        administer("STOP DATABASE $FILMS WAIT")
    }

    @Test
    fun `a copy onto the database's only primary is refused there until the database is recreated from that server`() {
        val cluster = cluster(secondariesFrom = 2)
        cluster.films(
            "CREATE DATABASE $FILMS TOPOLOGY 1 PRIMARY 1 SECONDARY WAIT",
            listOf(cluster.members[2]),
        )
        val primary = cluster.hostOf("primary")
        val secondary = cluster.hostOf("secondary")
        copy(primary, FILMS)
        assertThat(primary.marker(FILMS)).exists()

        cluster.administer("START DATABASE $FILMS")

        eventually {
            val status = cluster.status(primary)
            assertThat(status["currentStatus"]).isNotEqualTo("online")
            assertThat(status["statusMessage"] as String)
                .contains(
                    "neo4j-admin database copy replaced the store of database '$FILMS' on this " +
                        "server"
                )
                .contains(
                    "CALL dbms.recreateDatabase('$FILMS', {seedingServers: " +
                        "['${primary.serverId}']})"
                )
        }

        cluster.administer(
            "CALL dbms.recreateDatabase('$FILMS', {seedingServers: ['${primary.serverId}']})"
        )

        eventually {
            for (member in listOf(primary, secondary)) {
                assertThat(member.count(FILMS, "Person")).describedAs("$member").isEqualTo(2)
                assertThat(member.count(FILMS, "Temp")).describedAs("$member").isZero
            }
        }
        assertThat(primary.marker(FILMS)).doesNotExist()
        cluster.write(FILMS, "CREATE (:Person {name: 'c'})")
        eventually { assertThat(secondary.count(FILMS, "Person")).isEqualTo(3) }
        cluster.members.forEach { it.stop() }
        for (member in listOf(primary, secondary)) {
            assertThat(member.checkConsistency(FILMS).isSuccessful)
                .describedAs("$member consistent")
                .isTrue
        }
    }

    @Test
    fun `a copy onto a secondary is refused there`() {
        val cluster = cluster(secondariesFrom = 2)
        cluster.films(
            "CREATE DATABASE $FILMS TOPOLOGY 1 PRIMARY 1 SECONDARY WAIT",
            listOf(cluster.members[2]),
        )
        val secondary = cluster.hostOf("secondary")
        copy(secondary, FILMS)

        cluster.administer("START DATABASE $FILMS")

        eventually {
            val status = cluster.status(secondary)
            assertThat(status["currentStatus"]).isNotEqualTo("online")
            assertThat(status["statusMessage"] as String)
                .contains(
                    "neo4j-admin database copy replaced the store of database '$FILMS' on this " +
                        "server, which hosts a secondary of it"
                )
        }
        assertThat(cluster.status(cluster.hostOf("primary"))["currentStatus"]).isEqualTo("online")
    }

    @Test
    fun `a copy to a new name seeds the database created from it on every server`() {
        val cluster = cluster()
        cluster.films("CREATE DATABASE $FILMS TOPOLOGY 3 PRIMARIES WAIT", cluster.members)
        val seed = cluster.members[0]
        val result = copy(seed, COPY)
        assertThat(result.out)
            .contains(
                "CREATE DATABASE $COPY OPTIONS {existingData: 'use', existingDataSeedServer: " +
                    "'${seed.serverId}'}"
            )
        assertThat(seed.home.resolve("data/databases/$COPY")).doesNotExist()

        cluster.administer(
            "CREATE DATABASE $COPY TOPOLOGY 3 PRIMARIES OPTIONS {existingData: 'use', " +
                "existingDataSeedServer: '${seed.serverId}'} WAIT"
        )

        eventually {
            for (member in cluster.members) {
                assertThat(member.count(COPY, "Person")).describedAs("$member").isEqualTo(2)
                assertThat(member.count(COPY, "Temp")).describedAs("$member").isZero
            }
        }
        assertThat(seed.home.resolve("data/databases/.dodeca-seed-$COPY")).doesNotExist()
        cluster.write(COPY, "CREATE (:Person {name: 'c'})")
        eventually {
            cluster.members.forEach { assertThat(it.count(COPY, "Person")).isEqualTo(3) }
        }
        cluster.members.forEach { it.stop() }
        for (member in cluster.members) {
            assertThat(member.checkConsistency(COPY).isSuccessful)
                .describedAs("$member consistent")
                .isTrue
        }
    }

    private companion object {
        const val FILMS = "films"
        const val COPY = "films2"
        const val SYSTEM = "system"
    }
}
