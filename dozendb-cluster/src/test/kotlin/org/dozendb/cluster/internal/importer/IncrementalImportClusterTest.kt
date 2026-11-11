package org.dozendb.cluster.internal.importer

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
import org.dozendb.cluster.internal.ClusterHarness
import org.dozendb.core.DozenDbSettings
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
 * `neo4j-admin database import incremental` on a server of a cluster, by Neo4j Enterprise's
 * procedure (the database's only primary is the server imported on) and against it.
 */
class IncrementalImportClusterTest {
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
                            DozenDbSettings.initialServerModeConstraint to
                                InstanceModeConstraint.SECONDARY
                        )
                }
                // neo4j-admin reads the server's configuration from its conf directory.
                Files.createDirectories(member.home.resolve("conf"))
                Files.writeString(
                    member.home.resolve("conf/neo4j.conf"),
                    "dbms.dozendb.cluster.enabled=true\n",
                )
            }
            it.start()
        }

    private fun ClusterHarness.administer(cypher: String): List<Map<String, Any?>> =
        awaitLeader(SYSTEM).database(SYSTEM).executeTransactionally(cypher, emptyMap()) {
            it.asSequence().map { row -> row.toMap() }.toList()
        }

    private fun ClusterHarness.write(cypher: String) {
        awaitLeader(FILMS) { members.filter { it.running && it.status(FILMS) != null } }
            .database(FILMS)
            .executeTransactionally(cypher)
    }

    private fun ClusterHarness.Member.persons(): Long =
        database(FILMS).executeTransactionally(
            "MATCH (p:Person) RETURN count(p) AS c",
            emptyMap(),
        ) {
            it.next()["c"] as Long
        }

    /** The status of films on [member], as `system` shows it. */
    private fun ClusterHarness.status(member: ClusterHarness.Member): Map<String, Any?> =
        administer(
                "SHOW DATABASE $FILMS YIELD serverID, role, currentStatus, statusMessage " +
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

    /** Imports two persons, and a relationship to an existing one, into films on [member]. */
    private fun import(member: ClusterHarness.Member): Result {
        fun csv(name: String, vararg lines: String): String {
            val file = root.resolve(name)
            Files.write(file, lines.toList())
            return file.toString()
        }
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
                "import",
                "incremental",
                "--force",
                "--report-file=${root.resolve("import.report")}",
                FILMS,
                "--nodes=" + csv("persons.csv", "uuid:ID{label:Person}", "c", "d"),
                "--relationships=" + csv("knows.csv", ":START_ID,:END_ID,:TYPE", "a,c,KNOWS"),
            )
        return Result(exitCode, out.toString(), err.toString())
    }

    private fun ClusterHarness.Member.marker(): Path =
        home.resolve("data/databases/.$FILMS.imported")

    /** Films with persons a and b on every server hosting it, stopped. */
    private fun ClusterHarness.films(create: String, hosts: List<ClusterHarness.Member>) {
        administer(create)
        write("CREATE CONSTRAINT FOR (p:Person) REQUIRE p.uuid IS UNIQUE")
        write("CREATE (:Person {uuid: 'a'}), (:Person {uuid: 'b'})")
        eventually { hosts.forEach { assertThat(it.persons()).describedAs("$it").isEqualTo(2) } }
        administer("STOP DATABASE $FILMS WAIT")
    }

    @Test
    fun `an import on one of several primaries is refused there until the database is recreated from that server`() {
        val cluster = cluster()
        cluster.films("CREATE DATABASE $FILMS TOPOLOGY 3 PRIMARIES WAIT", cluster.members)
        val imported = cluster.members[0]
        val result = import(imported)
        assertThat(result.exitCode).describedAs(result.toString()).isZero
        assertThat(imported.marker()).exists()

        cluster.administer("START DATABASE $FILMS")

        eventually {
            val status = cluster.status(imported)
            assertThat(status["currentStatus"]).isNotEqualTo("online")
            assertThat(status["statusMessage"] as String)
                .contains(
                    "An incremental import merged into database '$FILMS' on this server while 3 " +
                        "servers host it as primaries"
                )
                .contains(
                    "CALL dbms.cluster.recreateDatabase('$FILMS', {seedingServers: " +
                        "['${imported.serverId}']})"
                )
        }
        assertThat(imported.marker()).exists()

        cluster.administer(
            "CALL dbms.cluster.recreateDatabase('$FILMS', {seedingServers: ['${imported.serverId}']})"
        )

        eventually {
            cluster.members.forEach { assertThat(it.persons()).describedAs("$it").isEqualTo(4) }
        }
        assertThat(imported.marker()).doesNotExist()
        cluster.write("CREATE (:Person {uuid: 'e'})")
        eventually {
            cluster.members.forEach { assertThat(it.persons()).describedAs("$it").isEqualTo(5) }
        }
        cluster.members.forEach { it.stop() }
        for (member in cluster.members) {
            assertThat(member.checkConsistency(FILMS).isSuccessful)
                .describedAs("$member consistent")
                .isTrue
        }
    }

    @Test
    fun `an import on the database's only primary is taken, and its secondary pulls it`() {
        val cluster = cluster(secondariesFrom = 2)
        cluster.films(
            "CREATE DATABASE $FILMS TOPOLOGY 1 PRIMARY 1 SECONDARY WAIT",
            listOf(cluster.members[2]),
        )
        val primary = cluster.hostOf("primary")
        val secondary = cluster.hostOf("secondary")
        val result = import(primary)
        assertThat(result.exitCode).describedAs(result.toString()).isZero

        cluster.administer("START DATABASE $FILMS WAIT")

        assertThat(primary.persons()).isEqualTo(4)
        assertThat(primary.marker()).doesNotExist()
        eventually { assertThat(secondary.persons()).isEqualTo(4) }
        cluster.write("CREATE (:Person {uuid: 'e'})")
        eventually { assertThat(secondary.persons()).isEqualTo(5) }
        cluster.members.forEach { it.stop() }
        for (member in listOf(primary, secondary)) {
            assertThat(member.checkConsistency(FILMS).isSuccessful)
                .describedAs("$member consistent")
                .isTrue
        }
    }

    @Test
    fun `an import on a secondary is refused there`() {
        val cluster = cluster(secondariesFrom = 2)
        cluster.films(
            "CREATE DATABASE $FILMS TOPOLOGY 1 PRIMARY 1 SECONDARY WAIT",
            listOf(cluster.members[2]),
        )
        val secondary = cluster.hostOf("secondary")
        val result = import(secondary)
        assertThat(result.exitCode).describedAs(result.toString()).isZero

        cluster.administer("START DATABASE $FILMS")

        eventually {
            val status = cluster.status(secondary)
            assertThat(status["currentStatus"]).isNotEqualTo("online")
            assertThat(status["statusMessage"] as String)
                .contains(
                    "An incremental import merged into database '$FILMS' on this server, which " +
                        "hosts a secondary of it"
                )
        }
        assertThat(cluster.status(cluster.hostOf("primary"))["currentStatus"]).isEqualTo("online")
    }

    private companion object {
        const val FILMS = "films"
        const val SYSTEM = "system"
    }
}
