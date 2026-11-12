package org.dozendb.cluster.internal.admin

import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.PrintStream
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.zip.ZipFile
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.dozendb.cluster.MemberState
import org.dozendb.cluster.internal.ClusterHarness
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.cli.ExecutionContext
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import picocli.CommandLine

/**
 * `neo4j-admin server unbind` as Neo4j Enterprise documents it: the server starts as a new server,
 * which is added, enabled, and takes the place of the old one once that is deallocated and dropped.
 */
class UnbindCommandTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    private class Result(val exitCode: Int, val out: String, val err: String) {
        override fun toString() = "exit code $exitCode\n$out\n$err"
    }

    /** Runs `neo4j-admin server unbind` for the server in [home]. */
    private fun unbind(home: Path, vararg arguments: String): Result {
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
        val exitCode = CommandLine(UnbindCommand(context)).execute(*arguments)
        return Result(exitCode, out.toString(), err.toString())
    }

    private fun ClusterHarness.Member.rows(cypher: String, database: String = SYSTEM) =
        database(database).executeTransactionally(cypher, emptyMap()) { result ->
            result.asSequence().map { it.toMap() }.toList()
        }

    private fun ClusterHarness.administer(members: List<ClusterHarness.Member>, cypher: String) =
        awaitLeader(SYSTEM) { members }.rows(cypher)

    private fun eventually(assertion: () -> Unit) {
        await withPollInterval
            Duration.ofMillis(100) atMost
            ClusterHarness.AWAIT untilAsserted
            {
                assertion()
            }
    }

    @Test
    fun `the server starts as a new one, which takes the old one's place, its files kept aside`() {
        val cluster = ClusterHarness(root, 3).also { harness = it }
        cluster.start()
        cluster.awaitLeader().database().executeTransactionally("CREATE (:Film)")
        val member = cluster.members[2]
        val others = cluster.members.take(2)
        val old = member.serverId
        // Its copies are set aside, and the copies it makes anew are in no quarantine.
        member
            .database()
            .executeTransactionally(
                "CYPHER 5 CALL dbms.quarantineDatabase('neo4j', true, 'Inspecting it')"
            )
        member.stop()
        val archives = Files.createDirectories(root.resolve("archives"))

        val result = unbind(member.home, "--archive-cluster-state", "--archive-path=$archives")

        assertThat(result.exitCode).describedAs("$result").isZero
        assertThat(result.out).contains(old)
        val data = member.home.resolve("data")
        assertThat(data.resolve("server_id")).doesNotExist()
        assertThat(data.resolve("raft")).doesNotExist()
        assertThat(data.resolve("quarantine")).doesNotExist()
        val aside = Files.list(data.resolve("unbound")).use { it.toList() }.single()
        for (path in listOf("databases/neo4j", "databases/system", "transactions/neo4j")) {
            assertThat(aside.resolve(path)).isDirectory()
        }
        assertThat(data.resolve("databases/neo4j")).doesNotExist()
        val archive = Files.list(archives).use { it.toList() }.single()
        assertThat(archive.fileName.toString()).matches("unbound_cluster_state\\.\\d{12}\\.zip")
        ZipFile(archive.toFile()).use { zip ->
            assertThat(zip.entries().asSequence().map { it.name }.toList())
                .contains("raft/membership", "server_id", "quarantine")
        }

        member.start()
        assertThat(member.serverId).isNotEqualTo(old)
        assertThat(member.cluster.state).isEqualTo(MemberState.JOINING)
        // Refused while the new server still starts.
        eventually {
            cluster.administer(
                others,
                "CALL dozendb.cluster.addMember('127.0.0.1:${member.raftPort}')",
            )
        }
        // The harness enables a server once it joined; DEALLOCATE needs it to place the copies.
        eventually {
            val state =
                cluster.administer(others, "SHOW SERVERS YIELD serverId, state").associate {
                    it["serverId"] to it["state"]
                }
            assertThat(state[member.serverId]).isEqualTo("Enabled")
        }
        cluster.administer(others, "DEALLOCATE DATABASES FROM SERVER '$old'")
        eventually {
            val state =
                cluster.administer(others, "SHOW SERVERS YIELD serverId, state").associate {
                    it["serverId"] to it["state"]
                }
            assertThat(state[old]).isEqualTo("Deallocated")
        }
        cluster.administer(others, "DROP SERVER '$old'")

        eventually {
            assertThat(
                    cluster.administer(others, "SHOW SERVERS YIELD serverId").map { it["serverId"] }
                )
                .containsExactlyInAnyOrderElementsOf(cluster.members.map { it.serverId })
            assertThat(member.rows("MATCH (f:Film) RETURN count(f) AS c", "neo4j"))
                .containsExactly(mapOf("c" to 1L))
        }
        // The cluster bears one failure again.
        others[0].stop()
        cluster
            .awaitLeader { listOf(others[1], member) }
            .database()
            .executeTransactionally("CREATE (:Film)")
    }

    @Test
    fun `refuses while the server runs`() {
        val cluster = ClusterHarness(root, 1).also { harness = it }
        cluster.start()

        val result = unbind(cluster.members[0].home)

        assertThat(result.exitCode).describedAs("$result").isNotZero
        assertThat(result.err).contains("shut it down first")
        assertThat(cluster.members[0].home.resolve("data/raft")).isDirectory()
    }

    @Test
    fun `a server without cluster state loses its id only`() {
        val home = Files.createDirectories(root.resolve("standalone"))
        val store = Files.createDirectories(home.resolve("data/databases/neo4j"))
        Files.writeString(home.resolve("data/server_id"), "id")

        val result = unbind(home)

        assertThat(result.exitCode).describedAs("$result").isZero
        assertThat(home.resolve("data/server_id")).doesNotExist()
        assertThat(store).isDirectory()
    }

    @Test
    fun `the archive needs both options`() {
        val home = Files.createDirectories(root.resolve("home"))

        for (arguments in
            listOf(arrayOf("--archive-cluster-state"), arrayOf("--archive-path=$root"))) {
            val result = unbind(home, *arguments)

            assertThat(result.exitCode).describedAs("$result").isNotZero
        }
    }

    private companion object {
        const val SYSTEM = "system"
    }
}
