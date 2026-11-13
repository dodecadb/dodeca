package dev.dodeca.cluster.internal.admin

import dev.dodeca.cluster.DatabaseGroups
import dev.dodeca.cluster.MembershipFile
import dev.dodeca.cluster.internal.ClusterHarness
import java.nio.file.Files
import java.nio.file.Path
import java.util.ServiceLoader
import java.util.zip.ZipFile
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.cli.CommandProvider
import org.neo4j.cli.CommandType
import org.neo4j.storageengine.api.TransactionIdStore

/**
 * `neo4j-admin dbms unbind-system-db` as Neo4j Enterprise documents it: removes the server's Raft
 * state of `system` only, archived if asked, and keeps its store, whose last transaction it prints.
 */
class UnbindSystemCommandTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    @Test
    fun `removes the Raft state of system only, archived, and tells its last transaction`() {
        val cluster = ClusterHarness(root, 3).also { harness = it }
        cluster.start()
        cluster.awaitLeader().database().executeTransactionally("CREATE (:Film)")
        val member = cluster.members[2]
        val neo4j = member.database().databaseId().databaseId().uuid().toString()
        member.database("system").executeTransactionally("CALL db.checkpoint()")
        val last =
            member
                .database("system")
                .dependencyResolver
                .resolveDependency(TransactionIdStore::class.java)
                .lastCommittedTransactionId
        val serverId = member.serverId
        member.stop()
        val raft = member.home.resolve("data/raft")
        val archives = Files.createDirectories(root.resolve("archives"))

        val result =
            unbindSystemDb(member.home, "--archive-cluster-state=true", "--archive-path=$archives")

        assertThat(result.exitCode).describedAs("$result").isZero
        assertThat(result.out).contains(serverId).contains("lastCommittedTransaction $last")
        assertThat(raft.resolve(SYSTEM_GROUP)).doesNotExist()
        assertThat(raft.resolve(neo4j)).isDirectory()
        assertThat(member.home.resolve("data/databases/system")).isDirectory()
        assertThat(member.home.resolve("data/server_id")).exists()
        val record = MembershipFile(raft.resolve(MembershipFile.FILE_NAME)).read()!!
        assertThat(record.systemUnbound).isTrue()
        assertThat(record.memberId?.uuid?.toString()).isEqualTo(serverId)
        val archive = Files.list(archives).use { it.toList() }.single()
        assertThat(archive.fileName.toString())
            .matches("unbound_system_cluster_state\\.\\d{12}\\.zip")
        ZipFile(archive.toFile()).use { zip ->
            val names = zip.entries().asSequence().map { it.name }.toList()
            assertThat(names).contains("raft/membership")
            assertThat(names).anySatisfy { assertThat(it).startsWith("raft/$SYSTEM_GROUP/") }
            assertThat(names).noneSatisfy { assertThat(it).startsWith("raft/$neo4j/") }
        }

        // A run cut short is run again.
        assertThat(unbindSystemDb(member.home).exitCode).isZero
    }

    @Test
    fun `refuses while the server runs, and without cluster state`() {
        val cluster = ClusterHarness(root, 1).also { harness = it }
        cluster.start()

        val running = unbindSystemDb(cluster.members[0].home)

        assertThat(running.exitCode).describedAs("$running").isNotZero
        assertThat(running.err).contains("shut it down first")
        assertThat(cluster.members[0].home.resolve("data/raft/$SYSTEM_GROUP")).isDirectory()

        val none = unbindSystemDb(Files.createDirectories(root.resolve("standalone")))

        assertThat(none.exitCode).describedAs("$none").isNotZero
        assertThat(none.err).contains("there is nothing to unbind")
    }

    @Test
    fun `is a command of neo4j-admin dbms`() {
        assertThat(ServiceLoader.load(CommandProvider::class.java).map { it.commandType() })
            .contains(CommandType.UNBIND_SYSTEM)
        assertThat(CommandType.UNBIND_SYSTEM.commandGroup.name).isEqualTo("DBMS")
    }

    private companion object {
        val SYSTEM_GROUP = DatabaseGroups.SYSTEM_DATABASE.toString()
    }
}
