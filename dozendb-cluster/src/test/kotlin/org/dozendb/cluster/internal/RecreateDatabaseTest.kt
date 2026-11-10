package org.dozendb.cluster.internal

import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.dozendb.backup.cli.BackupCommand
import org.dozendb.cluster.internal.ClusterHarness.Companion.AWAIT
import org.dozendb.cluster.internal.admin.CommandResult
import org.dozendb.core.DozenDbSettings
import org.dozendb.core.security.PrivilegeRecords
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.cli.ExecutionContext
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import picocli.CommandLine

/**
 * `dbms.cluster.recreateDatabase`, as Neo4j Enterprise documents it: a database, also one that lost
 * a majority of its primaries, is recreated from a seed under a new id, keeping its name and
 * privileges.
 */
class RecreateDatabaseTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    @Test
    fun `a database recreated from a backup gets a new id, keeps its privileges, and takes writes`() {
        val cluster = ClusterHarness(root, 3).also { harness = it }
        cluster.start()
        cluster.write(FILMS, 3)
        cluster.administer("CREATE ROLE viewer")
        cluster.administer("GRANT MATCH {*} ON GRAPH $FILMS TO viewer")
        val seed = cluster.backup(cluster.members[0])
        cluster.write(FILMS, 2)
        val old = cluster.members[0].databaseId(FILMS)
        val privileges = cluster.privileges("viewer")

        cluster.administer(
            "CALL dbms.cluster.recreateDatabase('$FILMS', {seedURI: '${seed.toUri()}'})"
        )

        cluster.awaitFilms(cluster.members, 3)
        val new = cluster.members[0].databaseId(FILMS)
        assertThat(new).isNotEqualTo(old)
        assertThat(cluster.members.map { it.databaseId(FILMS) }).containsOnly(new)
        assertThat(cluster.privileges("viewer")).isEqualTo(privileges)
        cluster.awaitLeader(SYSTEM).database(SYSTEM).beginTx().use { tx ->
            val databases =
                tx.findNodes(PrivilegeRecords.PRIVILEGE).use { nodes ->
                    nodes
                        .asSequence()
                        .mapNotNull { it.getProperty(PrivilegeRecords.DATABASE, null) }
                        .toList()
                }
            assertThat(databases).contains(new.toString()).doesNotContain(old.toString())
        }
        // The old copies and their Raft group are gone everywhere.
        await atMost
            AWAIT untilAsserted
            {
                for (member in cluster.members) {
                    assertThat(member.home.resolve("data/raft/$old"))
                        .describedAs("$member")
                        .doesNotExist()
                }
            }
        cluster.write(FILMS, 4)
        cluster.awaitFilms(cluster.members, 7)
    }

    @Test
    fun `a database that lost a majority of its primaries is recreated from a backup on the servers left`() {
        val cluster =
            ClusterHarness(root, 5, settings = mapOf(ClusterSettings.defaultPrimaries to 3)).also {
                harness = it
            }
        // films goes to servers 0 to 2.
        cluster.members.drop(3).forEach {
            it.settings = mapOf(DozenDbSettings.initialServerDeniedDatabases to listOf(FILMS))
        }
        cluster.start()
        val hosts = cluster.members.take(3)
        cluster.write(FILMS, 3, hosts)
        val seed = cluster.backup(hosts[2])
        val lost = hosts.take(2)
        val lostIds = lost.map { it.serverId }
        lost.forEach { it.stop() }
        val left = cluster.members.drop(2)
        for (id in lostIds) {
            cluster.administer("CALL dbms.cluster.cordonServer('$id')", among = left)
        }
        // The servers that may host it now.
        cluster.members.drop(3).forEach {
            cluster.administer(
                "ALTER SERVER '${it.serverId}' SET OPTIONS {deniedDatabases: []}",
                among = left,
            )
        }

        cluster.administer(
            "CALL dbms.cluster.recreateDatabase('$FILMS', {seedURI: '${seed.toUri()}'})",
            among = left,
        )

        cluster.awaitFilms(left, 3)
        cluster.write(FILMS, 2, left)
        cluster.awaitFilms(left, 5)
    }

    @Test
    fun `recreating refuses unknown databases and options, and other options than Enterprise's`() {
        val cluster = ClusterHarness(root, 1).also { harness = it }
        cluster.start()
        cluster.write(FILMS, 1)
        cluster.administer("CREATE ALIAS movies FOR DATABASE $FILMS")
        val seed = "file:///nowhere/films.backup"

        fun refusal(database: String, options: String): String? = catchThrowable {
            cluster.administer("CALL dbms.cluster.recreateDatabase('$database', $options)")
        }
            ?.let {
                generateSequence(it) { e -> e.cause }
                    .joinToString(" | ") { e -> e.message.orEmpty() }
            }

        assertThat(refusal(FILMS, "{}")).contains("give seedingServers")
        assertThat(refusal(FILMS, "{seedURI: '$seed', colour: 'red'}"))
            .contains("unknown option 'colour'")
        assertThat(refusal(FILMS, "{seedURI: '$seed', seedingServers: ['a']}"))
            .contains("seedingServers must be empty")
        assertThat(refusal(FILMS, "{seedURI: '$seed', secondaries: 1}")).contains("needs primaries")
        assertThat(refusal(FILMS, "{seedURI: '$seed', primaries: 12}")).contains("not 1 to 11")
        assertThat(refusal(FILMS, "{seedingServers: []}")).contains("not supported yet")
        assertThat(refusal("system", "{seedURI: '$seed'}")).contains("unbind-system-db")
        assertThat(refusal("movies", "{seedURI: '$seed'}")).contains("it is an alias")
        assertThat(refusal("series", "{seedURI: '$seed'}")).contains("it does not exist")
        assertThat(cluster.members[0].count(FILMS)).isEqualTo(1)
    }

    /** Creates [FILMS] on [among] if needed and writes [count] films, as its leader. */
    private fun ClusterHarness.write(
        database: String,
        count: Int,
        among: List<ClusterHarness.Member> = members,
    ) {
        administer("CREATE DATABASE $database IF NOT EXISTS WAIT", among = among)
        awaitLeader(database) { among }
            .database(database)
            .executeTransactionally("UNWIND range(1, $count) AS i CREATE (:Film {i: i})")
    }

    /** Waits until each of [members] holds [count] films. */
    private fun ClusterHarness.awaitFilms(members: List<ClusterHarness.Member>, count: Long) {
        await atMost
            AWAIT untilAsserted
            {
                for (member in members) {
                    assertThat(runCatching { member.count(FILMS) }.getOrNull())
                        .describedAs("$member")
                        .isEqualTo(count)
                }
            }
    }

    private fun ClusterHarness.Member.count(database: String): Long =
        database(database).executeTransactionally(
            "MATCH (n:Film) RETURN count(n) AS count",
            emptyMap(),
        ) {
            it.next()["count"] as Long
        }

    private fun ClusterHarness.Member.databaseId(name: String): UUID =
        database(name).databaseId().databaseId().uuid()

    private fun ClusterHarness.privileges(role: String): List<Map<String, Any?>> =
        awaitLeader(SYSTEM).database(SYSTEM).executeTransactionally(
            "SHOW ROLE $role PRIVILEGES YIELD *",
            emptyMap(),
        ) {
            it.asSequence().map { row -> row.toMap() }.toList()
        }

    private fun ClusterHarness.administer(
        command: String,
        among: List<ClusterHarness.Member> = members,
    ) {
        awaitLeader(SYSTEM) { among }.database(SYSTEM).executeTransactionally(command)
    }

    /** A full backup of [FILMS] from [member]. */
    private fun ClusterHarness.backup(member: ClusterHarness.Member): Path {
        val backups = Files.createDirectories(root.resolve("backups"))
        val printed =
            admin(
                Files.createDirectories(root.resolve("client")),
                "--from=127.0.0.1:${member.backupPort()}",
                FILMS,
                "--to-path=$backups",
                "--type=FULL",
            )
        return Path.of(checkNotNull(Regex("to (\\S+\\.backup):").find(printed.out)).groupValues[1])
    }

    private fun admin(home: Path, vararg arguments: String): CommandResult {
        val out = java.io.ByteArrayOutputStream()
        val err = java.io.ByteArrayOutputStream()
        val context =
            ExecutionContext(
                home,
                home.resolve("conf"),
                java.io.PrintStream(out, true),
                java.io.PrintStream(err, true),
                java.io.InputStream.nullInputStream(),
                DefaultFileSystemAbstraction(),
            )
        val exitCode = CommandLine(BackupCommand(context)).execute(*arguments)
        val result = CommandResult(exitCode, out.toString(), err.toString())
        assertThat(exitCode).describedAs("$result").isZero
        return result
    }

    private companion object {
        const val SYSTEM = "system"
        const val FILMS = "films"
    }
}
