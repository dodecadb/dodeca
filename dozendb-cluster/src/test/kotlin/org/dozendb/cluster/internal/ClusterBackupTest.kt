package org.dozendb.cluster.internal

import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.PrintStream
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import kotlin.concurrent.thread
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.dozendb.backup.cli.BackupCommand
import org.dozendb.backup.cli.RestoreCommand
import org.dozendb.core.DozenDbSettings
import org.dozendb.core.edition.LatestWrites
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.cli.ExecutionContext
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.consistency.ConsistencyCheckService
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder
import org.neo4j.dbms.systemgraph.InstanceModeConstraint
import org.neo4j.io.ByteUnit
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import org.neo4j.io.layout.Neo4jLayout
import org.neo4j.storageengine.api.TransactionIdStore
import picocli.CommandLine

/** Backups of a clustered database, from any member, as `neo4j-admin database backup` takes. */
class ClusterBackupTest {
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

    /** Runs a `neo4j-admin` command for the server in [home], in this JVM. */
    private fun admin(
        home: Path,
        command: (ExecutionContext) -> Any,
        vararg arguments: String,
    ): Result {
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
        return Result(exitCode, out.toString(), err.toString())
    }

    private val backups: Path by lazy { Files.createDirectories(root.resolve("backups")) }

    private fun backup(member: ClusterHarness.Member, vararg arguments: String): Path {
        val result =
            admin(
                Files.createDirectories(root.resolve("client")),
                ::BackupCommand,
                "--from=127.0.0.1:${member.backupPort()}",
                "--to-path=$backups",
                *arguments,
            )
        assertThat(result.exitCode).describedAs(result.toString()).isZero
        val written = Regex("to (\\S+\\.backup):").find(result.out)
        return Path.of(checkNotNull(written) { result.toString() }.groupValues[1])
    }

    /**
     * Restores the chain ending with [backup] on a Neo4j Community server and returns the highest
     * `n` of the `Written` nodes and their count; checks the store is consistent.
     */
    private fun restored(backup: Path): Pair<Long, Long> {
        val home = Files.createDirectories(root.resolve("restored-${backup.fileName}"))
        val restore = admin(home, ::RestoreCommand, "--from-path=$backup")
        assertThat(restore.exitCode).describedAs(restore.toString()).isZero
        val dbms =
            DatabaseManagementServiceBuilder(home)
                .setConfig(GraphDatabaseSettings.pagecache_memory, ByteUnit.mebiBytes(16))
                .setConfig(BoltConnector.enabled, false)
                .build()
        val written =
            try {
                dbms.database("neo4j").executeTransactionally(
                    "MATCH (w:Written) RETURN max(w.n) AS max, count(w) AS count",
                    emptyMap(),
                ) {
                    val row = it.next()
                    (row["max"] as Long) to (row["count"] as Long)
                }
            } finally {
                dbms.shutdown()
            }
        val config = Config.defaults(GraphDatabaseSettings.neo4j_home, home)
        val layout = Neo4jLayout.of(config).databaseLayout("neo4j")
        assertThat(
                ConsistencyCheckService(layout).with(config).runFullConsistencyCheck().isSuccessful
            )
            .describedAs("$backup restored consistently")
            .isTrue
        return written
    }

    @Test
    fun `a follower that applies late waits until it holds the writes acknowledged before`() {
        val fileSystems = mutableMapOf<Int, SlowLogFileSystem>()
        val cluster =
            ClusterHarness(
                    root,
                    3,
                    fileSystem = { fileSystems.getOrPut(it.index) { SlowLogFileSystem() } },
                )
                .also { harness = it }
        cluster.start()
        val leader = cluster.awaitLeader()
        val follower = (cluster.members - leader).first()
        val latestWrites =
            follower
                .database("system")
                .dependencyResolver
                .resolveDependency(LatestWrites::class.java)
        val database = follower.database().databaseId()
        val written = AtomicLong()
        /** Writes 100 nodes on the leader, from several threads. */
        fun write() {
            val writers =
                (1..4).map {
                    thread {
                        repeat(25) {
                            leader
                                .database()
                                .executeTransactionally(
                                    "CREATE (:Written {n: ${'$'}n})",
                                    mapOf("n" to written.getAndIncrement()),
                                )
                        }
                    }
                }
            writers.forEach { it.join() }
        }
        fileSystems.getValue(follower.index).slow = true
        repeat(3) { round ->
            write()
            val acknowledged = leader.lastClosed()
            assertThat(follower.lastClosed())
                .describedAs("round $round: the follower applies late")
                .isLessThan(acknowledged)

            latestWrites.await(database, Duration.ofSeconds(60))

            assertThat(follower.lastClosed())
                .describedAs("round $round: the follower holds what the leader acknowledged")
                .isGreaterThanOrEqualTo(acknowledged)
        }

        write()
        assertThat(follower.lastClosed()).isLessThan(leader.lastClosed())
        val (max, count) = restored(backup(follower, "--type=FULL"))
        assertThat(count).isEqualTo(written.get())
        assertThat(max).isEqualTo(written.get() - 1)
    }

    private fun ClusterHarness.Member.lastClosed(): Long =
        database()
            .dependencyResolver
            .resolveDependency(TransactionIdStore::class.java)
            .lastClosedTransactionId

    @Test
    fun `backups from any member hold every write acknowledged before them, and chain`() {
        val cluster = ClusterHarness(root, 3).also { harness = it }
        cluster.start()
        val leader = cluster.awaitLeader()
        val (first, second) = cluster.members - leader
        val acknowledged = AtomicLong(-1)
        val writing = AtomicBoolean(true)
        val failure = AtomicReference<Throwable>()
        val writer = thread {
            var n = 0L
            try {
                while (writing.get()) {
                    leader
                        .database()
                        .executeTransactionally("CREATE (:Written {n: ${'$'}n})", mapOf("n" to n))
                    acknowledged.set(n++)
                }
            } catch (e: Throwable) {
                failure.set(e)
            }
        }
        /** Waits for more writes, then backs up from [member]: the backup and the writes before. */
        fun backupUnderWrites(
            member: ClusterHarness.Member,
            vararg arguments: String,
        ): Pair<Path, Long> {
            val start = acknowledged.get()
            while (acknowledged.get() < start + 50 && failure.get() == null) Thread.sleep(5)
            val before = acknowledged.get()
            return backup(member, *arguments) to before
        }
        val backups =
            try {
                listOf(
                    backupUnderWrites(first, "--type=FULL"),
                    backupUnderWrites(second),
                    backupUnderWrites(leader),
                )
            } finally {
                writing.set(false)
                writer.join()
            }
        assertThat(failure.get()).isNull()

        for ((backup, before) in backups) {
            val (max, count) = restored(backup)
            assertThat(max).describedAs("the last write in $backup").isGreaterThanOrEqualTo(before)
            assertThat(count).describedAs("the writes in $backup").isEqualTo(max + 1)
        }
    }

    @Test
    fun `a backup from a secondary holds every write acknowledged before it`() {
        val cluster =
            ClusterHarness(
                    root,
                    3,
                    settings =
                        mapOf(
                            ClusterSettings.defaultSecondaries to 1,
                            // Only the backup makes it pull what it lacks.
                            ClusterSettings.pullInterval to Duration.ofHours(1),
                        ),
                )
                .also { harness = it }
        val secondary = cluster.members[2]
        secondary.settings =
            mapOf(DozenDbSettings.initialServerModeConstraint to InstanceModeConstraint.SECONDARY)
        cluster.start()
        await withPollInterval
            Duration.ofMillis(100) atMost
            ClusterHarness.AWAIT untilAsserted
            {
                assertThat(secondary.database().isAvailable).isTrue()
            }
        val primaries = cluster.members - secondary

        repeat(100) {
            cluster
                .awaitLeader { primaries }
                .database()
                .executeTransactionally("CREATE (:Written {n: ${'$'}n})", mapOf("n" to it.toLong()))
        }

        val (max, count) = restored(backup(secondary, "--type=FULL"))
        assertThat(count).isEqualTo(100)
        assertThat(max).isEqualTo(99)
    }

    @Test
    fun `a database created with WAIT is backed up at once, from any server that hosts it`() {
        val cluster = ClusterHarness(root, 3).also { harness = it }
        cluster.start()

        for ((index, member) in cluster.members.withIndex()) {
            cluster
                .awaitLeader(SYSTEM)
                .database(SYSTEM)
                .executeTransactionally("CREATE DATABASE films$index WAIT")
            backup(member, "films$index")
        }
    }

    @Test
    fun `a pattern backed up through a server that hosts none of them takes every database`() {
        val cluster =
            ClusterHarness(
                    root,
                    3,
                    settings = mapOf(ClusterSettings.autoEnableFreeServers to false),
                )
                .also { harness = it }
        cluster.start()
        val hosts = cluster.members.take(3)
        val leader = cluster.awaitLeader(SYSTEM) { hosts }
        leader.database(SYSTEM).executeTransactionally("CREATE DATABASE films WAIT")
        // A Free server, which hosts only system.
        val free = cluster.newMember().also { it.start() }
        leader
            .database(SYSTEM)
            .executeTransactionally("CALL dozendb.cluster.addMember('127.0.0.1:${free.raftPort}')")
        await withPollInterval
            Duration.ofMillis(100) atMost
            ClusterHarness.AWAIT untilAsserted
            {
                assertThat(
                        free.database(SYSTEM).executeTransactionally(
                            "SHOW DATABASE films YIELD name",
                            emptyMap(),
                        ) {
                            it.stream().count()
                        }
                    )
                    .isPositive
            }

        val result =
            admin(
                Files.createDirectories(root.resolve("client")),
                ::BackupCommand,
                "--from=127.0.0.1:${free.backupPort()},127.0.0.1:${hosts[0].backupPort()}",
                "--to-path=$backups",
                "*",
            )

        assertThat(result.exitCode).describedAs(result.toString()).isZero
        assertThat(
                Files.list(backups).use { files -> files.map { it.fileName.toString() }.toList() }
            )
            .anyMatch { it.startsWith("films-") }
            .anyMatch { it.startsWith("neo4j-") }
            .anyMatch { it.startsWith("system-") }
        assertThat(result.err).contains("database 'films' is not hosted on this server")
    }

    private companion object {
        const val SYSTEM = "system"
    }
}
