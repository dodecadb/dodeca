package org.dozendb.backup.cli

import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.dozendb.backup.Admin
import org.dozendb.backup.Artifact
import org.dozendb.backup.ArtifactChains
import org.dozendb.backup.BackupSettings
import org.dozendb.backup.CommunityServer
import org.dozendb.consensus.testing.TestAuthority
import org.dozendb.consensus.testing.TestPorts
import org.dozendb.core.StandaloneServer
import org.dozendb.core.pagecache.PageCacheWarmup
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.ConnectorPortRegister
import org.neo4j.configuration.connectors.ConnectorType
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.configuration.ssl.SslPolicyConfig
import org.neo4j.configuration.ssl.SslPolicyScope
import org.neo4j.graphdb.config.Setting
import org.neo4j.io.ByteUnit
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import org.neo4j.wal.checkpoint.CheckPointer
import org.neo4j.wal.checkpoint.SimpleTriggerInfo

class BackupCommandTest {
    // Kept when a test fails, for the servers' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private val fs = DefaultFileSystemAbstraction()
    private val running = mutableListOf<AutoCloseable>()
    private val policy = SslPolicyConfig.forScope(SslPolicyScope.BACKUP)

    @AfterEach
    fun stop() {
        running.reversed().forEach { it.close() }
    }

    private fun source(vararg settings: Pair<Setting<*>, Any>): StandaloneServer =
        StandaloneServer(
                root.resolve("source"),
                mapOf(BackupSettings.listenAddress to SocketAddress("127.0.0.1", 0)) + settings,
            )
            .also { running += it }

    private val StandaloneServer.backupPort: Int
        get() =
            database(StandaloneServer.SYSTEM)
                .dependencyResolver
                .resolveDependency(ConnectorPortRegister::class.java)
                .getLocalAddress(ConnectorType.BACKUP)
                .port

    private val clientHome: Path by lazy { Files.createDirectories(root.resolve("client")) }
    private val backups: Path by lazy { Files.createDirectories(root.resolve("backups")) }

    private fun backup(
        source: StandaloneServer,
        vararg arguments: String,
        from: String = "127.0.0.1:${source.backupPort}",
    ): Admin.Result =
        Admin.run(clientHome, ::BackupCommand, "--from=$from", "--to-path=$backups", *arguments)

    /** The backups of [database] in the order they were taken. */
    private fun artifacts(database: String = "neo4j"): List<Artifact> =
        ArtifactChains.find(listOf(backups), fs)
            .artifacts
            .filter { it.header.databaseName == database }
            .sortedBy { Files.getLastModifiedTime(it.path) }

    /** A Community server with `neo4j` restored from [backup] and its chain, started. */
    private fun restored(backup: Artifact, vararg arguments: String): CommunityServer {
        val target = CommunityServer(root.resolve("target-${running.size}"))
        val restore =
            Admin.run(target.home, ::RestoreCommand, "--from-path=${backup.path}", *arguments)
        assertThat(restore.exitCode).describedAs(restore.toString()).isZero
        running += target
        return target.start()
    }

    private fun count(server: CommunityServer, label: String): Long =
        server.rows("MATCH (n:$label) RETURN count(n) AS count").single()["count"] as Long

    @Test
    fun `a full backup carries the page cache profiles, from which the restored database warms up`() {
        val source =
            source(
                    GraphDatabaseSettings.pagecache_warmup_profiling_interval to
                        Duration.ofMillis(100)
                )
                .apply { start() }
        source.rows("UNWIND range(1, 20000) AS i CREATE (:Person {id: i, name: 'person ' + i})")
        val profile = "data/databases/neo4j/profiles/neostore.nodestore.db.cacheprof"
        await withPollInterval
            Duration.ofMillis(50) atMost
            Duration.ofSeconds(60) untilAsserted
            {
                assertThat(source.home.resolve(profile)).isRegularFile()
            }

        val result = backup(source, "--type=FULL")
        assertThat(result.exitCode).describedAs(result.toString()).isZero
        val target = restored(artifacts().single())

        assertThat(target.home.resolve(profile)).isRegularFile()
        val warmup =
            target
                .database()
                .dependencyResolver
                .resolveTypeDependencies(PageCacheWarmup::class.java)
                .single()
        await withPollInterval
            Duration.ofMillis(50) atMost
            Duration.ofSeconds(60) untilAsserted
            {
                assertThat(warmup.warmedUp).isTrue()
            }
        assertThat(warmup.pagesLoaded).isPositive()
    }

    @Test
    fun `keeps the users, roles and privileges of the database, which a restore writes out`() {
        val source = source().apply { start() }
        source.administer("CREATE ROLE curator")
        source.administer("GRANT MATCH {*} ON GRAPH neo4j TO curator")
        source.administer("CREATE USER alice SET PASSWORD 'alice-secret-1' CHANGE NOT REQUIRED")
        source.administer("GRANT ROLE curator TO alice")
        source.administer("CREATE USER bob SET PASSWORD 'bob-secret-1' CHANGE NOT REQUIRED")
        source.administer("GRANT ROLE curator TO bob")

        for (arguments in
            listOf(
                arrayOf("--type=FULL"),
                arrayOf("--include-metadata=roles"),
                arrayOf("--include-metadata=users=alice"),
                arrayOf("--include-metadata=none"),
                arrayOf("system"),
            )) {
            // So that no differential backup is empty.
            source.rows("CREATE (:Batch)")
            val result = backup(source, *arguments)
            assertThat(result.exitCode).describedAs(result.toString()).isZero
        }
        val (all, roles, alice, none) = artifacts()

        assertThat(all.header.metadataScript)
            .contains(
                "CREATE ROLE `curator` IF NOT EXISTS;\n",
                "GRANT MATCH {*} ON GRAPH \$database NODE * TO `curator`;\n",
                "GRANT ROLE `curator` TO `alice`;\n",
            )
            .containsPattern("CREATE USER `alice` IF NOT EXISTS SET ENCRYPTED PASSWORD '1,")
        assertThat(roles.header.metadataScript)
            .contains("CREATE ROLE `curator` IF NOT EXISTS;\n")
            .doesNotContain("CREATE USER")
        assertThat(alice.header.metadataScript)
            .contains("GRANT ROLE `curator` TO `alice`;\n")
            .doesNotContain("CREATE ROLE", "`bob`")
        assertThat(none.header.metadataScript).isNull()
        assertThat(artifacts("system").single().header.metadataScript).isNull()
        assertThat(backup(source, "--include-metadata=some").err)
            .contains("'some' is not one of none, all, users, users=<user>[,<user>...] and roles")
        assertThat(backup(source, "--include-metadata=users=").err)
            .contains("'users=' is not one of none, all, users, users=<user>[,<user>...] and roles")

        // The restore of the chain writes out the latest metadata it holds, alice's.
        val home = Files.createDirectories(root.resolve("restored"))
        val restore = Admin.run(home, ::RestoreCommand, "--from-path=${none.path}")
        val script = home.resolve("data/scripts/neo4j/restore_metadata.cypher")

        assertThat(restore.exitCode).describedAs(restore.toString()).isZero
        assertThat(Files.readString(script)).isEqualTo(alice.header.metadataScript)
        assertThat(restore.out)
            .contains(
                "Wrote the users, roles and privileges of database 'neo4j' to $script.",
                "cat $script | bin/cypher-shell -d system --param \"database => 'neo4j'\"",
            )
    }

    @Test
    fun `a full backup under writes holds every write acknowledged before it`() {
        val source = source().apply { start() }
        CommunityServer.populate(source.database())
        val expected = CommunityServer.summary(source.database())
        val acknowledged = AtomicLong(-1)
        val writing = AtomicBoolean(true)
        val writer = thread {
            var n = 0L
            while (writing.get()) {
                source
                    .database()
                    .executeTransactionally("CREATE (:Written {n: ${'$'}n})", mapOf("n" to n))
                acknowledged.set(n++)
            }
        }
        val before =
            try {
                while (acknowledged.get() < 100) Thread.sleep(5)
                val before = acknowledged.get()
                val result = backup(source, "--type=FULL")
                assertThat(result.exitCode).describedAs(result.toString()).isZero
                assertThat(result.out)
                    .contains("Backed up database 'neo4j' from 127.0.0.1:${source.backupPort} to")
                    .contains(": a full backup up to transaction")
                before
            } finally {
                writing.set(false)
                writer.join()
            }

        val full = artifacts().single()
        assertThat(full.header.full).isTrue
        assertThat(full.header.compressed).isTrue
        val target = restored(full)
        assertThat(CommunityServer.summary(target)).isEqualTo(expected)
        val written =
            target.rows("MATCH (w:Written) RETURN max(w.n) AS max, count(w) AS count").single()
        assertThat(written["max"] as Long).isGreaterThanOrEqualTo(before)
        assertThat(written["count"]).isEqualTo((written["max"] as Long) + 1)
        target.stop()
        assertThat(target.consistent()).describedAs("consistent").isTrue
    }

    @Test
    fun `AUTO continues the latest backup, and the chain restores to any point of it`() {
        val source = source().apply { start() }
        CommunityServer.populate(source.database())
        assertThat(backup(source).exitCode).isZero
        source.database().executeTransactionally("CREATE (:Batch {n: 1})")
        val first = backup(source)
        assertThat(first.exitCode).describedAs(first.toString()).isZero
        assertThat(first.out).contains(": a differential backup of transactions")
        val empty = backup(source)
        assertThat(empty.exitCode).describedAs(empty.toString()).isZero
        assertThat(empty.out).contains(": an empty differential backup: no transactions since")
        source.database().executeTransactionally("CREATE (:Batch {n: 2})")
        assertThat(backup(source).exitCode).isZero

        val chain = artifacts()
        assertThat(chain.map { it.header.full }).containsExactly(true, false, false, false)
        assertThat(chain.map { it.header.empty }).containsExactly(false, false, true, false)
        val (full, firstBatch, _, secondBatch) = chain
        assertThat(firstBatch.continues(full)).isTrue
        assertThat(secondBatch.continues(firstBatch)).isTrue

        val latest = restored(secondBatch)
        assertThat(count(latest, "Batch")).isEqualTo(2)
        latest.stop()
        assertThat(latest.consistent()).describedAs("consistent").isTrue
        val earlier =
            restored(secondBatch, "--restore-until=${secondBatch.header.lowestAppendIndex}")
        assertThat(count(earlier, "Batch")).isEqualTo(1)
    }

    @Test
    fun `writes no empty differential backup with --skip-empty-diffs and --include-metadata=none`() {
        val source = source().apply { start() }
        assertThat(backup(source, "--type=FULL").exitCode).isZero

        val refused = backup(source, "--skip-empty-diffs")
        assertThat(refused.exitCode).isNotZero
        assertThat(refused.err)
            .contains(
                "--skip-empty-diffs needs --include-metadata=none: an empty differential " +
                    "backup still keeps the users, roles and privileges"
            )
        val skipped = backup(source, "--skip-empty-diffs", "--include-metadata=none")
        assertThat(skipped.exitCode).describedAs("$skipped").isZero
        assertThat(skipped.out)
            .contains("Did not back up database 'neo4j'", "no transactions since", ".backup")
        assertThat(artifacts()).hasSize(1)

        source.rows("CREATE (:Batch)")
        val taken = backup(source, "--skip-empty-diffs=true", "--include-metadata=none")
        assertThat(taken.exitCode).describedAs("$taken").isZero
        assertThat(artifacts()).hasSize(2)
        // Without it, an empty one is written, as before.
        assertThat(backup(source, "--include-metadata=none").out)
            .contains("an empty differential backup")
        assertThat(artifacts()).hasSize(3)
    }

    @Test
    fun `with --prefer-diff-as-parent, a differential backup continues the latest differential one`() {
        val source = source().apply { start() }
        for (arguments in listOf(arrayOf("--type=FULL"), arrayOf(), arrayOf("--type=FULL"))) {
            source.rows("CREATE (:Batch)")
            val result = backup(source, *arguments)
            assertThat(result.exitCode).describedAs("$result").isZero
        }
        source.rows("CREATE (:Batch)")

        val preferred = backup(source, "--type=DIFF", "--prefer-diff-as-parent")

        assertThat(preferred.exitCode).describedAs("$preferred").isZero
        val (_, differential, full, continued) = artifacts()
        assertThat(full.header.full).isTrue
        // Even though the full backup taken after it is newer.
        assertThat(continued.header.lowestAppendIndex)
            .isEqualTo(differential.header.highestAppendIndex + 1)
        assertThat(count(restored(continued), "Batch")).isEqualTo(4)
        // Without differential backups to continue, the latest backup, as without it.
        Files.delete(differential.path)
        Files.delete(continued.path)
        source.rows("CREATE (:Batch)")
        assertThat(backup(source, "--prefer-diff-as-parent=true").exitCode).isZero
        assertThat(artifacts().last().header.lowestAppendIndex)
            .isEqualTo(full.header.highestAppendIndex + 1)
    }

    @Test
    fun `a differential backup needs a backup to continue`() {
        val source = source().apply { start() }

        val result = backup(source, "--type=DIFF")

        assertThat(result.exitCode).isEqualTo(1)
        assertThat(result.err)
            .contains(
                "Could not back up database 'neo4j': there is no backup of it in $backups to " +
                    "continue; take a full backup first"
            )
            .contains("Backup failed for databases: 'neo4j'")
        assertThat(artifacts()).isEmpty()
    }

    @Test
    fun `falls back to a full backup when the logs no longer hold the transactions`() {
        val source =
            source(
                    GraphDatabaseSettings.logical_log_rotation_threshold to ByteUnit.kibiBytes(128),
                    GraphDatabaseSettings.keep_logical_logs to "false",
                )
                .apply { start() }
        assertThat(backup(source, "--type=FULL").exitCode).isZero
        val after = artifacts().single().header.highestAppendIndex
        CommunityServer.populate(source.database())
        source
            .database()
            .dependencyResolver
            .resolveDependency(CheckPointer::class.java)
            .forceCheckPoint(SimpleTriggerInfo("test"))

        val result = backup(source, "--type=DIFF")

        assertThat(result.exitCode).describedAs(result.toString()).isZero
        assertThat(result.out)
            .contains(
                "Taking a full backup of database 'neo4j' instead of a differential one: the " +
                    "transaction logs of 'neo4j' no longer hold the transactions after append " +
                    "index $after."
            )
            .contains(": a full backup up to transaction")
        assertThat(artifacts().map { it.header.full }).containsExactly(true, true)
    }

    @Test
    fun `falls back to a full backup when the database was re-created`() {
        val source = source().apply { start() }
        source.administer("CREATE DATABASE other WAIT")
        source.database("other").executeTransactionally("UNWIND range(1, 50) AS i CREATE (:Old)")
        assertThat(backup(source, "other").exitCode).isZero
        source.administer("DROP DATABASE other WAIT")
        source.administer("CREATE DATABASE other WAIT")
        source.database("other").executeTransactionally("CREATE (:New)")

        val result = backup(source, "other")

        assertThat(result.exitCode).describedAs(result.toString()).isZero
        assertThat(result.out)
            .contains("Taking a full backup of database 'other' instead of a differential one: ")
        val (old, new) = artifacts("other").sortedBy { it.header.time }
        assertThat(new.header.full).isTrue
        assertThat(new.header.databaseId).isNotEqualTo(old.header.databaseId)
    }

    @Test
    fun `backs up the databases a pattern matches, and names as Neo4j normalizes them`() {
        val source = source().apply { start() }
        source.administer("CREATE DATABASE other WAIT")

        val all = backup(source, "*")
        assertThat(all.exitCode).describedAs(all.toString()).isZero
        assertThat(
                ArtifactChains.find(listOf(backups), fs).artifacts.map { it.header.databaseName }
            )
            .containsExactlyInAnyOrder("neo4j", "other", "system")

        val named = backup(source, "OTHER")
        assertThat(named.exitCode).describedAs(named.toString()).isZero
        assertThat(artifacts("other").map { it.header.full }).containsExactly(true, false)
    }

    @Test
    fun `tells why a database cannot be backed up`() {
        val source = source().apply { start() }
        source.administer("CREATE DATABASE other WAIT")
        source.administer("STOP DATABASE other WAIT")
        val server = "127.0.0.1:${source.backupPort}"

        val result = backup(source, "nope", "other", "neo4j")

        assertThat(result.exitCode).isEqualTo(1)
        assertThat(result.err)
            .contains("Could not back up database 'nope' from $server: there is no database 'nope'")
            .contains(
                "Could not back up database 'other' from $server: database 'other' is not " +
                    "available: it must be running to be backed up"
            )
            .contains("Backup failed for databases: 'nope', 'other'")
        assertThat(artifacts()).hasSize(1)
    }

    @Test
    fun `tries the servers in order, and fails when one could not serve`() {
        val source = source().apply { start() }
        val unused = "127.0.0.1:${TestPorts.free()}"

        val result = backup(source, from = "$unused,127.0.0.1:${source.backupPort}")

        assertThat(result.exitCode).isEqualTo(1)
        assertThat(result.err)
            .contains("Could not back up database 'neo4j' from $unused: UNAVAILABLE")
            .contains("Every database was backed up, but some servers failed, as reported above")
        assertThat(artifacts()).hasSize(1)
    }

    @Test
    fun `backs up over TLS with the backup policy on both sides`() {
        val authority = TestAuthority()
        val source = source(policy.enabled to true)
        authority.issue().writeTo(source.home.resolve("certificates/backup"))
        source.start()
        authority.issue().writeTo(clientHome.resolve("certificates/backup"))

        val plaintext = backup(source)
        assertThat(plaintext.exitCode).isEqualTo(1)
        assertThat(plaintext.err).contains("Could not back up database 'neo4j' from")

        Files.createDirectories(clientHome.resolve("conf"))
        Files.writeString(clientHome.resolve("conf/neo4j.conf"), "${policy.enabled.name()}=true\n")
        val tls = backup(source)
        assertThat(tls.exitCode).describedAs(tls.toString()).isZero
        assertThat(artifacts()).hasSize(1)
    }

    @Test
    fun `writes uncompressed backups on request`() {
        val source = source().apply { start() }
        assertThat(backup(source, "--compress=false").exitCode).isZero
        source.database().executeTransactionally("CREATE (:Later)")
        assertThat(backup(source, "--compress=false").exitCode).isZero

        assertThat(artifacts().map { it.header.compressed }).containsExactly(false, false)
        assertThat(count(restored(artifacts().last()), "Later")).isEqualTo(1)
    }
}
