package org.dozendb.backup.cli

import java.nio.file.Files
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.dozendb.backup.Admin
import org.dozendb.backup.BackupSettings
import org.dozendb.backup.CommunityServer
import org.dozendb.cloud.internal.MemoryCloud
import org.dozendb.core.StandaloneServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.connectors.ConnectorPortRegister
import org.neo4j.configuration.connectors.ConnectorType
import org.neo4j.configuration.helpers.SocketAddress

/**
 * Backups in cloud storage, through Neo4j's storage interface and DozenDB's cloud provider, on a
 * cloud in memory: `memory://<bucket>/<key>`.
 */
class CloudBackupTest {
    // Kept when a test fails, for the servers' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private val running = mutableListOf<AutoCloseable>()

    @BeforeEach fun clear() = MemoryCloud.clear()

    @AfterEach
    fun stop() {
        running.reversed().forEach { it.close() }
        MemoryCloud.clear()
    }

    private fun source(): StandaloneServer =
        StandaloneServer(
                root.resolve("source"),
                mapOf(BackupSettings.listenAddress to SocketAddress("127.0.0.1", 0)),
            )
            .also { running += it }
            .apply { start() }

    private val StandaloneServer.backupPort: Int
        get() =
            database(StandaloneServer.SYSTEM)
                .dependencyResolver
                .resolveDependency(ConnectorPortRegister::class.java)
                .getLocalAddress(ConnectorType.BACKUP)
                .port

    private val client: Path by lazy { Files.createDirectories(root.resolve("client")) }
    private val temp: Path by lazy { Files.createDirectories(root.resolve("temp")) }

    private fun backup(source: StandaloneServer, vararg arguments: String): Admin.Result =
        Admin.run(client, ::BackupCommand, "--from=127.0.0.1:${source.backupPort}", *arguments)

    /** The keys in [bucket]. */
    private fun keys(bucket: String) =
        MemoryCloud.objects()
            .keys
            .filter { it.startsWith("$bucket/") }
            .map {
                it.removePrefix("$bucket/")
            }

    /** A Community server with `neo4j` restored by [arguments], started. */
    private fun restored(vararg arguments: String): CommunityServer {
        val target = CommunityServer(root.resolve("target-${running.size}"))
        val restore = Admin.run(target.home, ::RestoreCommand, *arguments)
        assertThat(restore.exitCode).describedAs(restore.toString()).isZero
        assertThat(restore.out).contains("from memory://backups/films/neo4j-")
        running += target
        return target.start()
    }

    private fun batches(server: CommunityServer): Long =
        server.rows("MATCH (n:Batch) RETURN count(n) AS count").single()["count"] as Long

    @Test
    fun `backups go to cloud storage, and are inspected, aggregated and restored from it`() {
        val source = source()
        CommunityServer.populate(source.database())
        val full = backup(source, "--to-path=memory://backups/films/", "--temp-path=$temp")
        assertThat(full.exitCode).describedAs(full.toString()).isZero
        assertThat(full.out).contains("to memory://backups/films/neo4j-", ": a full backup up to")
        source.database().executeTransactionally("CREATE (:Batch {n: 1})")
        // A folder named without its slash, as a local one.
        val differential = backup(source, "--to-path=memory://backups/films", "--temp-path=$temp")
        assertThat(differential.exitCode).describedAs(differential.toString()).isZero
        assertThat(differential.out).contains(": a differential backup of transaction")

        assertThat(keys("backups")).hasSize(2).allMatch {
            it.startsWith("films/neo4j-") && it.endsWith(".backup")
        }
        assertThat(temp).isEmptyDirectory()
        val inspected = Admin.run(client, ::InspectBackupCommand, "memory://backups/films/")
        assertThat(inspected.exitCode).describedAs(inspected.toString()).isZero
        assertThat(inspected.out.lines().filter { "memory://backups/films/neo4j-" in it })
            .hasSize(2)
        assertThat(batches(restored("--from-path=memory://backups/films/"))).isEqualTo(1)

        val aggregated =
            Admin.run(
                client,
                ::AggregateBackupCommand,
                "--from-path=memory://backups/films/",
                "--temp-path=$temp",
                "neo4j",
            )
        assertThat(aggregated.exitCode).describedAs(aggregated.toString()).isZero
        assertThat(aggregated.out).contains("into memory://backups/films/neo4j-")
        assertThat(keys("backups")).hasSize(1)
        assertThat(temp).isEmptyDirectory()
        val file = keys("backups").single()
        assertThat(batches(restored("--from-path=memory://backups/$file"))).isEqualTo(1)
    }

    @Test
    fun `split backups go to cloud storage in parts, and are restored and aggregated whole`() {
        val source = source()
        CommunityServer.populate(source.database())
        // Neo4j's minimum, 1 GiB, is lifted for tests.
        Files.createDirectories(client.resolve("conf"))
        Files.writeString(
            client.resolve("conf/neo4j.conf"),
            "${GraphDatabaseInternalSettings.allow_small_split_archive_size.name()}=true\n",
        )
        val split = arrayOf("--split-archive-part-size=8KiB", "--temp-path=$temp")
        val full = backup(source, "--to-path=memory://backups/films/", *split)
        assertThat(full.exitCode).describedAs(full.toString()).isZero
        source.database().executeTransactionally("CREATE (:Batch {n: 1})")
        val differential = backup(source, "--to-path=memory://backups/films/", *split)
        assertThat(differential.exitCode).describedAs(differential.toString()).isZero

        val backups = keys("backups").filter { it.endsWith(".backup") }
        assertThat(backups).hasSize(2)
        assertThat(keys("backups") - backups.toSet()).isNotEmpty.allMatch { key ->
            backups.any { key.startsWith("$it.") }
        }
        assertThat(batches(restored("--from-path=memory://backups/films/"))).isEqualTo(1)

        val aggregated =
            Admin.run(
                client,
                ::AggregateBackupCommand,
                "--from-path=memory://backups/films/",
                "--temp-path=$temp",
                "neo4j",
            )
        assertThat(aggregated.exitCode).describedAs(aggregated.toString()).isZero
        assertThat(keys("backups")).hasSize(1)
        assertThat(batches(restored("--from-path=memory://backups/films/"))).isEqualTo(1)
    }

    @Test
    fun `a cloud folder needs a local temporary directory, and credentials that list it`() {
        val source = source()

        val noTemp = backup(source, "--to-path=memory://backups/films/")
        assertThat(noTemp.exitCode).isNotZero
        assertThat(noTemp.err)
            .contains(
                "--to-path memory://backups/films/ is in cloud storage: give --temp-path, a " +
                    "local directory for the copy of a database before it is uploaded"
            )
        MemoryCloud.deny("denied")
        val denied = backup(source, "--to-path=memory://denied/films/", "--temp-path=$temp")
        assertThat(denied.exitCode).isNotZero
        assertThat(denied.err)
            .contains(
                "Cannot list --to-path memory://denied/films/: memory://denied/films/: denied"
            )
        val aggregate =
            Admin.run(
                client,
                ::AggregateBackupCommand,
                "--from-path=memory://backups/films/",
                "neo4j",
            )
        assertThat(aggregate.exitCode).isNotZero
        assertThat(aggregate.err)
            .contains("--from-path memory://backups/films/ is in cloud storage: give --temp-path")
    }

    @Test
    fun `a backup that fails to upload leaves nothing in the folder`() {
        val source = source()
        CommunityServer.populate(source.database())
        MemoryCloud.failPart(1)

        val failed = backup(source, "--to-path=memory://backups/films/", "--temp-path=$temp")

        assertThat(failed.exitCode).isNotZero
        assertThat(failed.err).contains("Could not back up database 'neo4j'", "part 1 failed")
        assertThat(keys("backups")).isEmpty()
        assertThat(temp).isEmptyDirectory()
    }
}
