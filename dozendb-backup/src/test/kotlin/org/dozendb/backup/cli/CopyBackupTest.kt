package org.dozendb.backup.cli

import java.nio.file.Files
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.dozendb.backup.Admin
import org.dozendb.backup.ArtifactChains
import org.dozendb.backup.CommunityServer
import org.dozendb.backup.PhasedSource
import org.dozendb.backup.SplitArchives
import org.dozendb.cloud.internal.MemoryCloud
import org.dozendb.core.copy.CopyCommand
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.io.fs.DefaultFileSystemAbstraction

/**
 * `neo4j-admin database copy` from and to backups, with `--source-format=backup` and
 * `--target-format=backup`, as Neo4j Enterprise copies.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CopyBackupTest {
    private lateinit var root: Path
    private lateinit var source: PhasedSource
    private val fs = DefaultFileSystemAbstraction()

    @BeforeAll
    fun writeSource(
        // Kept when a test fails, for the servers' logs.
        @TempDir(cleanup = CleanupMode.ON_SUCCESS) root: Path
    ) {
        this.root = root
        source = PhasedSource.write(root)
    }

    private fun copy(home: Path, vararg arguments: String) =
        Admin.run(home, ::CopyCommand, *arguments, "--to-path-schema=$home")

    private fun directory(name: String): Path = Files.createDirectories(root.resolve(name))

    /** Restores the backup in [backups] of [database] on a new Community server; its state. */
    private fun restored(backups: Path, database: String): Map<String, Any?> {
        val home = root.resolve("restored-${backups.fileName}")
        val restore = Admin.run(home, ::RestoreCommand, "--from-path=$backups")
        assertThat(restore.exitCode).describedAs("$restore").isZero
        return CommunityServer(home, database).start().use { server ->
            val state = PhasedSource.state(server)
            server.stop()
            assertThat(server.consistent()).describedAs("$home consistent").isTrue
            state
        }
    }

    @Test
    fun `copies a database into a full backup, which restores it under the new name`() {
        val home = root.resolve("source")
        val backups = directory("copied-backups")

        val result =
            copy(
                home,
                "neo4j",
                "copied",
                "--copy-schema",
                "--target-format=backup",
                "--target-location=$backups",
            )

        assertThat(result.exitCode).describedAs("$result").isZero
        assertThat(result.out)
            .contains("Wrote the copy of database 'neo4j' as a full backup of 'copied' to $backups")
        val artifact = ArtifactChains.find(listOf(backups), fs).artifacts.single()
        assertThat(artifact.header.databaseName).isEqualTo("copied")
        assertThat(artifact.header.full).isTrue
        // Not compressed unless asked, as in Enterprise.
        assertThat(artifact.header.compressed).isFalse
        assertThat(artifact.header.metadataScript).isNull()
        assertThat(restored(backups, "copied")).isEqualTo(source.states[2])
        // Nothing is left of the copy's staging.
        assertThat(Files.list(backups).use { it.count() }).isEqualTo(1)
    }

    @Test
    fun `copies a database from the chain of backups ending with the one named`() {
        val home = root.resolve("from-chain")

        val result =
            copy(
                home,
                "neo4j",
                "films",
                "--copy-schema",
                "--source-format=backup",
                "--source-location=${source.first}",
            )

        assertThat(result.exitCode).describedAs("$result").isZero
        assertThat(result.out)
            .contains(
                "Restored database 'neo4j' from ${source.full} and 1 differential backup, up to " +
                    "transaction "
            )
        val state =
            CommunityServer(home, "films").start().use { server -> PhasedSource.state(server) }
        assertThat(state).isEqualTo(source.states[1])
        assertThat(Files.list(home.resolve("data/databases")).use { it.toList() }).noneMatch {
            it.fileName.toString().startsWith(".dozendb-")
        }
    }

    @Test
    fun `copies a chain of backups into a compressed split backup`() {
        val home = directory("backup-to-backup")
        // Neo4j's minimum, 1 GiB, is lifted for tests.
        Files.createDirectories(home.resolve("conf"))
        Files.writeString(
            home.resolve("conf/neo4j.conf"),
            "${GraphDatabaseInternalSettings.allow_small_split_archive_size.name()}=true\n",
        )
        val backups = directory("split-backups")

        val result =
            copy(
                home,
                "neo4j",
                "films",
                "--copy-schema",
                "--source-format=backup",
                "--source-location=${source.second}",
                "--target-format=backup",
                "--target-location=$backups",
                "--compress",
                "--split-archive-part-size=8KiB",
            )

        assertThat(result.exitCode).describedAs("$result").isZero
        val artifact = ArtifactChains.find(listOf(backups), fs).artifacts.single()
        assertThat(artifact.header.compressed).isTrue
        assertThat(SplitArchives.parts(fs, artifact.path)).hasSizeGreaterThan(1)
        assertThat(restored(backups, "films")).isEqualTo(source.states[2])
    }

    @Test
    fun `copies to and from backups in cloud storage, staged in --temp-path`() {
        MemoryCloud.clear()
        try {
            val home = root.resolve("source")
            val temp = directory("cloud-temp")
            val noTemp =
                copy(
                    home,
                    "neo4j",
                    "films",
                    "--target-format=backup",
                    "--target-location=memory://backups/films/",
                )
            assertThat(noTemp.exitCode).isEqualTo(1)
            assertThat(noTemp.err).contains("give --temp-path")

            val upload =
                copy(
                    home,
                    "neo4j",
                    "films",
                    "--copy-schema",
                    "--target-format=backup",
                    "--target-location=memory://backups/films/",
                    "--temp-path=$temp",
                )
            assertThat(upload.exitCode).describedAs("$upload").isZero
            val key = MemoryCloud.objects().keys.single()
            assertThat(key).startsWith("backups/films/films-").endsWith(".backup")
            assertThat(temp).isEmptyDirectory()

            val target = root.resolve("from-cloud")
            val download =
                copy(
                    target,
                    "films",
                    "series",
                    "--copy-schema",
                    "--source-format=backup",
                    "--source-location=memory://$key",
                )
            assertThat(download.exitCode).describedAs("$download").isZero
            val state =
                CommunityServer(target, "series").start().use { server ->
                    PhasedSource.state(server)
                }
            assertThat(state).isEqualTo(source.states[2])
        } finally {
            MemoryCloud.clear()
        }
    }

    @Test
    fun `refuses an incomplete chain, a backup of another database, and options that do not go together`() {
        val home = root.resolve("refused")
        val gap = directory("gap")
        Files.copy(source.full, gap.resolve(source.full.fileName))
        Files.copy(source.second, gap.resolve(source.second.fileName))

        val incomplete =
            copy(
                home,
                "neo4j",
                "films",
                "--source-format=backup",
                "--source-location=${gap.resolve(source.second.fileName)}",
            )
        assertThat(incomplete.exitCode).isEqualTo(1)
        assertThat(incomplete.err)
            .contains("incomplete chain: no chain of backups leads to ")
            .contains("must be in its folder")

        val other =
            copy(
                home,
                "films",
                "copied",
                "--source-format=backup",
                "--source-location=${source.full}",
            )
        assertThat(other.exitCode).isEqualTo(1)
        assertThat(other.err)
            .contains(
                "--source-location ${source.full} is a backup of database 'neo4j', not of 'films'"
            )

        for ((arguments, message) in
            listOf(
                listOf("--target-location=$gap") to
                    "--target-location and --target-format=backup go together.",
                listOf("--source-format=backup") to
                    "--source-location and --source-format=backup go together.",
                listOf("--compress") to "--compress goes with --target-format=backup.",
                listOf("--split-archive-part-size=2GiB") to
                    "--split-archive-part-size goes with --target-format=backup.",
                listOf(
                    "--source-format=backup",
                    "--source-location=${source.full}",
                    "--from-path-data=$gap",
                    "--from-path-txn=$gap",
                ) to "--source-format=backup cannot be combined with --from-path-data",
                listOf("--target-format=tape") to
                    "Invalid --target-format tape: give database or backup.",
            )) {
            val result = copy(home, "neo4j", "copied", *arguments.toTypedArray())
            assertThat(result.exitCode).describedAs("$arguments: $result").isEqualTo(64)
            assertThat(result.err).describedAs("$arguments").contains(message)
        }
    }
}
