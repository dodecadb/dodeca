package org.dozendb.backup.cli

import java.nio.file.Files
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.dozendb.backup.Admin
import org.dozendb.backup.ArtifactChains
import org.dozendb.backup.Artifacts
import org.dozendb.backup.CommunityServer
import org.dozendb.backup.PhasedSource
import org.dozendb.backup.SplitArchives
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.consistency.CheckCommand
import org.neo4j.io.fs.DefaultFileSystemAbstraction

class AggregateBackupCommandTest {
    // Kept when a test fails, for the servers' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path

    private fun files(directory: Path) =
        Files.list(directory).use { it.map { path -> path.fileName.toString() }.sorted().toList() }

    private fun restored(backups: Path, name: String): Map<String, Any?> {
        val home = root.resolve(name)
        val result = Admin.run(home, ::RestoreCommand, "--from-path=$backups")
        assertThat(result.exitCode).describedAs("$result").isZero
        return CommunityServer(home).start().use { PhasedSource.state(it) }
    }

    @Test
    fun `aggregates a chain into a full backup that the next differential backup continues`() {
        val backups = Files.createDirectories(root.resolve("backups"))
        CommunityServer(root.resolve("source")).start().use { server ->
            CommunityServer.populate(server)
            var after =
                server
                    .backupFull(backups.resolve("neo4j-a${Artifacts.EXTENSION}"))
                    .highestAppendIndex
            for (step in listOf("b", "c")) {
                server
                    .database()
                    .executeTransactionally(
                        "UNWIND range(1, 50) AS n CREATE (:Later {phase: '$step', n: n})"
                    )
                after =
                    server
                        .backupDifferential(
                            after,
                            backups.resolve("neo4j-$step${Artifacts.EXTENSION}"),
                        )
                        .highestAppendIndex
            }
            val expected = PhasedSource.state(server)

            val result = Admin.run(root, ::AggregateBackupCommand, "--from-path=$backups", "neo4j")

            assertThat(result.exitCode).describedAs("$result").isZero
            assertThat(result.out).contains("Aggregated the chain of 3 backups of 'neo4j' into ")
            val aggregate = files(backups).single()
            val header =
                backups.resolve(aggregate).toFile().inputStream().use(Artifacts::readHeader)
            assertThat(header.full).isTrue
            assertThat(header.highestAppendIndex).isEqualTo(after)
            assertThat(aggregate).isEqualTo(Artifacts.fileName("neo4j", header.time))
            assertThat(restored(backups, "aggregated")).isEqualTo(expected)

            server
                .database()
                .executeTransactionally(
                    "UNWIND range(1, 50) AS n CREATE (:Later {phase: 'd', n: n})"
                )
            server.backupDifferential(after, backups.resolve("neo4j-d${Artifacts.EXTENSION}"))
            assertThat(restored(backups, "continued")).isEqualTo(PhasedSource.state(server))
        }
    }

    @Test
    fun `is neo4j-admin backup aggregate, and still database aggregate-backup, which is deprecated`() {
        for (command in
            listOf(listOf("backup", "aggregate"), listOf("database", "aggregate-backup"))) {
            val source = PhasedSource.write(root.resolve(command.last()))

            // --parallel-recovery as Enterprise has it, experimental; DozenDB recovers alike.
            val result =
                Admin.tool(
                    root,
                    *command.toTypedArray(),
                    "--from-path=${source.second}",
                    "--parallel-recovery",
                )

            assertThat(result.exitCode).describedAs("$command: $result").isZero
            assertThat(result.out).contains("Aggregated the chain of 3 backups of 'neo4j' into ")
        }
        assertThat(Admin.tool(root, "backup", "aggregate", "--help").out)
            .doesNotContain("Deprecated")
        assertThat(Admin.tool(root, "database", "aggregate-backup", "--help").out)
            .contains("Deprecated: use neo4j-admin backup aggregate.")
    }

    @Test
    fun `splits the aggregated backup into parts with --split-archive-part-size`() {
        val source = PhasedSource.write(root)
        // Neo4j's minimum, 1 GiB, is lifted for tests.
        Files.createDirectories(root.resolve("conf"))
        Files.writeString(
            root.resolve("conf/neo4j.conf"),
            "${GraphDatabaseInternalSettings.allow_small_split_archive_size.name()}=true\n",
        )

        val result =
            Admin.run(
                root,
                ::AggregateBackupCommand,
                "--from-path=${source.second}",
                "--split-archive-part-size=8KiB",
            )

        assertThat(result.exitCode).describedAs("$result").isZero
        val fs = DefaultFileSystemAbstraction()
        val artifacts = ArtifactChains.find(listOf(source.backups), fs).artifacts
        val aggregate = artifacts.single { it.header.full }
        val parts = SplitArchives.parts(fs, aggregate.path)
        assertThat(parts).hasSizeGreaterThan(1)
        assertThat(files(source.backups))
            .containsExactlyInAnyOrderElementsOf(
                (parts + artifacts.map { it.path } + listOf(source.dump)).map {
                    it.fileName.toString()
                }
            )
        assertThat(restored(source.backups, "restored")).isEqualTo(source.states[2])
    }

    @Test
    fun `keeps the old chain if asked`() {
        val source = PhasedSource.write(root)
        val before = files(source.backups)

        val result =
            Admin.run(
                root,
                ::AggregateBackupCommand,
                "--from-path=${source.second}",
                "--keep-old-backup",
            )

        assertThat(result.exitCode).describedAs("$result").isZero
        assertThat(files(source.backups)).containsAll(before).hasSize(before.size + 1)
        assertThat(restored(source.backups, "restored")).isEqualTo(source.states[2])
    }

    @Test
    fun `Community's database check checks the latest chain`() {
        val source = PhasedSource.write(root)

        val result =
            Admin.run(
                root.resolve("checker"),
                ::CheckCommand,
                "--from-path=${source.backups}",
                "neo4j",
            )

        assertThat(result.exitCode).describedAs("$result").isZero
    }
}
