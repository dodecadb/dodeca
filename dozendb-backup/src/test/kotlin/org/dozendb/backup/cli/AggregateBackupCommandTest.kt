package org.dozendb.backup.cli

import java.nio.file.Files
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.dozendb.backup.Admin
import org.dozendb.backup.Artifacts
import org.dozendb.backup.CommunityServer
import org.dozendb.backup.PhasedSource
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.consistency.CheckCommand

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
