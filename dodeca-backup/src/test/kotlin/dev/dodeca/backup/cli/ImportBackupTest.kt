package dev.dodeca.backup.cli

import dev.dodeca.backup.Admin
import dev.dodeca.backup.ArtifactChains
import dev.dodeca.backup.CommunityServer
import dev.dodeca.backup.SplitArchives
import java.nio.file.Files
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.cli.ExitCode
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.io.fs.DefaultFileSystemAbstraction

/**
 * `neo4j-admin database import full` and `incremental` with `--target-format=backup`, which write
 * the imported database as a full backup, as Neo4j Enterprise's do.
 */
class ImportBackupTest {
    // Kept when a test fails, for the servers' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private val fs = DefaultFileSystemAbstraction()

    private fun csv(name: String, vararg lines: String): String {
        val file = root.resolve(name)
        Files.write(file, lines.toList())
        return file.toString()
    }

    private fun directory(name: String): Path = Files.createDirectories(root.resolve(name))

    private fun count(server: CommunityServer, cypher: String): Long =
        server.rows(cypher).single().values.single() as Long

    /** Restores the backup in [backups] of [database] on a new Community server, started. */
    private fun restored(backups: Path, database: String): CommunityServer {
        val home = root.resolve("restored-${backups.fileName}")
        val restore = Admin.run(home, ::RestoreCommand, "--from-path=$backups")
        assertThat(restore.exitCode).describedAs("$restore").isZero
        return CommunityServer(home, database).start()
    }

    @Test
    fun `a full import writes the database as a full backup, split and compressed on request`() {
        val home = directory("home")
        // Neo4j's minimum, 1 GiB, is lifted for tests.
        Files.createDirectories(home.resolve("conf"))
        Files.writeString(
            home.resolve("conf/neo4j.conf"),
            "${GraphDatabaseInternalSettings.allow_small_split_archive_size.name()}=true\n",
        )
        val backups = directory("backups")
        val nodes =
            csv(
                "persons.csv",
                "id:ID,name,:LABEL",
                *(1..2000).map { "$it,person $it,Person" }.toTypedArray(),
            )
        val relationships =
            csv(
                "knows.csv",
                ":START_ID,:END_ID,:TYPE",
                *(1 until 2000).map { "$it,${it + 1},KNOWS" }.toTypedArray(),
            )
        val arguments =
            arrayOf(
                "database",
                "import",
                "full",
                "--target-format=backup",
                "--target-location=$backups",
                // --nodes and --relationships take several files: the database comes first.
                "films",
                "--nodes=$nodes",
                "--relationships=$relationships",
            )

        val dryRun = Admin.tool(home, *arguments, "--dry-run")
        assertThat(dryRun.exitCode).describedAs("$dryRun").isZero
        assertThat(dryRun.out).contains("Estimated entity counts:")
        assertThat(backups).isEmptyDirectory()

        val result = Admin.tool(home, *arguments, "--compress", "--split-archive-part-size=8KiB")

        assertThat(result.exitCode).describedAs("$result").isZero
        assertThat(result.out)
            .contains("Wrote the imported database 'films' as a full backup to $backups")
        val artifact = ArtifactChains.find(listOf(backups), fs).artifacts.single()
        assertThat(artifact.header.databaseName).isEqualTo("films")
        assertThat(artifact.header.full).isTrue
        assertThat(artifact.header.compressed).isTrue
        assertThat(SplitArchives.parts(fs, artifact.path)).hasSizeGreaterThan(1)
        // The import was made beside the backup, and its database is not in the DBMS.
        assertThat(Files.list(backups).use { files -> files.toList() }).noneMatch {
            it.fileName.toString().startsWith(".dodeca-")
        }
        assertThat(home.resolve("data/databases/films")).doesNotExist()
        restored(backups, "films").use { server ->
            assertThat(count(server, "MATCH (p:Person) RETURN count(p)")).isEqualTo(2000)
            assertThat(count(server, "MATCH ()-[k:KNOWS]->() RETURN count(k)")).isEqualTo(1999)
        }
    }

    @Test
    fun `an incremental import writes a copy of the database with the import as a full backup`() {
        val source = CommunityServer(root.resolve("source")).start()
        source.rows("CREATE CONSTRAINT FOR (p:Person) REQUIRE p.uuid IS UNIQUE")
        source.rows("CREATE (:Person {uuid: 'a'}), (:Person {uuid: 'b'})")
        source.stop()
        val backups = directory("incremental-backups")
        val files =
            arrayOf(
                "--nodes=" + csv("new.csv", "uuid:ID{label:Person},:LABEL", "c,Person", "d,Person"),
                "--relationships=" + csv("rels.csv", ":START_ID,:END_ID,:TYPE", "a,c,KNOWS"),
            )
        val arguments =
            arrayOf(
                "database",
                "import",
                "incremental",
                "--force",
                "--target-format=backup",
                "--target-location=$backups",
                "neo4j",
            )

        val staged = Admin.tool(source.home, *arguments, "--stage=prepare", *files)
        assertThat(staged.exitCode).isEqualTo(ExitCode.USAGE)
        assertThat(staged.err).contains("--target-format=backup imports with --stage=all only")

        val result = Admin.tool(source.home, *arguments, *files)

        assertThat(result.exitCode).describedAs("$result").isZero
        assertThat(result.out)
            .contains("Wrote the imported database 'neo4j' as a full backup to $backups")
        assertThat(Files.list(backups).use { it.count() }).isEqualTo(1)
        restored(backups, "neo4j").use { server ->
            assertThat(count(server, "MATCH (p:Person) RETURN count(p)")).isEqualTo(4)
            assertThat(count(server, "MATCH (:Person {uuid: 'a'})-[k:KNOWS]->() RETURN count(k)"))
                .isEqualTo(1)
        }
        // The database itself is as it was.
        source.start()
        assertThat(count(source, "MATCH (p:Person) RETURN count(p)")).isEqualTo(2)
        source.stop()
    }
}
