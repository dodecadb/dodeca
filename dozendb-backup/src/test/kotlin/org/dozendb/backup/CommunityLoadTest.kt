package org.dozendb.backup

import java.nio.file.Files
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.neo4j.commandline.dbms.LoadCommand
import org.neo4j.io.fs.DefaultFileSystemAbstraction

/**
 * Community's `neo4j-admin database load` loads DozenDB's full backups and refuses differential
 * ones, as it does Enterprise's.
 */
class CommunityLoadTest {
    // Kept when a test fails, for the servers' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private val fs = DefaultFileSystemAbstraction()

    private fun load(home: Path, vararg arguments: String) =
        Admin.run(home, ::LoadCommand, *arguments)

    private fun artifact(): Path =
        Files.createDirectories(root.resolve("backups"))
            .resolve("neo4j-2026-10-21T10-15-30${Artifacts.EXTENSION}")

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `Community loads a full backup and serves the same data`(compressed: Boolean) {
        val source = CommunityServer(root.resolve("source")).start()
        CommunityServer.populate(source)
        val expected = CommunityServer.summary(source)
        val header = source.fullHeader(compressed)
        source.stop()
        val artifact = artifact()
        Artifacts.writeFull(Files.newOutputStream(artifact), header, source.layout, fs)
        assertThat(Files.newInputStream(artifact).use { Artifacts.readHeader(it) })
            .isEqualTo(header)
        val target = CommunityServer(root.resolve("target"))

        val info = load(target.home, "--info", "--from-path=${artifact.parent}", "neo4j")
        assertThat(info.exitCode).describedAs(info.err).isZero
        assertThat(info.out).contains("Format: Neo4j Full Backup")
        val loaded = load(target.home, "--from-path=${artifact.parent}", "neo4j")
        assertThat(loaded.exitCode).describedAs(loaded.err).isZero

        target.start()
        assertThat(CommunityServer.summary(target)).isEqualTo(expected)
        target.stop()
        assertThat(target.consistent()).describedAs("consistent").isTrue
    }

    @Test
    fun `Community describes a differential backup and refuses to load it`() {
        val source = CommunityServer(root.resolve("source")).start()
        val after = source.lastCommitted().appendIndex()
        CommunityServer.populate(source)
        val transactions = source.transactionsAfter(after)
        val header = source.differentialHeader(transactions, compressed = true)
        source.stop()
        val artifact = artifact()
        Artifacts.writeDifferential(
            Files.newOutputStream(artifact),
            header,
            transactions.asSequence(),
            root.resolve("staging"),
            fs,
        )
        val target = CommunityServer(root.resolve("target"))

        val info = load(target.home, "--info", "--from-path=${artifact.parent}", "neo4j")
        assertThat(info.exitCode).describedAs(info.err).isZero
        assertThat(info.out)
            .contains("Format: Neo4j Differential Backup", "Files: ${transactions.size}\n")
        val fromPath = load(target.home, "--from-path=${artifact.parent}", "neo4j")
        assertThat(fromPath.exitCode).isNotZero
        assertThat(fromPath.err).contains("No matching archives found")
        val fromStdin =
            Files.newInputStream(artifact).use {
                Admin.run(target.home, ::LoadCommand, "--from-stdin", "neo4j", input = it)
            }
        assertThat(fromStdin.exitCode).isNotZero
        assertThat(fromStdin.err)
            .contains(
                "Loading of differential Neo4j backup is not supported. Use restore database instead."
            )
        assertThat(target.layout.databaseDirectory().resolve("neostore")).doesNotExist()
    }
}
