package dev.dodeca.backup.cli

import dev.dodeca.backup.Admin
import dev.dodeca.backup.Artifacts
import dev.dodeca.backup.PhasedSource
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class InspectBackupCommandTest {
    private lateinit var root: Path
    private lateinit var source: PhasedSource

    @BeforeAll
    fun writeSource(
        // Kept when a test fails, for the servers' logs.
        @TempDir(cleanup = CleanupMode.ON_SUCCESS) root: Path
    ) {
        this.root = root
        source = PhasedSource.write(root)
    }

    private fun inspect(vararg arguments: String): List<String> {
        val result = Admin.run(root, ::InspectBackupCommand, *arguments)
        assertThat(result.exitCode).describedAs("$result").isZero
        return result.out.lines().filter { it.isNotEmpty() }
    }

    /** The cells of a table's lines, after its titles. */
    private fun rows(lines: List<String>): List<List<String>> {
        val cells = lines.map { line -> line.trim('|').split("|").map { it.trim() } }
        assertThat(cells.first().first()).isEqualTo("FILE")
        return cells.drop(1)
    }

    private fun uri(path: Path) = path.toAbsolutePath().toUri().toString()

    private fun header(path: Path) = path.toFile().inputStream().use(Artifacts::readHeader)

    @Test
    fun `lists the backups with transactions, latest first`() {
        val lines = inspect("${source.backups}")

        assertThat(rows(lines))
            .containsExactly(
                listOf(uri(source.second)),
                listOf(uri(source.first)),
                listOf(uri(source.full)),
            )
        // Aligned as Enterprise's table.
        assertThat(lines.map { it.length }.distinct()).hasSize(1)
        assertThat(lines.first()).matches("\\| +FILE \\|")
    }

    @Test
    fun `shows what the headers tell, empty backups too if asked`() {
        val rows = rows(inspect("${source.backups}", "--show-metadata", "--empty"))

        val full = header(source.full)
        assertThat(rows)
            .contains(
                listOf(
                    uri(source.full),
                    "neo4j",
                    full.databaseId.uuid().toString(),
                    full.time.toString().removeSuffix("Z"),
                    "true",
                    "true",
                    "1",
                    "${full.highestAppendIndex}",
                    "true",
                )
            )
        assertThat(rows.single { it[0] == uri(source.empty) }.drop(4))
            .containsExactly("false", "true", "0", "0", "false")
        assertThat(rows).hasSize(4)
    }

    @Test
    fun `shows the latest backup or chain`() {
        assertThat(rows(inspect("${source.backups}", "--latest-backup")))
            .containsExactly(listOf(uri(source.second)))
        assertThat(rows(inspect("${source.backups}", "--latest-chain", "--database=neo4j")))
            .containsExactly(
                listOf(uri(source.second)),
                listOf(uri(source.first)),
                listOf(uri(source.full)),
            )
        assertThat(rows(inspect("${source.first}", "--latest-chain")))
            .containsExactly(listOf(uri(source.first)), listOf(uri(source.full)))
    }

    @Test
    fun `writes JSON`() {
        val first = header(source.first)

        val json = inspect("${source.first}", "--show-metadata", "--format=JSON").single()

        assertThat(json)
            .isEqualTo(
                "[{\"file\":\"${uri(source.first)}\",\"database\":\"neo4j\"," +
                    "\"databaseId\":\"${first.databaseId.uuid()}\"," +
                    "\"time\":\"${first.time.toString().removeSuffix("Z")}\",\"full\":false," +
                    "\"compressed\":true,\"lowestTx\":${first.lowestAppendIndex}," +
                    "\"highestTx\":${first.highestAppendIndex},\"recovered\":false}]"
            )
    }
}
