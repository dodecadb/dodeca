package dev.dodeca.backup

import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.neo4j.io.fs.DefaultFileSystemAbstraction

class ArtifactsTest {
    @TempDir lateinit var directory: Path
    private val fs = DefaultFileSystemAbstraction()
    private val time = Instant.parse("2026-10-21T10:15:30Z")

    private fun files(): List<String> =
        Files.list(directory).use { files ->
            files.map { it.fileName.toString() }.sorted().toList()
        }

    @Test
    fun `a new backup gets its name once written, and the next one of that second the next name`() {
        val first =
            Artifacts.writeNew(directory, "neo4j", time, fs) { it.use { out -> out.write(1) } }
        val second =
            Artifacts.writeNew(directory, "neo4j", time, fs) { it.use { out -> out.write(2) } }

        assertThat(first.fileName.toString()).isEqualTo("neo4j-2026-10-21T10-15-30.backup")
        assertThat(second.fileName.toString()).isEqualTo("neo4j-2026-10-21T10-15-30-2.backup")
        assertThat(Files.readAllBytes(second)).containsExactly(2)
        assertThat(files())
            .containsExactlyInAnyOrder(first.fileName.toString(), second.fileName.toString())
    }

    @Test
    fun `a backup that fails to be written leaves nothing behind`() {
        assertThatThrownBy {
                Artifacts.writeNew(directory, "neo4j", time, fs) {
                    it.use { out -> out.write(1) }
                    throw IOException("disk full")
                }
            }
            .hasMessage("disk full")

        assertThat(files()).isEmpty()
    }
}
