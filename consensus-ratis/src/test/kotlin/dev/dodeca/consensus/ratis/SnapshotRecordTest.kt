package dev.dodeca.consensus.ratis

import dev.dodeca.consensus.LogPosition
import dev.dodeca.consensus.Snapshot
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class SnapshotRecordTest {
    @TempDir lateinit var directory: Path

    private val record = SnapshotRecord(Snapshot(LogPosition(42, 3), 40), installed = true)

    private fun files(): List<String> =
        Files.list(directory).use { files ->
            files.map { it.fileName.toString() }.sorted().toList()
        }

    @Test
    fun `a written record reads back and replaces the previous one`() {
        assertThat(SnapshotRecord.read(directory)).isNull()

        SnapshotRecord.write(directory, record.copy(installed = false))
        SnapshotRecord.write(directory, record)

        assertThat(SnapshotRecord.read(directory)).isEqualTo(record)
        assertThat(files()).containsExactly(SnapshotRecord.FILE_NAME)
    }

    @Test
    fun `a damaged record is refused`() {
        SnapshotRecord.write(directory, record)
        val file = directory.resolve(SnapshotRecord.FILE_NAME)
        val bytes = Files.readAllBytes(file)
        bytes[10] = (bytes[10] + 1).toByte()
        Files.write(file, bytes)

        assertThatThrownBy { SnapshotRecord.read(directory) }
            .isInstanceOf(IOException::class.java)
            .hasMessageContaining("checksum")
    }

    @Test
    fun `covered segments are discarded and the rest kept`() {
        listOf("log_0-9", "log_10-42", "log_43-50", "log_inprogress_51", "raft-meta").forEach {
            Files.createFile(directory.resolve(it))
        }

        SnapshotRecord.discardCoveredSegments(directory, 42)

        assertThat(files()).containsExactly("log_43-50", "log_inprogress_51", "raft-meta")
    }

    @Test
    fun `an open segment that starts at or before the snapshot is discarded`() {
        listOf("log_0-9", "log_inprogress_10").forEach { Files.createFile(directory.resolve(it)) }

        SnapshotRecord.discardCoveredSegments(directory, 42)

        assertThat(files()).isEmpty()
    }
}
