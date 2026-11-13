package dev.dodeca.backup

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.time.Instant
import java.time.ZoneOffset.UTC
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.neo4j.dbms.archive.ArchiveInput
import org.neo4j.dbms.archive.DumpZstdFormatV1
import org.neo4j.dbms.archive.StandardCompressionFormat
import org.neo4j.dbms.archive.backup.BackupDescription
import org.neo4j.dbms.archive.backup.BackupFormatSelector
import org.neo4j.dbms.archive.backup.BackupTarFormatV2
import org.neo4j.dbms.archive.backup.BackupZstdFormatV2
import org.neo4j.kernel.database.DatabaseIdFactory
import org.neo4j.storageengine.api.StoreId

class ArtifactContainerTest {
    private fun header(compressed: Boolean = true, full: Boolean = false) =
        ArtifactHeader(
            databaseName = "films",
            databaseId =
                DatabaseIdFactory.from(UUID.fromString("6a4b6c9e-0d2f-4c4b-9a52-6f0b5b8c1d2e")),
            storeId = StoreId(1_700_000_000_000L, 42L, "record", "aligned", 1, 1),
            time = Instant.parse("2026-10-21T10:15:30Z"),
            full = full,
            compressed = compressed,
            lowestAppendIndex = if (full) 1 else 101,
            highestAppendIndex = 250,
            previousChecksum = if (full) null else -123456789,
            lastChecksum = 987654321,
            lastCommitTime = Instant.parse("2026-10-21T10:15:29.123Z"),
            source = "member-2",
        )

    private fun artifact(
        header: ArtifactHeader,
        body: ByteArray = byteArrayOf(1, 2, 3),
    ): ByteArray {
        val out = ByteArrayOutputStream()
        ArtifactContainer.open(out, header).use { it.write(body) }
        return out.toByteArray()
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `reads back the header and the body`(compressed: Boolean) {
        val header = header(compressed)

        val opened = ArtifactContainer.read(ByteArrayInputStream(artifact(header)))

        assertThat(opened.header).isEqualTo(header)
        opened.body.use { assertThat(it.readAllBytes()).containsExactly(1, 2, 3) }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `Community reads the header`(compressed: Boolean) {
        val header = header(compressed, full = true)

        val description =
            BackupFormatSelector.readDescription(
                ArchiveInput.StreamInput.of(ByteArrayInputStream(artifact(header)), "artifact")
            )

        assertThat(description.databaseName).isEqualTo("films")
        assertThat(description.databaseId).isEqualTo(header.databaseId)
        assertThat(description.storeId).isEqualTo(header.storeId)
        assertThat(description.backupTime.toInstant(UTC)).isEqualTo(header.time)
        assertThat(description.isFull).isTrue
        assertThat(description.isRecovered).isTrue
        assertThat(description.isCompressed).isEqualTo(compressed)
        assertThat(description.lowestAppendIndex).isEqualTo(1)
        assertThat(description.highestAppendIndex).isEqualTo(250)
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `keeps the users and roles of the database, where Community keeps them`(
        compressed: Boolean
    ) {
        val script = "CREATE ROLE `curator` IF NOT EXISTS;\nGRANT ROLE `curator` TO `alice`;\n"
        val header = header(compressed, full = true).copy(metadataScript = script)

        val artifact = artifact(header)

        assertThat(ArtifactContainer.read(ByteArrayInputStream(artifact)).header).isEqualTo(header)
        assertThat(
                BackupFormatSelector.readDescription(
                        ArchiveInput.StreamInput.of(ByteArrayInputStream(artifact), "artifact")
                    )
                    .metadataScript
            )
            .isEqualTo(script)
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `without Dodeca's keys, the metadata is Community's byte for byte`(compressed: Boolean) {
        val header = header(compressed, full = true)
        val community =
            (if (compressed) BackupZstdFormatV2() else BackupTarFormatV2()).apply {
                setMetadata(
                    BackupDescription(
                        header.databaseName,
                        header.storeId,
                        header.databaseId,
                        header.time.atOffset(UTC).toLocalDateTime(),
                        true,
                        compressed,
                        true,
                        header.lowestAppendIndex,
                        header.highestAppendIndex,
                    )
                )
            }
        val expected = ByteArrayOutputStream().also { community.compress(it).close() }
        val actual =
            ByteArrayOutputStream().also { ArtifactContainer.open(it, header, emptyMap()).close() }

        assertThat(content(actual.toByteArray(), compressed))
            .isEqualTo(content(expected.toByteArray(), compressed))
    }

    // The magic and the metadata, decompressed: zstd's output may differ between runs.
    private fun content(artifact: ByteArray, compressed: Boolean): List<Byte> {
        if (!compressed) return artifact.toList()
        val input = ByteArrayInputStream(artifact)
        val magic = input.readNBytes(4)
        return (magic + StandardCompressionFormat.ZSTD.decompress(input).readAllBytes()).toList()
    }

    @Test
    fun `refuses what is not a Dodeca backup`() {
        val community =
            ByteArrayOutputStream().also { out ->
                BackupTarFormatV2()
                    .apply {
                        val header = header(compressed = false, full = true)
                        setMetadata(
                            BackupDescription(
                                header.databaseName,
                                header.storeId,
                                header.databaseId,
                                header.time.atOffset(UTC).toLocalDateTime(),
                                true,
                                false,
                                true,
                                1,
                                250,
                            )
                        )
                    }
                    .compress(out)
                    .close()
            }
        val dump = ByteArrayOutputStream().also { DumpZstdFormatV1().compress(it).close() }

        for ((bytes, message) in
            listOf(
                community.toByteArray() to "not a Dodeca backup: it has no dodeca.format",
                dump.toByteArray() to "a database dump, not a backup",
                "hello world".toByteArray() to "not a backup",
                "BZV1".toByteArray() to "not a Dodeca backup: unsupported backup format BZV1",
            )) {
            assertThatThrownBy { ArtifactContainer.read(ByteArrayInputStream(bytes)) }
                .isInstanceOf(IOException::class.java)
                .hasMessage(message)
        }
    }

    @Test
    fun `refuses a header that contradicts itself`() {
        assertThatThrownBy { header(full = true).copy(previousChecksum = 1) }
            .hasMessageContaining("only one, follows a previous checksum")
        assertThatThrownBy { header().copy(previousChecksum = null) }
            .hasMessageContaining("only one, follows a previous checksum")
        assertThatThrownBy { header(full = true).copy(lowestAppendIndex = 2) }
            .hasMessage("a full backup starts at transaction 1")
        assertThatThrownBy { header().copy(lowestAppendIndex = 300) }
            .hasMessage("invalid transactions 300 to 250")
        assertThatThrownBy { header().copy(time = Instant.parse("2026-10-21T10:15:30.5Z")) }
            .hasMessageContaining("is not in whole seconds")
        assertThatThrownBy {
                header().copy(lowestAppendIndex = 0, highestAppendIndex = 0, lastChecksum = null)
            }
            .hasMessage("an empty backup has no checksums and no last commit time")
    }
}
