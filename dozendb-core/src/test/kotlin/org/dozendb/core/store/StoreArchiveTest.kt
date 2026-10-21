package org.dozendb.core.store

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID
import kotlin.random.Random
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.neo4j.io.fs.DefaultFileSystemAbstraction

class StoreArchiveTest {
    @TempDir lateinit var root: Path
    private val fs = DefaultFileSystemAbstraction()

    private fun sourceFile(relative: String, size: Int): Path =
        root.resolve("source").resolve(relative).also {
            Files.createDirectories(it.parent)
            Files.write(it, Random(relative.hashCode()).nextBytes(size))
        }

    private val metadata = byteArrayOf(4, 2)

    private fun archive(build: StoreArchive.Writer.() -> Unit): ByteArray {
        val bytes = ByteArrayOutputStream()
        StoreArchive.Writer(bytes, fs).apply {
            header("neo4j", DATABASE_ID, "test copy", metadata)
            build()
        }
        return bytes.toByteArray()
    }

    private fun extract(bytes: ByteArray): StoreArchive.Summary {
        val database = Files.createDirectories(root.resolve("target/database"))
        val logs = Files.createDirectories(root.resolve("target/logs"))
        return StoreArchive.read(ByteArrayInputStream(bytes), fs, database, logs)
    }

    @Test
    fun `files arrive under their roots with their contents`() {
        val store = sourceFile("neostore", 3_000_000)
        val index = sourceFile("schema/index/1/index-1", 10_000)
        val empty = sourceFile("empty", 0)
        val log = sourceFile("neostore.transaction.db.0", 70_000)

        val summary =
            extract(
                archive {
                    file(StoreArchive.Root.DATABASE, Path.of("neostore"), store)
                    file(
                        StoreArchive.Root.DATABASE,
                        Path.of("schema", "index", "1", "index-1"),
                        index,
                    )
                    file(StoreArchive.Root.DATABASE, Path.of("empty"), empty)
                    file(
                        StoreArchive.Root.TRANSACTION_LOGS,
                        Path.of("neostore.transaction.db.0"),
                        log,
                    )
                    end(byteArrayOf(7))
                }
            )

        assertThat(summary.database).isEqualTo("neo4j")
        assertThat(summary.databaseId).isEqualTo(DATABASE_ID)
        assertThat(summary.description).isEqualTo("test copy")
        assertThat(summary.metadata).containsExactly(4, 2)
        assertThat(summary.trailer).containsExactly(7)
        assertThat(summary.files).isEqualTo(4)
        assertThat(summary.bytes).isEqualTo(3_080_000)
        assertThat(root.resolve("target/database/neostore")).hasSameBinaryContentAs(store)
        assertThat(root.resolve("target/database/schema/index/1/index-1"))
            .hasSameBinaryContentAs(index)
        assertThat(root.resolve("target/database/empty")).isEmptyFile()
        assertThat(root.resolve("target/logs/neostore.transaction.db.0"))
            .hasSameBinaryContentAs(log)
    }

    @Test
    fun `runs of zeros, as in preallocated transaction logs, are sent compactly`() {
        val log = root.resolve("source").resolve("neostore.transaction.db.0")
        Files.createDirectories(log.parent)
        val content = ByteArray(8_000_000)
        Random(7).nextBytes(100_000).copyInto(content, destinationOffset = 0)
        Random(8).nextBytes(10).copyInto(content, destinationOffset = 5_000_000)
        Files.write(log, content)

        val bytes = archive {
            file(StoreArchive.Root.TRANSACTION_LOGS, Path.of("neostore.transaction.db.0"), log)
            end()
        }
        val summary = extract(bytes)

        assertThat(bytes.size).isLessThan(300_000)
        assertThat(summary.bytes).isEqualTo(8_000_000)
        assertThat(root.resolve("target/logs/neostore.transaction.db.0"))
            .hasSameBinaryContentAs(log)
    }

    @Test
    fun `a stream that ends early is rejected`() {
        val store = sourceFile("neostore", 100_000)
        val bytes = archive {
            file(StoreArchive.Root.DATABASE, Path.of("neostore"), store)
            end()
        }

        assertThatThrownBy { extract(bytes.copyOf(bytes.size - 20)) }
            .isInstanceOf(IOException::class.java)
        assertThatThrownBy { extract(bytes.copyOf(50_000)) }.isInstanceOf(IOException::class.java)
    }

    @Test
    fun `totals that disagree with the files are rejected`() {
        val bytes = archive {}
        val forged = ByteArrayOutputStream()
        DataOutputStream(forged).use {
            it.write(bytes)
            it.writeByte(0)
            it.writeInt(1)
            it.writeLong(10)
        }

        assertThatThrownBy { extract(forged.toByteArray()) }
            .isInstanceOf(IOException::class.java)
            .hasMessageContaining("incomplete")
    }

    @Test
    fun `paths that leave the target directory are rejected`() {
        val bytes = archive {}
        listOf("../escape", "a/../../b", "/absolute", "a//b", "c:\\x").forEach { path ->
            val forged = ByteArrayOutputStream()
            DataOutputStream(forged).use {
                it.write(bytes)
                it.writeByte(1)
                it.writeByte(0)
                it.writeUTF(path)
                it.writeLong(0)
            }

            assertThatThrownBy { extract(forged.toByteArray()) }
                .describedAs(path)
                .isInstanceOf(IOException::class.java)
                .hasMessageContaining("invalid path")
        }
    }

    @Test
    fun `sends the first bytes of a file, as a transaction log cut at a transaction`() {
        val log = sourceFile("neostore.transaction.db.3", 50_000)

        val summary =
            extract(
                archive {
                    file(
                        StoreArchive.Root.TRANSACTION_LOGS,
                        Path.of("neostore.transaction.db.3"),
                        log,
                        1234,
                    )
                    end()
                }
            )

        assertThat(summary.bytes).isEqualTo(1234)
        assertThat(root.resolve("target/logs/neostore.transaction.db.3"))
            .hasBinaryContent(Files.readAllBytes(log).copyOf(1234))
        assertThat(summary.trailer).isEmpty()
    }

    private companion object {
        val DATABASE_ID: UUID = UUID.randomUUID()
    }
}
