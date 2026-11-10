package org.dozendb.cloud.internal

import java.io.ByteArrayOutputStream
import java.io.IOException
import java.net.URI
import java.nio.ByteBuffer
import java.nio.file.DirectoryNotEmptyException
import java.nio.file.FileAlreadyExistsException
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.nio.file.StandardOpenOption.APPEND
import java.nio.file.StandardOpenOption.CREATE
import java.nio.file.StandardOpenOption.CREATE_NEW
import java.nio.file.StandardOpenOption.WRITE
import java.nio.file.attribute.FileTime
import java.time.Instant
import kotlin.random.Random
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.dozendb.cloud.CloudStorageSettings
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.neo4j.cloud.storage.SchemeFileSystemAbstraction
import org.neo4j.cloud.storage.StoragePath
import org.neo4j.cloud.storage.StorageSystemProviderFactory
import org.neo4j.cloud.storage.StorageSystemProviderFactory.ChunkChannelSupplier
import org.neo4j.cloud.storage.io.PathBasedOutputStream
import org.neo4j.configuration.Config
import org.neo4j.io.ByteUnit.mebiBytes
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import org.neo4j.logging.InternalLogProvider
import org.neo4j.logging.NullLogProvider
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.MemoryTracker

/**
 * Neo4j's view of a cloud's objects as files, through its storage interface, on objects in memory.
 */
class ObjectStorageProviderTest {
    private val store = MemoryStore()
    private val config =
        Config.newBuilder()
            .set(CloudStorageSettings.s3PullChunk, mebiBytes(1))
            .set(CloudStorageSettings.s3PushChunk, mebiBytes(1))
            .build()
    private val factory =
        object : StorageSystemProviderFactory("s3") {
            override fun storageSystemProviderClass() = ObjectStorageProvider::class.java.name

            override fun createStorageSystemProvider(
                chunks: ChunkChannelSupplier,
                config: Config,
                logs: InternalLogProvider,
                memory: MemoryTracker,
                classLoader: ClassLoader,
            ) = ObjectStorageProvider(store, chunks, config, logs, memory)
        }
    private val fs =
        SchemeFileSystemAbstraction(
            DefaultFileSystemAbstraction(),
            listOf(factory),
            config,
            NullLogProvider.getInstance(),
            EmptyMemoryTracker.INSTANCE,
        )

    @TempDir lateinit var local: Path

    @AfterEach fun close() = fs.close()

    private fun path(uri: String) = fs.resolve(uri) as StoragePath

    private fun bytes(size: Int) = Random(size).nextBytes(size)

    private fun write(uri: String, data: ByteArray) =
        fs.openAsOutputStream(path(uri), false).use { it.write(data) }

    private fun read(uri: String) = fs.openAsInputStream(path(uri)).use { it.readAllBytes() }

    @Test
    fun `URIs name a bucket and a key, and folders end with a slash`() {
        val dump = path("s3://films/dumps/neo4j.dump")

        assertThat(ObjectStorageProvider.key(dump)).isEqualTo("dumps/neo4j.dump")
        assertThat(dump.toUri()).isEqualTo(URI("s3://films/dumps/neo4j.dump"))
        assertThat(dump.isDirectory).isFalse()
        assertThat(path("s3://films/dumps/").isDirectory).isTrue()
        assertThat(ObjectStorageProvider.key(path("s3://films"))).isEmpty()
        assertThat(ObjectStorageProvider.key(path("s3://films/a/../b.csv"))).isEqualTo("b.csv")
        assertThat(ObjectStorageProvider.key(path("s3://films/my%20file.csv")))
            .isEqualTo("my file.csv")
        assertThat(dump.fileSystem).isSameAs(path("s3://films/other").fileSystem)
        assertThat(dump.fileSystem).isNotSameAs(path("s3://series/other").fileSystem)
    }

    @Test
    fun `S3 and Google name buckets, Azure storage accounts and containers`() {
        assertThat(Location.bucket(URI("gs://films/a/b.csv")))
            .isEqualTo(Location("films", "/a/b.csv"))
        assertThat(Location.bucket(URI("s3://films"))).isEqualTo(Location("films", "/"))
        assertThatThrownBy { Location.bucket(URI("s3:///b.csv")) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessage("s3:///b.csv names no bucket: s3://<bucket>/<key>")
        assertThat(Location.accountContainer(URI("azb://studio/films/a/b.csv")))
            .isEqualTo(Location("studio/films", "/a/b.csv"))
        assertThat(Location.accountContainer(URI("azb://studio/films")))
            .isEqualTo(Location("studio/films", "/"))
        assertThat(Location.accountContainer(URI("azb://studio/films/")))
            .isEqualTo(Location("studio/films", "/"))
        assertThatThrownBy { Location.accountContainer(URI("azb://studio/")) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessage(
                "azb://studio/ names no storage account and container: " +
                    "azb://<storage account>/<container>/<key>"
            )
    }

    @Test
    fun `objects no bigger than a part are written in one request`() {
        write("s3://films/small.csv", "a,b\n".toByteArray())
        write("s3://films/one-part.bin", bytes(1024))

        assertThat(read("s3://films/small.csv")).isEqualTo("a,b\n".toByteArray())
        assertThat(read("s3://films/one-part.bin")).isEqualTo(bytes(1024))
        assertThat(store.puts).hasValue(2)
        assertThat(store.uploads).hasValue(0)
        assertThat(Files.size(path("s3://films/small.csv"))).isEqualTo(4)
    }

    @Test
    fun `an empty object is written and read`() {
        fs.openAsOutputStream(path("s3://films/empty"), false).close()

        assertThat(store.data("films", "empty")).isEmpty()
        assertThat(read("s3://films/empty")).isEmpty()
        Files.newByteChannel(path("s3://films/empty")).use {
            assertThat(it.size()).isZero()
            assertThat(it.read(ByteBuffer.allocate(1))).isEqualTo(-1)
        }
    }

    @Test
    fun `bigger objects are sent in parts and appear once the writer closes`() {
        val data = bytes(10_000)
        val out = fs.openAsOutputStream(path("s3://films/big.bin"), false)
        var offset = 0
        for (size in generateSequence(1) { it * 3 }) {
            val count = minOf(size, data.size - offset)
            out.write(data, offset, count)
            offset += count
            if (offset == data.size) break
        }

        assertThat(store.head("films", "big.bin")).isNull()
        out.close()
        assertThat(store.data("films", "big.bin")).isEqualTo(data)
        assertThat(store.uploads).hasValue(1)
        assertThat(store.partsSent).hasValue(10)
        assertThat(store.puts).hasValue(0)
    }

    @Test
    fun `an object of exactly two parts is sent in two`() {
        write("s3://films/two.bin", bytes(2048))

        assertThat(store.data("films", "two.bin")).isEqualTo(bytes(2048))
        assertThat(store.partsSent).hasValue(2)
    }

    @Test
    fun `a part that fails gives the upload up and fails the close`() {
        store.failingPart = 3

        assertThatThrownBy { write("s3://films/big.bin", bytes(10_000)) }
            .isInstanceOf(IOException::class.java)
            .hasMessage("part 3 failed")
        assertThat(store.aborted).hasValue(1)
        assertThat(store.head("films", "big.bin")).isNull()
    }

    @Test
    fun `reads fetch ranges ahead and seek within and beyond them`() {
        val data = bytes((5 shl 20) + 123)
        store.store("films", "big.bin", data)

        Files.newByteChannel(path("s3://films/big.bin")).use { channel ->
            fun readAt(position: Long, count: Int): ByteArray {
                channel.position(position)
                val buffer = ByteBuffer.allocate(count)
                while (buffer.hasRemaining() && channel.read(buffer) >= 0) {
                    // Until the buffer is full or the object ends.
                }
                return buffer.array().copyOf(buffer.position())
            }

            assertThat(channel.size()).isEqualTo(data.size.toLong())
            assertThat(readAt(0, 100)).isEqualTo(data.copyOfRange(0, 100))
            val far = (3L shl 20) + 7
            assertThat(readAt(far, 1000))
                .isEqualTo(data.copyOfRange(far.toInt(), far.toInt() + 1000))
            assertThat(readAt(10, 2 shl 20)).isEqualTo(data.copyOfRange(10, 10 + (2 shl 20)))
            assertThat(readAt(data.size - 10L, 100))
                .isEqualTo(data.copyOfRange(data.size - 10, data.size))
            assertThat(channel.read(ByteBuffer.allocate(1))).isEqualTo(-1)
        }
        // Ranges of 1 MiB, four ahead: the far seek lands in one fetched already, the seek back
        // fetches anew from where it lands.
        assertThat(store.ranges).allMatch { it.last - it.first < (1 shl 20) }
        assertThat(store.ranges.map { it.first })
            .contains(0, 10)
            .doesNotContain((3L shl 20) + 7)
    }

    @Test
    fun `a whole object copies to a stream and to a local file`() {
        val data = bytes((3 shl 20) + 5)
        store.store("films", "big.bin", data)

        val out = ByteArrayOutputStream()
        fs.openAsInputStream(path("s3://films/big.bin")).use { it.transferTo(out) }
        val file = local.resolve("copy.bin")
        fs.openAsInputStream(path("s3://films/big.bin")).use {
            it.transferTo(PathBasedOutputStream(file))
        }
        Files.copy(path("s3://films/big.bin"), local.resolve("files-copy.bin"))

        assertThat(out.toByteArray()).isEqualTo(data)
        assertThat(file).hasBinaryContent(data)
        assertThat(local.resolve("files-copy.bin")).hasBinaryContent(data)
    }

    @Test
    fun `an object copied to another of the same cloud is copied by the cloud`() {
        store.store("films", "big.bin", bytes(10_000))

        fs.openAsInputStream(path("s3://films/big.bin")).use { input ->
            fs.openAsOutputStream(path("s3://series/copy.bin"), false).use { input.transferTo(it) }
        }
        Files.copy(path("s3://films/big.bin"), path("s3://films/copy.bin"))

        assertThat(store.data("series", "copy.bin")).isEqualTo(bytes(10_000))
        assertThat(store.data("films", "copy.bin")).isEqualTo(bytes(10_000))
        assertThat(store.copies).hasValue(2)
        assertThat(store.partsSent).hasValue(0)
        assertThat(store.puts).hasValue(0)
    }

    @Test
    fun `an object that changes while it is read fails the read`() {
        val data = bytes((6 shl 20))
        store.store("films", "big.bin", data)

        fs.openAsInputStream(path("s3://films/big.bin")).use { input ->
            assertThat(input.read()).isEqualTo(data[0].toInt() and 0xFF)
            store.store("films", "big.bin", bytes(10))

            assertThatThrownBy { input.readAllBytes() }
                .isInstanceOf(IOException::class.java)
                .hasMessage("s3://films/big.bin changed while it was read")
        }
    }

    @Test
    fun `folders list their objects and folders, without their markers`() {
        for (key in listOf("a.csv", "b/c.csv", "b/d/e.csv", "b/", "bb.csv")) {
            store.store("films", key, bytes(3))
        }

        fun listed(uri: String) =
            fs.listFiles(path(uri)).map { it as StoragePath }.map { it.toUri().toString() }

        assertThat(listed("s3://films/"))
            .containsExactly("s3://films/a.csv", "s3://films/b/", "s3://films/bb.csv")
        assertThat(listed("s3://films/b/")).containsExactly("s3://films/b/c.csv", "s3://films/b/d/")
        assertThat(listed("s3://films/b")).containsExactly("s3://films/b/c.csv", "s3://films/b/d/")
        assertThat(listed("s3://films/none/")).isEmpty()
        assertThat(fs.listFiles(path("s3://films/")) { it.toString().endsWith(".csv") }).hasSize(2)
    }

    @Test
    fun `objects have a size and a time, folders exist, other paths do not`() {
        store.store("films", "a.csv", bytes(42))
        store.store("films", "b/c.csv", bytes(3))
        val object_ = path("s3://films/a.csv")

        assertThat(Files.size(object_)).isEqualTo(42)
        assertThat(Files.getLastModifiedTime(object_))
            .isEqualTo(FileTime.from(Instant.ofEpochSecond(1_700_000_000)))
        assertThat(Files.isRegularFile(object_)).isTrue()
        assertThat(Files.isDirectory(path("s3://films/b"))).isTrue()
        assertThat(Files.isDirectory(path("s3://films/nothing/"))).isTrue()
        assertThat(Files.exists(path("s3://films/missing.csv"))).isFalse()
        assertThat(fs.fileExists(object_)).isTrue()
        assertThat(Files.readAttributes(object_, "size,isDirectory"))
            .isEqualTo(mapOf("size" to 42L, "isDirectory" to false))
        assertThat(Files.readAttributes(object_, "basic:*")).containsEntry("isRegularFile", true)
        assertThatThrownBy { fs.openAsInputStream(path("s3://films/missing.csv")) }
            .isInstanceOf(NoSuchFileException::class.java)
            .hasMessage("s3://films/missing.csv")
    }

    @Test
    fun `deleting an object, a folder of objects, and what is not there`() {
        for (key in listOf("a.csv", "b/c.csv", "b/d/e.csv", "b/")) {
            store.store("films", key, bytes(3))
        }

        Files.delete(path("s3://films/a.csv"))
        assertThatThrownBy { Files.delete(path("s3://films/a.csv")) }
            .isInstanceOf(NoSuchFileException::class.java)
        assertThatThrownBy { Files.delete(path("s3://films/b/")) }
            .isInstanceOf(DirectoryNotEmptyException::class.java)
        assertThatThrownBy { Files.delete(path("s3://films/b")) }
            .isInstanceOf(DirectoryNotEmptyException::class.java)
        fs.deleteRecursively(path("s3://films/b/"))

        assertThat(store.objects).isEmpty()
    }

    @Test
    fun `writes create or replace objects, and never append`() {
        store.store("films", "a.csv", bytes(3))
        val existing = path("s3://films/a.csv")

        assertThatThrownBy { Files.newOutputStream(existing, CREATE_NEW) }
            .isInstanceOf(FileAlreadyExistsException::class.java)
        assertThatThrownBy { Files.newOutputStream(existing, CREATE, APPEND) }
            .isInstanceOf(IOException::class.java)
            .hasMessage("Objects cannot be appended to: s3://films/a.csv")
        assertThatThrownBy { Files.newOutputStream(path("s3://films/missing.csv"), WRITE) }
            .isInstanceOf(NoSuchFileException::class.java)
        assertThatThrownBy { Files.copy(path("s3://films/a.csv"), existing) }
            .isInstanceOf(FileAlreadyExistsException::class.java)
        store.store("films", "b.csv", bytes(5))
        Files.copy(path("s3://films/b.csv"), existing, REPLACE_EXISTING)
        assertThat(store.data("films", "a.csv")).isEqualTo(bytes(5))
        Files.move(path("s3://films/b.csv"), path("s3://films/c.csv"))
        assertThat(store.objects.keys).containsExactly("films/a.csv", "films/c.csv")
    }
}
