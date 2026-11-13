package dev.dodeca.cloud.internal

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.util.UUID
import java.util.concurrent.CompletableFuture
import kotlin.random.Random
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.neo4j.cloud.storage.SchemeFileSystemAbstraction
import org.neo4j.cloud.storage.StoragePath
import org.neo4j.cloud.storage.StorageSystemProviderFactory
import org.neo4j.cloud.storage.StorageSystemProviderFactory.ChunkChannelSupplier
import org.neo4j.configuration.Config
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import org.neo4j.logging.InternalLogProvider
import org.neo4j.logging.NullLogProvider
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.MemoryTracker

/**
 * What every cloud's store does, run against the cloud's emulator by each subclass: its store lists
 * two entries a page, and [container] exists, [missingContainer] does not.
 */
abstract class ObjectStoreContract {
    internal abstract val store: ObjectStore
    protected abstract val container: String
    protected abstract val missingContainer: String

    /** The URI of [key] in [container]. */
    private fun uri(key: String) = "${store.scheme}://$container/$key"

    /** Keys of one test, apart from other tests' in the shared container. */
    private val prefix = "test-${UUID.randomUUID()}/"

    @TempDir lateinit var local: Path

    private fun bytes(size: Int) = Random(size).nextBytes(size)

    private fun put(key: String, data: ByteArray) =
        store.put(container, prefix + key, ByteBuffer.wrap(data))

    private fun readAll(key: String): ByteArray {
        val info = store.head(container, prefix + key)!!
        val data = store.read(container, prefix + key, info.version, 0, info.size)
        return ByteArray(data.remaining()).also { data.get(it) }
    }

    @Test
    fun `an object is written, described, read in ranges and deleted`() {
        put("a.txt", "0123456789".toByteArray())

        val info = store.head(container, prefix + "a.txt")!!
        assertThat(info.size).isEqualTo(10)
        assertThat(info.version).isNotEmpty()
        val range = store.read(container, prefix + "a.txt", info.version, 3, 7)
        assertThat(ByteArray(range.remaining()).also { range.get(it) })
            .isEqualTo("3456".toByteArray())
        store.delete(container, prefix + "a.txt")
        assertThat(store.head(container, prefix + "a.txt")).isNull()
        store.delete(container, prefix + "a.txt")
    }

    @Test
    fun `missing objects and containers are missing, with their URIs`() {
        assertThat(store.head(container, prefix + "missing")).isNull()
        assertThatThrownBy { store.read(container, prefix + "missing", null, 0, 1) }
            .isInstanceOf(NoSuchFileException::class.java)
            .hasMessageStartingWith(uri(prefix + "missing"))
        assertThatThrownBy { store.put(missingContainer, "a.txt", ByteBuffer.wrap(byteArrayOf(1))) }
            .isInstanceOf(NoSuchFileException::class.java)
            .hasMessageStartingWith("${store.scheme}://$missingContainer/a.txt")
    }

    @Test
    fun `an upload sends parts in any order and the object appears when completed`() {
        val sizes = listOf(store.partSize(0), store.partSize(1), 1234)
        val parts = sizes.mapIndexed { index, size -> Random(index).nextBytes(size) }

        val upload = store.upload(container, prefix + "big.bin")
        if (store.uploadConcurrency > 1) {
            parts.indices
                .reversed()
                .map { index ->
                    CompletableFuture.runAsync {
                        upload.part(index, ByteBuffer.wrap(parts[index]))
                    }
                }
                .forEach { it.join() }
        } else {
            parts.forEachIndexed { index, part -> upload.part(index, ByteBuffer.wrap(part)) }
        }
        assertThat(store.head(container, prefix + "big.bin")).isNull()
        upload.complete(parts.size)

        assertThat(readAll("big.bin")).isEqualTo(parts.reduce { a, b -> a + b })
    }

    @Test
    fun `an upload given up leaves no object`() {
        val upload = store.upload(container, prefix + "abandoned.bin")
        upload.part(0, ByteBuffer.wrap(bytes(store.partSize(0))))
        upload.abort()

        assertThat(store.head(container, prefix + "abandoned.bin")).isNull()
    }

    @Test
    fun `listings page through objects and folders in key order`() {
        for (key in listOf("a", "b/1", "b/2", "c", "d/1", "d/e/1", "e")) put("l/$key", bytes(3))

        fun listed(under: String) =
            store.list(container, prefix + under).asSequence().toList().map {
                it.key.removePrefix(prefix) + if (it is Listed.Folder) " (folder)" else ""
            }

        assertThat(listed("l/"))
            .containsExactly("l/a", "l/b/ (folder)", "l/c", "l/d/ (folder)", "l/e")
        assertThat(listed("l/d/")).containsExactly("l/d/1", "l/d/e/ (folder)")
        assertThat(listed("none/")).isEmpty()
        val listed = store.list(container, prefix + "l/").next() as Listed.Object
        assertThat(listed.info.size).isEqualTo(3)
    }

    @Test
    fun `reading a version that was replaced fails`() {
        put("v.txt", "first".toByteArray())
        val first = store.head(container, prefix + "v.txt")!!.version
        put("v.txt", "second".toByteArray())

        assertThatThrownBy { store.read(container, prefix + "v.txt", first, 0, 5) }
            .isInstanceOf(IOException::class.java)
            .hasMessageStartingWith("${uri(prefix + "v.txt")} changed while it was read")
    }

    @Test
    fun `objects are copied within the cloud`() {
        put("from.bin", bytes(10_000))

        store.copy(container, prefix + "from.bin", container, prefix + "to.bin")

        assertThat(readAll("to.bin")).isEqualTo(bytes(10_000))
    }

    @Test
    fun `Neo4j writes, reads, seeks, lists and copies objects through the provider`() {
        val factory =
            object : StorageSystemProviderFactory(store.scheme) {
                override fun storageSystemProviderClass() = ObjectStorageProvider::class.java.name

                override fun createStorageSystemProvider(
                    chunks: ChunkChannelSupplier,
                    config: Config,
                    logs: InternalLogProvider,
                    memory: MemoryTracker,
                    classLoader: ClassLoader,
                ) = ObjectStorageProvider(store, chunks, config, logs, memory)
            }
        val data = bytes(store.partSize(0) * 2 + 4321)
        val file = local.resolve("data.bin").also { Files.write(it, data) }
        SchemeFileSystemAbstraction(
                DefaultFileSystemAbstraction(),
                listOf(factory),
                Config.defaults(),
                NullLogProvider.getInstance(),
                EmptyMemoryTracker.INSTANCE,
            )
            .use { fs ->
                val folder = fs.resolve(uri(prefix + "files/")) as StoragePath
                val target = folder.resolve("data.bin")
                Files.copy(file, target)
                fs.openAsOutputStream(folder.resolve("small.txt"), false).use {
                    it.write("small".toByteArray())
                }

                assertThat(fs.listFiles(folder).map { it.fileName.toString() })
                    .containsExactly("data.bin", "small.txt")
                assertThat(Files.size(target)).isEqualTo(data.size.toLong())
                Files.newByteChannel(target).use { channel ->
                    val at = data.size - 100_000L
                    channel.position(at)
                    val tail = ByteBuffer.allocate(100_000)
                    while (tail.hasRemaining() && channel.read(tail) >= 0) {
                        // Until the end.
                    }
                    assertThat(tail.array()).isEqualTo(data.copyOfRange(at.toInt(), data.size))
                }
                Files.copy(target, local.resolve("back.bin"))
                assertThat(local.resolve("back.bin")).hasBinaryContent(data)
                Files.copy(target, folder.resolve("copy.bin"))
                assertThat(Files.size(folder.resolve("copy.bin"))).isEqualTo(data.size.toLong())
                fs.deleteRecursively(folder)
                assertThat(fs.listFiles(folder)).isEmpty()
            }
    }
}
