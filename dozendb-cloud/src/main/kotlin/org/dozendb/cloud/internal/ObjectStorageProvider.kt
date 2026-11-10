package org.dozendb.cloud.internal

import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.io.UncheckedIOException
import java.net.URI
import java.nio.channels.SeekableByteChannel
import java.nio.file.AccessMode
import java.nio.file.CopyOption
import java.nio.file.DirectoryIteratorException
import java.nio.file.DirectoryNotEmptyException
import java.nio.file.DirectoryStream
import java.nio.file.FileAlreadyExistsException
import java.nio.file.LinkOption
import java.nio.file.NoSuchFileException
import java.nio.file.OpenOption
import java.nio.file.Path
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.nio.file.StandardOpenOption.APPEND
import java.nio.file.StandardOpenOption.CREATE
import java.nio.file.StandardOpenOption.CREATE_NEW
import java.nio.file.StandardOpenOption.WRITE
import java.nio.file.attribute.BasicFileAttributeView
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileAttribute
import java.nio.file.attribute.FileAttributeView
import java.nio.file.attribute.FileTime
import java.time.Instant
import org.neo4j.cloud.storage.StoragePath
import org.neo4j.cloud.storage.StoragePathAttributes
import org.neo4j.cloud.storage.StoragePathAttributesView
import org.neo4j.cloud.storage.StorageSystem
import org.neo4j.cloud.storage.StorageSystemProvider
import org.neo4j.cloud.storage.StorageSystemProviderFactory.ChunkChannelSupplier
import org.neo4j.cloud.storage.io.SeekableReadableChannel
import org.neo4j.cloud.storage.io.SeekableWriteableChannel
import org.neo4j.configuration.Config
import org.neo4j.logging.InternalLogProvider
import org.neo4j.memory.MemoryTracker

/**
 * Neo4j's view of a cloud's objects as files, for the scheme of [store]: one storage system per
 * container, folders made of the keys' prefixes up to a `/`. Folders always exist, as in Neo4j
 * Enterprise, since clouds keep no empty folders: creating one does nothing, and deleting one only
 * checks that no object is in it. Reads fetch ranges of the object, a few at once, and writes send
 * it in parts, so that neither keeps a whole object in memory or on disk.
 */
internal open class ObjectStorageProvider(
    private val store: ObjectStore,
    chunks: ChunkChannelSupplier,
    config: Config,
    logs: InternalLogProvider,
    memory: MemoryTracker,
) : StorageSystemProvider(store.scheme, chunks, config, logs, memory) {
    private val log = logs.getLog(javaClass)

    override fun create(storageUri: URI): StorageSystem =
        ObjectStorageSystem(this, store.locate(storageUri).container)

    override fun resolve(uri: URI): StorageLocation {
        val location = store.locate(uri)
        return StorageLocation(URI.create("$scheme://${location.container}"), location.path)
    }

    override fun openAsInputStream(path: StoragePath): InputStream {
        val info = info(path)
        if (info.size == 0L) return InputStream.nullInputStream()
        val container = container(path)
        val key = key(path)
        return ObjectInputStream(
            info.size,
            {
                store.read(
                    container,
                    key,
                    info.version,
                    0,
                    minOf(ObjectInputStream.HEAD.toLong(), info.size),
                )
            },
            { reader(path, info) },
        )
    }

    override fun openAsOutputStream(path: StoragePath, options: Set<OpenOption>): OutputStream {
        checkWrite(path, options)
        return writer(path)
    }

    override fun openAsByteChannel(
        path: StoragePath,
        options: Set<OpenOption>,
    ): SeekableByteChannel {
        if (WRITE in options) {
            checkWrite(path, options)
            return SeekableWriteableChannel(writer(path))
        }
        val info = info(path)
        return if (info.size == 0L) EmptyChannel() else SeekableReadableChannel(reader(path, info))
    }

    override fun newDirectoryStream(
        dir: Path,
        filter: DirectoryStream.Filter<in Path>,
    ): DirectoryStream<Path> {
        val folder = ensureCorrectPath(dir)
        val prefix = key(folder).let { if (it.isEmpty() || it.endsWith("/")) it else "$it/" }
        val entries = store.list(container(folder), prefix)
        val system = folder.fileSystem
        return object : DirectoryStream<Path> {
            private var iterated = false

            override fun iterator(): MutableIterator<Path> {
                check(!iterated) { "The entries of $dir are listed once" }
                iterated = true
                val paths = generateSequence {
                    try {
                        if (entries.hasNext()) entries.next() else null
                    } catch (e: UncheckedIOException) {
                        throw DirectoryIteratorException(e.cause)
                    }
                }
                    .filter { it.key != prefix }
                    .map { system.getPath("/" + it.key) }
                    .filter { filter.accept(it) }
                    .iterator()
                return object : MutableIterator<Path> {
                    override fun hasNext() = paths.hasNext()

                    override fun next() = paths.next()

                    override fun remove() = throw UnsupportedOperationException("remove")
                }
            }

            override fun close() {}
        }
    }

    /** Nothing: folders are made of their objects' keys, and always exist. */
    override fun createDirectory(dir: Path, vararg attrs: FileAttribute<*>) {
        ensureCorrectPath(dir)
    }

    override fun delete(path: Path) {
        val storagePath = ensureCorrectPath(path)
        val container = container(storagePath)
        val key = key(storagePath)
        if (storagePath.isDirectory || key.isEmpty()) {
            if (holds(container, key)) throw DirectoryNotEmptyException(uri(storagePath))
            // A folder marker, as consoles write, if any.
            if (key.isNotEmpty()) store.delete(container, key)
        } else if (store.head(container, key) != null) {
            store.delete(container, key)
        } else if (holds(container, "$key/")) {
            throw DirectoryNotEmptyException(uri(storagePath))
        } else {
            throw NoSuchFileException(uri(storagePath))
        }
    }

    override fun copy(source: Path, target: Path, vararg options: CopyOption) {
        val from = ensureCorrectPath(source)
        val to = ensureCorrectPath(target)
        if (from.isDirectory) return
        if (REPLACE_EXISTING !in options && exists(to)) {
            throw FileAlreadyExistsException(uri(to))
        }
        store.head(container(from), key(from)) ?: throw NoSuchFileException(uri(from))
        store.copy(container(from), key(from), container(to), key(to))
    }

    override fun checkAccess(path: Path, vararg modes: AccessMode) {
        attributes(ensureCorrectPath(path))
    }

    @Suppress("UNCHECKED_CAST")
    override fun <V : FileAttributeView> getFileAttributeView(
        path: Path,
        type: Class<V>,
        vararg options: LinkOption,
    ): V? {
        val storagePath = ensureCorrectPath(path)
        if (
            type != BasicFileAttributeView::class.java &&
                type != StoragePathAttributesView::class.java
        ) {
            return null
        }
        return object : StoragePathAttributesView(storagePath) {
            override fun readAttributes(): BasicFileAttributes = attributes(storagePath)
        }
            as V
    }

    @Suppress("UNCHECKED_CAST")
    override fun <A : BasicFileAttributes> readAttributes(
        path: Path,
        type: Class<A>,
        vararg options: LinkOption,
    ): A {
        if (!type.isAssignableFrom(ObjectAttributes::class.java)) {
            throw UnsupportedOperationException("$scheme paths have basic attributes only")
        }
        return attributes(ensureCorrectPath(path)) as A
    }

    override fun readAttributes(
        path: Path,
        attributes: String,
        vararg options: LinkOption,
    ): Map<String, Any?> {
        val (view, names) =
            if (':' in attributes) attributes.substringBefore(':') to attributes.substringAfter(':')
            else "basic" to attributes
        if (view != "basic") throw UnsupportedOperationException("$scheme paths have no $view view")
        val wanted = names.split(',').map { it.trim() }.toSet()
        return attributes(ensureCorrectPath(path)).asMap { "*" in wanted || it in wanted }
    }

    /** The path's attributes: an object's, or a folder's if it is one. */
    private fun attributes(path: StoragePath): ObjectAttributes {
        val container = container(path)
        val key = key(path)
        if (path.isDirectory || key.isEmpty()) return ObjectAttributes.folder(path)
        store.head(container, key)?.let {
            return ObjectAttributes(path, it.size, FileTime.from(it.modified), false)
        }
        if (holds(container, "$key/")) return ObjectAttributes.folder(path)
        throw NoSuchFileException(uri(path))
    }

    private fun exists(path: StoragePath): Boolean =
        try {
            attributes(path)
            true
        } catch (_: NoSuchFileException) {
            false
        }

    /** Whether any object is under [prefix], other than its folder marker. */
    private fun holds(container: String, prefix: String): Boolean =
        try {
            store.list(container, prefix).asSequence().any { it.key != prefix }
        } catch (e: UncheckedIOException) {
            throw e.cause ?: e
        }

    private fun info(path: StoragePath): ObjectInfo {
        val storagePath = ensureNotDirectory(path)
        return store.head(container(storagePath), key(storagePath))
            ?: throw NoSuchFileException(uri(storagePath))
    }

    private fun checkWrite(path: StoragePath, options: Set<OpenOption>) {
        val storagePath = ensureNotDirectory(path)
        if (APPEND in options) {
            throw IOException("Objects cannot be appended to: ${uri(storagePath)}")
        }
        if (CREATE_NEW in options && exists(storagePath)) {
            throw FileAlreadyExistsException(uri(storagePath))
        }
        if (CREATE !in options && CREATE_NEW !in options && !exists(storagePath)) {
            throw NoSuchFileException(uri(storagePath))
        }
    }

    private fun reader(path: StoragePath, info: ObjectInfo) =
        ObjectReadChannel(store, container(path), key(path), info, path, uri(path), log)

    private fun writer(path: StoragePath) =
        ObjectWriteChannel(store, container(path), key(path), uri(path), memoryTracker)

    private fun container(path: StoragePath) = (path.fileSystem as ObjectStorageSystem).container

    /** The URI of the path, as the operator wrote it, for messages. */
    private fun uri(path: StoragePath) = "$scheme://${container(path)}/${key(path)}"

    companion object {
        /** The object key of a path: its normalized absolute form, without the leading `/`. */
        fun key(path: StoragePath): String = path.toRealPath().toString().removePrefix("/")
    }
}

/** One container's objects, as one file system. */
internal class ObjectStorageSystem(provider: StorageSystemProvider, val container: String) :
    StorageSystem(provider) {
    override fun uriPrefix() = "${scheme()}://$container"

    override fun internalClose() {}

    override fun canResolve(path: StoragePath) = path.fileSystem === this
}

/** An object's or a folder's attributes. */
internal class ObjectAttributes(
    path: StoragePath,
    private val size: Long,
    private val modified: FileTime,
    private val folder: Boolean,
) : StoragePathAttributes(path) {
    override fun size() = size

    override fun creationTime(): FileTime = modified

    override fun isDirectory() = folder

    override fun isRegularFile() = !folder

    override fun fileKey(): Any? = null

    companion object {
        fun folder(path: StoragePath) =
            ObjectAttributes(path, 0, FileTime.from(Instant.EPOCH), true)
    }
}

/** An empty object's channel: StoragePath reads need a positive size. */
private class EmptyChannel : SeekableByteChannel {
    private var open = true
    private var position = 0L

    override fun isOpen() = open

    override fun close() {
        open = false
    }

    override fun read(dst: java.nio.ByteBuffer) = -1

    override fun write(src: java.nio.ByteBuffer) =
        throw java.nio.channels.NonWritableChannelException()

    override fun position() = position

    override fun position(newPosition: Long): SeekableByteChannel = apply {
        position = newPosition
    }

    override fun size() = 0L

    override fun truncate(size: Long) = throw UnsupportedOperationException("truncate")
}
