package org.dozendb.cloud.internal

import java.io.IOException
import java.net.URI
import java.nio.ByteBuffer
import java.time.Instant

/**
 * A cloud's objects, as [ObjectStorageProvider] needs them. Containers are what URIs name before
 * the key: an S3 or GCS bucket, or an Azure storage account and one of its containers. Keys never
 * start with `/`; folders are the keys' prefixes up to a `/`.
 *
 * Failures are [java.nio.file.NoSuchFileException] for a missing container or object,
 * [java.nio.file.AccessDeniedException] when the credentials do not allow the operation, and other
 * [IOException]s, each with a message that names the URI and says what the cloud answered.
 */
internal interface ObjectStore {
    /** The URI scheme: `s3`, `gs` or `azb`. */
    val scheme: String

    /** How many parts of one upload it sends at once; 1 when parts must arrive in order. */
    val uploadConcurrency: Int

    /**
     * The container a URI names and the path of the object or folder in it, which starts with `/`.
     *
     * @throws IllegalArgumentException if the URI does not name a container.
     */
    fun locate(uri: URI): Location

    /** The object's size, modification and version, or null if there is none. */
    @Throws(IOException::class) fun head(container: String, key: String): ObjectInfo?

    /**
     * The bytes from [from] (inclusive) to [to] (exclusive) of the object, as of [version].
     *
     * @throws IOException also when the object changed since [version] was read.
     */
    @Throws(IOException::class)
    fun read(container: String, key: String, version: String?, from: Long, to: Long): ByteBuffer

    /**
     * The objects and folders right under [prefix], which is empty or ends with `/`, in key order,
     * read page by page as the iterator advances.
     */
    @Throws(IOException::class) fun list(container: String, prefix: String): Iterator<Listed>

    /** Deletes the object; nothing if there is none. */
    @Throws(IOException::class) fun delete(container: String, key: String)

    /** Copies the object within the cloud, replacing any object at the target. */
    @Throws(IOException::class)
    fun copy(fromContainer: String, fromKey: String, toContainer: String, toKey: String)

    /** Writes the whole object at once, replacing any object at the key. */
    @Throws(IOException::class) fun put(container: String, key: String, data: ByteBuffer)

    /** Starts writing an object in parts, for objects bigger than [partSize] of part 0. */
    @Throws(IOException::class) fun upload(container: String, key: String): Upload

    /** The size of part [index] of an upload: parts grow so that big objects stay in limits. */
    fun partSize(index: Int): Int
}

/** What a URI names: a [container] and the [path] in it, which starts with `/`. */
internal data class Location(val container: String, val path: String) {
    companion object {
        /** `<scheme>://<bucket>/<key>`, as S3 and Google Cloud Storage name objects. */
        fun bucket(uri: URI): Location {
            val bucket =
                uri.authority?.takeIf { it.isNotEmpty() }
                    ?: throw IllegalArgumentException(
                        "$uri names no bucket: ${uri.scheme}://<bucket>/<key>"
                    )
            return Location(bucket, uri.path?.takeIf { it.isNotEmpty() } ?: "/")
        }

        /**
         * `azb://<storage account>/<container>/<key>`, as Azure names blobs: the container is
         * `<storage account>/<container>`.
         */
        fun accountContainer(uri: URI): Location {
            val account = uri.authority?.takeIf { it.isNotEmpty() }
            val path = uri.path.orEmpty().removePrefix("/")
            val container = path.substringBefore('/')
            require(account != null && container.isNotEmpty()) {
                "$uri names no storage account and container: " +
                    "${uri.scheme}://<storage account>/<container>/<key>"
            }
            return Location("$account/$container", "/" + path.substringAfter('/', ""))
        }
    }
}

/**
 * An object's [size] in bytes, when it was last [modified], and its [version], if the cloud tells.
 */
internal data class ObjectInfo(val size: Long, val modified: Instant, val version: String?)

/** One entry of a listing. */
internal sealed interface Listed {
    val key: String

    /** An object. */
    data class Object(override val key: String, val info: ObjectInfo) : Listed

    /** A folder: other objects' keys start with [key], which ends with `/`. */
    data class Folder(override val key: String) : Listed
}

/**
 * An object being written in parts. Parts may be sent at once, up to
 * [ObjectStore.uploadConcurrency], and are numbered from 0. The object appears once [complete]
 * returns, and never after [abort].
 */
internal interface Upload {
    @Throws(IOException::class) fun part(index: Int, data: ByteBuffer)

    @Throws(IOException::class) fun complete(parts: Int)

    /** Gives the upload up; never throws. */
    fun abort()
}

/**
 * Parts of 8 MiB, twice as big every [every] parts, up to 1 GiB: big objects stay within a cloud's
 * number of parts while small ones are sent in small parts.
 */
internal fun growingPart(index: Int, every: Int): Int = (8 shl 20) shl minOf(index / every, 7)

/**
 * A listing of the pages a cloud answers: each page's entries in key order, without those an
 * earlier page gave (S3Mock repeats a folder across pages).
 */
internal fun pagedListing(pages: Sequence<List<Listed>>): Iterator<Listed> {
    var last: String? = null
    return pages
        .flatMap { page -> page.sortedBy { it.key } }
        .filter { entry ->
            val previous = last
            (previous == null || entry.key > previous).also { if (it) last = entry.key }
        }
        .iterator()
}
