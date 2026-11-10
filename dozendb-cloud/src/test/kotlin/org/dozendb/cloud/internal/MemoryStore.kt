package org.dozendb.cloud.internal

import java.io.IOException
import java.net.URI
import java.nio.ByteBuffer
import java.nio.file.NoSuchFileException
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * Objects in memory, as a cloud keeps them, for tests: under [scheme], with parts of [part] bytes.
 * Records what it is asked. Containers in [denied] refuse every request.
 */
internal class MemoryStore(
    private val part: Int = 1024,
    override val uploadConcurrency: Int = 3,
    override val scheme: String = "s3",
) : ObjectStore {
    class Stored(val data: ByteArray, val version: Int, val modified: Instant)

    val denied: MutableSet<String> = java.util.concurrent.ConcurrentHashMap.newKeySet()

    /** By `<container>/<key>`. */
    val objects = ConcurrentSkipListMap<String, Stored>()
    private val versions = AtomicInteger()

    val ranges: MutableList<LongRange> = java.util.Collections.synchronizedList(mutableListOf())
    val uploads = AtomicInteger()
    val partsSent = AtomicInteger()
    val aborted = AtomicInteger()
    val copies = AtomicInteger()
    val puts = AtomicInteger()

    /** The part that fails when sent, if any. */
    @Volatile var failingPart: Int? = null

    fun store(container: String, key: String, data: ByteArray) {
        objects["$container/$key"] =
            Stored(data, versions.incrementAndGet(), Instant.ofEpochSecond(1_700_000_000))
    }

    fun data(container: String, key: String): ByteArray? = objects["$container/$key"]?.data

    override fun locate(uri: URI) = Location.bucket(uri)

    private fun check(container: String, key: String) {
        if (container in denied) {
            throw java.nio.file.AccessDeniedException("$scheme://$container/$key", null, "denied")
        }
    }

    override fun head(container: String, key: String): ObjectInfo? {
        check(container, key)
        return objects["$container/$key"]?.let {
            ObjectInfo(it.data.size.toLong(), it.modified, it.version.toString())
        }
    }

    override fun read(
        container: String,
        key: String,
        version: String?,
        from: Long,
        to: Long,
    ): ByteBuffer {
        val stored = objects["$container/$key"] ?: throw NoSuchFileException("s3://$container/$key")
        if (version != null && version != stored.version.toString()) {
            throw IOException("$scheme://$container/$key changed while it was read")
        }
        ranges += from until to
        return ByteBuffer.wrap(stored.data.copyOfRange(from.toInt(), to.toInt()))
    }

    override fun list(container: String, prefix: String): Iterator<Listed> {
        check(container, prefix)
        val under = "$container/$prefix"
        val entries = sortedMapOf<String, Listed>()
        for ((name, stored) in objects.tailMap(under)) {
            if (!name.startsWith(under)) break
            val key = name.removePrefix("$container/")
            val rest = key.removePrefix(prefix)
            if ('/' in rest) {
                val folder = prefix + rest.substringBefore('/') + "/"
                entries[folder] = Listed.Folder(folder)
            } else {
                entries[key] =
                    Listed.Object(
                        key,
                        ObjectInfo(
                            stored.data.size.toLong(),
                            stored.modified,
                            stored.version.toString(),
                        ),
                    )
            }
        }
        return entries.values.iterator()
    }

    override fun delete(container: String, key: String) {
        check(container, key)
        objects.remove("$container/$key")
    }

    override fun copy(fromContainer: String, fromKey: String, toContainer: String, toKey: String) {
        check(toContainer, toKey)
        copies.incrementAndGet()
        val stored =
            objects["$fromContainer/$fromKey"]
                ?: throw NoSuchFileException("$scheme://$fromContainer/$fromKey")
        store(toContainer, toKey, stored.data)
    }

    override fun put(container: String, key: String, data: ByteBuffer) {
        check(container, key)
        puts.incrementAndGet()
        store(container, key, ByteArray(data.remaining()).also { data.get(it) })
    }

    override fun upload(container: String, key: String): Upload {
        check(container, key)
        uploads.incrementAndGet()
        val sent = ConcurrentHashMap<Int, ByteArray>()
        return object : Upload {
            override fun part(index: Int, data: ByteBuffer) {
                if (index == failingPart) throw IOException("part $index failed")
                partsSent.incrementAndGet()
                sent[index] = ByteArray(data.remaining()).also { data.get(it) }
            }

            override fun complete(parts: Int) {
                val all = (0 until parts).map { sent.getValue(it) }
                store(container, key, all.fold(ByteArray(0)) { a, b -> a + b })
            }

            override fun abort() {
                aborted.incrementAndGet()
            }
        }
    }

    override fun partSize(index: Int) = part
}
