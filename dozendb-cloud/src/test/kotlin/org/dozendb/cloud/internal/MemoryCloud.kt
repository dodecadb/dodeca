package org.dozendb.cloud.internal

import org.neo4j.cloud.storage.StorageSystemProviderFactory
import org.neo4j.cloud.storage.StorageSystemProviderFactory.ChunkChannelSupplier
import org.neo4j.configuration.Config
import org.neo4j.logging.InternalLogProvider
import org.neo4j.memory.MemoryTracker

/**
 * A cloud in this JVM's memory, under the `memory` scheme, `memory://<bucket>/<key>`, for tests of
 * commands and seeds in other modules: through the same provider as the real clouds, with parts of
 * 16 KiB.
 */
object MemoryCloud {
    internal val store = MemoryStore(part = 16 shl 10, scheme = "memory")

    /** The objects, by `<bucket>/<key>`. */
    fun objects(): Map<String, ByteArray> = store.objects.mapValues { it.value.data }

    fun put(bucket: String, key: String, data: ByteArray) = store.store(bucket, key, data)

    /** Makes [bucket] refuse every request, as without credentials for it. */
    fun deny(bucket: String) {
        store.denied += bucket
    }

    /** Makes the part [index] of uploads fail, or none. */
    fun failPart(index: Int?) {
        store.failingPart = index
    }

    fun clear() {
        store.objects.clear()
        store.denied.clear()
        store.failingPart = null
    }
}

/** The `memory` scheme, registered in the test jar's `META-INF/services`. */
class MemoryStorageFactory : StorageSystemProviderFactory("memory") {
    override fun storageSystemProviderClass() = MemoryStorageProvider::class.java.name
}

internal class MemoryStorageProvider(
    chunks: ChunkChannelSupplier,
    config: Config,
    logs: InternalLogProvider,
    memory: MemoryTracker,
) : ObjectStorageProvider(MemoryCloud.store, chunks, config, logs, memory)
