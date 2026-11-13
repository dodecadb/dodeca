package dev.dodeca.cloud.internal

import dev.dodeca.cloud.CloudStorageSettings
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import org.neo4j.cloud.storage.StoragePath
import org.neo4j.cloud.storage.StorageSettingsDeclaration.READ_IS_FOR_DESCRIPTION_FLAG
import org.neo4j.cloud.storage.StorageSettingsDeclaration.READ_IS_FOR_SAMPLING_FLAG
import org.neo4j.cloud.storage.StorageSystemProvider
import org.neo4j.cloud.storage.queues.RequestQueueConfigs.DESCRIPTION_PULL_QUEUE_CHUNK_SIZE
import org.neo4j.cloud.storage.queues.RequestQueueConfigs.DESCRIPTION_PULL_QUEUE_SIZE
import org.neo4j.cloud.storage.queues.RequestQueueConfigs.QueueConfig
import org.neo4j.cloud.storage.queues.RequestQueueConfigs.SAMPLING_PULL_QUEUE_CHUNK_SIZE
import org.neo4j.cloud.storage.queues.RequestQueueConfigs.SAMPLING_PULL_QUEUE_SIZE
import org.neo4j.configuration.Config
import org.neo4j.graphdb.config.Setting

/** The threads that fetch ranges and send parts, for every cloud, idle threads ending. */
internal object Transfers {
    private val threads = AtomicInteger()

    val executor: ExecutorService = Executors.newCachedThreadPool { task ->
        Thread(task, "dodeca-cloud-transfer-${threads.incrementAndGet()}").apply {
            isDaemon = true
        }
    }
}

/**
 * How many ranges of an object a read fetches at once and how big they are: [pull] for reads,
 * [push] for copies of the whole object. Reads that only sample the object's start, as imports do
 * to guess its size, or read an archive's description, fetch one small range.
 */
internal data class QueueConfigs(val push: QueueConfig, val pull: QueueConfig) {
    companion object {
        fun of(path: StoragePath): QueueConfigs {
            val config = StorageSystemProvider.config(path)
            val settings = CloudStorageSettings.queues(path.scheme())
            val sampling = path.metadata().get(READ_IS_FOR_SAMPLING_FLAG) == true
            val description = path.metadata().get(READ_IS_FOR_DESCRIPTION_FLAG) == true
            return QueueConfigs(
                QueueConfig(
                    get(config, settings.pushSlots),
                    Math.toIntExact(get(config, settings.pushChunk)),
                    get(config, settings.pushTimeout),
                ),
                if (description) {
                    QueueConfig(
                        DESCRIPTION_PULL_QUEUE_SIZE,
                        DESCRIPTION_PULL_QUEUE_CHUNK_SIZE,
                        get(config, settings.pullTimeout),
                    )
                } else if (sampling) {
                    QueueConfig(
                        SAMPLING_PULL_QUEUE_SIZE,
                        SAMPLING_PULL_QUEUE_CHUNK_SIZE,
                        get(config, settings.pullTimeout),
                    )
                } else {
                    QueueConfig(
                        get(config, settings.pullSlots),
                        Math.toIntExact(get(config, settings.pullChunk)),
                        get(config, settings.pullTimeout),
                    )
                },
            )
        }

        /** The setting's value, or its default where the configuration does not declare it. */
        private fun <T> get(config: Config, setting: Setting<T>): T =
            try {
                config.get(setting)
            } catch (_: IllegalArgumentException) {
                setting.defaultValue()
            }
    }
}
