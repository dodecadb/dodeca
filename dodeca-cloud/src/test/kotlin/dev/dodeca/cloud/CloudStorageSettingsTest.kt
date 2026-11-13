package dev.dodeca.cloud

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.neo4j.cloud.storage.SchemeFileSystemAbstraction
import org.neo4j.cloud.storage.StorageSystemProviderFactory
import org.neo4j.configuration.Config
import org.neo4j.io.ByteUnit.mebiBytes
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import org.neo4j.service.Services

/** The schemes and settings Dodeca declares to Neo4j. */
class CloudStorageSettingsTest {
    @Test
    fun `Neo4j accepts the settings, internal ones in their namespace, with their defaults`() {
        val config =
            Config.newBuilder()
                .setRaw(
                    mapOf(
                        "dbms.integrations.cloud_storage.azb.blob_endpoint_suffix" to
                            "blob.core.usgovcloudapi.net",
                        "internal.dbms.cloud.storage.gs.pull_queue_chunk_size" to "2MiB",
                    )
                )
                .build()

        assertThat(config.get(CloudStorageSettings.azbBlobEndpointSuffix))
            .isEqualTo("blob.core.usgovcloudapi.net")
        assertThat(config.get(CloudStorageSettings.azbAuthorityEndpoint)).isEmpty()
        assertThat(config.get(CloudStorageSettings.gsPullChunk)).isEqualTo(mebiBytes(2))
        assertThat(config.get(CloudStorageSettings.s3PullChunk)).isEqualTo(mebiBytes(8))
        assertThat(config.get(CloudStorageSettings.s3PullSlots)).isEqualTo(4)
        assertThat(config.get(CloudStorageSettings.azbPushSlots)).isEqualTo(16)
    }

    @Test
    fun `Neo4j finds the three schemes, each naming a provider it can create`() {
        val factories = Services.loadAll(StorageSystemProviderFactory::class.java)

        assertThat(SchemeFileSystemAbstraction(DefaultFileSystemAbstraction()).resolvableSchemes())
            .contains("s3", "gs", "azb")
        for (factory in factories.filter { it.javaClass.packageName == "dev.dodeca.cloud" }) {
            val provider = Class.forName(storageProviderClass(factory))
            assertThat(provider.constructors.map { it.parameterTypes.toList() })
                .contains(
                    listOf(
                        StorageSystemProviderFactory.ChunkChannelSupplier::class.java,
                        Config::class.java,
                        org.neo4j.logging.InternalLogProvider::class.java,
                        org.neo4j.memory.MemoryTracker::class.java,
                    )
                )
        }
    }

    private fun storageProviderClass(factory: StorageSystemProviderFactory): String =
        StorageSystemProviderFactory::class
            .java
            .getDeclaredMethod("storageSystemProviderClass")
            .apply { isAccessible = true }
            .invoke(factory) as String
}
