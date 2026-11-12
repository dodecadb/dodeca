package org.dozendb.cloud

import java.time.Duration
import org.neo4j.cloud.storage.StorageSettingsDeclaration
import org.neo4j.configuration.Description
import org.neo4j.configuration.Internal
import org.neo4j.configuration.SettingConstraints.min
import org.neo4j.configuration.SettingConstraints.range
import org.neo4j.configuration.SettingImpl.newBuilder
import org.neo4j.configuration.SettingValueParsers.BYTES
import org.neo4j.configuration.SettingValueParsers.DURATION
import org.neo4j.configuration.SettingValueParsers.INT
import org.neo4j.configuration.SettingValueParsers.STRING
import org.neo4j.configuration.SettingsDeclaration
import org.neo4j.graphdb.config.Setting
import org.neo4j.io.ByteUnit.gibiBytes
import org.neo4j.io.ByteUnit.mebiBytes

/**
 * Settings of cloud storage, named as Neo4j Enterprise's. Registered in `META-INF/services`. The
 * internal ones size the ranges that reads fetch at once, per scheme, under the names Neo4j's
 * storage interface reads.
 */
class CloudStorageSettings : SettingsDeclaration {
    /** The sizes of one scheme's reads, and how long a read waits for a range. */
    class Queues internal constructor(scheme: String) {
        val pushSlots: Setting<Int> =
            internal(scheme, "push", "slot", INT, PUSH_SLOTS).addConstraint(min(1)).build()
        val pushChunk: Setting<Long> =
            internal(scheme, "push", "chunk", BYTES, CHUNK).addConstraint(CHUNK_RANGE).build()
        val pullSlots: Setting<Int> =
            internal(scheme, "pull", "slot", INT, PULL_SLOTS).addConstraint(min(1)).build()
        val pullChunk: Setting<Long> =
            internal(scheme, "pull", "chunk", BYTES, CHUNK).addConstraint(CHUNK_RANGE).build()
        val pushTimeout: Setting<Duration> =
            internal(scheme, "push", "poll", DURATION, POLL_TIMEOUT, "timeout").build()
        val pullTimeout: Setting<Duration> =
            internal(scheme, "pull", "poll", DURATION, POLL_TIMEOUT, "timeout").build()

        private fun <T> internal(
            scheme: String,
            queue: String,
            option: String,
            parser: org.neo4j.configuration.SettingValueParser<T>,
            default: T,
            unit: String = "size",
        ) =
            newBuilder(
                "internal.dbms.cloud.storage.$scheme.${queue}_queue_${option}_$unit",
                parser,
                default,
            )
    }

    companion object {
        /**
         * Ranges a copy of a whole object fetches at once. Neo4j's interface says 64; 16 keeps a
         * copy's memory to 128 MiB.
         */
        private const val PUSH_SLOTS = 16

        /** Ranges a read fetches ahead, as Neo4j's interface says. */
        private const val PULL_SLOTS = 4

        private val CHUNK = mebiBytes(8)

        /** How long a read waits for a range, as Neo4j's interface says. */
        private val POLL_TIMEOUT = StorageSettingsDeclaration.DEFAULT_POLL_TIMEOUT
        private val CHUNK_RANGE = range(mebiBytes(1), gibiBytes(1))

        @JvmField
        @field:Description(
            "The suffix of the Azure Blob Storage endpoints, after the storage account's name: " +
                "for other Azure clouds, such as Azure Government, or private endpoints."
        )
        val azbBlobEndpointSuffix: Setting<String> =
            newBuilder(
                    "dbms.integrations.cloud_storage.azb.blob_endpoint_suffix",
                    STRING,
                    "blob.core.windows.net",
                )
                .build()

        @JvmField
        @field:Description(
            "The Microsoft Entra ID authority that Azure credentials sign in with, such as " +
                "https://login.microsoftonline.us for Azure Government. Empty uses the Azure " +
                "SDK's default, the public cloud's."
        )
        val azbAuthorityEndpoint: Setting<String> =
            newBuilder("dbms.integrations.cloud_storage.azb.authority_endpoint", STRING, "").build()

        @JvmField internal val s3Queues = Queues("s3")
        @JvmField internal val gsQueues = Queues("gs")
        @JvmField internal val azbQueues = Queues("azb")

        // Neo4j finds settings among a declaration's static fields.
        @JvmField @field:Internal val s3PushSlots = s3Queues.pushSlots
        @JvmField @field:Internal val s3PushChunk = s3Queues.pushChunk
        @JvmField @field:Internal val s3PullSlots = s3Queues.pullSlots
        @JvmField @field:Internal val s3PullChunk = s3Queues.pullChunk
        @JvmField @field:Internal val s3PushTimeout = s3Queues.pushTimeout
        @JvmField @field:Internal val s3PullTimeout = s3Queues.pullTimeout

        @JvmField @field:Internal val gsPushSlots = gsQueues.pushSlots
        @JvmField @field:Internal val gsPushChunk = gsQueues.pushChunk
        @JvmField @field:Internal val gsPullSlots = gsQueues.pullSlots
        @JvmField @field:Internal val gsPullChunk = gsQueues.pullChunk
        @JvmField @field:Internal val gsPushTimeout = gsQueues.pushTimeout
        @JvmField @field:Internal val gsPullTimeout = gsQueues.pullTimeout

        @JvmField @field:Internal val azbPushSlots = azbQueues.pushSlots
        @JvmField @field:Internal val azbPushChunk = azbQueues.pushChunk
        @JvmField @field:Internal val azbPullSlots = azbQueues.pullSlots
        @JvmField @field:Internal val azbPullChunk = azbQueues.pullChunk
        @JvmField @field:Internal val azbPushTimeout = azbQueues.pushTimeout
        @JvmField @field:Internal val azbPullTimeout = azbQueues.pullTimeout

        /** The read sizes of [scheme], or of `s3` for a scheme of tests. */
        internal fun queues(scheme: String): Queues =
            when (scheme) {
                "gs" -> gsQueues
                "azb" -> azbQueues
                else -> s3Queues
            }
    }
}
