package org.dozendb.cloud.s3

import java.net.URI
import java.nio.ByteBuffer
import kotlin.random.Random
import org.assertj.core.api.Assertions.assertThat
import org.dozendb.cloud.internal.ObjectStoreContract
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client

/** Amazon S3's store, against S3Mock. */
class S3StoreIT : ObjectStoreContract() {
    override val store by lazy {
        S3Store(client(false), checksums = false, pageSize = 2, copyLimit = 1 shl 20)
    }
    override val container = "films"
    override val missingContainer = "missing"

    @Test
    fun `parts carry CRC32 checksums, as on AWS, and big objects copy in ranges`() {
        val checked =
            S3Store(
                client(true),
                checksums = true,
                copyLimit = 1 shl 20,
                copyPart = 5L shl 20,
            )
        val data = Random(1).nextBytes((8 shl 20) + 3000)
        val upload = checked.upload("films", "checked.bin")
        upload.part(0, ByteBuffer.wrap(data, 0, 8 shl 20))
        upload.part(1, ByteBuffer.wrap(data, 8 shl 20, 3000))
        upload.complete(2)

        checked.copy("films", "checked.bin", "films", "copied.bin")

        val info = checked.head("films", "copied.bin")!!
        val copied = checked.read("films", "copied.bin", info.version, 0, info.size)
        assertThat(ByteArray(copied.remaining()).also { copied.get(it) }).isEqualTo(data)
    }

    @Test
    fun `the process's client follows an endpoint override, addressing buckets in the path`() {
        val properties =
            mapOf(
                "aws.endpointUrlS3" to endpoint,
                "aws.region" to "eu-west-3",
                "aws.accessKeyId" to "key",
                "aws.secretAccessKey" to "secret",
            )
        properties.forEach { (name, value) -> System.setProperty(name, value) }
        try {
            val store = S3Store.create()

            assertThat(store.checksums).isFalse()
            store.put("films", "overridden.txt", ByteBuffer.wrap("yes".toByteArray()))
            assertThat(store.head("films", "overridden.txt")!!.size).isEqualTo(3)
        } finally {
            properties.keys.forEach(System::clearProperty)
        }
    }

    companion object {
        private val emulator =
            GenericContainer(DockerImageName.parse(System.getProperty("s3mock.image")))
                .withExposedPorts(9090)
        private lateinit var endpoint: String

        private fun client(checksums: Boolean): S3Client =
            S3Client.builder()
                .endpointOverride(URI(endpoint))
                .forcePathStyle(true)
                .region(Region.US_EAST_1)
                .credentialsProvider(
                    StaticCredentialsProvider.create(AwsBasicCredentials.create("key", "secret"))
                )
                .apply {
                    if (!checksums) {
                        requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
                        responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED)
                    }
                }
                .build()

        @JvmStatic
        @BeforeAll
        fun start() {
            emulator.start()
            endpoint = "http://${emulator.host}:${emulator.getMappedPort(9090)}"
            client(false).use { it.createBucket { bucket -> bucket.bucket("films") } }
        }

        @JvmStatic
        @AfterAll
        fun stop() {
            emulator.stop()
        }
    }
}
