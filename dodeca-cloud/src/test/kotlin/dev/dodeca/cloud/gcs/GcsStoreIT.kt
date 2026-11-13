package dev.dodeca.cloud.gcs

import com.google.cloud.NoCredentials
import com.google.cloud.storage.BucketInfo
import com.google.cloud.storage.StorageException
import com.google.cloud.storage.StorageOptions
import dev.dodeca.cloud.internal.ObjectStoreContract
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse.BodyHandlers
import java.nio.ByteBuffer
import java.nio.file.AccessDeniedException
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName

/** Google Cloud Storage's store, against fake-gcs-server. */
class GcsStoreIT : ObjectStoreContract() {
    override val store by lazy { GcsStore(storage(), pageSize = 2) }
    override val container = "films"
    override val missingContainer = "missing"

    @Test
    fun `STORAGE_EMULATOR_HOST points the process's store at an emulator`() {
        val store = GcsStore.create(endpoint.removePrefix("http://"))

        store.put("films", "emulated.txt", ByteBuffer.wrap("yes".toByteArray()))

        assertThat(store.head("films", "emulated.txt")!!.size).isEqualTo(3)
    }

    @Test
    fun `a refusal of an anonymous request says why there were no credentials`() {
        val store = GcsStore(storage(), credentialsProblem = "no file at /secrets/key.json")

        val denied =
            store.failure("films", "a.csv", null, StorageException(401, "Anonymous caller"))

        assertThat(denied)
            .isInstanceOf(AccessDeniedException::class.java)
            .hasMessage(
                "gs://films/a.csv: Anonymous caller. The request was anonymous: Google's " +
                    "application default credentials were not found: no file at " +
                    "/secrets/key.json"
            )
    }

    companion object {
        private val emulator =
            GenericContainer(DockerImageName.parse(System.getProperty("fake-gcs.image")))
                .withExposedPorts(4443)
                .withCommand("-scheme", "http", "-port", "4443")
        private lateinit var endpoint: String

        private fun storage() =
            StorageOptions.http()
                .setHost(endpoint)
                .setProjectId("dodeca")
                .setCredentials(NoCredentials.getInstance())
                .build()
                .service

        @JvmStatic
        @BeforeAll
        fun start() {
            emulator.start()
            endpoint = "http://${emulator.host}:${emulator.getMappedPort(4443)}"
            // Resumable uploads continue at the URL the emulator gives, which must be the mapped
            // one.
            HttpClient.newHttpClient().let {
                val answer =
                    it.send(
                        HttpRequest.newBuilder(URI("$endpoint/_internal/config"))
                            .PUT(
                                HttpRequest.BodyPublishers.ofString(
                                    """{"externalUrl": "$endpoint"}"""
                                )
                            )
                            .header("Content-Type", "application/json")
                            .build(),
                        BodyHandlers.ofString(),
                    )
                check(answer.statusCode() == 200) { answer.body() }
            }
            storage().create(BucketInfo.of("films"))
        }

        @JvmStatic
        @AfterAll
        fun stop() {
            emulator.stop()
        }
    }
}
