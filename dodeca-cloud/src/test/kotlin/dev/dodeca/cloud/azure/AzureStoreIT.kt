package dev.dodeca.cloud.azure

import com.azure.storage.blob.BlobServiceClientBuilder
import dev.dodeca.cloud.internal.ObjectStoreContract
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.file.AccessDeniedException
import java.nio.file.NoSuchFileException
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName

/** Azure Blob Storage's store, against Azurite. */
class AzureStoreIT : ObjectStoreContract() {
    override val store by lazy { AzureStore("blob.core.windows.net", "", connection(KEY), 2) }
    override val container = "$ACCOUNT/films"
    override val missingContainer = "$ACCOUNT/missing"

    @Test
    fun `Azure's refusals and missing containers say what Azure said`() {
        val wrongKey = AzureStore("blob.core.windows.net", "", connection("d3Jvbmc="), 2)

        assertThatThrownBy { wrongKey.put(container, "a.csv", ByteBuffer.wrap(byteArrayOf(1))) }
            .isInstanceOf(AccessDeniedException::class.java)
            .hasMessageStartingWith("azb://$ACCOUNT/films/a.csv: Server failed to authenticate")
            .hasMessageEndingWith("(AuthorizationFailure)")
        assertThatThrownBy { store.put(missingContainer, "a.csv", ByteBuffer.wrap(byteArrayOf(1))) }
            .isInstanceOf(NoSuchFileException::class.java)
            .hasMessage(
                "azb://$ACCOUNT/missing/a.csv: The specified container does not exist. " +
                    "(ContainerNotFound)"
            )
    }

    @Test
    fun `a connection string reaches its storage account only`() {
        assertThatThrownBy { store.head("other/films", "a.csv") }
            .isInstanceOf(IOException::class.java)
            .hasMessage(
                "azb://other/films/a.csv: AZURE_STORAGE_CONNECTION_STRING is for the storage " +
                    "account $ACCOUNT, not other"
            )
    }

    companion object {
        /** Azurite's account and its key, which Azure's documentation publishes. */
        private const val ACCOUNT = "devstoreaccount1"
        private const val KEY =
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

        private val emulator =
            GenericContainer(DockerImageName.parse(System.getProperty("azurite.image")))
                .withExposedPorts(10000)
                .withCommand(
                    "azurite-blob",
                    "--blobHost",
                    "0.0.0.0",
                    "--skipApiVersionCheck",
                    "--loose",
                )

        private fun connection(key: String) =
            "DefaultEndpointsProtocol=http;AccountName=$ACCOUNT;AccountKey=$key;BlobEndpoint=" +
                "http://${emulator.host}:${emulator.getMappedPort(10000)}/$ACCOUNT;"

        @JvmStatic
        @BeforeAll
        fun start() {
            emulator.start()
            BlobServiceClientBuilder()
                .connectionString(connection(KEY))
                .buildClient()
                .createBlobContainer("films")
        }

        @JvmStatic
        @AfterAll
        fun stop() {
            emulator.stop()
        }
    }
}
