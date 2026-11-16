package dev.dodeca.core.ssl

import dev.dodeca.consensus.testing.TestCertificate
import dev.dodeca.core.StandaloneServer
import dev.dodeca.core.StandaloneServer.Companion.SYSTEM
import java.io.EOFException
import java.io.IOException
import java.net.Socket
import java.net.SocketTimeoutException
import java.security.Principal
import java.security.PrivateKey
import java.security.cert.X509Certificate
import javax.net.ssl.KeyManager
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLEngine
import javax.net.ssl.SSLSocket
import javax.net.ssl.TrustManager
import javax.net.ssl.X509ExtendedKeyManager
import javax.net.ssl.X509TrustManager
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowableOfType
import org.neo4j.configuration.connectors.ConnectorPortRegister
import org.neo4j.configuration.connectors.ConnectorType

/**
 * A TLS connection to [address], which trusts any server, presents [certificate] if given, and
 * reads for 30 seconds at most.
 */
internal fun connect(address: String, certificate: TestCertificate? = null): SSLSocket {
    val keys = certificate?.let { arrayOf<KeyManager>(Presenting(it)) }
    val context = SSLContext.getInstance("TLS")
    context.init(keys, arrayOf<TrustManager>(TrustingAll), null)
    val (host, port) = address.split(":")
    return (context.socketFactory.createSocket(host, port.toInt()) as SSLSocket).apply {
        soTimeout = 30_000
    }
}

/** The status line of a `HEAD /` request on [this] kept-alive connection. */
internal fun SSLSocket.head(): String {
    outputStream.write("HEAD / HTTP/1.1\r\nHost: localhost\r\n\r\n".toByteArray())
    outputStream.flush()
    val response = StringBuilder()
    while (!response.endsWith("\r\n\r\n")) {
        val byte = inputStream.read()
        if (byte < 0) throw EOFException("The connection closed after: $response")
        response.append(byte.toChar())
    }
    return response.lineSequence().first()
}

/** Whether HTTPS on [address] serves a client that presents [certificate], or none. */
internal fun assertServed(address: String, certificate: TestCertificate? = null) {
    assertThat(connect(address, certificate).use { it.head() }).isEqualTo("HTTP/1.1 200 OK")
}

/**
 * Whether HTTPS on [address] refuses a client that presents [certificate], or none. TLS 1.3
 * completes the client's handshake before the server checks its certificate, so the client sees the
 * refusal as the server's alert or, as the server closes the connection, as a write or read that
 * fails, depending on timing: an IOException either way, but a timeout.
 */
internal fun assertRefused(address: String, certificate: TestCertificate? = null) {
    assertThat(
            catchThrowableOfType(IOException::class.java) {
                connect(address, certificate).use { it.head() }
            }
        )
        .isNotNull
        .isNotInstanceOf(SocketTimeoutException::class.java)
}

/** The address HTTPS of [server] listens on. */
internal fun httpsAddress(server: StandaloneServer): String {
    val address =
        server
            .database(SYSTEM)
            .dependencyResolver
            .resolveDependency(ConnectorPortRegister::class.java)
            .getLocalAddress(ConnectorType.HTTPS)
    return "${address.host}:${address.port}"
}

/**
 * Presents [certificate] whatever authorities the server asks for, so that the server, not the
 * client, decides whether it trusts it.
 */
private class Presenting(private val certificate: TestCertificate) : X509ExtendedKeyManager() {
    private val algorithm = certificate.privateKey.algorithm

    override fun chooseClientAlias(
        keyTypes: Array<String>?,
        issuers: Array<Principal>?,
        socket: Socket?,
    ): String? = ALIAS.takeIf { keyTypes.orEmpty().contains(algorithm) }

    override fun chooseEngineClientAlias(
        keyTypes: Array<String>?,
        issuers: Array<Principal>?,
        engine: SSLEngine?,
    ): String? = chooseClientAlias(keyTypes, issuers, null)

    override fun getClientAliases(keyType: String?, issuers: Array<Principal>?) = arrayOf(ALIAS)

    override fun getCertificateChain(alias: String?): Array<X509Certificate> =
        arrayOf(certificate.certificate, certificate.authority.certificate)

    override fun getPrivateKey(alias: String?): PrivateKey = certificate.privateKey

    override fun chooseServerAlias(
        keyType: String?,
        issuers: Array<Principal>?,
        socket: Socket?,
    ): String? = null

    override fun getServerAliases(keyType: String?, issuers: Array<Principal>?) =
        emptyArray<String>()

    private companion object {
        const val ALIAS = "client"
    }
}

internal object TrustingAll : X509TrustManager {
    override fun checkClientTrusted(chain: Array<X509Certificate>, authType: String) {}

    override fun checkServerTrusted(chain: Array<X509Certificate>, authType: String) {}

    override fun getAcceptedIssuers(): Array<X509Certificate> = emptyArray()
}
