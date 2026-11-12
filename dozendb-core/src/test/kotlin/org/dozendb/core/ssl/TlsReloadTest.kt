package org.dozendb.core.ssl

import java.math.BigInteger
import java.nio.file.Files
import java.nio.file.Path
import java.security.cert.X509Certificate
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLException
import javax.net.ssl.SSLSocket
import javax.net.ssl.TrustManager
import javax.net.ssl.X509TrustManager
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowableOfType
import org.dozendb.consensus.testing.TestAuthority
import org.dozendb.core.DozenDbSettings
import org.dozendb.core.StandaloneServer
import org.dozendb.core.StandaloneServer.Companion.SYSTEM
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.Config as Neo4jConfig
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.configuration.connectors.ConnectorPortRegister
import org.neo4j.configuration.connectors.ConnectorType
import org.neo4j.configuration.connectors.HttpsConnector
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.configuration.ssl.ClientAuth
import org.neo4j.configuration.ssl.SslPolicyConfig
import org.neo4j.configuration.ssl.SslPolicyScope
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Config
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.exceptions.Neo4jException
import org.neo4j.graphdb.config.Setting

/**
 * `dbms.security.reloadTLS()` with `dbms.security.tls_reload_enabled`, as Neo4j Enterprise
 * documents them: connections made after a reload get the new certificates, open ones keep theirs,
 * on Bolt and HTTPS.
 */
class TlsReloadTest {
    @TempDir lateinit var root: Path
    private val authority = TestAuthority()
    private val bolt = SslPolicyConfig.forScope(SslPolicyScope.BOLT)
    private val https = SslPolicyConfig.forScope(SslPolicyScope.HTTPS)
    private lateinit var server: StandaloneServer
    private val drivers = mutableListOf<Driver>()

    private val certificates: Path
        get() = root.resolve("certificates/bolt")

    private val httpsCertificates: Path
        get() = root.resolve("certificates/https")

    @AfterEach
    fun stop() {
        drivers.forEach(Driver::close)
        server.close()
    }

    /**
     * A server whose Bolt requires TLS, with a certificate of [authority]'s, and with HTTPS if
     * [withHttps], with another.
     */
    private fun start(reloadEnabled: Boolean = true, withHttps: Boolean = false): BigInteger {
        val first = authority.issue()
        first.writeTo(Files.createDirectories(certificates))
        val settings: MutableMap<Setting<*>, Any> =
            mutableMapOf(
                GraphDatabaseSettings.auth_enabled to true,
                BoltConnector.enabled to true,
                BoltConnector.encryption_level to BoltConnector.EncryptionLevel.REQUIRED,
                bolt.enabled to true,
                bolt.base_directory to certificates,
                bolt.client_auth to ClientAuth.NONE,
                DozenDbSettings.tlsReloadEnabled to reloadEnabled,
            )
        if (withHttps) {
            authority.issue().writeTo(Files.createDirectories(httpsCertificates))
            settings +=
                mapOf(
                    HttpsConnector.enabled to true,
                    HttpsConnector.listen_address to SocketAddress("localhost", 0),
                    https.enabled to true,
                    https.base_directory to httpsCertificates,
                    https.client_auth to ClientAuth.NONE,
                )
        }
        server = StandaloneServer(root, settings).also { it.start() }
        server.administer("ALTER USER neo4j SET PASSWORD 'admin-secret-1' CHANGE NOT REQUIRED")
        server.administer("CREATE USER alice SET PASSWORD 'alice-secret-1' CHANGE NOT REQUIRED")
        return first.certificate.serialNumber
    }

    private fun driver(user: String, password: String): Driver =
        GraphDatabase.driver(
                "bolt://${server.boltAddress()}",
                AuthTokens.basic(user, password),
                Config.builder()
                    .withEncryption()
                    .withTrustStrategy(Config.TrustStrategy.trustAllCertificates())
                    .build(),
            )
            .also { drivers += it }

    private val admin by lazy { driver("neo4j", "admin-secret-1") }

    private fun Driver.run(query: String): List<Map<String, Any?>> =
        session(SessionConfig.forDatabase(SYSTEM)).use { it.run(query).list { r -> r.asMap() } }

    private fun Driver.failure(query: String): String =
        catchThrowableOfType(Neo4jException::class.java) { run(query) }.message!!

    /** The serial number of the certificate [address], Bolt's by default, presents anew. */
    private fun presented(address: String = server.boltAddress()): BigInteger =
        connect(address).use {
            it.startHandshake()
            (it.session.peerCertificates.first() as X509Certificate).serialNumber
        }

    private fun connect(address: String): SSLSocket {
        val context = SSLContext.getInstance("TLS")
        context.init(null, arrayOf<TrustManager>(TrustingAll), null)
        val (host, port) = address.split(":")
        return context.socketFactory.createSocket(host, port.toInt()) as SSLSocket
    }

    private fun httpsAddress(): String {
        val address =
            server
                .database(SYSTEM)
                .dependencyResolver
                .resolveDependency(ConnectorPortRegister::class.java)
                .getLocalAddress(ConnectorType.HTTPS)
        return "${address.host}:${address.port}"
    }

    /** The status line of a `HEAD /` request on [this] kept-alive connection. */
    private fun SSLSocket.head(): String {
        outputStream.write("HEAD / HTTP/1.1\r\nHost: localhost\r\n\r\n".toByteArray())
        outputStream.flush()
        val response = StringBuilder()
        while (!response.endsWith("\r\n\r\n")) {
            val byte = inputStream.read()
            check(byte >= 0) { "The connection closed after: $response" }
            response.append(byte.toChar())
        }
        return response.lineSequence().first()
    }

    @Test
    fun `connections made after a reload get the new certificate, open ones keep working`() {
        val first = start()
        assertThat(presented()).isEqualTo(first)
        val open = admin.session(SessionConfig.forDatabase(SYSTEM))
        open.run("SHOW DATABASES").consume()

        val second = authority.issue()
        second.writeTo(certificates)
        assertThat(presented()).describedAs("before the reload").isEqualTo(first)
        admin.run("CALL dbms.security.reloadTLS()")

        assertThat(presented()).isEqualTo(second.certificate.serialNumber)
        assertThat(open.run("SHOW DATABASES YIELD name").list()).isNotEmpty
        open.close()
    }

    @Test
    fun `HTTPS connections made after each reload get its certificate, open ones keep working`() {
        start(withHttps = true)
        val address = httpsAddress()
        connect(address).use { open ->
            assertThat(open.head()).isEqualTo("HTTP/1.1 200 OK")

            // Again and again, as each reload must take, whatever the ones before it did.
            repeat(5) { reload ->
                val next = authority.issue()
                next.writeTo(httpsCertificates)
                admin.run("CALL dbms.security.reloadTLS()")

                assertThat(presented(address))
                    .describedAs("after reload ${reload + 1}")
                    .isEqualTo(next.certificate.serialNumber)
                assertThat(open.head()).isEqualTo("HTTP/1.1 200 OK")
            }
        }
    }

    @Test
    fun `HTTPS takes the settings of its policy on a reload, such as client_auth`() {
        start(withHttps = true)
        val address = httpsAddress()
        assertThat(connect(address).use { it.head() }).isEqualTo("HTTP/1.1 200 OK")

        server
            .database(SYSTEM)
            .dependencyResolver
            .resolveDependency(Neo4jConfig::class.java)
            .setDynamic(https.client_auth, ClientAuth.REQUIRE, javaClass.simpleName)
        admin.run("CALL dbms.security.reloadTLS()")

        // This client has no certificate to present.
        assertThat(
                catchThrowableOfType(SSLException::class.java) {
                    connect(address).use { it.head() }
                }
            )
            .isNotNull
    }

    @Test
    fun `only administrators reload, and only with dbms security tls_reload_enabled`() {
        start(reloadEnabled = false)

        assertThat(admin.failure("CALL dbms.security.reloadTLS()"))
            .contains(
                "Reloading TLS is turned off: set dbms.security.tls_reload_enabled=true and " +
                    "restart the server."
            )
        assertThat(driver("alice", "alice-secret-1").failure("CALL dbms.security.reloadTLS()"))
            .contains("Executing admin procedure 'dbms.security.reloadTLS' permission has not")
    }

    @Test
    fun `a reload that cannot read the certificates fails and keeps those in use`() {
        val first = start()
        Files.writeString(certificates.resolve("private.key"), "not a key")

        assertThat(admin.failure("CALL dbms.security.reloadTLS()"))
            .contains("Unable to reload TLS, the certificates in use stay:")

        assertThat(presented()).isEqualTo(first)
    }

    private object TrustingAll : X509TrustManager {
        override fun checkClientTrusted(chain: Array<X509Certificate>, authType: String) {}

        override fun checkServerTrusted(chain: Array<X509Certificate>, authType: String) {}

        override fun getAcceptedIssuers(): Array<X509Certificate> = emptyArray()
    }
}
