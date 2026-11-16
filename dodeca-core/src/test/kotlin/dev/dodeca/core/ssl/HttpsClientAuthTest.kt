package dev.dodeca.core.ssl

import dev.dodeca.consensus.testing.TestAuthority
import dev.dodeca.consensus.testing.TestCertificate
import dev.dodeca.core.StandaloneServer
import java.nio.file.Files
import java.nio.file.Path
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.connectors.HttpsConnector
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.configuration.ssl.ClientAuth
import org.neo4j.configuration.ssl.SslPolicyConfig
import org.neo4j.configuration.ssl.SslPolicyScope
import org.neo4j.graphdb.config.Setting

/**
 * HTTPS with client authentication serves the clients its SSL policy trusts: those with a
 * certificate that an authority in its `trusted` directory issued and that no revocation list in
 * its `revoked` directory revokes, as Bolt does. Neo4j Community's HTTPS trusts the JDK's
 * authorities instead.
 */
class HttpsClientAuthTest {
    @TempDir lateinit var root: Path
    private val authority = TestAuthority()
    private val https = SslPolicyConfig.forScope(SslPolicyScope.HTTPS)
    private lateinit var server: StandaloneServer

    @AfterEach fun stop() = server.close()

    /** A server whose HTTPS takes [clientAuth], its policy revoking [revoked]; its address. */
    private fun start(
        clientAuth: ClientAuth,
        revoked: List<TestCertificate> = emptyList(),
    ): String {
        val certificates = Files.createDirectories(root.resolve("certificates/https"))
        val revocations =
            if (revoked.isEmpty()) emptyList()
            else listOf(authority.revocationList(*revoked.toTypedArray()))
        authority.issue().writeTo(certificates, revocations = revocations)
        val settings: Map<Setting<*>, Any> =
            mapOf(
                HttpsConnector.enabled to true,
                HttpsConnector.listen_address to SocketAddress("localhost", 0),
                https.enabled to true,
                https.base_directory to certificates,
                https.client_auth to clientAuth,
            )
        server = StandaloneServer(root, settings).also { it.start() }
        return httpsAddress(server)
    }

    @Test
    fun `required client certificates must be the policy's trusted authorities' and not revoked`() {
        val revoked = authority.issue()
        val address = start(ClientAuth.REQUIRE, revoked = listOf(revoked))

        assertServed(address, authority.issue())
        assertRefused(address)
        assertRefused(address, TestAuthority("Another test CA").issue())
        assertRefused(address, revoked)
    }

    @Test
    fun `optional client certificates are checked when clients present them`() {
        val address = start(ClientAuth.OPTIONAL)

        assertServed(address)
        assertServed(address, authority.issue())
        assertRefused(address, TestAuthority("Another test CA").issue())
    }
}
