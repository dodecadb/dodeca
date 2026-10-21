package org.dozendb.backup.source

import java.io.IOException
import java.net.InetAddress
import java.net.ServerSocket
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.dozendb.backup.BackupSettings
import org.dozendb.backup.CommunityServer
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.ratis.BulkServer
import org.dozendb.consensus.ratis.TlsConfig
import org.dozendb.consensus.testing.TestAuthority
import org.dozendb.core.StandaloneServer
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.connectors.ConnectorPortRegister
import org.neo4j.configuration.connectors.ConnectorType
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.configuration.ssl.SslPolicyConfig
import org.neo4j.configuration.ssl.SslPolicyScope
import org.neo4j.graphdb.config.Setting
import org.neo4j.kernel.internal.GraphDatabaseAPI

class BackupListenerTest {
    // Kept when a test fails, for the server's logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private val authority = TestAuthority()
    private val policy = SslPolicyConfig.forScope(SslPolicyScope.BACKUP)

    private fun server(vararg settings: Pair<Setting<*>, Any>) =
        StandaloneServer(
            root,
            mapOf(BackupSettings.listenAddress to SocketAddress("127.0.0.1", 0)) + settings,
        )

    private fun GraphDatabaseAPI.backupPort(): Int? =
        dependencyResolver
            .resolveDependency(ConnectorPortRegister::class.java)
            .getLocalAddress(ConnectorType.BACKUP)
            ?.port

    private fun StandaloneServer.backupPort(): Int? = database(StandaloneServer.SYSTEM).backupPort()

    /** Asks the backup port for a source no server serves, which only a listener answers. */
    private fun ask(port: Int, tls: TlsConfig? = null): String =
        try {
            BulkServer.fetch(HostPort("127.0.0.1", port), tls, "nothing", ByteArray(0)) {
                it.readAllBytes()
                "served"
            }
        } catch (e: IOException) {
            e.message!!
        }

    private fun clientTls(certificate: org.dozendb.consensus.testing.TestCertificate) =
        TlsConfig(
            privateKey = certificate.privateKey,
            certificateChain = listOf(certificate.certificate),
            trustManagers = TestAuthority.trusting(authority).trustManagers.toList(),
            requireClientCertificates = true,
            verifyHostname = false,
        )

    @Test
    fun `serves backups on the configured address and registers its port`() {
        server().use { server ->
            server.start()
            val port = checkNotNull(server.backupPort())

            assertThat(ask(port)).contains("NOT_FOUND: no bulk source nothing")

            server.stop()
            assertThatCode { ServerSocket(port, 1, InetAddress.getLoopbackAddress()).close() }
                .describedAs("the port is free again")
                .doesNotThrowAnyException()
        }
    }

    @Test
    fun `serves no backups when disabled`() {
        server(BackupSettings.enabled to false).use { server ->
            server.start()
            assertThat(server.backupPort()).isNull()
        }
    }

    @Test
    fun `fails to start when the backup port is taken`() {
        ServerSocket(0, 1, InetAddress.getLoopbackAddress()).use { taken ->
            val address = SocketAddress("127.0.0.1", taken.localPort)
            server(BackupSettings.listenAddress to address).use { server ->
                assertThatThrownBy { server.start() }
                    .hasStackTraceContaining(
                        "Unable to serve backups on $address (server.backup.listen_address): " +
                            "Address already in use"
                    )
            }
        }
    }

    @Test
    fun `fails to start on an address that is not a loopback one without a choice for TLS`() {
        server(BackupSettings.listenAddress to SocketAddress("0.0.0.0", 0)).use { server ->
            assertThatThrownBy { server.start() }
                .rootCause()
                .hasMessageContaining("set dbms.ssl.policy.backup.enabled=true")
        }
    }

    @Test
    fun `serves backups over TLS to clients with a trusted certificate`() {
        server(policy.enabled to true).use { server ->
            authority.issue().writeTo(server.home.resolve("certificates/backup"))
            server.start()
            val port = checkNotNull(server.backupPort())

            assertThat(ask(port, clientTls(authority.issue())))
                .contains("NOT_FOUND: no bulk source nothing")
            assertThat(ask(port, clientTls(TestAuthority("Foreign CA").issue())))
                .contains("UNAVAILABLE")
            assertThat(ask(port)).contains("UNAVAILABLE")
        }
    }

    @Test
    fun `Neo4j Community with DozenDB's jar serves no backups`() {
        CommunityServer(
                root.resolve("community"),
                settings = mapOf(BackupSettings.listenAddress to SocketAddress("127.0.0.1", 0)),
            )
            .start()
            .use { server -> assertThat(server.database().backupPort()).isNull() }
    }
}
