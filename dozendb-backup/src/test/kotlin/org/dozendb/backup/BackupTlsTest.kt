package org.dozendb.backup

import java.io.ByteArrayOutputStream
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.dozendb.consensus.ratis.TlsConfig
import org.dozendb.consensus.testing.TestAuthority
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.SettingValueParsers.SOCKET_ADDRESS
import org.neo4j.configuration.ssl.ClientAuth
import org.neo4j.configuration.ssl.SslPolicyConfig
import org.neo4j.configuration.ssl.SslPolicyScope
import org.neo4j.graphdb.config.Setting
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import org.neo4j.logging.NullLogProvider
import org.neo4j.logging.log4j.Log4jLogProvider
import org.neo4j.ssl.config.DefaultSslPolicyProvider

class BackupTlsTest {
    @TempDir lateinit var home: Path
    private val certificate = TestAuthority().issue()
    private val output = ByteArrayOutputStream()
    private val policy = SslPolicyConfig.forScope(SslPolicyScope.BACKUP)

    private fun config(vararg settings: Pair<Setting<*>, Any>): Config {
        certificate.writeTo(home.resolve("certificates/backup"))
        val builder = Config.newBuilder().set(GraphDatabaseSettings.neo4j_home, home)
        @Suppress("UNCHECKED_CAST")
        settings.forEach { (setting, value) -> builder.set(setting as Setting<Any>, value) }
        return builder.build()
    }

    private fun policies(config: Config) =
        DefaultSslPolicyProvider(
            DefaultFileSystemAbstraction(),
            config,
            false,
            false,
            NullLogProvider.getInstance(),
        )

    private fun server(address: String, vararg settings: Pair<Setting<*>, Any>): TlsConfig? {
        val config = config(*settings)
        return BackupTls.server(
            config,
            SOCKET_ADDRESS.parse(address),
            policies(config),
            Log4jLogProvider(output).getLog(BackupTls::class.java),
        )
    }

    @Test
    fun `serves backups in plaintext on a loopback address by default`() {
        assertThat(server("127.0.0.1:6362")).isNull()
        assertThat(server("localhost:6362")).isNull()
        assertThat(server("[::1]:6362")).isNull()
        assertThat(output.toString()).isEmpty()
    }

    @Test
    fun `serves backups on other addresses only after an explicit choice`() {
        for (address in listOf("0.0.0.0:6362", "192.0.2.1:6362")) {
            assertThatThrownBy { server(address) }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessage(
                    "Backups are served without authentication: to serve them on $address, " +
                        "which is not a loopback address, set dbms.ssl.policy.backup.enabled=true " +
                        "for TLS, with certificates in dbms.ssl.policy.backup.base_directory, or " +
                        "dbms.ssl.policy.backup.enabled=false for plaintext."
                )
        }
    }

    @Test
    fun `plaintext on another address is chosen explicitly, with a warning`() {
        assertThat(server("0.0.0.0:6362", policy.enabled to false)).isNull()
        assertThat(output.toString())
            .contains("WARN")
            .contains("Backups are served in plaintext on 0.0.0.0:6362")
    }

    @Test
    fun `the backup policy configures TLS with client certificates and, by default, hostname verification`() {
        val tls = checkNotNull(server("0.0.0.0:6362", policy.enabled to true))

        assertThat(tls.certificateChain).containsExactly(certificate.certificate)
        assertThat(tls.privateKey.encoded).isEqualTo(certificate.privateKey.encoded)
        assertThat(tls.requireClientCertificates).isTrue
        // Neo4j 2026's default; Neo4j 5.26's was false.
        assertThat(tls.verifyHostname).isTrue
        // The JDK's defaults without the CBC and static (EC)DH suites, which Neo4j 2026 means to
        // drop but does not.
        assertThat(tls.cipherSuites).contains("TLS_AES_128_GCM_SHA256")
        assertThat(tls.cipherSuites).noneMatch { suite -> INSECURE.any { it in suite } }
        assertThat(output.toString()).isEmpty()
    }

    @Test
    fun `serving clients without a trusted certificate warns, and OPTIONAL is refused`() {
        val tls =
            server("127.0.0.1:6362", policy.enabled to true, policy.client_auth to ClientAuth.NONE)

        assertThat(checkNotNull(tls).requireClientCertificates).isFalse
        assertThat(output.toString()).contains("WARN").contains("client_auth=NONE")
        assertThatThrownBy {
                server(
                    "127.0.0.1:6362",
                    policy.enabled to true,
                    policy.client_auth to ClientAuth.OPTIONAL,
                )
            }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("dbms.ssl.policy.backup.client_auth=OPTIONAL is not supported")
    }

    @Test
    fun `a client takes backups over TLS when the backup policy is enabled`() {
        val disabled = config()
        assertThat(BackupTls.client(disabled, policies(disabled))).isNull()
        val enabled = config(policy.enabled to true, policy.verify_hostname to false)
        val tls = checkNotNull(BackupTls.client(enabled, policies(enabled)))
        assertThat(tls.certificateChain).containsExactly(certificate.certificate)
        assertThat(tls.verifyHostname).isFalse
        assertThat(tls.cipherSuites).noneMatch { suite -> INSECURE.any { it in suite } }
    }

    @Test
    fun `the cipher suites the operator chose are kept, CBC included`() {
        val cbc = "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256"
        val tls =
            server(
                "0.0.0.0:6362",
                policy.enabled to true,
                policy.ciphers to listOf("TLS_AES_128_GCM_SHA256", cbc),
            )

        assertThat(checkNotNull(tls).cipherSuites).containsExactly("TLS_AES_128_GCM_SHA256", cbc)
    }

    private companion object {
        val INSECURE = listOf("_CBC_", "_DH_", "_ECDH_")
    }
}
