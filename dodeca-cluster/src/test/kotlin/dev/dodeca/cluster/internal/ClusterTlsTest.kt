package dev.dodeca.cluster.internal

import dev.dodeca.consensus.ratis.TlsConfig
import dev.dodeca.consensus.testing.TestAuthority
import java.io.ByteArrayOutputStream
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.ssl.ClientAuth
import org.neo4j.graphdb.config.Setting
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import org.neo4j.logging.NullLogProvider
import org.neo4j.logging.log4j.Log4jLogProvider
import org.neo4j.ssl.config.DefaultSslPolicyProvider

class ClusterTlsTest {
    @TempDir lateinit var home: Path
    private val certificate = TestAuthority().issue()
    private val output = ByteArrayOutputStream()
    private val policy = ClusterHarness.CLUSTER_TLS

    private fun tls(vararg settings: Pair<Setting<*>, Any>): TlsConfig? {
        certificate.writeTo(home.resolve(ClusterHarness.CERTIFICATES))
        val builder = Config.newBuilder().set(GraphDatabaseSettings.neo4j_home, home)
        @Suppress("UNCHECKED_CAST")
        settings.forEach { (setting, value) -> builder.set(setting as Setting<Any>, value) }
        val config = builder.build()
        return ClusterTls.of(
            config,
            DefaultSslPolicyProvider(
                DefaultFileSystemAbstraction(),
                config,
                false,
                false,
                NullLogProvider.getInstance(),
            ),
            Log4jLogProvider(output).getLog(ClusterTls::class.java),
        )
    }

    @Test
    fun `clustering needs an explicit choice between TLS and plaintext`() {
        assertThatThrownBy { tls() }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("dbms.ssl.policy.cluster.enabled=true")
            .hasMessageContaining("dbms.ssl.policy.cluster.enabled=false")
    }

    @Test
    fun `plaintext is chosen explicitly, with a warning`() {
        assertThat(tls(policy.enabled to false)).isNull()
        assertThat(output.toString()).contains("WARN").contains("plaintext")
    }

    @Test
    fun `the cluster policy configures mutual TLS, by default with hostname verification`() {
        val tls = checkNotNull(tls(policy.enabled to true))

        assertThat(tls.privateKey).isEqualTo(certificate.privateKey)
        assertThat(tls.certificateChain).containsExactly(certificate.certificate)
        assertThat(tls.trustManagers).isNotEmpty()
        assertThat(tls.requireClientCertificates).isTrue()
        // Neo4j 2026's default; Neo4j 5.26's was false.
        assertThat(tls.verifyHostname).isTrue()
        assertThat(tls.protocols).containsExactly("TLSv1.2", "TLSv1.3")
        // Neo4j 2026 lists the JDK's default suites, where Neo4j 5.26 left them unset; Dodeca
        // drops the CBC and static (EC)DH ones, which Neo4j 2026 means to drop but does not.
        assertThat(tls.cipherSuites).contains("TLS_AES_128_GCM_SHA256", "TLS_AES_256_GCM_SHA384")
        assertThat(tls.cipherSuites).noneMatch { suite -> INSECURE.any { it in suite } }
        assertThat(output.toString()).isEmpty()
    }

    @Test
    fun `the cluster policy's hostname verification, TLS versions and cipher suites apply`() {
        val tls =
            checkNotNull(
                tls(
                    policy.enabled to true,
                    policy.verify_hostname to false,
                    policy.tls_versions to listOf("TLSv1.3"),
                    policy.ciphers to listOf("TLS_AES_128_GCM_SHA256", CBC),
                )
            )

        assertThat(tls.verifyHostname).isFalse()
        assertThat(tls.protocols).containsExactly("TLSv1.3")
        // The operator's choice, CBC included.
        assertThat(tls.cipherSuites).containsExactly("TLS_AES_128_GCM_SHA256", CBC)
    }

    @Test
    fun `client certificates may be not required, with a warning, but not optional`() {
        val tls = checkNotNull(tls(policy.enabled to true, policy.client_auth to ClientAuth.NONE))

        assertThat(tls.requireClientCertificates).isFalse()
        assertThat(output.toString()).contains("WARN").contains("client_auth=NONE")
        assertThatThrownBy {
                tls(policy.enabled to true, policy.client_auth to ClientAuth.OPTIONAL)
            }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("dbms.ssl.policy.cluster.client_auth=OPTIONAL is not supported")
    }

    private companion object {
        val INSECURE = listOf("_CBC_", "_DH_", "_ECDH_")
        const val CBC = "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256"
    }
}
