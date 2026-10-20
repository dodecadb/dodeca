package org.dozendb.cluster.internal

import java.io.ByteArrayOutputStream
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.dozendb.consensus.ratis.RatisTlsConfig
import org.dozendb.consensus.testing.TestAuthority
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.ssl.ClientAuth
import org.neo4j.graphdb.config.Setting
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import org.neo4j.logging.NullLogProvider
import org.neo4j.logging.log4j.Log4jLogProvider
import org.neo4j.ssl.config.SslPolicyLoader

class ClusterTlsTest {
    @TempDir lateinit var home: Path
    private val certificate = TestAuthority().issue()
    private val output = ByteArrayOutputStream()
    private val policy = ClusterHarness.CLUSTER_TLS

    private fun tls(vararg settings: Pair<Setting<*>, Any>): RatisTlsConfig? {
        certificate.writeTo(home.resolve(ClusterHarness.CERTIFICATES))
        val builder = Config.newBuilder().set(GraphDatabaseSettings.neo4j_home, home)
        @Suppress("UNCHECKED_CAST")
        settings.forEach { (setting, value) -> builder.set(setting as Setting<Any>, value) }
        val config = builder.build()
        return ClusterTls.of(
            config,
            SslPolicyLoader.create(
                DefaultFileSystemAbstraction(),
                config,
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
    fun `the cluster policy configures mutual TLS, by default without hostname verification`() {
        val tls = checkNotNull(tls(policy.enabled to true))

        assertThat(tls.privateKey).isEqualTo(certificate.privateKey)
        assertThat(tls.certificateChain).containsExactly(certificate.certificate)
        assertThat(tls.trustManagers).isNotEmpty()
        assertThat(tls.requireClientCertificates).isTrue()
        assertThat(tls.verifyHostname).isFalse()
        assertThat(tls.protocols).containsExactly("TLSv1.2", "TLSv1.3")
        assertThat(tls.cipherSuites).isNull()
        assertThat(output.toString()).isEmpty()
    }

    @Test
    fun `the cluster policy's hostname verification, TLS versions and cipher suites apply`() {
        val tls =
            checkNotNull(
                tls(
                    policy.enabled to true,
                    policy.verify_hostname to true,
                    policy.tls_versions to listOf("TLSv1.3"),
                    policy.ciphers to listOf("TLS_AES_128_GCM_SHA256"),
                )
            )

        assertThat(tls.verifyHostname).isTrue()
        assertThat(tls.protocols).containsExactly("TLSv1.3")
        assertThat(tls.cipherSuites).containsExactly("TLS_AES_128_GCM_SHA256")
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
}
