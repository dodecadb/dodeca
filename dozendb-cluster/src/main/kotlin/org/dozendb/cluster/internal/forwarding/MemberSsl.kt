package org.dozendb.cluster.internal.forwarding

import java.security.KeyStore
import java.security.SecureRandom
import javax.net.ssl.KeyManager
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLContextSpi
import javax.net.ssl.SSLEngine
import javax.net.ssl.SSLParameters
import javax.net.ssl.TrustManager
import org.dozendb.consensus.ratis.TlsConfig

/**
 * The TLS of a member's connections to other members' routing connectors: its key and certificate
 * chain, the trust and the protocols and cipher suites of the cluster SSL policy, as for Raft.
 */
internal object MemberSsl {
    fun context(tls: TlsConfig): SSLContext {
        val password = CharArray(0)
        val keyStore = KeyStore.getInstance("PKCS12")
        keyStore.load(null, null)
        keyStore.setKeyEntry(
            "member",
            tls.privateKey,
            password,
            tls.certificateChain.toTypedArray(),
        )
        val keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm())
        keys.init(keyStore, password)
        val context = SSLContext.getInstance("TLS")
        context.init(keys.keyManagers, tls.trustManagers.toTypedArray(), null)
        if (tls.protocols == null && tls.cipherSuites == null) return context
        return RestrictedContext(context, tls.protocols, tls.cipherSuites)
    }

    /** [context], whose engines use only [protocols] and [cipherSuites] where given. */
    private class RestrictedContext(
        context: SSLContext,
        protocols: List<String>?,
        cipherSuites: List<String>?,
    ) : SSLContext(Spi(context, protocols, cipherSuites), context.provider, context.protocol)

    private class Spi(
        private val context: SSLContext,
        private val allowedProtocols: List<String>?,
        private val allowedCipherSuites: List<String>?,
    ) : SSLContextSpi() {
        private fun restricted(engine: SSLEngine): SSLEngine = engine.apply {
            allowedProtocols?.let { enabledProtocols = it.toTypedArray() }
            allowedCipherSuites?.let { enabledCipherSuites = it.toTypedArray() }
        }

        override fun engineInit(
            keys: Array<out KeyManager>?,
            trust: Array<out TrustManager>?,
            random: SecureRandom?,
        ) = throw UnsupportedOperationException("already initialized")

        override fun engineGetSocketFactory() = context.socketFactory

        override fun engineGetServerSocketFactory() = context.serverSocketFactory

        override fun engineCreateSSLEngine(): SSLEngine = restricted(context.createSSLEngine())

        override fun engineCreateSSLEngine(host: String?, port: Int): SSLEngine =
            restricted(context.createSSLEngine(host, port))

        override fun engineGetServerSessionContext() = context.serverSessionContext

        override fun engineGetClientSessionContext() = context.clientSessionContext

        override fun engineGetDefaultSSLParameters(): SSLParameters =
            context.defaultSSLParameters.apply {
                allowedProtocols?.let { protocols = it.toTypedArray() }
                allowedCipherSuites?.let { cipherSuites = it.toTypedArray() }
            }

        override fun engineGetSupportedSSLParameters(): SSLParameters =
            context.supportedSSLParameters
    }
}
