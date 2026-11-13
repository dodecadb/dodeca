package dev.dodeca.cluster.internal.forwarding

import dev.dodeca.consensus.ratis.ReloadableTls
import java.security.SecureRandom
import javax.net.ssl.KeyManager
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLContextSpi
import javax.net.ssl.SSLEngine
import javax.net.ssl.SSLParameters
import javax.net.ssl.TrustManager

/**
 * The TLS of a member's connections to other members' routing connectors: the current key,
 * certificate chain and trust of the cluster SSL policy, and its protocols and cipher suites at
 * start, as for Raft.
 */
internal object MemberSsl {
    fun context(tls: ReloadableTls): SSLContext {
        val context = SSLContext.getInstance("TLS")
        context.init(arrayOf(tls.keyManager), arrayOf(tls.trustManager), null)
        val start = tls.start
        if (start.protocols == null && start.cipherSuites == null) return context
        return RestrictedContext(context, start.protocols, start.cipherSuites)
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
