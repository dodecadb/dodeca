package org.dozendb.consensus.ratis

import java.net.Socket
import java.security.cert.X509Certificate
import javax.net.ssl.SSLEngine
import javax.net.ssl.TrustManager
import javax.net.ssl.X509ExtendedTrustManager
import javax.net.ssl.X509TrustManager
import org.apache.ratis.grpc.GrpcTlsConfig
import org.apache.ratis.security.TlsConf.CertificatesConf
import org.apache.ratis.security.TlsConf.PrivateKeyConf
import org.apache.ratis.thirdparty.io.grpc.netty.GrpcSslContexts
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContext
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContextBuilder

/** What [config] means to Ratis, and to the channels DozenDB opens itself. */
internal class TlsContexts(config: TlsConfig) {
    private val trustManager: TrustManager =
        config.trustManagers.filterIsInstance<X509TrustManager>().firstOrNull()?.let {
            // gRPC clients always check the name in a server's certificate.
            if (config.verifyHostname) it else NameIgnoringTrustManager(it)
        } ?: throw IllegalArgumentException("there is no X.509 trust manager")

    /** For Ratis's server, clients and admin clients. */
    val ratis: GrpcTlsConfig =
        GrpcTlsConfig.newBuilder()
            .setPrivateKey(PrivateKeyConf(config.privateKey))
            .setKeyCertificates(CertificatesConf(config.certificateChain))
            .setTrustManager(trustManager)
            .setMutualTls(config.requireClientCertificates)
            .apply {
                config.protocols?.let { setProtocols(it) }
                config.cipherSuites?.let { setCipherSuites(it) }
            }
            .build()

    /** For the probe and bulk transfer clients. */
    val client: SslContext =
        GrpcSslContexts.configure(SslContextBuilder.forClient())
            .keyManager(config.privateKey, config.certificateChain)
            .trustManager(trustManager)
            .protocols(config.protocols)
            .ciphers(config.cipherSuites)
            .build()

    /**
     * Trusts what [delegate] trusts, without checking that a server's certificate names the address
     * dialled. Extended, so that the JDK does not add that check itself.
     */
    private class NameIgnoringTrustManager(private val delegate: X509TrustManager) :
        X509ExtendedTrustManager() {
        override fun checkClientTrusted(chain: Array<X509Certificate>, authType: String) =
            delegate.checkClientTrusted(chain, authType)

        override fun checkClientTrusted(
            chain: Array<X509Certificate>,
            authType: String,
            socket: Socket,
        ) =
            if (delegate is X509ExtendedTrustManager) {
                delegate.checkClientTrusted(chain, authType, socket)
            } else {
                delegate.checkClientTrusted(chain, authType)
            }

        override fun checkClientTrusted(
            chain: Array<X509Certificate>,
            authType: String,
            engine: SSLEngine,
        ) =
            if (delegate is X509ExtendedTrustManager) {
                delegate.checkClientTrusted(chain, authType, engine)
            } else {
                delegate.checkClientTrusted(chain, authType)
            }

        override fun checkServerTrusted(chain: Array<X509Certificate>, authType: String) =
            delegate.checkServerTrusted(chain, authType)

        override fun checkServerTrusted(
            chain: Array<X509Certificate>,
            authType: String,
            socket: Socket,
        ) = delegate.checkServerTrusted(chain, authType)

        override fun checkServerTrusted(
            chain: Array<X509Certificate>,
            authType: String,
            engine: SSLEngine,
        ) = delegate.checkServerTrusted(chain, authType)

        override fun getAcceptedIssuers(): Array<X509Certificate> = delegate.acceptedIssuers
    }
}
