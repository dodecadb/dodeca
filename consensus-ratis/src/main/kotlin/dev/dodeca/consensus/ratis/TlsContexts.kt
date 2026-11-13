package dev.dodeca.consensus.ratis

import org.apache.ratis.grpc.GrpcTlsConfig
import org.apache.ratis.thirdparty.io.grpc.netty.GrpcSslContexts
import org.apache.ratis.thirdparty.io.netty.handler.ssl.ClientAuth
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContext
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContextBuilder

/**
 * What [tls] means to Ratis, and to the channels Dodeca opens itself: each presents and trusts
 * [tls]'s current certificate and authorities, with its protocols, cipher suites and checks at
 * start.
 */
internal class TlsContexts(tls: ReloadableTls) {
    private val start = tls.start
    private val keyManager = tls.keyManager
    private val trustManager = tls.trustManager

    /** For Ratis's server, clients and admin clients. */
    val ratis: GrpcTlsConfig =
        GrpcTlsConfig.newBuilder()
            .setKeyManager(keyManager)
            .setTrustManager(trustManager)
            .setMutualTls(start.requireClientCertificates)
            .apply {
                start.protocols?.let { setProtocols(it) }
                start.cipherSuites?.let { setCipherSuites(it) }
            }
            .build()

    /** For the probe and bulk transfer clients. */
    val client: SslContext =
        GrpcSslContexts.configure(SslContextBuilder.forClient())
            .keyManager(keyManager)
            .trustManager(trustManager)
            .protocols(start.protocols)
            .ciphers(start.cipherSuites)
            .build()

    /** For a [BulkServer]. */
    val server: SslContext by lazy {
        GrpcSslContexts.configure(SslContextBuilder.forServer(keyManager))
            .trustManager(trustManager)
            .clientAuth(
                if (start.requireClientCertificates) ClientAuth.REQUIRE else ClientAuth.NONE
            )
            .protocols(start.protocols)
            .ciphers(start.cipherSuites)
            .build()
    }
}
