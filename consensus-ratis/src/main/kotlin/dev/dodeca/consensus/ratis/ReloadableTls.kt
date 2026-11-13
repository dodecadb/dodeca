package dev.dodeca.consensus.ratis

import java.net.Socket
import java.security.KeyStore
import java.security.Principal
import java.security.PrivateKey
import java.security.cert.X509Certificate
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.SSLEngine
import javax.net.ssl.X509ExtendedKeyManager
import javax.net.ssl.X509ExtendedTrustManager
import javax.net.ssl.X509KeyManager
import javax.net.ssl.X509TrustManager

/**
 * The TLS of a member's channels, whose certificate and trusted authorities may change while it
 * runs ([reload]): connections opened afterwards use the new ones, open connections keep theirs, as
 * Neo4j Enterprise's `dbms.security.reloadTLS`. [keyManager] and [trustManager] always use the
 * current ones. The protocols, cipher suites, whether clients need certificates and hostname
 * verification shape a channel when it is made, so they stay as at start until a restart.
 */
class ReloadableTls(initial: TlsConfig) {
    /** The configuration at start, whose protocols, cipher suites and checks stay. */
    val start: TlsConfig = initial

    @Volatile private var loaded = Loaded(initial)

    /** The configuration whose certificate and trusted authorities are used now. */
    val current: TlsConfig
        get() = loaded.config

    /** Presents the current certificate. */
    val keyManager: X509ExtendedKeyManager = CurrentKeyManager()

    /**
     * Trusts what the current configuration trusts, without checking that a server's certificate
     * names the address dialled unless [TlsConfig.verifyHostname] was set at start: gRPC clients
     * always ask for that check.
     */
    val trustManager: X509ExtendedTrustManager = CurrentTrustManager()

    /**
     * Uses [next]'s certificate and trusted authorities from now on. Returns what [next] changes
     * that waits for a restart, in words; empty if nothing.
     */
    fun reload(next: TlsConfig): List<String> {
        val replacement = Loaded(next)
        loaded = replacement
        return listOfNotNull(
            "protocols".takeIf { next.protocols != start.protocols },
            "cipher suites".takeIf { next.cipherSuites != start.cipherSuites },
            "whether clients need certificates"
                .takeIf { next.requireClientCertificates != start.requireClientCertificates },
            "hostname verification".takeIf { next.verifyHostname != start.verifyHostname },
        )
    }

    override fun toString(): String = "ReloadableTls(current=$current)"

    /** [config]'s key and trust managers, made once per reload. */
    private class Loaded(val config: TlsConfig) {
        val keys: X509KeyManager =
            KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm())
                .apply {
                    val password = CharArray(0)
                    val store = KeyStore.getInstance("PKCS12")
                    store.load(null, null)
                    store.setKeyEntry(
                        ALIAS,
                        config.privateKey,
                        password,
                        config.certificateChain.toTypedArray(),
                    )
                    init(store, password)
                }
                .keyManagers
                .filterIsInstance<X509KeyManager>()
                .first()

        val trust: X509TrustManager =
            config.trustManagers.filterIsInstance<X509TrustManager>().firstOrNull()
                ?: throw IllegalArgumentException("there is no X.509 trust manager")
    }

    private inner class CurrentKeyManager : X509ExtendedKeyManager() {
        private val keys
            get() = loaded.keys

        override fun getClientAliases(keyType: String?, issuers: Array<out Principal>?) =
            keys.getClientAliases(keyType, issuers)

        // The one key, whichever authorities the peer names: as Neo4j presents its certificate.
        override fun chooseClientAlias(
            keyType: Array<out String>?,
            issuers: Array<out Principal>?,
            socket: Socket?,
        ): String? = keys.chooseClientAlias(keyType, null, socket)

        override fun chooseEngineClientAlias(
            keyType: Array<out String>?,
            issuers: Array<out Principal>?,
            engine: SSLEngine?,
        ): String? = keys.chooseClientAlias(keyType, null, null)

        override fun getServerAliases(keyType: String?, issuers: Array<out Principal>?) =
            keys.getServerAliases(keyType, issuers)

        override fun chooseServerAlias(
            keyType: String?,
            issuers: Array<out Principal>?,
            socket: Socket?,
        ): String? = keys.chooseServerAlias(keyType, null, socket)

        override fun chooseEngineServerAlias(
            keyType: String?,
            issuers: Array<out Principal>?,
            engine: SSLEngine?,
        ): String? = keys.chooseServerAlias(keyType, null, null)

        override fun getCertificateChain(alias: String?): Array<X509Certificate>? =
            keys.getCertificateChain(alias)

        override fun getPrivateKey(alias: String?): PrivateKey? = keys.getPrivateKey(alias)
    }

    private inner class CurrentTrustManager : X509ExtendedTrustManager() {
        private val trust
            get() = loaded.trust

        override fun checkClientTrusted(chain: Array<X509Certificate>, authType: String) =
            trust.checkClientTrusted(chain, authType)

        override fun checkClientTrusted(
            chain: Array<X509Certificate>,
            authType: String,
            socket: Socket,
        ) = trust.let {
            if (it is X509ExtendedTrustManager) it.checkClientTrusted(chain, authType, socket)
            else it.checkClientTrusted(chain, authType)
        }

        override fun checkClientTrusted(
            chain: Array<X509Certificate>,
            authType: String,
            engine: SSLEngine,
        ) = trust.let {
            if (it is X509ExtendedTrustManager) it.checkClientTrusted(chain, authType, engine)
            else it.checkClientTrusted(chain, authType)
        }

        override fun checkServerTrusted(chain: Array<X509Certificate>, authType: String) =
            trust.checkServerTrusted(chain, authType)

        override fun checkServerTrusted(
            chain: Array<X509Certificate>,
            authType: String,
            socket: Socket,
        ) = trust.let {
            if (start.verifyHostname && it is X509ExtendedTrustManager) {
                it.checkServerTrusted(chain, authType, socket)
            } else {
                it.checkServerTrusted(chain, authType)
            }
        }

        override fun checkServerTrusted(
            chain: Array<X509Certificate>,
            authType: String,
            engine: SSLEngine,
        ) = trust.let {
            if (start.verifyHostname && it is X509ExtendedTrustManager) {
                it.checkServerTrusted(chain, authType, engine)
            } else {
                it.checkServerTrusted(chain, authType)
            }
        }

        override fun getAcceptedIssuers(): Array<X509Certificate> = trust.acceptedIssuers
    }

    private companion object {
        const val ALIAS = "member"
    }
}
