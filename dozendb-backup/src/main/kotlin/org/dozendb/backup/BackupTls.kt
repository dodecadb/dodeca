package org.dozendb.backup

import java.net.InetAddress
import java.net.UnknownHostException
import org.dozendb.consensus.ratis.TlsConfig
import org.dozendb.core.ssl.CipherSuites
import org.neo4j.configuration.Config
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.configuration.ssl.ClientAuth
import org.neo4j.configuration.ssl.SslPolicyConfig
import org.neo4j.configuration.ssl.SslPolicyScope
import org.neo4j.logging.Log
import org.neo4j.ssl.SslPolicy
import org.neo4j.ssl.config.SslPolicyProvider

/**
 * TLS for backups, configured with Neo4j's SSL policy of the backup scope,
 * `dbms.ssl.policy.backup.*`, on the server that serves them and on the client that takes them.
 * Backups have no other authentication, so a server serves them on an address that is not a
 * loopback one only after an explicit choice between TLS and plaintext.
 */
internal object BackupTls {
    private val policy = SslPolicyConfig.forScope(SslPolicyScope.BACKUP)

    /**
     * The TLS that [config] asks for to serve backups on [address], with the backup policy that
     * [policies] provides, or null for plaintext. Warns in [log] about choices that let any host
     * that reaches the address take backups.
     */
    fun server(
        config: Config,
        address: SocketAddress,
        policies: SslPolicyProvider,
        log: Log,
    ): TlsConfig? {
        val enabled = policy.enabled.name()
        if (!enabled(config)) {
            if (isLoopback(address)) return null
            require(config.isExplicitlySet(policy.enabled)) {
                "Backups are served without authentication: to serve them on $address, which is " +
                    "not a loopback address, set $enabled=true for TLS, with certificates in " +
                    "${policy.base_directory.name()}, or $enabled=false for plaintext."
            }
            log.warn(
                "Backups are served in plaintext on $address ($enabled=false): any host that " +
                    "reaches it can copy every database."
            )
            return null
        }
        val loaded = policies.getPolicy(SslPolicyScope.BACKUP)
        val clientAuth = policy.client_auth.name()
        require(loaded.clientAuth != ClientAuth.OPTIONAL) {
            "$clientAuth=OPTIONAL is not supported: set it to REQUIRE, so that only clients " +
                "with a trusted certificate can take backups, or to NONE."
        }
        if (loaded.clientAuth == ClientAuth.NONE) {
            log.warn(
                "Backups are served to clients without a trusted certificate " +
                    "($clientAuth=NONE): any host that reaches $address can copy every database."
            )
        }
        return of(config, loaded)
    }

    /** The TLS that [config] asks for to take backups, with [policies]; null for plaintext. */
    fun client(config: Config, policies: SslPolicyProvider): TlsConfig? =
        if (enabled(config)) of(config, policies.getPolicy(SslPolicyScope.BACKUP)) else null

    // Neo4j knows the policy's settings only once one of them is set.
    private fun enabled(config: Config): Boolean =
        config.isExplicitlySet(policy.enabled) && config.get(policy.enabled)

    private fun of(config: Config, loaded: SslPolicy) =
        TlsConfig(
            privateKey = loaded.privateKey(),
            certificateChain = loaded.certificateChain().toList(),
            trustManagers = loaded.trustManagerFactory.trustManagers.toList(),
            protocols = loaded.tlsVersions?.toList()?.takeIf { it.isNotEmpty() },
            cipherSuites = CipherSuites.of(config, SslPolicyScope.BACKUP, loaded),
            requireClientCertificates = loaded.clientAuth == ClientAuth.REQUIRE,
            verifyHostname = loaded.isVerifyHostname,
        )

    private fun isLoopback(address: SocketAddress): Boolean =
        try {
            InetAddress.getAllByName(address.hostname).all { it.isLoopbackAddress }
        } catch (_: UnknownHostException) {
            false
        }
}
