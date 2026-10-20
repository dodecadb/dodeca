package org.dozendb.cluster.internal

import org.dozendb.consensus.ratis.RatisTlsConfig
import org.neo4j.configuration.Config
import org.neo4j.configuration.ssl.ClientAuth
import org.neo4j.configuration.ssl.SslPolicyConfig
import org.neo4j.configuration.ssl.SslPolicyScope
import org.neo4j.logging.Log
import org.neo4j.ssl.config.SslPolicyLoader

/**
 * TLS between members, configured with Neo4j's SSL policy of the cluster scope,
 * `dbms.ssl.policy.cluster.*`. Clustering needs an explicit choice between TLS and plaintext, so
 * that traffic between members is never sent in plaintext by default.
 */
internal object ClusterTls {
    private val policy = SslPolicyConfig.forScope(SslPolicyScope.CLUSTER)

    /**
     * The TLS that [config] asks for, with the cluster policy that [policies] loaded, or null for
     * plaintext. Warns in [log] about choices that let any host that reaches the Raft port act as a
     * member.
     */
    fun of(config: Config, policies: SslPolicyLoader, log: Log): RatisTlsConfig? {
        val enabled = policy.enabled.name()
        require(config.isExplicitlySet(policy.enabled)) {
            "Clustering needs a choice for the traffic between members: set $enabled=true to " +
                "encrypt and authenticate it with TLS, with certificates in " +
                "${policy.base_directory.name()}, or $enabled=false to send it in plaintext."
        }
        if (!config.get(policy.enabled)) {
            log.warn(
                "Traffic between members is sent in plaintext ($enabled=false): any host that " +
                    "reaches the Raft port can read it and act as a member."
            )
            return null
        }
        val loaded = policies.getPolicy(SslPolicyScope.CLUSTER)
        val clientAuth = policy.client_auth.name()
        require(loaded.clientAuth != ClientAuth.OPTIONAL) {
            "$clientAuth=OPTIONAL is not supported: set it to REQUIRE, so that only members " +
                "with a trusted certificate can connect, or to NONE."
        }
        if (loaded.clientAuth == ClientAuth.NONE) {
            log.warn(
                "Members do not require certificates of the members that connect to them " +
                    "($clientAuth=NONE): any host that reaches the Raft port can act as a member."
            )
        }
        return RatisTlsConfig(
            privateKey = loaded.privateKey(),
            certificateChain = loaded.certificateChain().toList(),
            trustManagers = loaded.trustManagerFactory.trustManagers.toList(),
            protocols = loaded.tlsVersions?.toList()?.takeIf { it.isNotEmpty() },
            cipherSuites = loaded.cipherSuites?.takeIf { it.isNotEmpty() },
            requireClientCertificates = loaded.clientAuth == ClientAuth.REQUIRE,
            verifyHostname = loaded.isVerifyHostname,
        )
    }
}
