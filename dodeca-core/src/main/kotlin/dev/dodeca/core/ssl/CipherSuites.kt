package dev.dodeca.core.ssl

import org.neo4j.configuration.Config
import org.neo4j.configuration.ssl.SslPolicyConfig
import org.neo4j.configuration.ssl.SslPolicyScope
import org.neo4j.ssl.SslPolicy

/**
 * The cipher suites of Dodeca's own TLS channels, between members and for backups.
 *
 * Neo4j's documentation says that its default cipher suites leave out CBC suites since 2025.10.
 * Neo4j 2026's [SslPolicy] means to drop the JDK's default suites whose names hold `_CBC_`, `_DH_`
 * or `_ECDH_`, but compares whole names with those parts, so it drops none. Dodeca drops them on
 * its own channels; Neo4j's Bolt and HTTPS keep what Neo4j gives them.
 */
object CipherSuites {
    private val INSECURE = listOf("_CBC_", "_DH_", "_ECDH_")

    /**
     * The suites of [policy], the policy of [scope] in [config]: those the operator set, as they
     * are, or else the JDK's defaults without CBC and static (EC)DH suites. Null leaves the choice
     * to the TLS implementation.
     */
    fun of(config: Config, scope: SslPolicyScope, policy: SslPolicy): List<String>? {
        val suites = policy.cipherSuites?.takeIf { it.isNotEmpty() } ?: return null
        if (config.get(SslPolicyConfig.forScope(scope).ciphers) != null) return suites
        return suites.filterNot { suite -> INSECURE.any { it in suite } }
    }
}
