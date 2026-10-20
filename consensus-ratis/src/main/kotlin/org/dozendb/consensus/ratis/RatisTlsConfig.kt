package org.dozendb.consensus.ratis

import java.security.PrivateKey
import java.security.cert.X509Certificate
import javax.net.ssl.TrustManager

/**
 * TLS for all traffic between members: Raft, membership changes, probes and bulk transfers. Each
 * member presents [certificateChain], for [privateKey], when it accepts a connection and when it
 * opens one, and accepts the peers whose certificates [trustManagers] trust.
 *
 * @property protocols the TLS versions to allow; null for the defaults.
 * @property cipherSuites the cipher suites to allow; null for the defaults.
 * @property requireClientCertificates whether a member requires the members that connect to it to
 *   present a trusted certificate. Without it, any client that reaches the port can act as a
 *   member.
 * @property verifyHostname whether a member checks that a peer's certificate names the address it
 *   dialled.
 */
class RatisTlsConfig(
    val privateKey: PrivateKey,
    val certificateChain: List<X509Certificate>,
    val trustManagers: List<TrustManager>,
    val protocols: List<String>? = null,
    val cipherSuites: List<String>? = null,
    val requireClientCertificates: Boolean,
    val verifyHostname: Boolean,
) {
    init {
        require(certificateChain.isNotEmpty()) { "the certificate chain is empty" }
        require(trustManagers.isNotEmpty()) { "there are no trust managers" }
    }

    override fun toString(): String =
        "RatisTlsConfig(certificate=${certificateChain.first().subjectX500Principal}, " +
            "protocols=$protocols, cipherSuites=$cipherSuites, " +
            "requireClientCertificates=$requireClientCertificates, verifyHostname=$verifyHostname)"
}
