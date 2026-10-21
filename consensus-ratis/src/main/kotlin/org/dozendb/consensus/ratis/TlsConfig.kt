package org.dozendb.consensus.ratis

import java.security.PrivateKey
import java.security.cert.X509Certificate
import javax.net.ssl.TrustManager

/**
 * TLS for the traffic between members (Raft, membership changes, probes and bulk transfers), or for
 * a [BulkServer] and its clients. Each side presents [certificateChain], for [privateKey], when it
 * accepts a connection and when it opens one, and accepts the peers whose certificates
 * [trustManagers] trust.
 *
 * @property protocols the TLS versions to allow; null for the defaults.
 * @property cipherSuites the cipher suites to allow; null for the defaults.
 * @property requireClientCertificates whether the side that accepts connections requires the side
 *   that opens them to present a trusted certificate. Without it, any client that reaches the port
 *   is served.
 * @property verifyHostname whether the side that opens a connection checks that the other side's
 *   certificate names the address it dialled.
 */
class TlsConfig(
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
        "TlsConfig(certificate=${certificateChain.first().subjectX500Principal}, " +
            "protocols=$protocols, cipherSuites=$cipherSuites, " +
            "requireClientCertificates=$requireClientCertificates, verifyHostname=$verifyHostname)"
}
