package org.dozendb.consensus.testing

import java.math.BigInteger
import java.net.InetAddress
import java.nio.file.Files
import java.nio.file.Path
import java.security.KeyPair
import java.security.KeyPairGenerator
import java.security.KeyStore
import java.security.PrivateKey
import java.security.cert.X509CRL
import java.security.cert.X509Certificate
import java.security.spec.ECGenParameterSpec
import java.time.Duration
import java.time.Instant
import java.util.Base64
import java.util.Date
import java.util.concurrent.atomic.AtomicLong
import javax.net.ssl.TrustManagerFactory
import org.bouncycastle.asn1.x500.X500Name
import org.bouncycastle.asn1.x509.BasicConstraints
import org.bouncycastle.asn1.x509.CRLReason
import org.bouncycastle.asn1.x509.ExtendedKeyUsage
import org.bouncycastle.asn1.x509.Extension
import org.bouncycastle.asn1.x509.GeneralName
import org.bouncycastle.asn1.x509.GeneralNames
import org.bouncycastle.asn1.x509.KeyPurposeId
import org.bouncycastle.asn1.x509.KeyUsage
import org.bouncycastle.cert.X509v2CRLBuilder
import org.bouncycastle.cert.X509v3CertificateBuilder
import org.bouncycastle.cert.jcajce.JcaX509CRLConverter
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder

/**
 * A certificate authority for tests of TLS between members, which issues member certificates. Keys
 * are EC P-256, which are quick to generate.
 */
class TestAuthority(name: String = "DozenDB test CA") {
    private val keyPair = newKeyPair()
    private val subject = X500Name("CN=$name")

    val certificate: X509Certificate =
        sign(
            JcaX509v3CertificateBuilder(
                    subject,
                    serial(),
                    Date.from(Instant.now().minus(Duration.ofDays(1))),
                    Date.from(Instant.now().plus(Duration.ofDays(VALID_DAYS))),
                    subject,
                    keyPair.public,
                )
                .addExtension(Extension.basicConstraints, true, BasicConstraints(true))
                .addExtension(
                    Extension.keyUsage,
                    true,
                    KeyUsage(KeyUsage.keyCertSign or KeyUsage.cRLSign),
                )
                .addExtension(
                    Extension.subjectKeyIdentifier,
                    false,
                    extensions.createSubjectKeyIdentifier(keyPair.public),
                )
        )

    /**
     * A certificate for a member reachable as [names], DNS names or IP addresses, valid from
     * [notBefore] until [notAfter], for both ends of a connection.
     */
    fun issue(
        names: List<String> = listOf("localhost", "127.0.0.1"),
        notBefore: Instant = Instant.now().minus(Duration.ofDays(1)),
        notAfter: Instant = Instant.now().plus(Duration.ofDays(VALID_DAYS)),
    ): TestCertificate {
        val member = newKeyPair()
        val alternativeNames = names.map { name ->
            if (name.first().isDigit() || ':' in name) {
                GeneralName(GeneralName.iPAddress, InetAddress.getByName(name).hostAddress)
            } else {
                GeneralName(GeneralName.dNSName, name)
            }
        }
        val issued =
            sign(
                JcaX509v3CertificateBuilder(
                        certificate,
                        serial(),
                        Date.from(notBefore),
                        Date.from(notAfter),
                        X500Name("CN=${names.firstOrNull() ?: "member"}"),
                        member.public,
                    )
                    .addExtension(Extension.basicConstraints, true, BasicConstraints(false))
                    .addExtension(
                        Extension.keyUsage,
                        true,
                        KeyUsage(KeyUsage.digitalSignature or KeyUsage.keyAgreement),
                    )
                    .addExtension(
                        Extension.extendedKeyUsage,
                        false,
                        ExtendedKeyUsage(
                            arrayOf(KeyPurposeId.id_kp_serverAuth, KeyPurposeId.id_kp_clientAuth)
                        ),
                    )
                    .addExtension(
                        Extension.subjectAlternativeName,
                        false,
                        GeneralNames(alternativeNames.toTypedArray()),
                    )
                    .addExtension(
                        Extension.authorityKeyIdentifier,
                        false,
                        extensions.createAuthorityKeyIdentifier(certificate),
                    )
            )
        return TestCertificate(member.private, issued, this)
    }

    /** A certificate revocation list of this authority that revokes [revoked]. */
    fun revocationList(vararg revoked: TestCertificate): X509CRL {
        val now = Date()
        val builder = X509v2CRLBuilder(subject, now)
        builder.setNextUpdate(Date.from(Instant.now().plus(Duration.ofDays(VALID_DAYS))))
        revoked.forEach {
            builder.addCRLEntry(it.certificate.serialNumber, now, CRLReason.keyCompromise)
        }
        builder.addExtension(
            Extension.authorityKeyIdentifier,
            false,
            extensions.createAuthorityKeyIdentifier(certificate),
        )
        return JcaX509CRLConverter().getCRL(builder.build(signer()))
    }

    private fun sign(builder: X509v3CertificateBuilder): X509Certificate =
        JcaX509CertificateConverter().getCertificate(builder.build(signer()))

    private fun signer() = JcaContentSignerBuilder("SHA256withECDSA").build(keyPair.private)

    companion object {
        private const val VALID_DAYS = 30L
        private val serials = AtomicLong(System.currentTimeMillis())
        private val extensions = JcaX509ExtensionUtils()

        private fun serial(): BigInteger = BigInteger.valueOf(serials.incrementAndGet())

        private fun newKeyPair(): KeyPair =
            KeyPairGenerator.getInstance("EC")
                .apply { initialize(ECGenParameterSpec("secp256r1")) }
                .generateKeyPair()

        /** Trust managers that trust certificates [authorities] issued. */
        fun trusting(vararg authorities: TestAuthority): TrustManagerFactory {
            val store = KeyStore.getInstance(KeyStore.getDefaultType()).apply { load(null) }
            authorities.forEachIndexed { i, authority ->
                store.setCertificateEntry("authority-$i", authority.certificate)
            }
            return TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
                .apply { init(store) }
        }
    }
}

/** A member's private key and certificate, issued by [authority]. */
class TestCertificate(
    val privateKey: PrivateKey,
    val certificate: X509Certificate,
    val authority: TestAuthority,
) {
    /**
     * The private key, the certificate, the certificates of the [trusted] authorities and the
     * [revocations], as PEM files by their paths in the layout Neo4j's SSL policies read from their
     * base directory: `private.key`, `public.crt`, `trusted/` and `revoked/`.
     */
    fun files(
        trusted: List<TestAuthority> = listOf(authority),
        revocations: List<X509CRL> = emptyList(),
    ): Map<String, String> =
        mapOf(
            "private.key" to pem("PRIVATE KEY", privateKey.encoded),
            "public.crt" to pem("CERTIFICATE", certificate.encoded),
        ) +
            trusted.mapIndexed { i, it ->
                "trusted/authority-$i.crt" to pem("CERTIFICATE", it.certificate.encoded)
            } +
            revocations.mapIndexed { i, it ->
                "revoked/revoked-$i.crl" to pem("X509 CRL", it.encoded)
            }

    /** Writes the [files] into [directory], which gets `trusted` and `revoked` even if empty. */
    fun writeTo(
        directory: Path,
        trusted: List<TestAuthority> = listOf(authority),
        revocations: List<X509CRL> = emptyList(),
    ): Path {
        Files.createDirectories(directory.resolve("trusted"))
        Files.createDirectories(directory.resolve("revoked"))
        files(trusted, revocations).forEach { (path, text) ->
            Files.writeString(directory.resolve(path), text)
        }
        return directory
    }

    private fun pem(type: String, der: ByteArray): String =
        "-----BEGIN $type-----\n" +
            Base64.getMimeEncoder(64, "\n".toByteArray()).encodeToString(der) +
            "\n-----END $type-----\n"
}
