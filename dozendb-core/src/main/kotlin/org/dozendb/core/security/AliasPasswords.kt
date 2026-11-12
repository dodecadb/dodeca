package org.dozendb.core.security

import java.io.IOException
import java.nio.file.Files
import java.security.GeneralSecurityException
import java.security.KeyStore
import java.security.SecureRandom
import javax.crypto.Cipher
import javax.crypto.SecretKey
import javax.crypto.spec.GCMParameterSpec
import org.dozendb.core.DozenDbSettings
import org.neo4j.configuration.Config
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlParams
import org.neo4j.gqlstatus.GqlStatusInfoCodes
import org.neo4j.messages.MessageUtil

/**
 * The passwords of remote database aliases, which `system` keeps encrypted with the AES key that
 * the keystore settings name, as Neo4j Enterprise documents: AES/GCM, with a random iv for each
 * password. The key is read at each use, so that a changed keystore or setting applies without a
 * restart. A changed key cannot decrypt the passwords it did not encrypt: they must be set again.
 */
class AliasPasswords(private val config: Config) {
    /** A [password] encrypted with the random [iv]; the GCM tag is part of [password]. */
    class Encrypted(val password: ByteArray, val iv: ByteArray)

    fun encrypt(password: ByteArray): Encrypted {
        val key = key()
        val iv = ByteArray(IV_BYTES).also(random::nextBytes)
        val encrypted =
            try {
                cipher(Cipher.ENCRYPT_MODE, key, iv).doFinal(password)
            } catch (e: GeneralSecurityException) {
                throw failure(MessageUtil.failedToEncryptPassword(), e)
            }
        return Encrypted(encrypted, iv)
    }

    fun decrypt(encrypted: Encrypted): ByteArray {
        val key = key()
        return try {
            cipher(Cipher.DECRYPT_MODE, key, encrypted.iv).doFinal(encrypted.password)
        } catch (e: GeneralSecurityException) {
            throw failure(MessageUtil.failedToDecryptPassword(), e)
        }
    }

    private fun cipher(mode: Int, key: SecretKey, iv: ByteArray): Cipher =
        Cipher.getInstance(CIPHER).apply { init(mode, key, GCMParameterSpec(TAG_BITS, iv)) }

    private fun key(): SecretKey {
        val path = config.get(DozenDbSettings.keystorePath)
        val password = config.get(DozenDbSettings.keystorePassword)?.getString()?.toCharArray()
        val name = config.get(DozenDbSettings.keyName)
        if (path == null || password == null || name == null) throw unreadable(null)
        val store =
            try {
                KeyStore.getInstance(KEYSTORE_TYPE).apply {
                    Files.newInputStream(path).use { load(it, password) }
                }
            } catch (e: IOException) {
                throw unreadable(e)
            } catch (e: GeneralSecurityException) {
                throw unreadable(e)
            }
        val key =
            try {
                store.getKey(name, password)
            } catch (e: GeneralSecurityException) {
                throw unreadable(e)
            }
        return when (key) {
            null -> throw failure(MessageUtil.failedToFindEncryptionKeyInKeystore(name), null)
            is SecretKey -> key
            else -> throw unreadable(null)
        }
    }

    private fun unreadable(cause: Throwable?) =
        failure(
            MessageUtil.failedToReadEncryptionKey(
                DozenDbSettings.keystorePath.name(),
                DozenDbSettings.keystorePassword.name(),
                DozenDbSettings.keyName.name(),
            ),
            cause,
        )

    // Neo4j has no GQL status of its own for keystore failures: its general internal error.
    private fun failure(message: String, cause: Throwable?): InvalidArgumentException =
        InvalidArgumentException(
            ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_50N00)
                .withParam(GqlParams.StringParam.msgTitle, "Remote alias password")
                .withParam(GqlParams.StringParam.msg, message)
                .build(),
            message,
            cause,
        )

    private companion object {
        const val KEYSTORE_TYPE = "PKCS12"
        const val CIPHER = "AES/GCM/NoPadding"
        const val IV_BYTES = 12
        const val TAG_BITS = 128
        val random = SecureRandom()
    }
}
