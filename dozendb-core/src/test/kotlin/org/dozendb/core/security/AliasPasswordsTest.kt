package org.dozendb.core.security

import java.nio.file.Files
import java.nio.file.Path
import java.security.KeyStore
import javax.crypto.KeyGenerator
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.dozendb.core.DozenDbSettings
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.string.SecureString

/** Passwords of remote aliases, encrypted with the key of the configured keystore. */
class AliasPasswordsTest {
    @TempDir lateinit var home: Path

    /** A keystore at [file] with a new AES key called [name], protected by [password]. */
    private fun keystore(
        file: String,
        name: String = "aliases",
        password: String = "store-secret",
    ) =
        home.resolve(file).also { path ->
            val key = KeyGenerator.getInstance("AES").apply { init(256) }.generateKey()
            val store = KeyStore.getInstance("PKCS12").apply { load(null, null) }
            store.setEntry(
                name,
                KeyStore.SecretKeyEntry(key),
                KeyStore.PasswordProtection(password.toCharArray()),
            )
            Files.newOutputStream(path).use { store.store(it, password.toCharArray()) }
        }

    private fun config(
        path: String? = "keystore.p12",
        password: String? = "store-secret",
        name: String? = "aliases",
    ): Config =
        Config.newBuilder()
            .set(GraphDatabaseSettings.neo4j_home, home)
            .apply {
                path?.let { set(DozenDbSettings.keystorePath, Path.of(it)) }
                password?.let { set(DozenDbSettings.keystorePassword, SecureString(it)) }
                name?.let { set(DozenDbSettings.keyName, it) }
            }
            .build()

    @Test
    fun `encrypts each password with a random iv, and decrypts it`() {
        keystore("keystore.p12")
        val passwords = AliasPasswords(config())

        val first = passwords.encrypt("alice-secret".toByteArray())
        val second = passwords.encrypt("alice-secret".toByteArray())

        assertThat(first.iv).hasSize(12).isNotEqualTo(second.iv)
        assertThat(first.password).isNotEqualTo(second.password)
        assertThat(String(first.password)).doesNotContain("alice-secret")
        assertThat(String(passwords.decrypt(first))).isEqualTo("alice-secret")
        assertThat(String(passwords.decrypt(second))).isEqualTo("alice-secret")
    }

    @Test
    fun `refuses to work without a readable key, naming the settings`() {
        keystore("keystore.p12")
        val unreadable =
            "Failed to read the symmetric key from the configured keystore. Please verify the " +
                "keystore configurations: dbms.security.keystore.path, " +
                "dbms.security.keystore.password, dbms.security.key.name."

        for (config in
            listOf(
                config(path = null),
                config(password = null),
                config(name = null),
                config(path = "missing.p12"),
                config(password = "wrong-secret"),
            )) {
            assertThatThrownBy { AliasPasswords(config).encrypt("secret".toByteArray()) }
                .isInstanceOf(InvalidArgumentException::class.java)
                .hasMessage(unreadable)
        }
        assertThatThrownBy { AliasPasswords(config(name = "other")).encrypt(ByteArray(1)) }
            .hasMessage(
                "Failed to read the symmetric key from the configured keystore. The key 'other' " +
                    "was not found in the given keystore file."
            )
    }

    @Test
    fun `cannot decrypt with another key, or what was changed`() {
        keystore("keystore.p12")
        keystore("other.p12")
        val encrypted = AliasPasswords(config()).encrypt("alice-secret".toByteArray())
        val failed = "Failed to decrypt remote user password."

        assertThatThrownBy { AliasPasswords(config(path = "other.p12")).decrypt(encrypted) }
            .hasMessage(failed)
        encrypted.password[0] = (encrypted.password[0] + 1).toByte()
        assertThatThrownBy { AliasPasswords(config()).decrypt(encrypted) }.hasMessage(failed)
    }

    @Test
    fun `reads the key at each use, as the settings are dynamic`() {
        keystore("keystore.p12")
        val config = config(path = "missing.p12")
        val passwords = AliasPasswords(config)
        assertThatThrownBy { passwords.encrypt(ByteArray(1)) }
            .isInstanceOf(InvalidArgumentException::class.java)

        config.setDynamic(DozenDbSettings.keystorePath, home.resolve("keystore.p12"), "test")

        assertThat(String(passwords.decrypt(passwords.encrypt("secret".toByteArray()))))
            .isEqualTo("secret")
    }
}
