package dev.dodeca.core.cypher

import dev.dodeca.core.DodecaSettings
import dev.dodeca.core.StandaloneServer
import dev.dodeca.core.StandaloneServer.Companion.SYSTEM
import dev.dodeca.core.security.AliasPasswords
import java.nio.file.Files
import java.nio.file.Path
import java.security.KeyStore
import java.util.UUID
import javax.crypto.KeyGenerator
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowableOfType
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel
import org.neo4j.driver.AuthToken
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.Values
import org.neo4j.driver.exceptions.ClientException
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.config.Setting
import org.neo4j.string.SecureString

/**
 * CREATE, ALTER, DROP and SHOW ALIASES for remote aliases, over Bolt as drivers see them: what they
 * keep in `system`, and what they refuse.
 */
class RemoteAliasCommandsTest {
    // Kept when a test fails, for the server's logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var server: StandaloneServer? = null
    private val drivers = mutableListOf<Driver>()

    @AfterEach
    fun stop() {
        drivers.forEach(Driver::close)
        server?.close()
    }

    /** A keystore with an AES key, as Neo4j's documentation has it made with keytool. */
    private fun keystore(): Path =
        root.resolve("aliases.p12").also { path ->
            val key = KeyGenerator.getInstance("AES").apply { init(256) }.generateKey()
            val store = KeyStore.getInstance("PKCS12").apply { load(null, null) }
            store.setEntry(
                "aliases",
                KeyStore.SecretKeyEntry(key),
                KeyStore.PasswordProtection(KEYSTORE_SECRET.toCharArray()),
            )
            Files.newOutputStream(path).use { store.store(it, KEYSTORE_SECRET.toCharArray()) }
        }

    /** Starts the server, and returns a driver of its admin `neo4j`. */
    private fun start(withKeystore: Boolean = true, auth: Boolean = false): Driver {
        val settings =
            mutableMapOf<Setting<*>, Any>(
                BoltConnector.enabled to true,
                GraphDatabaseSettings.auth_enabled to auth,
            )
        if (withKeystore) {
            settings[DodecaSettings.keystorePath] = keystore()
            settings[DodecaSettings.keystorePassword] = SecureString(KEYSTORE_SECRET)
            settings[DodecaSettings.keyName] = "aliases"
        }
        val server = StandaloneServer(root, settings).also { it.start() }
        this.server = server
        if (!auth) return driver(AuthTokens.none())
        server.administer("ALTER USER neo4j SET PASSWORD 'neo4j-secret-1' CHANGE NOT REQUIRED")
        return driver(AuthTokens.basic("neo4j", "neo4j-secret-1"))
    }

    private fun driver(token: AuthToken): Driver =
        GraphDatabase.driver("bolt://${checkNotNull(server).boltAddress()}", token).also {
            drivers += it
        }

    private fun Driver.run(
        cypher: String,
        parameters: Map<String, Any> = emptyMap(),
    ): List<Map<String, Any?>> =
        session(SessionConfig.forDatabase(SYSTEM)).use { session ->
            session.run(cypher, parameters).list { it.asMap() }
        }

    private fun Driver.refused(
        cypher: String,
        parameters: Map<String, Any> = emptyMap(),
    ): ClientException =
        checkNotNull(
            catchThrowableOfType(ClientException::class.java) { run(cypher, parameters) }
        ) {
            "$cypher succeeded"
        }

    /**
     * Every column of the aliases, but Cypher 25's `credentials`, which tells whether an alias is
     * remote, as `location` does, and which `creates remote aliases with encrypted credentials, and
     * shows them` checks.
     */
    private fun Driver.aliases(): List<Map<String, Any?>> =
        run("SHOW ALIASES FOR DATABASE YIELD * RETURN *").map { alias ->
            if ("credentials" in alias) {
                assertThat(alias["credentials"])
                    .isEqualTo(
                        if (alias["location"] == "remote") "STORED NATIVE CREDENTIALS" else null
                    )
            }
            alias - "credentials"
        }

    /** The properties of the alias node [name] in `system`, and the password it decrypts to. */
    private fun stored(name: String): Pair<Map<String, Any?>, String> {
        val server = checkNotNull(server)
        return server.database(SYSTEM).beginTx().use { tx ->
            val node: Node =
                tx.findNodes(
                        TopologyGraphDbmsModel.DATABASE_NAME_LABEL,
                        TopologyGraphDbmsModel.NAME_PROPERTY,
                        name,
                    )
                    .stream()
                    .toList()
                    .single()
            val properties = node.allProperties
            val config =
                server.database(SYSTEM).dependencyResolver.resolveDependency(Config::class.java)
            val password =
                AliasPasswords(config)
                    .decrypt(
                        AliasPasswords.Encrypted(
                            properties["password"] as ByteArray,
                            properties["iv"] as ByteArray,
                        )
                    )
            properties to String(password)
        }
    }

    /** The routing table that drivers that route get for [name], as role and addresses. */
    private fun Driver.routing(name: String, context: Map<String, Any> = emptyMap()) =
        session(SessionConfig.forDatabase("neo4j")).use { session ->
            session
                .run(
                    "CALL dbms.routing.getRoutingTable(\$context, \$name) YIELD servers " +
                        "UNWIND servers AS server RETURN server.role AS role, " +
                        "server.addresses AS addresses",
                    mapOf("context" to context, "name" to name),
                )
                .list { it.asMap() }
        }

    @Test
    fun `routes drivers of a remote alias to this server, which forwards their queries`() {
        val admin = start()
        admin.run(
            "CREATE ALIAS `remote-films` FOR DATABASE films AT 'neo4j+s://films.example.com:7687' " +
                "USER alice PASSWORD 'alice-secret'"
        )

        // Drivers say in the routing context the address they connected to.
        val bolt = checkNotNull(server).boltAddress()
        val table = admin.routing("remote-films", mapOf("address" to bolt))

        assertThat(table.map { it["role"] }).containsExactlyInAnyOrder("WRITE", "READ", "ROUTE")
        assertThat(table.flatMap { it["addresses"] as List<*> }).containsExactly(bolt, bolt, bolt)
        // Another alias forwarding here would chain aliases, which Neo4j does not allow.
        assertThat(
                catchThrowableOfType(ClientException::class.java) {
                    admin.routing("remote-films", mapOf("alias" to "movies"))
                }
            )
            .hasMessageContaining("alias chains are not permitted")
        assertThat(catchThrowableOfType(ClientException::class.java) { admin.routing("missing") })
            .hasMessageContaining("'missing' because this database does not exist")
    }

    @Test
    fun `creates remote aliases with encrypted credentials, and shows them`() {
        val admin = start()

        admin.run(
            "CREATE ALIAS `remote-films` FOR DATABASE films AT 'neo4j+s://films.example.com:7687' " +
                "USER alice PASSWORD 'alice-secret' DRIVER {connection_timeout: duration('PT5S'), " +
                "connection_pool_max_size: 10, logging_level: 'debug'} PROPERTIES {Genre: 'drama'}"
        )

        val columns =
            mapOf(
                "name" to "remote-films",
                "composite" to null,
                "database" to "films",
                "location" to "remote",
                "url" to "neo4j+s://films.example.com:7687",
                "user" to "alice",
            )
        assertThat(admin.run("CYPHER 5 SHOW ALIASES FOR DATABASE")).containsExactly(columns)
        assertThat(admin.run("CYPHER 25 SHOW ALIASES FOR DATABASE"))
            .containsExactly(columns + ("credentials" to "STORED NATIVE CREDENTIALS"))
        val shown = admin.aliases().single()
        assertThat(shown["driver"])
            .isEqualTo(
                mapOf(
                    "connection_timeout" to Values.isoDuration(0, 0, 5, 0).asIsoDuration(),
                    "connection_pool_max_size" to 10L,
                    "logging_level" to "DEBUG",
                )
            )
        assertThat(shown["properties"]).isEqualTo(mapOf("genre" to "drama"))
        // Without a version of its own, it keeps the DBMS default it was created with.
        assertThat(shown["defaultLanguage"])
            .isEqualTo(
                if (System.getProperty(StandaloneServer.CYPHER_PROPERTY) == "Cypher25") "CYPHER 25"
                else "CYPHER 5"
            )
        val (properties, password) = stored("remote-films")
        assertThat(password).isEqualTo("alice-secret")
        assertThat(String(properties["password"] as ByteArray)).doesNotContain("alice-secret")
        assertThat(properties["iv"] as ByteArray).hasSize(12)
        assertThat(UUID.fromString(properties["version"] as String)).isNotNull()
        // A remote alias is no database's alias.
        assertThat(
                admin.run("SHOW DATABASES YIELD name, aliases").flatMap { it["aliases"] as List<*> }
            )
            .isEmpty()
    }

    @Test
    fun `takes parameters, and remote aliases without driver settings`() {
        val admin = start()

        admin.run(
            "CREATE ALIAS \$alias FOR DATABASE \$target AT \$url USER \$user PASSWORD \$password",
            mapOf(
                "alias" to "Remote.Films",
                "target" to "films",
                "url" to "bolt+ssc://films.example.com",
                "user" to "alice",
                "password" to "alice-secret",
            ),
        )

        val shown = admin.aliases().single()
        assertThat(shown)
            .containsEntry("name", "remote.films")
            .containsEntry("url", "bolt+ssc://films.example.com")
            .containsEntry("driver", emptyMap<String, Any>())
            .containsEntry("properties", emptyMap<String, Any>())
        assertThat(stored("remote.films").second).isEqualTo("alice-secret")
    }

    @Test
    fun `refuses URLs drivers cannot use, and insecure ones unless told to allow them`() {
        val admin = start()
        fun create(url: String) =
            "CREATE ALIAS remote FOR DATABASE films AT '$url' USER alice PASSWORD 'alice-secret'"

        assertThat(admin.refused(create("http://films.example.com")))
            .hasMessage(
                "The provided url 'http://films.example.com' has an invalid scheme. Please use " +
                    "one of the following schemes: neo4j, neo4j+s, neo4j+ssc, bolt, bolt+s, " +
                    "bolt+ssc."
            )
        assertThat(admin.refused(create("neo4j://films.example.com")))
            .hasMessage(
                "The provided url 'neo4j://films.example.com' is not a secure scheme. Please use " +
                    "one of the following schemes: neo4j+s, neo4j+ssc, bolt+s, bolt+ssc."
            )
        assertThat(admin.refused(create("neo4j+s://")))
            .hasMessage("Could not validate remote database alias url.")
        assertThat(admin.aliases()).isEmpty()

        admin.run("${create("neo4j://films.example.com")} DRIVER {ssl_enforced: false}")
        assertThat(admin.aliases().single())
            .containsEntry("url", "neo4j://films.example.com")
            .containsEntry("driver", mapOf("ssl_enforced" to false))
        // Insecure stays refused when the settings that allow it go.
        assertThat(admin.refused("ALTER ALIAS remote SET DATABASE DRIVER {}"))
            .hasMessageContaining("is not a secure scheme")
    }

    @Test
    fun `refuses driver settings remote aliases cannot have`() {
        val admin = start()
        val create =
            "CREATE ALIAS remote FOR DATABASE films AT 'neo4j+s://films.example.com' " +
                "USER alice PASSWORD 'alice-secret' DRIVER"

        assertThat(admin.refused("$create {colour: 'blue'}"))
            .hasMessageContaining("Failed to create the specified database alias 'remote'")
            .hasMessageContaining("colour")
        assertThat(admin.refused("$create {connection_pool_max_size: 0}"))
            .hasMessageContaining("connection_pool_max_size")
        assertThat(admin.refused("$create {connection_timeout: 'soon'}"))
            .hasMessageContaining("Invalid driver settings value for 'connection_timeout'")
        assertThat(admin.aliases()).isEmpty()
    }

    @Test
    fun `refuses remote aliases without a key to encrypt their passwords`() {
        val admin = start(withKeystore = false)

        assertThat(
                admin.refused(
                    "CREATE ALIAS remote FOR DATABASE films AT 'neo4j+s://films.example.com' " +
                        "USER alice PASSWORD 'alice-secret'"
                )
            )
            .hasMessage(
                "Failed to read the symmetric key from the configured keystore. Please verify " +
                    "the keystore configurations: dbms.security.keystore.path, " +
                    "dbms.security.keystore.password, dbms.security.key.name."
            )
        assertThat(admin.aliases()).isEmpty()
        // Local aliases need no key.
        admin.run("CREATE ALIAS movies FOR DATABASE neo4j")
    }

    @Test
    fun `alters what it is given of a remote alias`() {
        val admin = start()
        admin.run(
            "CREATE ALIAS remote FOR DATABASE films AT 'neo4j+s://films.example.com' " +
                "USER alice PASSWORD 'alice-secret' DRIVER {connection_pool_max_size: 10} " +
                "PROPERTIES {genre: 'drama'}"
        )
        val (before, _) = stored("remote")

        admin.run("ALTER ALIAS remote SET DATABASE PROPERTIES {genre: 'comedy'}")
        assertThat(stored("remote").first["version"]).isEqualTo(before["version"])

        admin.run(
            "ALTER ALIAS remote SET DATABASE TARGET movies AT 'bolt+s://movies.example.com' " +
                "USER bob PASSWORD 'bob-secret'"
        )
        val (after, password) = stored("remote")
        assertThat(password).isEqualTo("bob-secret")
        assertThat(after["version"]).isNotEqualTo(before["version"])
        assertThat(after["iv"]).isNotEqualTo(before["iv"])
        assertThat(admin.aliases().single())
            .containsEntry("database", "movies")
            .containsEntry("url", "bolt+s://movies.example.com")
            .containsEntry("user", "bob")
            .containsEntry("driver", mapOf("connection_pool_max_size" to 10L))
            .containsEntry("properties", mapOf("genre" to "comedy"))

        // Driver settings are replaced, not merged.
        admin.run("ALTER ALIAS remote SET DATABASE DRIVER {logging_level: 'info'}")
        assertThat(admin.aliases().single()["driver"]).isEqualTo(mapOf("logging_level" to "INFO"))
        admin.run("ALTER ALIAS remote SET DATABASE DRIVER {}")
        assertThat(admin.aliases().single()["driver"]).isEqualTo(emptyMap<String, Any>())
        admin.run("ALTER ALIAS missing IF EXISTS SET DATABASE USER carol")
    }

    @Test
    fun `keeps local aliases local and remote aliases remote`() {
        val admin = start()
        admin.run("CREATE DATABASE films")
        admin.run("CREATE ALIAS movies FOR DATABASE films")
        admin.run(
            "CREATE ALIAS remote FOR DATABASE films AT 'neo4j+s://films.example.com' " +
                "USER alice PASSWORD 'alice-secret'"
        )

        assertThat(admin.refused("ALTER ALIAS movies SET DATABASE USER bob"))
            .hasMessage(
                "Failed to alter the specified database alias 'movies': alter a local alias to " +
                    "a remote alias is not supported."
            )
        assertThat(admin.refused("ALTER ALIAS remote SET DATABASE TARGET films"))
            .hasMessage(
                "Failed to alter the specified database alias 'remote': alter a remote alias to " +
                    "a local alias is not supported."
            )
        // Properties are neither local nor remote.
        admin.run("ALTER ALIAS remote SET DATABASE PROPERTIES {genre: 'drama'}")

        // OR REPLACE replaces an alias of either kind with one of either kind.
        admin.run(
            "CREATE OR REPLACE ALIAS movies FOR DATABASE films AT 'neo4j+s://other' USER " +
                "alice PASSWORD 'alice-secret'"
        )
        admin.run("CREATE OR REPLACE ALIAS remote FOR DATABASE films")
        assertThat(admin.aliases().map { it["name"] to it["location"] })
            .containsExactlyInAnyOrder("movies" to "remote", "remote" to "local")
        admin.run("CREATE ALIAS movies IF NOT EXISTS FOR DATABASE films")
        assertThat(
                admin.refused(
                    "CREATE ALIAS films FOR DATABASE films AT 'neo4j+s://other' " +
                        "USER alice PASSWORD 'alice-secret'"
                )
            )
            .hasMessage(
                "Failed to create the specified database alias 'films': A database with that " +
                    "name already exists."
            )

        admin.run("DROP ALIAS movies FOR DATABASE")
        admin.run("DROP ALIAS remote FOR DATABASE")
        assertThat(admin.aliases()).isEmpty()
        // Nothing of the aliases is left in the system graph.
        checkNotNull(server).database(SYSTEM).beginTx().use { tx ->
            for (label in
                listOf(
                    TopologyGraphDbmsModel.DRIVER_SETTINGS_LABEL,
                    TopologyGraphDbmsModel.ALIAS_PROPERTIES_LABEL,
                    TopologyGraphDbmsModel.REMOTE_DATABASE_LABEL,
                )) {
                assertThat(tx.findNodes(label).stream().count()).describedAs("$label").isZero()
            }
        }
    }

    @Test
    fun `finds dotted names of aliases when a remote alias is named like their first part`() {
        val admin = start()
        admin.run(
            "CREATE ALIAS remote FOR DATABASE films AT 'neo4j+s://films.example.com' " +
                "USER alice PASSWORD 'alice-secret'"
        )
        admin.run("CREATE ALIAS remote.local FOR DATABASE neo4j")

        assertThat(admin.run("SHOW ALIAS remote.local FOR DATABASE YIELD name"))
            .containsExactly(mapOf("name" to "remote.local"))
        admin.run("DROP ALIAS remote.missing IF EXISTS FOR DATABASE")
        admin.run("ALTER ALIAS remote.local SET DATABASE PROPERTIES {dotted: true}")
        admin.run("DROP ALIAS remote.local FOR DATABASE")
        assertThat(admin.aliases().map { it["name"] }).containsExactly("remote")
    }

    @Test
    fun `manages remote aliases with the alias privileges`() {
        val admin = start(auth = true)
        admin.run("CREATE USER alice SET PASSWORD 'alice-secret-1' CHANGE NOT REQUIRED")
        admin.run("CREATE ROLE curator")
        admin.run("GRANT ROLE curator TO alice")
        val alice = driver(AuthTokens.basic("alice", "alice-secret-1"))
        val create =
            "CREATE ALIAS remote FOR DATABASE films AT 'neo4j+s://films.example.com' USER alice " +
                "PASSWORD 'alice-secret'"

        assertThat(alice.refused(create))
            .hasMessageStartingWith("Permission has not been granted for CREATE ALIAS.")
        admin.run("GRANT CREATE ALIAS ON DBMS TO curator")
        alice.run(create)
        assertThat(alice.refused("ALTER ALIAS remote SET DATABASE USER bob"))
            .hasMessageStartingWith("Permission has not been granted for ALTER ALIAS.")
    }

    private companion object {
        const val KEYSTORE_SECRET = "keystore-secret"
    }
}
