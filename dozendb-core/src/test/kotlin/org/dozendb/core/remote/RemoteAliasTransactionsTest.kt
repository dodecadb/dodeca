package org.dozendb.core.remote

import java.nio.file.Files
import java.nio.file.Path
import java.security.KeyStore
import javax.crypto.KeyGenerator
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowableOfType
import org.dozendb.core.DozenDbSettings
import org.dozendb.core.StandaloneServer
import org.dozendb.core.StandaloneServer.Companion.SYSTEM
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.driver.AuthToken
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.exceptions.Neo4jException
import org.neo4j.graphdb.config.Setting
import org.neo4j.string.SecureString

/**
 * Transactions in remote aliases, as drivers see them: they run on another DBMS, `there`, in the
 * database the alias targets, as the user it names, over `bolt` and `neo4j` URLs.
 */
class RemoteAliasTransactionsTest {
    // Kept when a test fails, for the servers' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private val servers = mutableListOf<StandaloneServer>()
    private val drivers = mutableListOf<Driver>()

    @AfterEach
    fun stop() {
        drivers.forEach(Driver::close)
        servers.forEach(StandaloneServer::close)
    }

    /** A server called [name], with a keystore for the passwords of its remote aliases. */
    private fun server(name: String, auth: Boolean = false): StandaloneServer {
        val home = Files.createDirectories(root.resolve(name))
        val keystore = home.resolve("aliases.p12")
        val key = KeyGenerator.getInstance("AES").apply { init(256) }.generateKey()
        val store = KeyStore.getInstance("PKCS12").apply { load(null, null) }
        store.setEntry(
            "aliases",
            KeyStore.SecretKeyEntry(key),
            KeyStore.PasswordProtection(KEYSTORE_SECRET.toCharArray()),
        )
        Files.newOutputStream(keystore).use { store.store(it, KEYSTORE_SECRET.toCharArray()) }
        val settings =
            mapOf<Setting<*>, Any>(
                BoltConnector.enabled to true,
                GraphDatabaseSettings.auth_enabled to auth,
                DozenDbSettings.keystorePath to keystore,
                DozenDbSettings.keystorePassword to SecureString(KEYSTORE_SECRET),
                DozenDbSettings.keyName to "aliases",
            )
        return StandaloneServer(home, settings).also {
            servers += it
            it.start()
        }
    }

    /** The DBMS that remote aliases name, with database films and alice, who publishes there. */
    private fun there(): StandaloneServer =
        server("there", auth = true).also {
            it.administer("CREATE DATABASE films WAIT")
            it.administer("CREATE USER alice SET PASSWORD 'alice-secret-1' CHANGE NOT REQUIRED")
            it.administer("GRANT ROLE publisher TO alice")
        }

    private fun StandaloneServer.driver(token: AuthToken = AuthTokens.none()): Driver =
        GraphDatabase.driver("bolt://${boltAddress()}", token).also { drivers += it }

    private fun Driver.run(
        cypher: String,
        database: String,
        parameters: Map<String, Any> = emptyMap(),
    ): List<Map<String, Any?>> =
        session(SessionConfig.forDatabase(database)).use { session ->
            session.run(cypher, parameters).list { it.asMap() }
        }

    private fun Driver.failure(cypher: String, database: String): Neo4jException =
        checkNotNull(catchThrowableOfType(Neo4jException::class.java) { run(cypher, database) }) {
            "$cypher succeeded"
        }

    /** Creates remote alias [alias] here for [target] there, with [scheme], as alice. */
    private fun StandaloneServer.alias(
        alias: String,
        there: StandaloneServer,
        scheme: String = "bolt",
        target: String = "films",
        password: String = "alice-secret-1",
    ) =
        administer(
            "CREATE ALIAS `$alias` FOR DATABASE `$target` AT '$scheme://${there.boltAddress()}' " +
                "USER alice PASSWORD '$password' DRIVER {ssl_enforced: false}"
        )

    private fun films(there: StandaloneServer): Long =
        there.rows("MATCH (f:Film) RETURN count(f) AS films", "films").single()["films"] as Long

    @Test
    fun `runs transactions in the database the alias targets, over bolt and neo4j URLs`() {
        val there = there()
        val here = server("here")
        here.alias("direct", there, "bolt")
        here.alias("routed", there, "neo4j")
        val driver = here.driver()

        for (alias in listOf("direct", "routed")) {
            driver.run(
                "CREATE (:Film {title: \$title, via: \$via})",
                alias,
                mapOf("title" to "Heat", "via" to alias),
            )
            driver.session(SessionConfig.forDatabase(alias)).use { session ->
                session.beginTransaction().use { tx ->
                    tx.run("CREATE (:Film {title: 'Ronin', via: \$via})", mapOf("via" to alias))
                        .consume()
                    tx.commit()
                }
                session.beginTransaction().use { tx ->
                    tx.run("CREATE (:Film {title: 'Rolled back'})").consume()
                    tx.rollback()
                }
                assertThat(
                        session.executeRead { tx ->
                            tx.run(
                                    "MATCH (f:Film {via: \$via}) RETURN f.title AS title ORDER BY title",
                                    mapOf("via" to alias),
                                )
                                .list { it["title"].asString() }
                        }
                    )
                    .containsExactly("Heat", "Ronin")
                // More rows than come in one batch.
                assertThat(
                        session
                            .run(
                                "UNWIND range(1, 2500) AS i RETURN sum(i) AS sum, count(i) AS rows"
                            )
                            .single()
                            .asMap()
                    )
                    .isEqualTo(mapOf("sum" to 3_126_250L, "rows" to 2500L))
                assertThat(session.run("UNWIND range(1, 2500) AS i RETURN i").list().size)
                    .isEqualTo(2500)
                assertThat(session.run("CALL db.info() YIELD name").single()["name"].asString())
                    .isEqualTo("films")
            }
        }
        assertThat(films(there)).isEqualTo(4)
        // USE names the alias in a query of another database's session.
        assertThat(driver.run("USE routed MATCH (f:Film) RETURN count(f) AS films", "neo4j"))
            .containsExactly(mapOf("films" to 4L))
        assertThat(here.rows("MATCH (f:Film) RETURN count(f) AS films").single()["films"])
            .isEqualTo(0L)
    }

    @Test
    fun `carries the other DBMS's bookmarks`() {
        val there = there()
        val here = server("here")
        here.alias("routed", there, "neo4j")
        val driver = here.driver()

        val bookmarks =
            driver.session(SessionConfig.forDatabase("routed")).use { session ->
                session.run("CREATE (:Film {title: 'Heat'})").consume()
                session.lastBookmarks()
            }
        assertThat(bookmarks).isNotEmpty()
        driver
            .session(
                SessionConfig.builder().withDatabase("routed").withBookmarks(bookmarks).build()
            )
            .use { session ->
                assertThat(
                        session
                            .run("MATCH (f:Film) RETURN count(f) AS films")
                            .single()["films"]
                            .asLong()
                    )
                    .isEqualTo(1L)
            }
    }

    @Test
    fun `logs in with what the alias has now, also while older transactions finish`() {
        val there = there()
        val here = server("here")
        here.alias("routed", there, "neo4j")
        val driver = here.driver()
        driver.run("CREATE (:Film {title: 'Heat'})", "routed")

        driver.session(SessionConfig.forDatabase("routed")).use { older ->
            older.beginTransaction().use { tx ->
                tx.run("CREATE (:Film {title: 'Ronin'})").consume()
                there.administer(
                    "ALTER USER alice SET PASSWORD 'alice-secret-2' CHANGE NOT REQUIRED"
                )
                here.administer("ALTER ALIAS routed SET DATABASE PASSWORD 'alice-secret-2'")

                assertThat(driver.run("MATCH (f:Film) RETURN count(f) AS films", "routed"))
                    .containsExactly(mapOf("films" to 1L))
                tx.commit()
            }
        }
        assertThat(films(there)).isEqualTo(2)
    }

    @Test
    fun `closes the connections of aliases that changed or were dropped`() {
        val there = there()
        val here = server("here")
        here.alias("direct", there, "bolt")
        here.alias("routed", there, "neo4j")
        val driver = here.driver()
        val aliases =
            here
                .database(SYSTEM)
                .dependencyResolver
                .resolveDependency(RemoteAliasTransactions::class.java)
        driver.run("RETURN 1", "direct")
        driver.run("RETURN 1", "routed")
        assertThat(aliases.pools()).isEqualTo(2)

        here.administer("ALTER ALIAS direct SET DATABASE PASSWORD 'alice-secret-1'")
        here.administer("DROP ALIAS routed FOR DATABASE")
        aliases.sweep()

        assertThat(aliases.pools()).isZero()
        driver.run("RETURN 1", "direct")
        assertThat(aliases.pools()).isEqualTo(1)
    }

    @Test
    fun `reports what fails there as remote execution failures`() {
        val there = there()
        val here = server("here")
        here.alias("routed", there, "neo4j")
        here.alias("wrong", there, "bolt", password = "not-alice-secret")
        here.alias("missing", there, "bolt", target = "missing")
        val driver = here.driver()

        // What the other DBMS reported is the cause, and its status is in the message.
        val division = driver.failure("RETURN 1 / 0", "routed")
        assertThat(division.code())
            .isEqualTo("Neo.ClientError.Statement.RemoteExecutionClientError")
        assertThat(division.gqlStatus()).isEqualTo("50N06")
        assertThat(division.message)
            .isEqualTo(
                "Remote execution in database 'films' of remote alias 'routed' failed with " +
                    "Neo.ClientError.Statement.ArithmeticError: / by zero"
            )
        val direct =
            there
                .driver(AuthTokens.basic("alice", "alice-secret-1"))
                .failure("RETURN 1 / 0", "films")
        assertThat(division.gqlCause().map { it.gqlStatus() to it.message })
            .hasValue(direct.gqlStatus() to direct.message)
        // Neo4j's query router parses queries here, to find where they run.
        assertThat(driver.failure("MATCH (f:Film RETURN f", "routed").code())
            .isEqualTo("Neo.ClientError.Statement.SyntaxError")

        // Neither the login nor the database there is this server's.
        val login = driver.failure("RETURN 1", "wrong")
        assertThat(login.code()).isEqualTo("Neo.ClientError.Statement.RemoteExecutionClientError")
        assertThat(login.message).contains("failed with Neo.ClientError.Security.Unauthorized")
        val missing = driver.failure("RETURN 1", "missing")
        assertThat(missing.code()).isEqualTo("Neo.ClientError.Statement.RemoteExecutionClientError")
        assertThat(missing.message)
            .contains("failed with Neo.ClientError.Database.DatabaseNotFound")
        assertThat(driver.run("RETURN 1 AS one", "neo4j")).containsExactly(mapOf("one" to 1L))

        there.stop()
        val down = driver.failure("RETURN 1", "routed")
        assertThat(down.code())
            .isEqualTo("Neo.TransientError.Statement.RemoteExecutionTransientError")
        assertThat(down.gqlStatus()).isEqualTo("08N17")
    }

    @Test
    fun `refuses alias chains through neo4j URLs`() {
        val there = there()
        val here = server("here")
        // On the other DBMS, a remote alias of its own.
        there.alias("onwards", here, "bolt", target = "neo4j")
        here.alias("chain", there, "neo4j", target = "onwards")

        val chain = here.driver().failure("RETURN 1", "chain")
        assertThat(chain.code()).isEqualTo("Neo.ClientError.Statement.RemoteExecutionClientError")
        assertThat(chain.message)
            .contains("failed with Neo.ClientError.Database.IllegalAliasChain")
            .contains("alias chains are not permitted")
    }

    @Test
    fun `refuses impersonation through remote aliases`() {
        val there = there()
        val here = server("here", auth = true)
        here.administer("ALTER USER neo4j SET PASSWORD 'neo4j-secret-1' CHANGE NOT REQUIRED")
        here.administer("CREATE USER bob SET PASSWORD 'bob-secret-1' CHANGE NOT REQUIRED")
        here.administer("GRANT IMPERSONATE (bob) ON DBMS TO admin")
        // Neo4j's query router checks first that bob may access the alias.
        here.administer("GRANT ACCESS ON DATABASE * TO PUBLIC")
        here.alias("routed", there, "neo4j")
        val admin = here.driver(AuthTokens.basic("neo4j", "neo4j-secret-1"))

        assertThat(admin.run("RETURN 1 AS one", "routed")).containsExactly(mapOf("one" to 1L))
        val impersonating =
            checkNotNull(
                catchThrowableOfType(Neo4jException::class.java) {
                    admin
                        .session(
                            SessionConfig.builder()
                                .withDatabase("routed")
                                .withImpersonatedUser("bob")
                                .build()
                        )
                        .use { it.run("RETURN 1").consume() }
                }
            )
        assertThat(impersonating.code())
            .isEqualTo("Neo.ClientError.Statement.UnsupportedOperationError")
        assertThat(impersonating.gqlStatus()).isEqualTo("51N31")
        assertThat(impersonating.message)
            .isEqualTo(
                "Impersonation is not supported through the remote database alias 'routed', " +
                    "whose transactions run as the user it names."
            )
    }

    private companion object {
        const val KEYSTORE_SECRET = "keystore-secret"
    }
}
