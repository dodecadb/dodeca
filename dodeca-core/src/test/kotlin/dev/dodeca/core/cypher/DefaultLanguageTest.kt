package dev.dodeca.core.cypher

import dev.dodeca.core.StandaloneServer
import java.nio.file.Path
import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.GraphDatabaseSettings.CypherVersion
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig

/**
 * The default Cypher version of a database, as Neo4j Enterprise 2025.06 has it: the version its
 * queries without a `CYPHER` prefix run in, given by `CREATE DATABASE` or `ALTER DATABASE`, or
 * recorded from `db.query.default_language` when the database is created.
 */
class DefaultLanguageTest {
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var server: StandaloneServer? = null

    @AfterEach
    fun stop() {
        server?.close()
    }

    private fun start(default: CypherVersion = CypherVersion.Cypher5): StandaloneServer =
        StandaloneServer(
                root,
                mapOf(
                    GraphDatabaseSettings.default_language to default,
                    BoltConnector.enabled to true,
                ),
            )
            .also {
                server = it
                it.start()
            }

    /**
     * The Cypher version that queries without a prefix run in on [database], through Bolt and
     * embedded: `coll.distinct` is a function of Cypher 25 only, and `SHOW FUNCTIONS` runs on every
     * database, `system` too.
     */
    private fun StandaloneServer.language(database: String): Pair<String, String> {
        val bolt =
            GraphDatabase.driver("bolt://${boltAddress()}").use { driver ->
                driver.session(SessionConfig.forDatabase(database)).use { session ->
                    session.run(PROBE).single()["count"].asLong()
                }
            }
        val embedded =
            database(database).executeTransactionally(PROBE, emptyMap()) {
                it.next()["count"] as Long
            }
        return version(bolt) to version(embedded)
    }

    private fun version(count: Long) =
        when (count) {
            0L -> CYPHER_5
            1L -> CYPHER_25
            else -> error("$count functions")
        }

    private fun StandaloneServer.awaitLanguage(database: String, language: String) =
        await atMost
            Duration.ofSeconds(30) untilAsserted
            {
                assertThat(language(database)).isEqualTo(language to language)
            }

    private fun StandaloneServer.shown(database: String): Any? =
        rows("SHOW DATABASE $database YIELD defaultLanguage", StandaloneServer.SYSTEM)
            .single()["defaultLanguage"]

    @Test
    fun `the probe tells the versions apart`() {
        val server = start()

        GraphDatabase.driver("bolt://${server.boltAddress()}").use { driver ->
            driver.session().use { session ->
                assertThat(session.run("CYPHER 5 $PROBE").single()["count"].asLong()).isEqualTo(0)
                assertThat(session.run("CYPHER 25 $PROBE").single()["count"].asLong()).isEqualTo(1)
            }
        }
    }

    @Test
    fun `ALTER DATABASE sets the version a database's queries run in, system's too`() {
        val server = start()
        assertThat(server.language("neo4j")).isEqualTo(CYPHER_5 to CYPHER_5)
        // The first start records the DBMS default on the databases it creates.
        assertThat(server.shown("neo4j")).isEqualTo(CYPHER_5)
        assertThat(server.shown(StandaloneServer.SYSTEM)).isEqualTo(CYPHER_5)

        server.administer("ALTER DATABASE neo4j SET DEFAULT LANGUAGE CYPHER 25")

        server.awaitLanguage("neo4j", CYPHER_25)
        assertThat(server.shown("neo4j")).isEqualTo(CYPHER_25)
        // A prefix overrides it.
        assertThat(
                server.database().executeTransactionally("CYPHER 5 $PROBE", emptyMap()) {
                    version(it.next()["count"] as Long)
                }
            )
            .isEqualTo(CYPHER_5)

        server.administer("ALTER DATABASE system SET DEFAULT LANGUAGE CYPHER 25")
        server.awaitLanguage(StandaloneServer.SYSTEM, CYPHER_25)
        assertThat(server.shown(StandaloneServer.SYSTEM)).isEqualTo(CYPHER_25)

        server.administer("ALTER DATABASE neo4j SET DEFAULT LANGUAGE CYPHER 5 WAIT")
        server.awaitLanguage("neo4j", CYPHER_5)
        assertThat(server.shown("neo4j")).isEqualTo(CYPHER_5)
        server.administer("ALTER DATABASE nowhere IF EXISTS SET DEFAULT LANGUAGE CYPHER 25")
    }

    @Test
    fun `CREATE DATABASE takes a version, in Cypher 5's and Cypher 25's forms`() {
        val server = start()

        server.administer("CREATE DATABASE films DEFAULT LANGUAGE CYPHER 25")
        server.administer("CYPHER 25 CREATE DATABASE books SET DEFAULT LANGUAGE CYPHER 25 WAIT")
        server.administer("CREATE OR REPLACE DATABASE comics DEFAULT LANGUAGE CYPHER 25")

        for (database in listOf("films", "books", "comics")) {
            assertThat(server.shown(database)).describedAs(database).isEqualTo(CYPHER_25)
            server.awaitLanguage(database, CYPHER_25)
        }
    }

    @Test
    fun `a database created without a version keeps the DBMS default it was created with`() {
        start(CypherVersion.Cypher25).use { server ->
            server.administer("CREATE DATABASE films")
            assertThat(server.shown("films")).isEqualTo(CYPHER_25)
            server.awaitLanguage("films", CYPHER_25)
        }

        val server = start(CypherVersion.Cypher5)
        server.administer("CREATE DATABASE books")

        assertThat(server.shown("books")).isEqualTo(CYPHER_5)
        server.awaitLanguage("books", CYPHER_5)
        // Changing the setting changes no database that exists.
        assertThat(server.shown("films")).isEqualTo(CYPHER_25)
        server.awaitLanguage("films", CYPHER_25)
    }

    private companion object {
        const val PROBE =
            "SHOW FUNCTIONS YIELD name WHERE name = 'coll.distinct' " + "RETURN count(*) AS count"
        const val CYPHER_5 = "CYPHER 5"
        const val CYPHER_25 = "CYPHER 25"
    }
}
