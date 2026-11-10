package org.dozendb.core.edition

import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.dozendb.core.StandaloneServer
import org.dozendb.core.StandaloneServer.Companion.SYSTEM
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.kernel.api.exceptions.Status

/** The default database, which `dbms.setDefaultDatabase` changes as in Neo4j Enterprise. */
class DefaultDatabaseTest {
    // Kept when a test fails, for the server's logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private lateinit var server: StandaloneServer

    @BeforeEach
    fun start() {
        server = StandaloneServer(root).also { it.start() }
    }

    @AfterEach
    fun stop() {
        server.close()
    }

    private fun defaultDatabase(): Any? =
        server.rows("SHOW DEFAULT DATABASE YIELD name", SYSTEM).single()["name"]

    private fun homeDatabase(): Any? =
        server.rows("SHOW HOME DATABASE YIELD name", SYSTEM).single()["name"]

    private fun statuses(): Map<String, String> =
        server.rows("SHOW DATABASES YIELD name, currentStatus", SYSTEM).associate {
            it["name"] as String to it["currentStatus"] as String
        }

    private fun films(): Long =
        server.rows("MATCH (f:Film) RETURN count(f) AS films", "films").single()["films"] as Long

    private fun failure(cypher: String): Throwable =
        checkNotNull(catchThrowable { server.administer(cypher) }) { "$cypher succeeded" }

    private fun statusOf(error: Throwable): Status? =
        generateSequence(error) { it.cause }
            .filterIsInstance<Status.HasStatus>()
            .firstOrNull()
            ?.status()

    @Test
    fun `another database becomes the default once the default is stopped, and the old one may go`() {
        server.administer("CREATE DATABASE films WAIT")
        server.database("films").executeTransactionally("CREATE (:Film {title: 'Alien'})")

        assertThat(failure("CALL dbms.setDefaultDatabase('films')"))
            .hasMessage(
                "Unable to set the default database to 'films': stop the default database " +
                    "'neo4j' first."
            )
        server.administer("STOP DATABASE neo4j")
        val result = server.rows("CALL dbms.setDefaultDatabase('films')", SYSTEM)

        assertThat(result).containsExactly(mapOf("result" to "Default database set to films"))
        assertThat(defaultDatabase()).isEqualTo("films")
        assertThat(homeDatabase()).isEqualTo("films")
        assertThat(failure("DROP DATABASE films"))
            .hasMessage("Not allowed to delete the default database 'films'.")
        server.administer("DROP DATABASE neo4j")
        assertThat(statuses()).doesNotContainKey("neo4j")

        // initial.dbms.default_database still names neo4j, which no longer counts.
        server.restart()

        assertThat(defaultDatabase()).isEqualTo("films")
        assertThat(statuses()).containsOnlyKeys("system", "films").containsEntry("films", "online")
        assertThat(films()).isEqualTo(1)
    }

    @Test
    fun `the old default database is an ordinary one, and a stopped new default stays stopped`() {
        server.administer("CREATE DATABASE films WAIT")
        server.administer("STOP DATABASE neo4j")
        server.administer("CALL dbms.setDefaultDatabase('films')")
        server.administer("START DATABASE neo4j")
        server.administer("STOP DATABASE films")

        server.restart()

        assertThat(defaultDatabase()).isEqualTo("films")
        assertThat(statuses()).containsEntry("films", "offline").containsEntry("neo4j", "online")
        server.administer("START DATABASE films")
        assertThat(statuses()).containsEntry("films", "online")
    }

    @Test
    fun `refuses a default that is not a standard database`() {
        server.administer("CREATE DATABASE films WAIT")
        server.administer("CREATE ALIAS movies FOR DATABASE films")
        server.administer("STOP DATABASE neo4j")

        val missing = failure("CALL dbms.setDefaultDatabase('nothing')")
        assertThat(missing)
            .hasMessage("Unable to set the default database to 'nothing': Database does not exist.")
        assertThat(statusOf(missing)).isEqualTo(Status.Database.DatabaseNotFound)
        assertThat(failure("CALL dbms.setDefaultDatabase('movies')"))
            .hasMessage(
                "Unable to set the default database to 'movies': it is an alias, of 'films'."
            )
        assertThat(failure("CALL dbms.setDefaultDatabase('system')"))
            .hasMessage(
                "Unable to set the default database to 'system': the system database cannot " +
                    "be the default."
            )
        // The default already: nothing to do, also while it runs.
        server.administer("START DATABASE neo4j")
        assertThat(server.rows("CALL dbms.setDefaultDatabase('neo4j')", SYSTEM))
            .containsExactly(mapOf("result" to "Default database set to neo4j"))
        assertThat(defaultDatabase()).isEqualTo("neo4j")
    }
}
