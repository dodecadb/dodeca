package org.dozendb.core.cypher

import java.nio.file.Files
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.dozendb.core.DozenDbSettings
import org.dozendb.core.StandaloneServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.graphdb.config.Setting
import org.neo4j.kernel.api.exceptions.Status

/** CREATE, DROP, START and STOP DATABASE: what they refuse, and how they say so. */
class DatabaseCommandsTest {
    // Kept when a test fails, for the server's logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var server: StandaloneServer? = null

    @AfterEach
    fun stop() {
        server?.close()
    }

    private fun start(settings: Map<Setting<*>, Any> = emptyMap()): StandaloneServer =
        StandaloneServer(root, settings).also {
            server = it
            it.start()
        }

    private fun StandaloneServer.failure(cypher: String): Throwable =
        checkNotNull(catchThrowable { administer(cypher) }) { "$cypher succeeded" }

    private fun statusOf(error: Throwable): Status? =
        generateSequence(error) { it.cause }
            .filterIsInstance<Status.HasStatus>()
            .firstOrNull()
            ?.status()

    private fun StandaloneServer.names(): List<String> =
        rows("SHOW DATABASES YIELD name", StandaloneServer.SYSTEM).map { it["name"] as String }

    @Test
    fun `refuses to create a database twice, unless asked not to mind`() {
        val server = start()
        server.administer("CREATE DATABASE films")

        val error = server.failure("CREATE DATABASE films")

        assertThat(error)
            .hasMessage("Failed to create the specified database 'films': Database already exists.")
        assertThat(statusOf(error)).isEqualTo(Status.Database.ExistingDatabaseFound)
        server.administer("CREATE DATABASE films IF NOT EXISTS")
        assertThat(server.names()).containsOnlyOnce("films")
    }

    @Test
    fun `refuses invalid names`() {
        val server = start()

        assertThat(server.failure("CREATE DATABASE `films!`"))
            .hasMessageContaining("Database name 'films!' contains illegal characters.")
    }

    @Test
    fun `refuses to act on databases that do not exist, unless asked not to mind`() {
        val server = start()

        for ((command, verb) in listOf("START" to "start", "STOP" to "stop", "DROP" to "delete")) {
            val error = server.failure("$command DATABASE films")
            assertThat(error)
                .hasMessage(
                    "Failed to $verb the specified database 'films': Database does not exist."
                )
            assertThat(statusOf(error)).isEqualTo(Status.Database.DatabaseNotFound)
        }
        server.administer("DROP DATABASE films IF EXISTS")
    }

    @Test
    fun `refuses to stop or drop system`() {
        val server = start()

        assertThat(server.failure("STOP DATABASE system"))
            .hasMessage("Not allowed to stop the system database.")
        assertThat(server.failure("DROP DATABASE system"))
            .hasMessage("Not allowed to delete the system database.")
        assertThat(server.names()).contains("system")
    }

    @Test
    fun `refuses what it does not support instead of ignoring it`() {
        val server = start()
        server.administer("CREATE DATABASE films")

        assertThat(server.failure("CREATE COMPOSITE DATABASE catalog"))
            .hasMessage(
                "Failed to create the specified database 'catalog': " +
                    "composite databases are not supported."
            )
        assertThat(server.failure("DROP DATABASE films DUMP DATA"))
            .hasMessage(
                "Failed to delete the specified database 'films': DUMP DATA is not supported."
            )
        assertThat(server.names()).contains("films").doesNotContain("catalog")
    }

    @Test
    fun `refuses options and topologies instead of ignoring them`() {
        val server = start()
        val refusal = { option: String ->
            "Failed to create the specified database 'films': $option not supported."
        }

        assertThat(server.failure("CREATE DATABASE films OPTIONS {seedURI: 's3://bucket/films'}"))
            .hasMessage(refusal("OPTIONS are"))
        assertThat(
                catchThrowable {
                    server.administer(
                        "CREATE DATABASE films OPTIONS \$options",
                        mapOf("options" to mapOf("storeFormat" to "block")),
                    )
                }
            )
            .hasMessage(refusal("OPTIONS are"))
        assertThat(server.failure("CREATE DATABASE films TOPOLOGY 1 PRIMARY"))
            .hasMessage(refusal("TOPOLOGY is"))
        assertThat(server.names()).doesNotContain("films")

        server.administer("CREATE DATABASE films OPTIONS {}")
        server.administer(
            "CREATE DATABASE books OPTIONS \$options",
            mapOf("options" to emptyMap<String, Any>()),
        )
        assertThat(server.names()).contains("films", "books")
    }

    /** The single row WAIT returns without clustering, without its address. */
    private fun StandaloneServer.waited(command: String): Map<String, Any?> =
        rows(command, StandaloneServer.SYSTEM).single().filterKeys { it != "address" }

    private val caughtUp = mapOf("state" to "CaughtUp", "message" to "caught up", "success" to true)

    private fun StandaloneServer.status(name: String): Map<String, Any?> =
        rows(
                "SHOW DATABASE $name YIELD currentStatus, statusMessage",
                StandaloneServer.SYSTEM,
            )
            .single()

    @Test
    fun `waits for database commands and reports how this server did`() {
        val server = start()

        assertThat(server.waited("CREATE DATABASE films WAIT")).isEqualTo(caughtUp)
        assertThat(server.status("films")["currentStatus"]).isEqualTo("online")
        assertThat(server.waited("STOP DATABASE films WAIT 5 SECONDS")).isEqualTo(caughtUp)
        assertThat(server.status("films")["currentStatus"]).isEqualTo("offline")
        assertThat(server.waited("START DATABASE films WAIT 5 SECONDS")).isEqualTo(caughtUp)
        assertThat(server.waited("CREATE OR REPLACE DATABASE films WAIT")).isEqualTo(caughtUp)
        assertThat(server.waited("DROP DATABASE films WAIT")).isEqualTo(caughtUp)
        assertThat(server.names()).doesNotContain("films")
        assertThat(server.rows("CREATE DATABASE neo4j IF NOT EXISTS WAIT", StandaloneServer.SYSTEM))
            .isEmpty()
    }

    @Test
    fun `reports a database that fails to start, with WAIT and without`() {
        val server = start()
        // Where the stores would be created: neither can be.
        Files.createFile(server.home.resolve("data/databases/films"))
        Files.createFile(server.home.resolve("data/databases/books"))

        val films = server.waited("CREATE DATABASE films WAIT")
        server.administer("CREATE DATABASE books")

        assertThat(films).containsEntry("state", "Failed").containsEntry("success", false)
        // The cause, which Neo4j's own message leaves out.
        assertThat(films["message"] as String).contains("Unable to start", "films", "Exception")
        for (name in listOf("films", "books")) {
            assertThat(server.status(name)["currentStatus"])
                .describedAs(name)
                .isNotEqualTo("online")
            assertThat(server.status(name)["statusMessage"] as String)
                .describedAs(name)
                .isNotBlank()
        }
    }

    @Test
    fun `limits the number of databases, including system`() {
        val server = start(mapOf(DozenDbSettings.maxDatabases to 3))
        server.administer("CREATE DATABASE films")

        val error = server.failure("CREATE DATABASE series")

        assertThat(error)
            .hasMessage(
                "Failed to create the specified database 'series': there may be at most 3 " +
                    "databases, including system. The total limit of databases is already " +
                    "reached. To create more you need to either drop databases or change the " +
                    "limit via the config setting 'dbms.max_databases'"
            )
        assertThat(server.names()).doesNotContain("series")
        server.administer("DROP DATABASE films")
        server.administer("CREATE DATABASE series")
        assertThat(server.names()).contains("series")
    }

    @Test
    fun `refuses to drop or replace the default database`() {
        val server = start()

        assertThat(server.failure("DROP DATABASE neo4j"))
            .hasMessage("Not allowed to delete the default database 'neo4j'.")
        assertThat(server.failure("CREATE OR REPLACE DATABASE neo4j"))
            .hasMessage("Not allowed to delete the default database 'neo4j'.")
        assertThat(server.names()).contains("neo4j")
    }
}
