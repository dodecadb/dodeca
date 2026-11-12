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
import org.neo4j.gqlstatus.ErrorGqlStatusObject
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

    /** The GQL statuses of [error] and of its causes, outermost first. */
    private fun gqlStatusesOf(error: Throwable): List<String> =
        generateSequence(error) { it.cause }
            .filterIsInstance<ErrorGqlStatusObject>()
            .flatMap { status -> generateSequence(status) { it.cause().orElse(null) } }
            .map { it.gqlStatus() }
            .toList()

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

        for ((command, verb) in listOf("STOP" to "stop", "DROP" to "delete")) {
            val error = server.failure("$command DATABASE system")
            assertThat(error).hasMessage("Not allowed to $verb the system database.")
            assertThat(gqlStatusesOf(error)).contains("51N69")
        }
        assertThat(server.names()).contains("system")
    }

    @Test
    fun `refuses what it does not support instead of ignoring it`() {
        val server = start()
        server.administer("CREATE DATABASE films")

        val composite = server.failure("CREATE COMPOSITE DATABASE catalog")
        assertThat(composite)
            .hasMessage(
                "Failed to create the specified database 'catalog': " +
                    "composite databases are not supported."
            )
        assertThat(gqlStatusesOf(composite)).contains("51N31")
        val dump = server.failure("DROP DATABASE films DUMP DATA")
        assertThat(dump)
            .hasMessage(
                "Failed to delete the specified database 'films': DUMP DATA is not supported."
            )
        assertThat(gqlStatusesOf(dump)).contains("51N31")
        assertThat(server.names()).contains("films").doesNotContain("catalog")
    }

    @Test
    fun `refuses options and topologies it does not support instead of ignoring them`() {
        val server = start()
        val failed = "Failed to create the specified database 'films'"

        // Seeds need DozenDB's backup module, which this server lacks.
        assertThat(server.failure("CREATE DATABASE films OPTIONS {seedURI: 's3://bucket/films'}"))
            .hasMessage("$failed: this server cannot create databases from seeds.")
        // seedSourceDatabase in Cypher 25 only, as in Neo4j Enterprise.
        for ((version, sourceDatabase) in listOf("5" to "", "25" to "'seedSourceDatabase', ")) {
            val option = catchThrowable {
                server.administer(
                    "CYPHER $version CREATE DATABASE films OPTIONS \$options",
                    mapOf("options" to mapOf("txLogEnrichment" to "FULL")),
                )
            }
            assertThat(option)
                .hasMessage(
                    "$failed: unsupported option(s) 'txLogEnrichment'. DozenDB supports " +
                        "'existingData', 'seedURI', 'seedRestoreUntil', $sourceDatabase" +
                        "'existingDataSeedServer', 'existingDataSeedInstance', 'storeFormat'."
                )
            assertThat(gqlStatusesOf(option)).contains("22N04")
        }
        // Neo4j Enterprise's S3SeedProvider took credentials in the command.
        val credentials = catchThrowable {
            server.administer(
                "CREATE DATABASE films OPTIONS \$options",
                mapOf(
                    "options" to
                        mapOf(
                            "seedURI" to "s3://bucket/films.backup",
                            "seedConfig" to "region=eu-west-1",
                            "seedCredentials" to "key;secret",
                        )
                ),
            )
        }
        assertThat(credentials)
            .hasMessageStartingWith(
                "$failed: unsupported option(s) 'seedConfig', 'seedCredentials', which Neo4j " +
                    "Enterprise's deprecated S3SeedProvider takes. DozenDB reads seeds in cloud " +
                    "storage with each cloud's own credentials"
            )
        assertThat(gqlStatusesOf(credentials)).contains("22N04")
        val notAMap = catchThrowable {
            server.administer(
                "CREATE DATABASE films OPTIONS \$options",
                mapOf("options" to "seedURI"),
            )
        }
        assertThat(notAMap)
            .hasMessage(
                "Could not create database with options 'String(\"seedURI\")'. Expected a map value."
            )
        assertThat(gqlStatusesOf(notAMap)).contains("22N27")
        val topology = server.failure("CREATE DATABASE films TOPOLOGY 2 PRIMARIES")
        assertThat(topology)
            .hasMessage(
                "$failed: 'films' needs 2 servers that can host it as a primary, and this server " +
                    "runs without clustering."
            )
        val secondaries = server.failure("CREATE DATABASE films TOPOLOGY 1 PRIMARY 1 SECONDARY")
        assertThat(secondaries)
            .hasMessage(
                "$failed: 'films' needs 1 server that can host it as a secondary, and this server " +
                    "runs without clustering."
            )
        assertThat(server.failure("CREATE DATABASE films TOPOLOGY 1 PRIMARY 21 SECONDARIES"))
            .hasMessage("$failed: a topology has 0 to 20 secondaries, not 21.")
        assertThat(server.names()).doesNotContain("films")

        server.administer("CREATE DATABASE films OPTIONS {}")
        server.administer(
            "CREATE DATABASE books OPTIONS \$options",
            mapOf("options" to emptyMap<String, Any>()),
        )
        assertThat(server.names()).contains("films", "books")
    }

    /** The warnings of [cypher] on `system`: their GQL statuses and descriptions. */
    private fun StandaloneServer.warnings(cypher: String): List<Pair<String, String>> =
        database(StandaloneServer.SYSTEM).executeTransactionally(cypher, emptyMap()) { result ->
            result.forEachRemaining {}
            result.gqlStatusObjects
                .filter { it.gqlStatus().startsWith("01") }
                .map { it.gqlStatus() to it.statusDescription() }
        }

    private fun StandaloneServer.stores(): Map<String, String> =
        rows("SHOW DATABASES YIELD name, store", StandaloneServer.SYSTEM).associate {
            it["name"] as String to it["store"] as String
        }

    @Test
    fun `creates a database in the store format asked for, warning of a deprecated one`() {
        val server = start()

        val standard = server.warnings("CREATE DATABASE films OPTIONS {storeFormat: 'standard'}")
        val aligned = server.warnings("CREATE DATABASE books OPTIONS {storeFormat: 'aligned'}")
        server.administer("CREATE OR REPLACE DATABASE series OPTIONS {storeFormat: 'standard'}")

        assertThat(standard)
            .singleElement()
            .satisfies({
                assertThat(it.first).isEqualTo("01N00")
                assertThat(it.second).contains("The targeted store format: standard is deprecated.")
            })
        assertThat(aligned).isEmpty()
        assertThat(server.stores()["films"]).contains("standard")
        assertThat(server.stores()["books"]).contains("aligned")
        assertThat(server.stores()["series"]).contains("standard")
        server.restart()
        assertThat(server.stores()["films"]).contains("standard")
        val block = server.failure("CREATE DATABASE comics OPTIONS {storeFormat: 'block'}")
        assertThat(block).hasMessageContaining("storeFormat").hasMessageContaining("'block'")
        assertThat(server.names()).doesNotContain("comics")
    }

    @Test
    fun `takes a seeding server by its deprecated option name, which needs clustering, not both`() {
        val server = start()

        val error =
            server.failure(
                "CREATE DATABASE films OPTIONS {existingData: 'use', " +
                    "existingDataSeedInstance: '6c0a1b1e-0000-4000-8000-000000000000'}"
            )

        assertThat(error)
            .hasMessageStartingWith(
                "Failed to create the specified database 'films': existingDataSeedInstance " +
                    "needs clustering."
            )
        assertThat(
                server.failure(
                    "CREATE DATABASE films OPTIONS {existingData: 'use', " +
                        "existingDataSeedServer: '6c0a1b1e-0000-4000-8000-000000000000', " +
                        "existingDataSeedInstance: '6c0a1b1e-0000-4000-8000-000000000000'}"
                )
            )
            .hasMessage(
                "Failed to create the specified database 'films': existingDataSeedInstance is " +
                    "the deprecated name of existingDataSeedServer. Give existingDataSeedServer " +
                    "alone."
            )
    }

    @Test
    fun `takes a topology of one primary, which a server without clustering has`() {
        val server = start()

        server.administer("CREATE DATABASE films TOPOLOGY 1 PRIMARY 0 SECONDARIES")
        server.administer(
            "CREATE DATABASE books TOPOLOGY \$p PRIMARY",
            mapOf("p" to 1),
        )
        server.administer("ALTER DATABASE films SET TOPOLOGY 1 PRIMARY")

        val counts =
            server.rows(
                "SHOW DATABASES YIELD name, requestedPrimariesCount, requestedSecondariesCount " +
                    "WHERE name IN ['films', 'books'] RETURN *",
                StandaloneServer.SYSTEM,
            )
        assertThat(counts.map { it["requestedPrimariesCount"] }).containsOnly(1L)
        assertThat(counts.map { it["requestedSecondariesCount"] }).containsOnly(0L)
        assertThat(server.failure("ALTER DATABASE films SET TOPOLOGY 3 PRIMARIES"))
            .hasMessageContaining("Failed to alter the specified database 'films'")
        assertThat(server.failure("ALTER DATABASE films SET TOPOLOGY 12 PRIMARIES"))
            .hasMessageContaining("a topology has 1 to 11 primaries, not 12")
        assertThat(server.failure("ALTER DATABASE nothing SET TOPOLOGY 1 PRIMARY"))
            .hasMessageContaining("nothing")
        server.administer("ALTER DATABASE nothing IF EXISTS SET TOPOLOGY 1 PRIMARY")
    }

    @Test
    fun `makes a database read-only and read-write again, by its name or an alias`() {
        val server = start()
        server.administer("CREATE DATABASE films WAIT")
        server.administer("CREATE ALIAS movies FOR DATABASE films")

        server.administer("ALTER DATABASE movies SET ACCESS READ ONLY")

        assertThat(server.rows("SHOW DATABASE films YIELD access", StandaloneServer.SYSTEM))
            .containsExactly(mapOf("access" to "read-only"))
        val films = server.database("films")
        assertThat(catchThrowable { films.executeTransactionally("CREATE (:Film)") })
            .hasMessageContaining("read-only")
        server.administer("ALTER DATABASE films SET ACCESS READ WRITE")
        films.executeTransactionally("CREATE (:Film)")
        assertThat(server.failure("ALTER DATABASE films SET OPTION txLogEnrichment 'FULL'"))
            .hasMessage(
                "Failed to alter the specified database 'films': options are not supported."
            )
    }

    /**
     * The rows WAIT [command] returns on `system`, and the GQL statuses and descriptions of the
     * notifications Neo4j Enterprise's Cypher 25 adds for each server.
     */
    private fun StandaloneServer.waitedWithStatuses(
        command: String
    ): Pair<List<Map<String, Any?>>, List<Pair<String, String>>> =
        database(StandaloneServer.SYSTEM).executeTransactionally(command, emptyMap()) { result ->
            val rows = result.asSequence().map { it.toMap() }.toList()
            rows to
                result.gqlStatusObjects
                    .filter { it.gqlStatus() in WAIT_STATUSES }
                    .map { it.gqlStatus() to it.statusDescription() }
        }

    /** This server's name, as SHOW SERVERS shows it. */
    private fun StandaloneServer.name(): String =
        rows("SHOW SERVERS YIELD name", StandaloneServer.SYSTEM).single()["name"] as String

    /** The single row WAIT returns without clustering, without its address. */
    private fun StandaloneServer.waited(command: String): Map<String, Any?> =
        rows(command, StandaloneServer.SYSTEM).single().filterKeys { it != "address" }

    private val caughtUp = mapOf("state" to "CaughtUp", "message" to "caught up", "success" to true)

    private fun StandaloneServer.status(name: String): Map<String, Any?> =
        rows(
                "SHOW DATABASE $name YIELD requestedStatus, currentStatus, statusMessage",
                StandaloneServer.SYSTEM,
            )
            .single()

    @Test
    fun `waits for database commands and reports how this server did`() {
        val server = start()

        assertThat(server.waited("CREATE DATABASE films WAIT")).isEqualTo(caughtUp)
        assertThat(server.status("films")["currentStatus"]).isEqualTo("online")
        assertThat(server.waited("STOP DATABASE films WAIT 5 SECONDS")).isEqualTo(caughtUp)
        assertThat(server.status("films"))
            .containsEntry("requestedStatus", "offline")
            .containsEntry("currentStatus", "offline")
        assertThat(server.waited("START DATABASE films WAIT 5 SECONDS")).isEqualTo(caughtUp)
        assertThat(server.status("films"))
            .containsEntry("requestedStatus", "online")
            .containsEntry("currentStatus", "online")
        assertThat(server.waited("CREATE OR REPLACE DATABASE films WAIT")).isEqualTo(caughtUp)
        assertThat(server.waited("DROP DATABASE films WAIT")).isEqualTo(caughtUp)
        assertThat(server.names()).doesNotContain("films")
        assertThat(server.rows("CREATE DATABASE neo4j IF NOT EXISTS WAIT", StandaloneServer.SYSTEM))
            .isEmpty()

        // Cypher 25 also notifies how the server did, as Neo4j Enterprise does; Cypher 5 does not.
        val (rows, statuses) = server.waitedWithStatuses("CYPHER 25 CREATE DATABASE films WAIT")
        assertThat(rows.single().filterKeys { it != "address" }).isEqualTo(caughtUp)
        assertThat(statuses)
            .singleElement()
            .satisfies({ (code, description) ->
                assertThat(code).isEqualTo("03N85")
                assertThat(description)
                    .contains(
                        "Server `'${server.name()}'` at address `'${rows.single()["address"]}'` has " +
                            "caught up."
                    )
            })
        assertThat(server.waitedWithStatuses("CYPHER 5 DROP DATABASE films WAIT").second).isEmpty()
    }

    @Test
    fun `reports a database that fails to start, with WAIT and without`() {
        val server = start()
        // Where the stores would be created: neither can be.
        Files.createFile(server.home.resolve("data/databases/films"))
        Files.createFile(server.home.resolve("data/databases/books"))

        val (rows, statuses) = server.waitedWithStatuses("CYPHER 25 CREATE DATABASE films WAIT")
        val films = rows.single()
        server.administer("CREATE DATABASE books")

        assertThat(films).containsEntry("state", "Failed").containsEntry("success", false)
        // The cause, which Neo4j's own message leaves out.
        assertThat(films["message"] as String).contains("Unable to start", "films", "Exception")
        // In Cypher 25 as a notification too, as in Neo4j Enterprise.
        assertThat(statuses)
            .singleElement()
            .satisfies({ (code, description) ->
                assertThat(code).isEqualTo("01N80")
                assertThat(description)
                    .contains(
                        "Server `'${server.name()}'` at address `'${films["address"]}'` failed: " +
                            films["message"]
                    )
            })
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
        // Aliases are not databases.
        server.administer("CREATE ALIAS movies FOR DATABASE films")
        server.administer("CREATE ALIAS pictures FOR DATABASE films")

        val error = server.failure("CREATE DATABASE series")

        assertThat(gqlStatusesOf(error)).contains("51N55")
        assertThat(error)
            .hasMessage(
                "Failed to create the specified database 'series': there may be at most 3 " +
                    "databases, including system. The total limit of databases is already " +
                    "reached. To create more you need to either drop databases or change the " +
                    "limit via the config setting 'dbms.max_databases'"
            )
        assertThat(server.names()).doesNotContain("series")
        server.administer("DROP DATABASE films CASCADE ALIASES")
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

    private companion object {
        /** Caught up, failed, still catching up, not available. */
        val WAIT_STATUSES = setOf("03N85", "01N80", "01N81", "01N82")
    }
}
