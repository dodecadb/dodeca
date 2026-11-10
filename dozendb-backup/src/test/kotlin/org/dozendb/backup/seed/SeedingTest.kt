package org.dozendb.backup.seed

import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.nio.file.Files
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assertions.catchThrowable
import org.dozendb.backup.Admin
import org.dozendb.backup.Artifacts
import org.dozendb.backup.BackupSettings
import org.dozendb.backup.CommunityServer
import org.dozendb.backup.cli.RestoreCommand
import org.dozendb.cloud.internal.MemoryCloud
import org.dozendb.core.StandaloneServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.commandline.dbms.DumpCommand
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.consistency.ConsistencyCheckService
import org.neo4j.graphdb.config.Setting
import org.neo4j.io.layout.Neo4jLayout

/** CREATE DATABASE ... OPTIONS {seedURI: ...} on a server without clustering. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SeedingTest {
    // Kept when a test fails, for the servers' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var server: StandaloneServer? = null

    private lateinit var seeds: Path
    private lateinit var backup: Path
    private lateinit var dump: Path
    private lateinit var expected: Map<String, Any?>
    private lateinit var chain: Path
    private lateinit var differential: Path
    private lateinit var expectedChain: Map<String, Any?>

    /**
     * A full backup and a dump of the same database, which Neo4j Community wrote, and in a folder
     * of its own a chain of that full backup and a differential backup of a later write.
     */
    @BeforeAll
    fun seeds(@TempDir(cleanup = CleanupMode.ON_SUCCESS) directory: Path) {
        seeds = Files.createDirectories(directory.resolve("seeds"))
        val source = CommunityServer(directory.resolve("source")).start()
        CommunityServer.populate(source)
        expected = CommunityServer.summary(source)
        backup = seeds.resolve("films${Artifacts.EXTENSION}")
        val full = source.backupFull(backup)
        source.stop()
        val dumped = Admin.run(source.home, ::DumpCommand, "neo4j", "--to-path=$seeds")
        assertThat(dumped.exitCode).describedAs(dumped.toString()).isZero
        dump = seeds.resolve("neo4j.dump")
        source.start()
        source.database().executeTransactionally("CREATE (:Batch {n: 1})")
        expectedChain = CommunityServer.summary(source)
        chain = Files.createDirectories(directory.resolve("chain"))
        Files.copy(backup, chain.resolve("films-1${Artifacts.EXTENSION}"))
        differential = chain.resolve("films-2${Artifacts.EXTENSION}")
        source.backupDifferential(full.highestAppendIndex, differential)
        source.stop()
    }

    @AfterEach
    fun stop() {
        server?.close()
        MemoryCloud.clear()
    }

    private fun start(vararg settings: Pair<Setting<*>, Any>): StandaloneServer =
        StandaloneServer(
                root.resolve("server-${System.nanoTime()}"),
                mapOf(BackupSettings.enabled to false) + settings,
            )
            .also {
                server = it
                it.start()
            }

    private fun StandaloneServer.failure(
        cypher: String,
        parameters: Map<String, Any> = emptyMap(),
    ) = checkNotNull(catchThrowable { administer(cypher, parameters) }) { "$cypher succeeded" }

    private fun StandaloneServer.waited(command: String): Map<String, Any?> =
        rows(command, StandaloneServer.SYSTEM).single()

    private fun StandaloneServer.status(name: String): Map<String, Any?> =
        rows("SHOW DATABASE $name YIELD currentStatus, statusMessage", StandaloneServer.SYSTEM)
            .single()

    private fun StandaloneServer.consistent(name: String): Boolean {
        val config = Config.defaults(GraphDatabaseSettings.neo4j_home, home)
        val layout = Neo4jLayout.of(config).databaseLayout(name)
        return ConsistencyCheckService(layout).with(config).runFullConsistencyCheck().isSuccessful
    }

    @Test
    fun `creates a database from a full backup or a dump, as its seed`() {
        val server = start()

        for ((name, seed) in listOf("films" to backup, "books" to dump)) {
            val waited =
                server.waited(
                    "CREATE DATABASE $name OPTIONS {existingData: 'use', seedURI: '${seed.toUri()}'} WAIT"
                )
            assertThat(waited["success"]).describedAs("$waited").isEqualTo(true)
            assertThat(CommunityServer.summary(server.database(name))).isEqualTo(expected)
        }
        // As Neo4j Enterprise documents, a seed keeps its store format.
        server.administer(
            "CREATE DATABASE series OPTIONS {existingData: 'use', seedURI: '${backup.toUri()}', " +
                "storeFormat: 'standard'} WAIT"
        )
        assertThat(
                server.rows("SHOW DATABASE series YIELD store", StandaloneServer.SYSTEM).single()
            )
            .containsEntry("store", "record-aligned-1.1")
        server.administer("STOP DATABASE films WAIT")
        assertThat(server.consistent("films")).describedAs("consistent").isTrue
        // Nothing is left of the seeding.
        assertThat(Files.list(server.home.resolve("data/databases")).use { it.toList() })
            .noneMatch { it.fileName.toString().startsWith(".dozendb-") }
    }

    @Test
    fun `creates a database with existing data that a restore put in place`() {
        val server = start()
        val restored = Admin.run(server.home, ::RestoreCommand, "--from-path=$backup", "films")
        assertThat(restored.exitCode).describedAs(restored.toString()).isZero

        val waited = server.waited("CREATE DATABASE films OPTIONS {existingData: 'use'} WAIT")

        assertThat(waited["success"]).describedAs("$waited").isEqualTo(true)
        assertThat(CommunityServer.summary(server.database("films"))).isEqualTo(expected)
    }

    @Test
    fun `refuses what it cannot seed from, when the database is created`() {
        val server = start()
        val failed = "Failed to create the specified database 'films'"

        assertThat(server.failure("CREATE DATABASE films OPTIONS {seedURI: 'foo://bucket/films'}"))
            .hasMessage(
                "$failed: no seed provider for 'foo' URIs is enabled: " +
                    "dbms.databases.seed_from_uri_providers is FileSeedProvider, CloudSeedProvider."
            )
        assertThat(server.failure("CREATE DATABASE films OPTIONS {seedURI: 'https://host/films'}"))
            .hasMessage(
                "$failed: no seed provider for 'https' URIs is enabled: " +
                    "dbms.databases.seed_from_uri_providers is FileSeedProvider, " +
                    "CloudSeedProvider; URLConnectionSeedProvider reads them."
            )
        assertThat(server.failure("CREATE DATABASE films OPTIONS {seedURI: 'films.backup'}"))
            .hasMessage("$failed: seedURI 'films.backup' has no scheme.")
        assertThat(
                server.failure(
                    "CREATE DATABASE films OPTIONS \$options",
                    mapOf(
                        "options" to
                            mapOf("seedURI" to backup.toUri().toString(), "seedCredentials" to "x")
                    ),
                )
            )
            .hasMessageStartingWith(
                "$failed: unsupported option(s) 'seedCredentials', which Neo4j Enterprise's " +
                    "deprecated S3SeedProvider takes."
            )
        assertThat(server.failure("CREATE DATABASE films OPTIONS {existingData: 'reuse'}"))
            .hasMessageContaining("existingData")
            .hasMessageContaining("reuse")
        assertThat(
                server.failure(
                    "CREATE OR REPLACE DATABASE films OPTIONS {seedURI: '${backup.toUri()}'}"
                )
            )
            .hasMessage(
                "$failed: OR REPLACE cannot be combined with existingData, seedURI or " +
                    "existingDataSeedServer. Drop the database first."
            )
        assertThat(
                server.failure(
                    "CREATE DATABASE films OPTIONS {existingData: 'use', " +
                        "existingDataSeedServer: '2f3b4c5d-0000-4000-8000-000000000000'}"
                )
            )
            .hasMessage(
                "$failed: existingDataSeedServer needs clustering. Without it, restore the " +
                    "database with 'neo4j-admin database restore' and create it without options."
            )
        assertThat(server.failure("CREATE DATABASE films OPTIONS {existingData: 'use'}"))
            .hasMessage(
                "$failed: existingData 'use' needs its files in place, as 'neo4j-admin " +
                    "database restore' puts them, or a seedURI. There are no files."
            )
        assertThat(
                server.rows("SHOW DATABASES YIELD name", StandaloneServer.SYSTEM).map { it["name"] }
            )
            .doesNotContain("films")
    }

    @Test
    fun `a seed that cannot be read fails the database visibly, and a restart tries again`() {
        val later = seeds.resolve("later${Artifacts.EXTENSION}")
        val server = start()

        val waited =
            server.waited("CREATE DATABASE films OPTIONS {seedURI: '${later.toUri()}'} WAIT")

        assertThat(waited["success"]).isEqualTo(false)
        val message =
            "Unable to create `films` from its seed ${later.toUri()}: $later does not exist"
        assertThat(waited["message"] as String).contains(message).doesNotContain("Cause")
        assertThat(server.status("films")["currentStatus"]).isNotEqualTo("online")
        assertThat(server.status("films")["statusMessage"] as String).contains(message)

        Files.copy(backup, later)
        try {
            server.restart()
            assertThat(server.status("films")["currentStatus"]).isEqualTo("online")
            assertThat(CommunityServer.summary(server.database("films"))).isEqualTo(expected)
            // A database is seeded once only: what was written after stays.
            server.database("films").executeTransactionally("CREATE (:After)")
            server.restart()
            assertThat(
                    server
                        .rows("MATCH (n:After) RETURN count(n) AS count", "films")
                        .single()["count"]
                )
                .isEqualTo(1L)
        } finally {
            Files.delete(later)
        }
    }

    @Test
    fun `reads seeds over http with the URL connection provider`() {
        val http = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        http.createContext("/films.backup") { exchange ->
            exchange.sendResponseHeaders(200, Files.size(backup))
            exchange.responseBody.use { Files.copy(backup, it) }
        }
        http.start()
        try {
            val server =
                start(
                    BackupSettings.seedProviders to
                        listOf("FileSeedProvider", "URLConnectionSeedProvider")
                )
            val uri = "http://127.0.0.1:${http.address.port}/films.backup"

            val waited = server.waited("CREATE DATABASE films OPTIONS {seedURI: '$uri'} WAIT")

            assertThat(waited["success"]).describedAs("$waited").isEqualTo(true)
            assertThat(CommunityServer.summary(server.database("films"))).isEqualTo(expected)
        } finally {
            http.stop(0)
        }
    }

    @Test
    fun `refuses to start with a seed provider DozenDB does not have`() {
        assertThatThrownBy { start(BackupSettings.seedProviders to listOf("GcsSeedProvider")) }
            .rootCause()
            .hasMessageContaining(
                "DozenDB has no seed provider GcsSeedProvider: it has FileSeedProvider, " +
                    "URLConnectionSeedProvider, CloudSeedProvider, S3SeedProvider"
            )
    }

    @Test
    fun `creates a database from a chain of backups, a differential one or a folder of them`() {
        val server = start()

        for ((name, seed) in listOf("films" to differential, "series" to chain)) {
            val waited =
                server.waited(
                    "CREATE DATABASE $name OPTIONS {existingData: 'use', seedURI: " +
                        "'${seed.toUri()}'} WAIT"
                )
            assertThat(waited["success"]).describedAs("$waited").isEqualTo(true)
            assertThat(CommunityServer.summary(server.database(name))).isEqualTo(expectedChain)
        }
        val waited =
            server.waited("CREATE DATABASE books OPTIONS {seedURI: '${seeds.parent.toUri()}'} WAIT")
        assertThat(waited["message"] as String).contains("${seeds.parent} holds no backups")
    }

    @Test
    fun `creates databases from seeds in cloud storage, where they are`() {
        MemoryCloud.put("seeds", "films.backup", Files.readAllBytes(backup))
        MemoryCloud.put("seeds", "neo4j.dump", Files.readAllBytes(dump))
        for (file in listOf("films-1.backup", "films-2.backup")) {
            MemoryCloud.put("seeds", "chain/$file", Files.readAllBytes(chain.resolve(file)))
        }
        val server = start()

        for ((name, seed, summary) in
            listOf(
                Triple("films", "memory://seeds/films.backup", expected),
                Triple("books", "memory://seeds/neo4j.dump", expected),
                Triple("series", "memory://seeds/chain/films-2.backup", expectedChain),
                Triple("shows", "memory://seeds/chain/", expectedChain),
            )) {
            val waited =
                server.waited(
                    "CREATE DATABASE $name OPTIONS {existingData: 'use', seedURI: '$seed'} WAIT"
                )
            assertThat(waited["success"]).describedAs("$name: $waited").isEqualTo(true)
            assertThat(CommunityServer.summary(server.database(name))).isEqualTo(summary)
        }
        val missing =
            server.waited(
                "CREATE DATABASE plays OPTIONS {seedURI: 'memory://seeds/no.backup'} WAIT"
            )
        assertThat(missing["message"] as String)
            .contains(
                "from its seed memory://seeds/no.backup: memory://seeds/no.backup does not exist"
            )
    }

    @Test
    fun `S3SeedProvider reads s3 seeds as CloudSeedProvider does, and nothing else`() {
        assertThat(SeedProvider.S3.reads("s3")).isTrue
        assertThat(SeedProvider.S3.reads("gs")).isFalse
        assertThat(SeedProvider.Cloud.reads("gs")).isTrue
        val server = start(BackupSettings.seedProviders to listOf("S3SeedProvider"))

        assertThat(server.failure("CREATE DATABASE films OPTIONS {seedURI: 'gs://bucket/films'}"))
            .hasMessage(
                "Failed to create the specified database 'films': no seed provider for 'gs' URIs " +
                    "is enabled: dbms.databases.seed_from_uri_providers is S3SeedProvider; " +
                    "CloudSeedProvider reads them."
            )
    }
}
