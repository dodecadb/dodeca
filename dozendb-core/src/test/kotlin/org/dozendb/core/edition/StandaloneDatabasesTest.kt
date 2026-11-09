package org.dozendb.core.edition

import java.io.IOException
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicBoolean
import org.assertj.core.api.Assertions.assertThat
import org.dozendb.core.StandaloneServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.io.fs.watcher.FileWatcher
import org.neo4j.io.fs.watcher.resource.WatchedResource
import org.neo4j.monitoring.DatabaseHealth

/** With clustering disabled, databases follow the system graph as soon as a command commits. */
class StandaloneDatabasesTest {
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

    private fun statuses(): Map<String, String> =
        server.rows("SHOW DATABASES YIELD name, currentStatus", StandaloneServer.SYSTEM).associate {
            it["name"] as String to it["currentStatus"] as String
        }

    private fun films(database: String = "films"): Long =
        server.rows("MATCH (f:Film) RETURN count(f) AS films", database).single()["films"] as Long

    private fun storeOf(database: String): Path = server.home.resolve("data/databases/$database")

    @Test
    fun `creates, stops, starts and drops databases`() {
        server.administer("CREATE DATABASE films")
        assertThat(statuses()).containsEntry("films", "online")
        server.database("films").executeTransactionally("CREATE (:Film {title: 'Alien'})")

        server.administer("STOP DATABASE films")
        assertThat(statuses()).containsEntry("films", "offline")
        server.administer("START DATABASE films")
        assertThat(statuses()).containsEntry("films", "online")
        assertThat(films()).isEqualTo(1)

        server.administer("DROP DATABASE films")
        assertThat(statuses()).doesNotContainKey("films")
        assertThat(storeOf("films")).doesNotExist()
    }

    @Test
    fun `a database starts although an entry of a directory went while Neo4j began to watch it`() {
        server.close()
        val fileSystem = VanishingEntries()
        server = StandaloneServer(root.resolve("watched"), fileSystem = fileSystem)
        server.start()
        server.administer("CREATE DATABASE films")
        server.administer("STOP DATABASE films")

        fileSystem.vanishOnNextWatch()
        server.administer("START DATABASE films")

        assertThat(fileSystem.vanished).isTrue()
        assertThat(statuses()).containsEntry("films", "online")
    }

    /**
     * Neo4j's file system, whose file watcher fails to watch a directory once after
     * [vanishOnNextWatch], as the JDK's polling watch service (macOS) does when an entry of the
     * directory goes while it reads them, as another database's copy moves into place.
     */
    private class VanishingEntries(
        private val delegate: FileSystemAbstraction = DefaultFileSystemAbstraction()
    ) : FileSystemAbstraction by delegate {
        private val vanish = AtomicBoolean()

        @Volatile
        var vanished = false
            private set

        fun vanishOnNextWatch() = vanish.set(true)

        override fun fileWatcher(): FileWatcher {
            val watcher = delegate.fileWatcher()
            return object : FileWatcher by watcher {
                override fun watch(path: Path): WatchedResource {
                    if (vanish.getAndSet(false)) {
                        vanished = true
                        throw NoSuchFileException(path.resolve(".dozendb-copy-gone").toString())
                    }
                    return watcher.watch(path)
                }
            }
        }
    }

    @Test
    fun `a database left half-moved by an interrupted restore does not start`() {
        server.administer("CREATE DATABASE films")
        server.administer("STOP DATABASE films")
        val marker = server.home.resolve("data/databases/.dozendb-incomplete-films")
        Files.createFile(marker)

        server.administer("START DATABASE films")

        val films =
            server
                .rows(
                    "SHOW DATABASE films YIELD currentStatus, statusMessage",
                    StandaloneServer.SYSTEM,
                )
                .single()
        assertThat(films["currentStatus"]).isNotEqualTo("online")
        assertThat(films["statusMessage"] as String)
            .contains("its files were left half-moved into place")
        Files.delete(marker)
        // As for any database that failed to start.
        server.administer("STOP DATABASE films")
        server.administer("START DATABASE films")
        assertThat(statuses()).containsEntry("films", "online")
    }

    /** Neo4j Community shows a panicked database as online; Neo4j documents this as offline. */
    @Test
    fun `a panicked database is shown offline with why, until restarted`() {
        server.administer("CREATE DATABASE films")
        server
            .database("films")
            .dependencyResolver
            .resolveDependency(DatabaseHealth::class.java)
            .panic(IOException("No space left on device"))

        val films =
            server
                .rows(
                    "SHOW DATABASE films YIELD currentStatus, statusMessage",
                    StandaloneServer.SYSTEM,
                )
                .single()
        assertThat(films["currentStatus"]).isEqualTo("offline")
        assertThat(films["statusMessage"]).isEqualTo("No space left on device")
        assertThat(statuses()).containsEntry("neo4j", "online")

        server.administer("STOP DATABASE films")
        server.administer("START DATABASE films")
        assertThat(statuses()).containsEntry("films", "online")
        assertThat(films()).isZero
    }

    @Test
    fun `follows parameterised and dotted names, as the management API uses them`() {
        val dbms = checkNotNull(server.dbms)
        dbms.createDatabase("films")
        assertThat(statuses()).containsEntry("films", "online")
        dbms.shutdownDatabase("films")
        assertThat(statuses()).containsEntry("films", "offline")
        server.administer("START DATABASE \$name", mapOf("name" to "films"))
        assertThat(statuses()).containsEntry("films", "online")
        dbms.dropDatabase("films")
        assertThat(statuses()).doesNotContainKey("films")

        server.administer("CREATE DATABASE film.archive")
        assertThat(statuses()).containsEntry("film.archive", "online")
        server.administer("STOP DATABASE film.archive")
        assertThat(statuses()).containsEntry("film.archive", "offline")
        server.administer("DROP DATABASE `film.archive`")
        assertThat(statuses()).doesNotContainKey("film.archive")
    }

    @Test
    fun `replaces a database, existing or not`() {
        server.administer("CREATE DATABASE films")
        server.database("films").executeTransactionally("CREATE (:Film {title: 'Alien'})")

        server.administer("CREATE OR REPLACE DATABASE films")
        server.administer("CREATE OR REPLACE DATABASE series")

        assertThat(statuses()).containsEntry("films", "online").containsEntry("series", "online")
        assertThat(films()).isZero()
    }

    @Test
    fun `keeps databases and their status across restarts`() {
        server.administer("CREATE DATABASE films")
        server.administer("CREATE DATABASE series")
        server.database("films").executeTransactionally("CREATE (:Film {title: 'Alien'})")
        server.administer("STOP DATABASE series")
        server.administer("STOP DATABASE neo4j")

        server.restart()

        assertThat(statuses())
            .containsEntry("films", "online")
            .containsEntry("series", "offline")
            .containsEntry("neo4j", "offline")
        assertThat(films()).isEqualTo(1)
        server.administer("START DATABASE series")
        server.administer("START DATABASE neo4j")
        assertThat(statuses()).containsEntry("series", "online").containsEntry("neo4j", "online")
    }
}
