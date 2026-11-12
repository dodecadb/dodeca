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
import org.neo4j.kernel.KernelVersionProvider

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

    // Enterprise's documented retry: START DATABASE once what failed the start is gone.
    @Test
    fun `START DATABASE starts a database whose start failed, once what failed it is gone`() {
        server.administer("CREATE DATABASE films")
        server.database("films").executeTransactionally("CREATE (:Film {title: 'Alien'})")
        server.administer("STOP DATABASE films")
        // A directory where a store file belongs: nobody can open the store, root neither.
        val nodes = storeOf("films").resolve("neostore.nodestore.db")
        val aside = root.resolve("nodestore.aside")
        Files.move(nodes, aside)
        Files.createDirectory(nodes)

        assertThat(startWaiting("films")).isFalse()
        assertThat(statuses()).containsEntry("films", "offline")
        assertThat(statusMessage("films")).contains("Unable to start")

        Files.delete(nodes)
        Files.move(aside, nodes)
        assertThat(startWaiting("films")).isTrue()

        assertThat(statuses()).containsEntry("films", "online")
        assertThat(films()).isEqualTo(1)
        assertStartedAfresh("films")
    }

    @Test
    fun `a database whose start failed late starts afresh, by START DATABASE or STOP and START`() {
        server.close()
        val fileSystem = VanishingEntries()
        server = StandaloneServer(root.resolve("watched"), fileSystem = fileSystem)
        server.start()
        server.administer("CREATE DATABASE films")
        server.database("films").executeTransactionally("CREATE (:Film {title: 'Alien'})")
        server.administer("STOP DATABASE films")

        // Neo4j watches a database's directories once its transaction logs are open.
        fileSystem.failWatches(true)
        assertThat(startWaiting("films")).isFalse()
        fileSystem.failWatches(false)
        assertThat(startWaiting("films")).isTrue()
        assertThat(films()).isEqualTo(1)
        assertStartedAfresh("films")

        server.administer("STOP DATABASE films")
        fileSystem.failWatches(true)
        assertThat(startWaiting("films")).isFalse()
        fileSystem.failWatches(false)
        server.administer("STOP DATABASE films")
        server.administer("START DATABASE films")
        assertThat(statuses()).containsEntry("films", "online")
        server.database("films").executeTransactionally("CREATE (:Film {title: 'Aliens'})")
        assertThat(films()).isEqualTo(2)
        assertStartedAfresh("films")
    }

    @Test
    fun `other commands leave a database whose start failed as it is`() {
        server.close()
        val fileSystem = VanishingEntries()
        server = StandaloneServer(root.resolve("watched"), fileSystem = fileSystem)
        server.start()
        server.administer("CREATE DATABASE films")
        server.administer("STOP DATABASE films")
        fileSystem.failWatches(true)
        assertThat(startWaiting("films")).isFalse()
        fileSystem.failWatches(false)
        val starts = starts("films")

        server.administer("CREATE DATABASE series")
        server.administer("ALTER DATABASE films SET ACCESS READ ONLY")

        assertThat(starts("films")).isEqualTo(starts)
        assertThat(statuses()).containsEntry("films", "offline")
    }

    /** Runs START DATABASE [name] WAIT; whether it reports success. */
    private fun startWaiting(name: String): Boolean =
        server.rows("START DATABASE $name WAIT", StandaloneServer.SYSTEM).single()["success"]
            as Boolean

    private fun statusMessage(name: String): String =
        server
            .rows(
                "SHOW DATABASE $name YIELD statusMessage",
                StandaloneServer.SYSTEM,
            )
            .single()["statusMessage"]
            as String

    /** How often [name] began to start. */
    private fun starts(name: String): Int =
        server.debugEntries().count {
            it.message.startsWith("Starting 'DatabaseId{") && it.message.contains("[$name]}'")
        }

    /**
     * Asserts that the last start of [name] started a new database object: one that began to start
     * before keeps what it started then, so that its transaction log starts twice, or Neo4j finds
     * two of its parts.
     */
    private fun assertStartedAfresh(name: String) {
        val entries = server.debugEntries()
        val last = entries.indexOfLast {
            it.message.startsWith("Starting 'DatabaseId{") && it.message.contains("[$name]}'")
        }
        val logs =
            entries.drop(last).count {
                it.message.startsWith("[$name/") &&
                    it.message.contains("Starting transaction log [") &&
                    it.message.contains("neostore.transaction.db")
            }
        assertThat(logs).describedAs("transaction log starts").isEqualTo(1)
        assertThat(
                server
                    .database(name)
                    .dependencyResolver
                    .resolveDependency(KernelVersionProvider::class.java)
            )
            .isNotNull()
    }

    /**
     * Neo4j's file system, whose file watcher fails to watch a directory once after
     * [vanishOnNextWatch], as the JDK's polling watch service (macOS) does when an entry of the
     * directory goes while it reads them, as another database's copy moves into place; and every
     * time while [failWatches], as when the system's watches run out.
     */
    private class VanishingEntries(
        private val delegate: FileSystemAbstraction = DefaultFileSystemAbstraction()
    ) : FileSystemAbstraction by delegate {
        private val vanish = AtomicBoolean()
        private val fail = AtomicBoolean()

        @Volatile
        var vanished = false
            private set

        fun vanishOnNextWatch() = vanish.set(true)

        fun failWatches(failing: Boolean) = fail.set(failing)

        override fun fileWatcher(): FileWatcher {
            val watcher = delegate.fileWatcher()
            return object : FileWatcher by watcher {
                override fun watch(path: Path): WatchedResource {
                    if (fail.get()) throw IOException("No watches left for $path")
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
