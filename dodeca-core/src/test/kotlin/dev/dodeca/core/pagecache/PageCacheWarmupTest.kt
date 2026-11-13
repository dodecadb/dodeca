package dev.dodeca.core.pagecache

import dev.dodeca.core.StandaloneServer
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.graphdb.ExecutionPlanDescription
import org.neo4j.graphdb.config.Setting
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import org.neo4j.kernel.database.Database

/** Page cache warmup on a server: profiles taken as a database runs and stops, loaded on start. */
class PageCacheWarmupTest {
    // Kept when a test fails, for the logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var running: StandaloneServer? = null

    @AfterEach
    fun stop() {
        running?.close()
    }

    private fun start(settings: Map<Setting<*>, Any> = emptyMap()): StandaloneServer {
        running?.close()
        return StandaloneServer(root, settings).also {
            running = it
            it.start()
        }
    }

    private val StandaloneServer.directory: Path
        get() = home.resolve("data/databases/neo4j")

    private fun StandaloneServer.warmup(): PageCacheWarmup =
        database().dependencyResolver.resolveTypeDependencies(PageCacheWarmup::class.java).single()

    private fun StandaloneServer.awaitWarmedUp(): PageCacheWarmup {
        val warmup = warmup()
        await withPollInterval
            Duration.ofMillis(50) atMost
            Duration.ofSeconds(60) untilAsserted
            {
                assertThat(warmup.warmedUp).isTrue()
            }
        return warmup
    }

    /** Persons with a name each, a few MiB of store files that the page cache holds whole. */
    private fun StandaloneServer.persons() {
        rows("UNWIND range(1, $PERSONS) AS i CREATE (:Person {id: i, name: 'person ' + i})")
    }

    /** The page cache misses of reading every person. */
    private fun StandaloneServer.missesReadingPersons(): Long =
        database().executeTransactionally(
            "PROFILE MATCH (p:Person) RETURN sum(size(p.name)) AS size",
            emptyMap(),
        ) { result ->
            assertThat(result.next()["size"]).isNotNull()
            result.resultAsString()
            result.executionPlanDescription.misses()
        }

    private fun ExecutionPlanDescription.misses(): Long =
        profilerStatistics.pageCacheMisses + children.sumOf { it.misses() }

    private fun debugLog(): String = root.resolve("server/logs/debug.log").toFile().readText()

    @Test
    fun `a database profiles the pages it holds when it stops, and loads them when it starts`() {
        // No profiles while it runs.
        val settings =
            mapOf<Setting<*>, Any>(
                GraphDatabaseSettings.pagecache_warmup_profiling_interval to Duration.ofHours(1)
            )
        val first = start(settings)
        first.persons()
        first.awaitWarmedUp()
        assertThat(first.missesReadingPersons()).isZero()
        assertThat(first.directory.resolve("profiles")).doesNotExist()
        first.stop()
        assertThat(first.directory.resolve("profiles/neostore.nodestore.db.cacheprof"))
            .isRegularFile()
        assertThat(first.directory.resolve("profiles/neostore.propertystore.db.cacheprof"))
            .isRegularFile()

        val warm = start(settings)
        val warmup = warm.awaitWarmedUp()

        assertThat(warmup.pagesLoaded).isPositive()
        assertThat(warm.missesReadingPersons()).describedAs("misses after the warmup").isZero()
        assertThat(debugLog())
            .containsPattern("Page cache warmup loaded ${warmup.pagesLoaded} pages in \\d+ ms")

        val cold = start(settings + (GraphDatabaseSettings.pagecache_warmup_enabled to false))
        assertThat(cold.missesReadingPersons()).describedAs("misses without a warmup").isPositive()
        assertThat(cold.warmup().warmedUp).isFalse()
    }

    @Test
    fun `a database profiles the pages it holds every interval`() {
        val server =
            start(
                mapOf(
                    GraphDatabaseSettings.pagecache_warmup_profiling_interval to
                        Duration.ofMillis(100)
                )
            )
        server.awaitWarmedUp()
        server.persons()
        val profiles = PageCacheProfiles(DefaultFileSystemAbstraction(), server.directory)
        val nodes = profiles.profileOf(server.directory.resolve("neostore.nodestore.db"))

        await.ignoreExceptions() withPollInterval
            Duration.ofMillis(50) atMost
            Duration.ofSeconds(60) untilAsserted
            {
                val profile = profiles.read(nodes)
                assertThat(profile.pages.cardinality().toLong()).isEqualTo(profile.pageCount)
                assertThat(profile.pageCount).isGreaterThan(1)
            }
    }

    @Test
    fun `the profiles are among the files a store copy takes whole`() {
        val server =
            start(
                mapOf(
                    GraphDatabaseSettings.pagecache_warmup_profiling_interval to
                        Duration.ofMillis(100)
                )
            )
        server.persons()
        val nodes = server.directory.resolve("profiles/neostore.nodestore.db.cacheprof")
        await withPollInterval
            Duration.ofMillis(50) atMost
            Duration.ofSeconds(60) untilAsserted
            {
                assertThat(nodes).isRegularFile()
            }
        val database = server.database().dependencyResolver.resolveDependency(Database::class.java)

        val listed =
            database.storeFileListing
                .builder()
                .excludeAll()
                .includeAdditionalProviders()
                .build()
                .use { files -> files.asSequence().toList() }

        assertThat(listed).contains(nodes).allMatch {
            it.startsWith(server.directory.resolve("profiles"))
        }
    }

    @Test
    fun `preload loads every page of the files the allowlist names, and profiles none`() {
        val settings =
            mapOf<Setting<*>, Any>(
                GraphDatabaseSettings.pagecache_warmup_prefetch to true,
                GraphDatabaseSettings.pagecache_warmup_prefetch_allowlist to
                    "neostore\\.(nodestore|propertystore)\\.db",
            )
        start(settings).apply {
            persons()
            stop()
        }
        val nodes = root.resolve("server/data/databases/neo4j/neostore.nodestore.db")
        val properties = root.resolve("server/data/databases/neo4j/neostore.propertystore.db")
        val pages = listOf(nodes, properties).sumOf { (Files.size(it) + PAGE - 1) / PAGE }

        val server = start(settings)
        val warmup = server.awaitWarmedUp()

        assertThat(warmup.pagesLoaded).isEqualTo(pages)
        assertThat(server.directory.resolve("profiles")).doesNotExist()
        server.stop()
        assertThat(root.resolve("server/data/databases/neo4j/profiles")).doesNotExist()
    }

    private companion object {
        const val PERSONS = 20_000
        const val PAGE = 8192L
    }
}
