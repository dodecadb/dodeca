package dev.dodeca.core.pagecache

import java.io.ByteArrayOutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.BitSet
import org.assertj.core.api.Assertions.assertThat
import org.eclipse.collections.api.factory.Sets
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.GraphDatabaseSettings.WarmupOrder
import org.neo4j.io.async.AsyncBlockAccessor
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import org.neo4j.io.pagecache.PageCache
import org.neo4j.io.pagecache.PageCursor
import org.neo4j.io.pagecache.PagedFile
import org.neo4j.io.pagecache.context.CursorContext
import org.neo4j.io.pagecache.context.CursorContextFactory
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory
import org.neo4j.io.pagecache.impl.muninn.StoreFile
import org.neo4j.io.pagecache.tracing.FileFlushEvent
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory
import org.neo4j.logging.log4j.Log4jLogProvider

/** Profiles and warmups on Neo4j's page cache, over files in a temporary directory. */
class PageCacheWarmerTest {
    @TempDir lateinit var root: Path
    private val fs = DefaultFileSystemAbstraction()
    private val scheduler = JobSchedulerFactory.createInitialisedScheduler()
    private val output = ByteArrayOutputStream()
    private val log = Log4jLogProvider(output).getLog(PageCacheWarmer::class.java)
    private var pageCache: PageCache? = null
    private val mapped = mutableListOf<PagedFile>()

    private val database: Path
        get() = root.resolve("films")

    private val nodes: Path
        get() = database.resolve("neostore.nodestore.db")

    private val index: Path
        get() = database.resolve("schema/index/range-1.0/3/index-3")

    @AfterEach
    fun close() {
        restart(open = emptyList())
        scheduler.close()
        fs.close()
    }

    /** Writes [pages] pages to [file], each holding its number. */
    private fun create(file: Path, pages: Int) {
        Files.createDirectories(file.parent)
        StandalonePageCacheFactory.createPageCache(fs, scheduler).use { cache ->
            cache
                .map(
                    StoreFile(file),
                    PAGE_SIZE,
                    "films",
                    Sets.immutable.of(StandardOpenOption.CREATE),
                )
                .use {
                    it.io(0, PagedFile.PF_SHARED_WRITE_LOCK, CursorContext.NULL_CONTEXT).use {
                        cursor ->
                        for (page in 0L until pages) {
                            check(cursor.next(page))
                            cursor.putLong(page)
                        }
                    }
                    it.flushAndForce(
                        FileFlushEvent.NULL,
                        AsyncBlockAccessor.EMPTY_ASYNC_BLOCK_ACCESSOR,
                    )
                }
        }
    }

    /** A new page cache, which holds no pages, with [open] mapped. */
    private fun restart(open: List<Path> = listOf(nodes, index)): PageCache? {
        mapped.forEach { it.close() }
        mapped.clear()
        pageCache?.close()
        pageCache = null
        if (open.isEmpty()) return null
        val cache = StandalonePageCacheFactory.createPageCache(fs, scheduler)
        pageCache = cache
        open.forEach { mapped += cache.map(StoreFile(it), PAGE_SIZE, "films") }
        return cache
    }

    private fun file(path: Path): PagedFile = mapped.single { it.path() == path }

    /** Reads [pages] of [file], which loads them. */
    private fun read(file: Path, pages: Iterable<Long>) {
        file(file).io(0, PagedFile.PF_SHARED_READ_LOCK, CursorContext.NULL_CONTEXT).use { cursor ->
            for (page in pages) {
                check(cursor.next(page))
                do {
                    cursor.getLong(0)
                } while (cursor.shouldRetry())
            }
        }
    }

    /** The pages of [file] the page cache holds, without loading any. */
    private fun held(file: Path): Set<Long> {
        val paged = file(file)
        val held = mutableSetOf<Long>()
        paged
            .io(
                0,
                PagedFile.PF_SHARED_READ_LOCK or PagedFile.PF_NO_FAULT,
                CursorContext.NULL_CONTEXT,
            )
            .use { cursor ->
                for (page in 0..paged.lastPageId) {
                    if (cursor.next(page) && cursor.currentPageId != PageCursor.UNBOUND_PAGE_ID) {
                        held += page
                    }
                }
            }
        return held
    }

    private fun warmer(stopping: () -> Boolean = { false }) =
        PageCacheWarmer(
            checkNotNull(pageCache),
            database,
            PageCacheProfiles(fs, database),
            CursorContextFactory.NULL_CONTEXT_FACTORY,
            log,
            stopping,
        )

    private fun profile(file: Path): Path = PageCacheProfiles(fs, database).profileOf(file)

    @Test
    fun `the pages held are profiled, and loaded again into a page cache that holds none`() {
        create(nodes, 100)
        create(index, 50)
        restart()
        read(nodes, listOf(1, 2, 3, 50, 99))
        read(index, 0L..9L)

        val warmer = warmer()
        assertThat(warmer.profile()).isEqualTo(2)
        assertThat(database.resolve("profiles/neostore.nodestore.db.cacheprof")).isRegularFile()
        assertThat(database.resolve("profiles/schema/index/range-1.0/3/index-3.cacheprof"))
            .isRegularFile()
        assertThat(warmer.profile()).describedAs("unchanged profiles").isZero()
        read(nodes, listOf(4))
        assertThat(warmer.profile()).describedAs("a page more").isEqualTo(1)
        assertThat(PageCacheProfiles(fs, database).read(profile(nodes)))
            .isEqualTo(
                PageCacheProfile(100, BitSet().apply { listOf(1, 2, 3, 4, 50, 99).forEach(::set) })
            )

        restart()
        assertThat(held(nodes)).isEmpty()
        assertThat(warmer().warmUp()).isEqualTo(16)

        assertThat(held(nodes)).containsExactlyInAnyOrder(1, 2, 3, 4, 50, 99)
        assertThat(held(index)).isEqualTo((0L..9L).toSet())
    }

    // Neo4j gives an empty file a negative last page id, not necessarily -1.
    @Test
    fun `an empty file has a profile of no pages, which a warmup reads`() {
        create(nodes, 100)
        create(index, 0)
        restart()
        read(nodes, listOf(1))

        assertThat(warmer().profile()).isEqualTo(2)
        assertThat(PageCacheProfiles(fs, database).read(profile(index)))
            .isEqualTo(PageCacheProfile(0, BitSet()))

        restart()
        assertThat(warmer().warmUp()).isEqualTo(1)
        assertThat(output.toString()).doesNotContain("left out")
    }

    @Test
    fun `a profile of a file that shrank or grew loads the pages both have`() {
        create(nodes, 100)
        create(index, 10)
        restart()
        read(nodes, 90L..99L)
        read(index, 0L..9L)
        warmer().profile()
        restart(open = emptyList())
        // Fewer pages for the store file, more for the index's.
        Files.newByteChannel(nodes, StandardOpenOption.WRITE).use {
            it.truncate(95L * PAGE_SIZE)
        }
        create(index, 20)

        restart()
        assertThat(warmer().warmUp()).isEqualTo(15)

        assertThat(held(nodes)).isEqualTo((90L..94L).toSet())
        assertThat(held(index)).isEqualTo((0L..9L).toSet())
    }

    @Test
    fun `preload loads every page of the files whose names match whole`() {
        create(nodes, 100)
        create(index, 50)
        restart()

        assertThat(warmer().preload(Regex("neostore\\..*"))).isEqualTo(100)
        assertThat(held(nodes)).hasSize(100)
        assertThat(held(index)).isEmpty()
        assertThat(warmer().preload(Regex("index"))).describedAs("not part of a name").isZero()
        assertThat(warmer().preload(Regex(".*"))).isEqualTo(150)
        assertThat(held(index)).hasSize(50)
    }

    @Test
    fun `the profiles of files that are gone are deleted, and those of unmapped files kept`() {
        val relationships = database.resolve("neostore.relationshipstore.db")
        create(nodes, 10)
        create(index, 10)
        create(relationships, 10)
        restart(open = listOf(nodes, index, relationships))
        listOf(nodes, index, relationships).forEach { read(it, listOf(0)) }
        warmer().profile()

        restart(open = listOf(nodes))
        Files.delete(index)
        warmer().profile()

        assertThat(PageCacheProfiles(fs, database).list())
            .containsExactly(profile(nodes), profile(relationships))
    }

    @Test
    fun `a damaged profile is left out, and the others loaded`() {
        create(nodes, 10)
        create(index, 10)
        restart()
        read(nodes, listOf(4))
        read(index, listOf(5))
        warmer().profile()
        Files.write(profile(index), byteArrayOf(1, 2, 3))

        restart()
        assertThat(warmer().warmUp()).isEqualTo(1)

        assertThat(held(nodes)).containsExactly(4)
        assertThat(held(index)).isEmpty()
        assertThat(output.toString())
            .contains("Page cache warmup left out the profile ${profile(index)}")
    }

    @Test
    fun `a warmup that is stopping loads nothing more`() {
        create(nodes, 10)
        create(index, 10)
        restart()
        read(nodes, 0L..9L)
        warmer().profile()

        restart()
        assertThat(warmer(stopping = { true }).warmUp()).isZero()
        assertThat(warmer(stopping = { true }).preload(Regex(".*"))).isZero()
        assertThat(held(nodes)).isEmpty()
    }

    @Test
    fun `profiles warm up in the order of db_memory_pagecache_warmup_order`() {
        val ids = database.resolve("neostore.nodestore.db.id")
        val files = listOf(nodes, index, ids)
        files.forEach { create(it, 4) }
        restart(files)
        files.forEach { read(it, 0L..3L) }
        warmer().profile()

        assertThat(warmedUp(WarmupOrder.PRIORITY, files)).containsExactly(ids, index, nodes)
        assertThat(warmedUp(WarmupOrder.ALPHABETIC, files)).containsExactly(nodes, ids, index)
        assertThat(warmedUp(WarmupOrder.NONE, files)).containsExactlyInAnyOrder(ids, index, nodes)
    }

    /** The [files] a warmup in [order] loads, in the order it loads them. */
    private fun warmedUp(order: WarmupOrder, files: List<Path>): List<Path> {
        restart(files)
        val loaded = mutableListOf<Path>()
        // The warmup asks whether it is stopping before each profile.
        fun look() =
            files.filter { it !in loaded && held(it).isNotEmpty() }.forEach { loaded.add(it) }
        warmer(stopping = { look().let { false } }).warmUp(order)
        look()
        return loaded
    }

    private companion object {
        const val PAGE_SIZE = PageCache.PAGE_SIZE
    }
}
