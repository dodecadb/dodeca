package org.dozendb.core.pagecache

import java.io.IOException
import java.nio.file.Path
import java.util.BitSet
import org.neo4j.io.pagecache.PageCache
import org.neo4j.io.pagecache.PageCursor
import org.neo4j.io.pagecache.PagedFile
import org.neo4j.io.pagecache.context.CursorContextFactory
import org.neo4j.logging.InternalLog

/**
 * Profiles which pages of a database's files the page cache holds, and loads them again: the files
 * [pageCache] has mapped in [databaseDirectory], the database's page cache, which maps only its
 * files. Profiles are kept in [profiles]. Loading stops early once [stopping] says so.
 */
internal class PageCacheWarmer(
    private val pageCache: PageCache,
    private val databaseDirectory: Path,
    private val profiles: PageCacheProfiles,
    private val contexts: CursorContextFactory,
    private val log: InternalLog,
    private val stopping: () -> Boolean = { false },
) {
    /** The profiles written last, which are not written again while they hold. */
    private val written = HashMap<Path, PageCacheProfile>()

    /**
     * Writes a profile of each mapped file whose pages held changed since its last one, and deletes
     * the profiles of files that are gone, such as dropped indexes. Returns how many it wrote.
     */
    fun profile(): Int {
        var wrote = 0
        for (file in mapped()) {
            val profile =
                try {
                    held(file)
                } catch (_: IOException) {
                    // Unmapped meanwhile, as a dropped index's file.
                    continue
                }
            val path = profiles.profileOf(file.path())
            if (written[path] == profile) continue
            profiles.write(path, profile)
            written[path] = profile
            wrote++
        }
        written -= profiles.deleteStale().toSet()
        return wrote
    }

    /**
     * Loads the pages that the profiles name of the files mapped now, up to the pages a file has
     * now. Returns how many it loaded, those held already counted too.
     */
    fun warmUp(): Long {
        var loaded = 0L
        // Not getExistingMapping, which Neo4j's page cache counts as a mapping to close.
        val files = mapped().associateBy { it.path() }
        for (path in profiles.list()) {
            if (stopping()) break
            val file = files[profiles.fileOf(path)] ?: continue
            val profile =
                try {
                    profiles.read(path)
                } catch (e: IOException) {
                    log.warn("Page cache warmup left out the profile %s: %s", path, e.toString())
                    continue
                }
            loaded += touch(file, profile.pages, minOf(profile.pageCount, file.pageCount()))
        }
        return loaded
    }

    /**
     * Loads every page of the mapped files whose names [allowed] matches whole. Returns how many it
     * loaded, those held already counted too.
     */
    fun preload(allowed: Regex): Long {
        var loaded = 0L
        for (file in mapped()) {
            if (stopping()) break
            if (!allowed.matches(file.path().fileName.toString())) continue
            val pages = file.pageCount()
            val all = BitSet().apply { set(0, pages.toInt()) }
            loaded += touch(file, all, pages)
        }
        return loaded
    }

    /** The database's mapped files. */
    private fun mapped(): List<PagedFile> =
        pageCache.listExistingMappings().filter { it.path().startsWith(databaseDirectory) }

    /** The pages of [file] that the page cache holds, without loading any. */
    private fun held(file: PagedFile): PageCacheProfile {
        val pages = file.pageCount()
        val held = BitSet()
        contexts.create(TAG).use { context ->
            file.io(0, PROFILE, context).use { cursor ->
                for (page in 0 until pages) {
                    if (cursor.next(page) && cursor.currentPageId != PageCursor.UNBOUND_PAGE_ID) {
                        held.set(page.toInt())
                    }
                }
            }
        }
        return PageCacheProfile(pages, held)
    }

    /** Loads the [pages] of [file] below [count], a run at a time; returns how many. */
    private fun touch(file: PagedFile, pages: BitSet, count: Long): Long {
        var loaded = 0L
        try {
            contexts.create(TAG).use { context ->
                var page = pages.nextSetBit(0)
                while (page >= 0 && page < count && !stopping()) {
                    val end = minOf(pages.nextClearBit(page).toLong(), count)
                    val run = minOf(end - page, RUN.toLong()).toInt()
                    loaded += file.touch(page.toLong(), run, context)
                    page = pages.nextSetBit(page + run)
                }
            }
        } catch (_: IOException) {
            // Unmapped meanwhile.
        }
        return loaded
    }

    /**
     * The pages of the file, no more than a profile counts. Neo4j gives an empty file a negative
     * last page id, not necessarily -1.
     */
    private fun PagedFile.pageCount(): Long = lastPageId.let {
        if (it < 0) 0 else minOf(it + 1, Int.MAX_VALUE.toLong())
    }

    private companion object {
        const val TAG = "pageCacheWarmup"

        /** Pages held are visited without loading the others, nor counting as accessed. */
        const val PROFILE =
            PagedFile.PF_SHARED_READ_LOCK or PagedFile.PF_NO_FAULT or PagedFile.PF_TRANSIENT

        /** The most pages loaded at once, so that a stop does not wait long. */
        const val RUN = 1024
    }
}
