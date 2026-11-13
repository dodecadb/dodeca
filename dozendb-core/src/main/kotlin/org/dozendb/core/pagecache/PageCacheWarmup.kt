package org.dozendb.core.pagecache

import java.nio.file.Path
import java.util.concurrent.TimeUnit
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.graphdb.Resource
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.io.pagecache.PageCache
import org.neo4j.io.pagecache.context.CursorContextFactory
import org.neo4j.kernel.database.Database
import org.neo4j.kernel.extension.ExtensionFactory
import org.neo4j.kernel.extension.ExtensionType
import org.neo4j.kernel.extension.context.ExtensionContext
import org.neo4j.kernel.lifecycle.Lifecycle
import org.neo4j.kernel.lifecycle.LifecycleAdapter
import org.neo4j.logging.InternalLog
import org.neo4j.logging.internal.LogService
import org.neo4j.scheduler.Group
import org.neo4j.scheduler.JobHandle
import org.neo4j.scheduler.JobMonitoringParams
import org.neo4j.scheduler.JobScheduler

/**
 * Page cache warmup, as Neo4j Enterprise has it and Community only declares its settings: each
 * database's page cache profiles which pages of the database's files it holds, every
 * `db.memory.pagecache.warmup.profile.interval` and when the database stops, and loads them again
 * when the database starts, in the background, once it is available. With
 * `db.memory.pagecache.warmup.preload` it loads every page of the files whose names
 * `db.memory.pagecache.warmup.preload.allowlist` matches instead, and takes no profiles. All of it
 * only while `db.memory.pagecache.warmup.enable`.
 *
 * Profiles are taken only once the warmup ended, so that a profile of a page cache still warming up
 * does not replace a fuller one; a database that stops before then keeps its profiles.
 *
 * The profiles are among the database's store files that must be copied whole, as Neo4j's store
 * file listing has them, so that store copies and backups carry them and a new copy warms up from
 * them too; none is written while a copy lists them.
 *
 * A database extension, registered in `META-INF/services`; Neo4j makes one for each database, and
 * the database's dependencies hold it, where the database's metrics find it.
 */
class PageCacheWarmup
internal constructor(
    private val config: Config,
    private val scheduler: JobScheduler,
    private val database: Database,
    private val profiles: PageCacheProfiles,
    private val log: InternalLog,
    warmer: (stopping: () -> Boolean) -> PageCacheWarmer,
) : LifecycleAdapter() {
    @Volatile private var stopping = false
    private val warmer = warmer { stopping }

    /** Held while the warmup or a profile runs. */
    private val lock = Any()
    private var profiling: JobHandle<*>? = null

    /** Held while profiles are written, or a copy lists them; guards [copying]. */
    private val files = Any()

    /** The copies that list the profiles now, while which none is written. */
    private var copying = 0

    /** Whether the warmup, or the preload, ended, since the database started. */
    @Volatile
    var warmedUp: Boolean = false
        private set

    /** The pages that the warmup, or the preload, loaded, those held already counted too. */
    @Volatile
    var pagesLoaded: Long = 0
        private set

    /** The milliseconds the warmup, or the preload, took, once it ended; else 0. */
    @Volatile
    var durationMillis: Long = 0
        private set

    private val preload: Boolean
        get() = config.get(GraphDatabaseSettings.pagecache_warmup_prefetch)

    private val name: String
        get() = database.namedDatabaseId.name()

    override fun start() {
        stopping = false
        warmedUp = false
        pagesLoaded = 0
        durationMillis = 0
        if (!config.get(GraphDatabaseSettings.pagecache_warmup_enabled)) return
        database.storeFileListing.registerStoreFileProvider(::listProfiles)
        scheduler.schedule(
            Group.FILE_IO_HELPER,
            JobMonitoringParams.systemJob(name, "Page cache warmup"),
            ::warmUp,
        )
    }

    /** Adds the profiles to a copy's [listed] files; none is written until it is closed. */
    private fun listProfiles(listed: MutableCollection<Path>): Resource {
        synchronized(files) {
            copying++
            listed += profiles.list()
        }
        return Resource { synchronized(files) { copying-- } }
    }

    private fun warmUp() {
        // The database's files are mapped once it is available.
        val guard = database.databaseAvailabilityGuard
        while (!stopping && !guard.isAvailable(AVAILABILITY_WAIT_MILLIS)) {
            if (guard.isShutdown) return
        }
        synchronized(lock) {
            if (stopping) return
            val started = System.nanoTime()
            val what = if (preload) "preload" else "warmup"
            try {
                pagesLoaded =
                    if (preload) {
                        warmer.preload(
                            Regex(
                                config.get(
                                    GraphDatabaseSettings.pagecache_warmup_prefetch_allowlist
                                )
                            )
                        )
                    } else {
                        warmer.warmUp(config.get(GraphDatabaseSettings.pagecache_warmup_order))
                    }
            } catch (e: Exception) {
                log.warn("Page cache $what failed", e)
                return
            }
            val millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)
            if (stopping) {
                log.info("Page cache %s stopped after loading %d pages", what, pagesLoaded)
                return
            }
            durationMillis = millis
            warmedUp = true
            log.info("Page cache %s loaded %d pages in %d ms", what, pagesLoaded, millis)
            if (preload) return
            val interval =
                config.get(GraphDatabaseSettings.pagecache_warmup_profiling_interval).toMillis()
            profiling =
                scheduler.scheduleRecurring(
                    Group.FILE_IO_HELPER,
                    JobMonitoringParams.systemJob(name, "Page cache profile"),
                    ::profile,
                    interval,
                    interval,
                    TimeUnit.MILLISECONDS,
                )
        }
    }

    private fun profile() {
        synchronized(lock) {
            if (stopping) return
            takeProfile()
        }
    }

    private fun takeProfile() {
        try {
            synchronized(files) {
                if (copying > 0) return
                val wrote = warmer.profile()
                log.debug("Page cache profiles: %d written", wrote)
            }
        } catch (e: Exception) {
            log.warn("Taking page cache profiles failed: %s", e.toString())
        }
    }

    override fun stop() {
        stopping = true
        // Waits for the warmup or a profile that runs, which end soon once stopping. Neither is
        // cancelled while it runs: an interrupt during file I/O closes the file's channel.
        synchronized(lock) {
            profiling?.cancel()
            profiling = null
            if (warmedUp && !preload) takeProfile()
        }
    }

    private companion object {
        const val AVAILABILITY_WAIT_MILLIS = 1000L
    }
}

/** Makes the [PageCacheWarmup] of each database. */
class PageCacheWarmupExtensionFactory :
    ExtensionFactory<PageCacheWarmupExtensionFactory.Dependencies>(
        ExtensionType.DATABASE,
        "dozendb-page-cache-warmup",
    ) {
    /** What it needs of the database, which Neo4j provides from its dependencies. */
    interface Dependencies {
        fun config(): Config

        /** The database's page cache, which maps only its files. */
        fun pageCache(): PageCache

        fun fileSystemAbstraction(): FileSystemAbstraction

        fun jobScheduler(): JobScheduler

        fun logService(): LogService

        fun database(): Database

        fun cursorContextFactory(): CursorContextFactory
    }

    override fun newInstance(context: ExtensionContext, dependencies: Dependencies): Lifecycle {
        val log = dependencies.logService().getInternalLog(PageCacheWarmup::class.java)
        val database = dependencies.database()
        val directory = database.databaseLayout.databaseDirectory()
        val profiles = PageCacheProfiles(dependencies.fileSystemAbstraction(), directory)
        return PageCacheWarmup(
            dependencies.config(),
            dependencies.jobScheduler(),
            database,
            profiles,
            log,
        ) { stopping ->
            PageCacheWarmer(
                dependencies.pageCache(),
                directory,
                profiles,
                dependencies.cursorContextFactory(),
                log,
                stopping,
            )
        }
    }
}
