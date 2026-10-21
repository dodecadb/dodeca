package org.dozendb.backup.cli

import org.dozendb.backup.StorePreparer
import org.neo4j.configuration.Config
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.io.pagecache.PageCache
import org.neo4j.io.pagecache.tracing.PageCacheTracer
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory
import org.neo4j.logging.InternalLogProvider
import org.neo4j.logging.NullLog
import org.neo4j.memory.MemoryPools
import org.neo4j.scheduler.JobScheduler
import org.neo4j.time.Clocks

/**
 * What a command that prepares stores needs: a page cache sized by the configuration, as Neo4j's
 * own commands size theirs, and the job scheduler it runs on.
 */
internal class AdminResources(
    val fs: FileSystemAbstraction,
    val config: Config,
    val logProvider: InternalLogProvider,
) : AutoCloseable {
    private val scheduler: JobScheduler = JobSchedulerFactory.createInitialisedScheduler()
    val pageCache: PageCache =
        try {
            ConfiguringPageCacheFactory(
                    fs,
                    config,
                    PageCacheTracer.NULL,
                    NullLog.getInstance(),
                    scheduler,
                    Clocks.nanoClock(),
                    MemoryPools(),
                )
                .orCreatePageCache
        } catch (e: RuntimeException) {
            scheduler.close()
            throw e
        }

    fun preparer() = StorePreparer(fs, pageCache, config, logProvider)

    override fun close() {
        try {
            pageCache.close()
        } finally {
            scheduler.close()
        }
    }
}
