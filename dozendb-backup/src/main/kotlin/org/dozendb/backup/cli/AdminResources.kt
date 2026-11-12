package org.dozendb.backup.cli

import java.io.OutputStream
import org.dozendb.backup.SplitArchives
import org.dozendb.backup.StorePreparer
import org.neo4j.cli.CommandFailedException
import org.neo4j.commandline.Util
import org.neo4j.configuration.Config
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.io.pagecache.PageCache
import org.neo4j.io.pagecache.tracing.PageCacheTracer
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory
import org.neo4j.logging.InternalLogProvider
import org.neo4j.logging.Level
import org.neo4j.logging.NullLog
import org.neo4j.logging.log4j.Log4jLogProvider
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

/**
 * Where Neo4j logs what a command does, such as recovery's progress: only warnings and errors, or
 * with --verbose everything.
 */
internal fun commandLogProvider(out: OutputStream, verbose: Boolean): Log4jLogProvider =
    if (verbose) Util.configuredLogProvider(out, true) else Log4jLogProvider(out, Level.WARN)

/**
 * The size of each part of the backups a command writes, as `--split-archive-part-size`, [option],
 * or else `server.split_archive.part_size` in [config] gives it; 0 for unsplit backups.
 */
internal fun partSize(config: Config, option: Long): Long =
    try {
        SplitArchives.partSize(config, option)
    } catch (e: IllegalArgumentException) {
        throw CommandFailedException(e.message, e)
    }
