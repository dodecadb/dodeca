package org.dozendb.core.edition

import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import org.neo4j.io.fs.watcher.FileWatcher
import org.neo4j.io.fs.watcher.resource.WatchedResource
import org.neo4j.logging.InternalLog

/**
 * Neo4j's file [watcher], which watches a directory again when an entry of it went while the
 * watcher began to: the JDK's polling watch service (macOS and AIX) reads each entry as it
 * registers a directory, and fails if one goes meanwhile, as when another database's copy moves
 * into place or a database is dropped. Neo4j 2026 cannot start a database again once a start failed
 * so, as its shutdown checkpoint then finds the transaction logs not started.
 */
internal class RetryingFileWatcher(private val watcher: FileWatcher, private val log: InternalLog) :
    FileWatcher by watcher {
    override fun watch(path: Path): WatchedResource {
        var attempt = 1
        while (true) {
            try {
                return watcher.watch(path)
            } catch (e: NoSuchFileException) {
                // The directory itself missing is no entry that went.
                if (attempt == ATTEMPTS || !Files.isDirectory(path)) throw e
                log.warn("Watching %s again: %s went while Neo4j began to watch it", path, e.file)
                attempt++
            }
        }
    }

    private companion object {
        /** How often a directory is watched while its entries go. */
        const val ATTEMPTS = 3
    }
}
