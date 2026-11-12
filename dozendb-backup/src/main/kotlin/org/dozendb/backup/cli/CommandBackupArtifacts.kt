package org.dozendb.backup.cli

import java.io.IOException
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import org.dozendb.backup.ArtifactChains
import org.dozendb.backup.ArtifactHeader
import org.dozendb.backup.Artifacts
import org.dozendb.core.store.BackupArtifacts
import org.neo4j.cloud.storage.SchemeFileSystemAbstraction
import org.neo4j.configuration.Config
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.io.layout.DatabaseLayout
import org.neo4j.io.pagecache.context.CursorContext
import org.neo4j.kernel.database.DatabaseIdFactory
import org.neo4j.kernel.database.DatabaseTracers
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.kernel.recovery.LogTailExtractor
import org.neo4j.logging.InternalLogProvider
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.storageengine.api.StorageEngineFactory

/**
 * The backup module's [BackupArtifacts], registered in META-INF/services, for `neo4j-admin database
 * copy` and `import`.
 */
class CommandBackupArtifacts : BackupArtifacts {
    override fun write(
        layout: DatabaseLayout,
        database: String,
        source: String,
        location: String,
        option: String,
        compress: Boolean,
        partSize: Long,
        config: Config,
        fs: FileSystemAbstraction,
        logProvider: InternalLogProvider,
    ): BackupArtifacts.Written =
        SchemeFileSystemAbstraction(fs, config, logProvider).use { schemes ->
            val directory = CommandPaths.resolve(schemes, location, option)
            if (!CommandPaths.isCloud(directory) && !schemes.isDirectory(directory)) {
                throw IOException("$option $location is not an existing directory")
            }
            val engine =
                StorageEngineFactory.selectStorageEngine(fs, layout).orElseThrow {
                    IOException("${layout.databaseDirectory()} holds no store")
                }
            val last =
                LogTailExtractor(fs, config, engine, DatabaseTracers.EMPTY)
                    .getTailMetadata(layout, EmptyMemoryTracker.INSTANCE)
                    .lastCommittedTransaction
            val storeId =
                AdminResources(fs, config, logProvider).use {
                    engine.retrieveStoreId(fs, layout, it.pageCache, CursorContext.NULL_CONTEXT)
                }
            val header =
                ArtifactHeader(
                    databaseName = database,
                    // A new database's, as the store is new; a database created from the backup
                    // gets its own.
                    databaseId = DatabaseIdFactory.from(UUID.randomUUID()),
                    storeId = storeId,
                    time = Instant.now().truncatedTo(ChronoUnit.SECONDS),
                    full = true,
                    compressed = compress,
                    lowestAppendIndex = 1,
                    highestAppendIndex = last.appendIndex(),
                    previousChecksum = null,
                    lastChecksum = last.checksum(),
                    lastCommitTime = Instant.ofEpochMilli(last.commitTimestamp()),
                    source = source,
                )
            val file =
                Artifacts.writeNew(directory, database, header.time, schemes, partSize) {
                    Artifacts.writeFull(it, header, layout, fs)
                }
            BackupArtifacts.Written(CommandPaths.display(file), last.id())
        }

    override fun restore(
        location: String,
        option: String,
        database: String,
        target: DatabaseLayout,
        config: Config,
        fs: FileSystemAbstraction,
        logProvider: InternalLogProvider,
    ): String =
        SchemeFileSystemAbstraction(fs, config, logProvider).use { schemes ->
            val path = CommandPaths.resolve(schemes, location, option)
            val shown = CommandPaths.display(path)
            if (!schemes.fileExists(path) || schemes.isDirectory(path)) {
                throw IOException("$option $shown is not a backup file")
            }
            val end = ArtifactChains.find(listOf(path), schemes).artifacts.single()
            if (NormalizedDatabaseName(end.header.databaseName).name() != database) {
                throw IOException(
                    "$option $shown is a backup of database '${end.header.databaseName}', not " +
                        "of '$database'"
                )
            }
            val folder = ArtifactChains.find(listOfNotNull(path.toAbsolutePath().parent), schemes)
            val chain =
                try {
                    ArtifactChains.select(
                        folder.artifacts.filter {
                            it.header.databaseName == end.header.databaseName
                        } + end,
                        null,
                        end,
                    )
                } catch (e: IOException) {
                    throw IOException(
                        "incomplete chain: ${e.message}; the backups $shown continues must be " +
                            "in its folder",
                        e,
                    )
                }
            val prepared =
                AdminResources(schemes, config, logProvider).use {
                    it.preparer().prepare(chain, null, target, detach = false)
                }
            RestoreCommand.describe(chain, prepared)
        }
}
