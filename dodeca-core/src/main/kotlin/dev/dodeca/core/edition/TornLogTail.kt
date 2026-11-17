package dev.dodeca.core.edition

import org.neo4j.configuration.Config
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.kernel.BinarySupportedKernelVersions
import org.neo4j.kernel.KernelVersion
import org.neo4j.kernel.KernelVersionProviders
import org.neo4j.kernel.database.Database
import org.neo4j.kernel.recovery.CorruptedLogsTruncator
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.storageengine.AppendIndexProvider.BASE_APPEND_INDEX
import org.neo4j.storageengine.api.StorageEngineFactory
import org.neo4j.wal.CheckpointInfo
import org.neo4j.wal.LogEntryCursor
import org.neo4j.wal.LogPosition
import org.neo4j.wal.LogVersionBridge.NO_MORE_CHANNELS
import org.neo4j.wal.ReaderLogVersionBridge
import org.neo4j.wal.WALService
import org.neo4j.wal.entry.LogEntryEmpty
import org.neo4j.wal.entry.LogEntryStart
import org.neo4j.wal.entry.LogFormat
import org.neo4j.wal.entry.VersionAwareLogEntryReader
import org.neo4j.wal.entry.v520.LogEntryChunkStart
import org.neo4j.wal.entry.v520.LogEntryRollback
import org.neo4j.wal.files.LogFilesBuilder

/**
 * Cuts a database's transaction log, before the database starts, where a power loss left only the
 * beginning of the first entry after the last checkpoint, the rest of it reading as zeros, as a log
 * file allocated ahead holds them: the first sector of the entry reached the disk and the next did
 * not.
 *
 * Neo4j then sees no entry after the checkpoint, as the entry's append index reads 0, below any
 * real one: it does not recover, and appends after the torn entry. Readers of the log, as
 * differential backups, then fail on it, and once the database stops without a checkpoint it can no
 * longer start, as its recovery fails on the checksums that follow. Cut there, as recovery cuts a
 * torn tail, the log ends at the checkpoint: the entry belongs to a transaction that did not
 * commit, which a cluster's member gets again from Raft. The bytes cut are kept, as recovery keeps
 * them, in a `corrupted-neostore.transaction.db-*.zip` file in the database's directory.
 */
internal class TornLogTail(private val fs: FileSystemAbstraction) {
    /**
     * Cuts the log of [database], which does not run, at its last checkpoint if the first entry
     * after it is torn so; returns where it cut it, or null. A database without a store yet has no
     * such log.
     */
    fun cut(database: Database): LogPosition? {
        val layout = database.databaseLayout
        val engine =
            StorageEngineFactory.selectStorageEngine(fs, layout).orElse(null) ?: return null
        val config = database.config
        val versions = KernelVersionProviders.latestFromConfig(config)
        val wal =
            LogFilesBuilder.readableBuilder(layout, fs, versions) {
                    LogFormat.fromConfigAndKernelVersion(config, versions.kernelVersion())
                }
                .withConfig(config)
                .withMemoryTracker(EmptyMemoryTracker.INSTANCE)
                .withDatabaseTracers(database.tracers)
                .withStorageEngineFactory(engine)
                .withNoInit()
                .build()
        val tail = wal.tailMetadata
        if (tail.logsMissing() || tail.isRecoveryRequired) return null
        val checkpoint = tail.lastCheckPoint.orElse(null) ?: return null
        // Older entries have no append index; Neo4j finds those after a checkpoint as they are.
        if (!checkpoint.kernelVersion().isAtLeast(KernelVersion.VERSION_APPEND_INDEX_INTRODUCED)) {
            return null
        }
        if (!tornAfter(wal, checkpoint, engine, config)) return null
        val position = checkpoint.transactionLogPosition()
        CorruptedLogsTruncator(
                layout.databaseDirectory(),
                wal,
                fs,
                EmptyMemoryTracker.INSTANCE,
            )
            .truncate(position, checkpoint)
        return position
    }

    /**
     * Whether the first entry after [checkpoint] has an append index below any real one, which only
     * zeros in place of its end give it.
     */
    private fun tornAfter(
        wal: WALService,
        checkpoint: CheckpointInfo,
        engine: StorageEngineFactory,
        config: Config,
    ): Boolean {
        val logFile = wal.logFile
        val position = checkpoint.transactionLogPosition()
        if (!logFile.versionExists(position.logVersion)) return false
        val bridge =
            if (logFile.extractHeader(position.logVersion).logFormatVersion.usesSegments()) {
                ReaderLogVersionBridge.forFile(logFile)
            } else {
                NO_MORE_CHANNELS
            }
        val entries =
            VersionAwareLogEntryReader(
                engine.commandReaderFactory(),
                BinarySupportedKernelVersions(config),
                EmptyMemoryTracker.INSTANCE,
            )
        logFile.getReader(position, bridge).use { reader ->
            reader.reloadChannelStateBeforeCurrentPosition()
            LogEntryCursor(entries, reader).use { cursor ->
                // What cannot be read Neo4j finds as it scans the log, and recovers.
                if (!runCatching { cursor.next() }.getOrDefault(false)) return false
                val appendIndex =
                    when (val entry = cursor.get()) {
                        is LogEntryStart -> entry.appendIndex
                        is LogEntryChunkStart -> entry.appendIndex
                        is LogEntryRollback -> entry.appendIndex
                        is LogEntryEmpty -> entry.appendIndex
                        else -> return false
                    }
                return appendIndex < BASE_APPEND_INDEX
            }
        }
    }
}
