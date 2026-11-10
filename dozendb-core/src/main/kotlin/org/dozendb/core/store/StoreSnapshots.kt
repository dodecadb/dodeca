package org.dozendb.core.store

import java.time.Duration
import org.dozendb.core.checkpoint.CheckpointIoController
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.kernel.database.Database
import org.neo4j.kernel.impl.store.DefaultStoreSnapshotFactory
import org.neo4j.kernel.impl.transaction.log.files.LogFiles
import org.neo4j.storageengine.api.StoreSnapshot
import org.neo4j.storageengine.api.TransactionId
import org.neo4j.storageengine.api.TransactionIdStore

/**
 * Copies of a running database's store, as crash images that Neo4j recovery completes: Neo4j's
 * store snapshot checkpoints the database and holds off further checkpoints while it is open; the
 * files recovery cannot rebuild go first, then the store files, then transaction logs that hold
 * every transaction since the checkpoint up to at least the moment the store files were read.
 */
object StoreSnapshots {
    private const val POLL_MILLIS = 5L

    /**
     * Takes a snapshot of [database], to close once its files are sent. Its checkpoint, or the one
     * under way, goes without the I/O limit.
     */
    fun take(database: Database, fs: FileSystemAbstraction): StoreSnapshot =
        CheckpointIoController.unlimited(database.ioController) {
                DefaultStoreSnapshotFactory(database, fs).createStoreSnapshot()
            }
            .orElseThrow {
                IllegalStateException("database ${database.namedDatabaseId.name()} is unavailable")
            }

    /** Writes the files of [snapshot] but the transaction logs to [archive]. */
    fun writeStoreFiles(snapshot: StoreSnapshot, database: Database, archive: StoreArchive.Writer) {
        val directory = database.databaseLayout.databaseDirectory()
        snapshot.unrecoverableFiles().forEach { resource ->
            archive.file(
                StoreArchive.Root.DATABASE,
                directory.relativize(resource.path()),
                resource.path(),
            )
        }
        for (path in snapshot.recoverableFiles()) {
            archive.file(StoreArchive.Root.DATABASE, directory.relativize(path), path)
        }
    }

    /**
     * Writes every file of [database]'s transaction log directory, as each is now, to [archive].
     */
    fun writeAllLogs(database: Database, fs: FileSystemAbstraction, archive: StoreArchive.Writer) {
        val directory = database.databaseLayout.transactionLogsDirectory
        for (path in fs.listFiles(directory).filterNot(fs::isDirectory).sorted()) {
            archive.file(StoreArchive.Root.TRANSACTION_LOGS, directory.relativize(path), path)
        }
    }

    /**
     * Writes to [archive], once the store files of the open snapshot are written, the checkpoint
     * files and the transaction logs from the snapshot's checkpoint on, the last one cut after the
     * last transaction closed by then, which it returns: strict recovery completes such a copy
     * whatever was written meanwhile. Waits up to [timeout] for the transactions committed before
     * to close.
     */
    fun writeLogsToLastClosed(
        database: Database,
        fs: FileSystemAbstraction,
        archive: StoreArchive.Writer,
        timeout: Duration,
    ): TransactionId {
        val dependencies = database.dependencyResolver
        val transactionIds = dependencies.resolveDependency(TransactionIdStore::class.java)
        val committed = transactionIds.lastCommittedTransactionId
        val deadline = System.nanoTime() + timeout.toNanos()
        while (transactionIds.lastClosedTransactionId < committed) {
            check(System.nanoTime() < deadline) {
                "transaction $committed of ${database.namedDatabaseId.name()} did not close " +
                    "within $timeout"
            }
            Thread.sleep(POLL_MILLIS)
        }
        val closed = transactionIds.lastClosedTransaction
        val end = closed.logPosition()
        val logFiles = dependencies.resolveDependency(LogFiles::class.java)
        val checkpoint =
            logFiles.checkpointFile.findLatestCheckpoint().orElseThrow {
                IllegalStateException("${database.namedDatabaseId.name()} has no checkpoint")
            }
        val from =
            minOf(
                checkpoint.transactionLogPosition().logVersion,
                checkpoint.oldestNotVisibleTransactionLogPosition().logVersion,
            )
        val directory = database.databaseLayout.transactionLogsDirectory
        for (path in logFiles.checkpointFile.detachedCheckpointFiles.sorted()) {
            archive.file(StoreArchive.Root.TRANSACTION_LOGS, directory.relativize(path), path)
        }
        val logFile = logFiles.logFile
        for (version in from..end.logVersion) {
            val path = logFile.getLogFileForVersion(version)
            val length = if (version == end.logVersion) end.byteOffset else fs.getFileSize(path)
            archive.file(
                StoreArchive.Root.TRANSACTION_LOGS,
                directory.relativize(path),
                path,
                length,
            )
        }
        return closed.transactionId()
    }
}
