package org.dozendb.cluster.core.internal.storecopy

import java.io.OutputStream
import org.dozendb.cluster.api.BulkSource
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.kernel.database.Database
import org.neo4j.kernel.impl.store.DefaultStoreSnapshotFactory
import org.neo4j.logging.InternalLog

/**
 * Serves copies of running databases to members that have none.
 *
 * A copy is a crash image that Neo4j recovery completes: Neo4j's own store snapshot checkpoints the
 * database and holds off further checkpoints, then the files that recovery cannot rebuild are sent,
 * then the store files, and last the transaction logs, which contain every transaction since the
 * checkpoint up to at least the moment the store files were read.
 */
internal class StoreCopySource(
    private val databases: () -> DatabaseContextProvider<*>,
    private val fs: FileSystemAbstraction,
    private val log: InternalLog,
) : BulkSource {
    override fun serve(request: ByteArray, out: OutputStream) {
        val name = request.decodeToString()
        val database =
            databases()
                .getDatabaseContext(name)
                .orElseThrow { IllegalStateException("no database $name") }
                .database() as? Database
                ?: throw IllegalStateException("database $name cannot be copied")
        check(database.isStarted) { "database $name is not running" }
        val snapshot =
            DefaultStoreSnapshotFactory(database, fs).createStoreSnapshot().orElseThrow {
                IllegalStateException("database $name is unavailable")
            }
        val layout = database.databaseLayout
        val databaseDirectory = layout.databaseDirectory()
        val transactionLogs = layout.transactionLogsDirectory
        log.info(
            "Sending a copy of database %s, checkpoint at %s",
            name,
            snapshot.lastAppliedTransactionId(),
        )
        snapshot.use {
            val archive = StoreArchive.Writer(out, fs)
            archive.header(
                name,
                "store ${snapshot.storeId()}, checkpoint ${snapshot.lastAppliedTransactionId()}",
            )
            snapshot.unrecoverableFiles().forEach { resource ->
                archive.file(
                    StoreArchive.Root.DATABASE,
                    databaseDirectory.relativize(resource.path()),
                    resource.path(),
                )
            }
            for (path in snapshot.recoverableFiles()) {
                archive.file(StoreArchive.Root.DATABASE, databaseDirectory.relativize(path), path)
            }
            for (path in fs.listFiles(transactionLogs).filterNot(fs::isDirectory).sorted()) {
                archive.file(
                    StoreArchive.Root.TRANSACTION_LOGS,
                    transactionLogs.relativize(path),
                    path,
                )
            }
            archive.end()
        }
        log.info("Sent a copy of database %s", name)
    }

    companion object {
        const val NAME = "dozendb.storeCopy"
    }
}
