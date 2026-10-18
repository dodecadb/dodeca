package org.dozendb.cluster.core.internal.storecopy

import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException
import java.io.OutputStream
import java.time.Duration
import java.util.UUID
import org.dozendb.cluster.api.BulkSource
import org.dozendb.cluster.api.Snapshot
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.kernel.database.Database
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.impl.store.DefaultStoreSnapshotFactory
import org.neo4j.logging.InternalLog

/**
 * Serves copies of replicating databases to members that have none or fell behind.
 *
 * A copy is a crash image that Neo4j recovery completes: Neo4j's own store snapshot checkpoints the
 * database and holds off further checkpoints, then the files that recovery cannot rebuild are sent,
 * then the store files, and last the transaction logs, which contain every transaction since the
 * checkpoint up to at least the moment the store files were read.
 *
 * Only a database whose transactions go through Raft here is copied: a new database is not, until
 * its creator has initialised it. A request may name the database's id, which a database of that
 * name here must have: a database dropped and created again keeps its name.
 *
 * The copy states the Raft snapshot it holds, read before the store snapshot is taken: every
 * transaction up to it is closed by then, so the copied transaction logs reach it. A request may
 * name the lowest index the snapshot must reach; a member that has not applied that far waits for a
 * while, then refuses. Without one, as for a member's first copy, the copy states a snapshot only
 * if the member has applied entries.
 *
 * @property replicating whether a database's transactions go through Raft here.
 * @property snapshots the current snapshot of a database replicating here, by name.
 */
internal class StoreCopySource(
    private val databases: () -> DatabaseContextProvider<*>,
    private val fs: FileSystemAbstraction,
    private val replicating: (NamedDatabaseId) -> Boolean,
    private val snapshots: (String) -> Snapshot?,
    private val log: InternalLog,
    private val minimumWait: Duration = Duration.ofSeconds(10),
) : BulkSource {
    override fun serve(request: ByteArray, out: OutputStream) {
        val (name, id, minimumIndex) = decodeRequest(request)
        val database =
            databases()
                .getDatabaseContext(name)
                .orElseThrow { IllegalStateException("no database $name") }
                .database() as? Database
                ?: throw IllegalStateException("database $name cannot be copied")
        val namedDatabaseId = database.namedDatabaseId
        check(id == null || namedDatabaseId.databaseId().uuid() == id) {
            "database $name is $namedDatabaseId here, not the requested $id"
        }
        check(database.isStarted && replicating(namedDatabaseId)) {
            "database $name is not replicating here"
        }
        val snapshot =
            if (minimumIndex < 0) {
                // A member's first copy starts from the beginning of the log; a new group may not
                // have applied anything yet.
                snapshots(name)
            } else {
                awaitSnapshot(name, minimumIndex)
            }
        val storeSnapshot =
            DefaultStoreSnapshotFactory(database, fs).createStoreSnapshot().orElseThrow {
                IllegalStateException("database $name is unavailable")
            }
        val layout = database.databaseLayout
        val databaseDirectory = layout.databaseDirectory()
        val transactionLogs = layout.transactionLogsDirectory
        log.info(
            "Sending a copy of database %s, checkpoint at %s, holding Raft snapshot %s",
            name,
            storeSnapshot.lastAppliedTransactionId(),
            snapshot,
        )
        storeSnapshot.use {
            val archive = StoreArchive.Writer(out, fs)
            archive.header(
                name,
                "store ${storeSnapshot.storeId()}, " +
                    "checkpoint ${storeSnapshot.lastAppliedTransactionId()}",
                snapshot,
            )
            storeSnapshot.unrecoverableFiles().forEach { resource ->
                archive.file(
                    StoreArchive.Root.DATABASE,
                    databaseDirectory.relativize(resource.path()),
                    resource.path(),
                )
            }
            for (path in storeSnapshot.recoverableFiles()) {
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

    /** This member's snapshot of [name] once it reaches [minimumIndex]. */
    private fun awaitSnapshot(name: String, minimumIndex: Long): Snapshot {
        val deadline = System.nanoTime() + minimumWait.toNanos()
        while (true) {
            val snapshot =
                snapshots(name) ?: throw IllegalStateException("database $name is not replicating")
            if (snapshot.position.index >= minimumIndex) return snapshot
            if (System.nanoTime() > deadline) {
                throw IllegalStateException(
                    "database $name has applied up to ${snapshot.position}, " +
                        "not yet up to the requested index $minimumIndex"
                )
            }
            Thread.sleep(MINIMUM_POLL_MILLIS)
        }
    }

    /** A request for a copy of the database [name], which must have the id [id] if one is given. */
    data class Request(val name: String, val id: UUID?, val minimumIndex: Long)

    companion object {
        const val NAME = "dozendb.storeCopy"
        private const val REQUEST_VERSION = 2
        private const val MINIMUM_POLL_MILLIS = 50L

        /**
         * A request for a copy of the database [name] holding every Raft entry up to
         * [minimumIndex]; with an [id], only the database of that name with that id is copied.
         */
        fun encodeRequest(name: String, id: UUID?, minimumIndex: Long): ByteArray =
            ByteArrayOutputStream().use { buffer ->
                DataOutputStream(buffer).use {
                    it.writeByte(REQUEST_VERSION)
                    it.writeUTF(name)
                    it.writeBoolean(id != null)
                    if (id != null) {
                        it.writeLong(id.mostSignificantBits)
                        it.writeLong(id.leastSignificantBits)
                    }
                    it.writeLong(minimumIndex)
                }
                buffer.toByteArray()
            }

        fun decodeRequest(request: ByteArray): Request =
            DataInputStream(request.inputStream()).use {
                val version = it.readByte().toInt()
                if (version != REQUEST_VERSION) {
                    throw IOException("unsupported copy request version $version")
                }
                val name = it.readUTF()
                val id = if (it.readBoolean()) UUID(it.readLong(), it.readLong()) else null
                Request(name, id, it.readLong())
            }
    }
}
