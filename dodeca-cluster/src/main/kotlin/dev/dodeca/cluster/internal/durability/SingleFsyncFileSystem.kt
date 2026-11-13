package dev.dodeca.cluster.internal.durability

import java.nio.ByteBuffer
import java.nio.file.OpenOption
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import org.neo4j.io.fs.DelegatingStoreChannel
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.io.fs.StoreChannel
import org.neo4j.wal.files.TransactionLogFilesHelper

/** Decides, per database, whether commits rely on the Raft log alone for durability. */
internal fun interface FsyncPolicy {
    fun singleFsync(database: String): Boolean
}

/**
 * The DBMS file system with single fsync: commits to a database that [policy] names do not force
 * its transaction log, the Raft log being durable already.
 *
 * Neo4j forces a transaction log channel at every commit (`flush`). For such a database these
 * forces are skipped, except the first one after a channel opens, which makes a new file's header
 * durable. Closing a channel forces it, so only the newest file can lose its tail.
 *
 * The write-ahead rule still holds: before a store file of the database is written (the page cache
 * writing back pages) and before its checkpoint files are written or forced, every transaction log
 * channel of the database with unforced writes is forced. No store page or checkpoint record can
 * therefore reach the disk before the transactions it reflects, so recovery after a crash never
 * finds the store ahead of the log; the transactions it loses are replayed from the Raft log.
 *
 * Transaction log files are `<transactions root>/<database>/neostore.transaction.db.<n>`,
 * checkpoint files `checkpoint.<n>` in the same directory, store files anything under `<databases
 * root>/<database>/`. Other files and channels opened only for reading are left as they are.
 */
internal class SingleFsyncFileSystem(
    private val delegate: FileSystemAbstraction,
    databasesRoot: Path,
    transactionLogsRoot: Path,
) : FileSystemAbstraction by delegate {
    @Volatile var policy: FsyncPolicy = FsyncPolicy { false }

    private val databasesRoot = databasesRoot.toAbsolutePath().normalize()
    private val logsRoot = transactionLogsRoot.toAbsolutePath().normalize()
    private val databases = ConcurrentHashMap<String, DatabaseLogs>()

    override fun write(fileName: Path): StoreChannel =
        decorate(fileName, delegate.write(fileName), writable = true)

    override fun open(fileName: Path, options: Set<OpenOption>): StoreChannel =
        decorate(
            fileName,
            delegate.open(fileName, options),
            writable = StandardOpenOption.WRITE in options || StandardOpenOption.APPEND in options,
        )

    /** Forces the transaction log channels of [database] that have unforced writes. */
    fun forceTransactionLogs(database: String) {
        databases[database]?.forceUnforced()
    }

    private fun decorate(path: Path, channel: StoreChannel, writable: Boolean): StoreChannel {
        if (!writable) return channel
        return when (val file = classify(path)) {
            is File.TransactionLog -> {
                val logs = databases.computeIfAbsent(file.database) { DatabaseLogs() }
                TransactionLogChannel(channel, path, file.database, logs).also { logs.open += it }
            }
            is File.Checkpoint -> BarrierChannel(channel, file.database, beforeForce = true)
            is File.Store -> BarrierChannel(channel, file.database, beforeForce = false)
            File.Other -> channel
        }
    }

    private fun classify(path: Path): File {
        val file = path.toAbsolutePath().normalize()
        val directory = file.parent
        if (directory != null && directory.parent == logsRoot) {
            val database = directory.fileName.toString()
            val name = file.fileName.toString()
            if (name.startsWith(TRANSACTION_LOG_PREFIX)) return File.TransactionLog(database)
            if (name.startsWith(CHECKPOINT_PREFIX)) return File.Checkpoint(database)
        }
        if (file.startsWith(databasesRoot)) {
            val relative = databasesRoot.relativize(file)
            if (relative.nameCount >= 2) return File.Store(relative.getName(0).toString())
        }
        return File.Other
    }

    private sealed interface File {
        data class TransactionLog(val database: String) : File

        data class Checkpoint(val database: String) : File

        data class Store(val database: String) : File

        data object Other : File
    }

    /** The open transaction log channels of one database. */
    private class DatabaseLogs {
        val open: MutableSet<TransactionLogChannel> = ConcurrentHashMap.newKeySet()

        fun forceUnforced() {
            for (channel in open) channel.forceIfUnforced()
        }
    }

    /**
     * A transaction log channel opened for writing, on [path]. An interrupt of a thread that uses
     * it closes the channel without closing this: its writes are then forced through the file
     * opened anew, as fsync forces a file whatever channel wrote it, so that a store write that
     * must wait for them neither fails nor goes first.
     */
    private inner class TransactionLogChannel(
        channel: StoreChannel,
        private val path: Path,
        private val database: String,
        private val logs: DatabaseLogs,
    ) : DelegatingStoreChannel<StoreChannel>(channel) {
        /** Completed writes and truncations, counted after they return. */
        private val changes = AtomicLong()

        /** The changes the last force covered. */
        @Volatile private var forcedChanges = 0L
        @Volatile private var forcedOnce = false
        @Volatile private var closed = false
        private val forceLock = ReentrantLock()

        private inline fun <T> change(action: () -> T): T =
            action().also { changes.incrementAndGet() }

        override fun write(src: ByteBuffer): Int = change { super.write(src) }

        override fun write(srcs: Array<out ByteBuffer>): Long = change { super.write(srcs) }

        override fun write(srcs: Array<out ByteBuffer>, offset: Int, length: Int): Long = change {
            super.write(srcs, offset, length)
        }

        override fun writeAll(src: ByteBuffer) = change { super.writeAll(src) }

        override fun writeAll(src: ByteBuffer, position: Long) = change {
            super.writeAll(src, position)
        }

        override fun truncate(size: Long): StoreChannel {
            change { super.truncate(size) }
            return this
        }

        override fun flush() = force(false)

        override fun force(metaData: Boolean) {
            if (forcedOnce && policy.singleFsync(database)) return
            forceLock.withLock { if (!closed) forceNow(metaData) }
        }

        fun forceIfUnforced() {
            if (changes.get() == forcedChanges) return
            forceLock.withLock {
                if (!closed && changes.get() != forcedChanges) forceNow(false)
            }
        }

        private fun forceNow(metaData: Boolean) {
            val covered = changes.get()
            if (delegate.isOpen) {
                delegate.force(metaData)
            } else if (this@SingleFsyncFileSystem.delegate.fileExists(path)) {
                this@SingleFsyncFileSystem.delegate
                    .open(path, setOf(StandardOpenOption.READ, StandardOpenOption.WRITE))
                    .use { it.force(metaData) }
            }
            forcedChanges = covered
            forcedOnce = true
        }

        /** Forces before closing. Neo4j closes some channels twice; later closes do nothing. */
        override fun close() {
            forceLock.withLock {
                if (closed) return
                closed = true
                try {
                    if (delegate.isOpen) delegate.force(false)
                } finally {
                    logs.open -= this
                    delegate.close()
                }
            }
        }
    }

    /**
     * A store or checkpoint file channel opened for writing: the database's transaction logs are
     * forced before every write, and for checkpoint files also before every force.
     */
    private inner class BarrierChannel(
        channel: StoreChannel,
        private val database: String,
        private val beforeForce: Boolean,
    ) : DelegatingStoreChannel<StoreChannel>(channel) {
        private fun barrier() = forceTransactionLogs(database)

        override fun write(src: ByteBuffer): Int {
            barrier()
            return super.write(src)
        }

        override fun write(srcs: Array<out ByteBuffer>): Long {
            barrier()
            return super.write(srcs)
        }

        override fun write(srcs: Array<out ByteBuffer>, offset: Int, length: Int): Long {
            barrier()
            return super.write(srcs, offset, length)
        }

        override fun writeAll(src: ByteBuffer) {
            barrier()
            super.writeAll(src)
        }

        override fun writeAll(src: ByteBuffer, position: Long) {
            barrier()
            super.writeAll(src, position)
        }

        override fun flush() = force(false)

        override fun force(metaData: Boolean) {
            if (beforeForce) barrier()
            super.force(metaData)
        }
    }

    private companion object {
        const val TRANSACTION_LOG_PREFIX = TransactionLogFilesHelper.DEFAULT_NAME + "."
        const val CHECKPOINT_PREFIX = TransactionLogFilesHelper.CHECKPOINT_FILE_PREFIX + "."
    }
}
