package org.dozendb.cluster.internal.durability

import java.io.OutputStream
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.CopyOption
import java.nio.file.Files
import java.nio.file.OpenOption
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.function.Predicate
import kotlin.concurrent.read
import kotlin.concurrent.write
import kotlin.random.Random
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import org.neo4j.io.fs.DelegatingStoreChannel
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.io.fs.StoreChannel
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper

/**
 * Neo4j's file system, able to lose power.
 *
 * It records, for every transaction log and checkpoint file, how far it was written and how far the
 * last real force reached. [cut] is the power loss: from that instant writes, truncations, forces,
 * deletions, renames and new files are dropped, so the files stay as they were while the DBMS shuts
 * down. [loseUnforcedWrites] and [loseFirstUnforcedSector] then zero parts of what each log file
 * wrote after its last force: lost tails, holes, torn transactions. Store files keep what was
 * written before the cut, which a power loss may also leave; Neo4j's recovery covers that.
 */
internal class PowerLossFileSystem(
    private val delegate: FileSystemAbstraction = DefaultFileSystemAbstraction()
) : FileSystemAbstraction by delegate {
    private val lock = ReentrantReadWriteLock()
    @Volatile private var cut = false
    private val logs = ConcurrentHashMap<Path, LogFile>()

    private class LogFile {
        var written = 0L
        var forced = 0L
    }

    /** Loses power: nothing reaches the files from now on. */
    fun cut() {
        lock.write { cut = true }
    }

    /**
     * Loses power at the first moment when a transaction log of [database] has at least
     * [minimumBytes] unforced, polling for at most [timeout]. Returns whether it found one; the
     * power is off either way.
     */
    fun cutWithUnforcedWrites(database: String, minimumBytes: Long, timeout: Duration): Boolean {
        val deadline = System.nanoTime() + timeout.toNanos()
        while (true) {
            lock.write {
                val unforced = unforced(database) >= minimumBytes
                if (unforced || System.nanoTime() > deadline) {
                    cut = true
                    return unforced
                }
            }
            Thread.sleep(1)
        }
    }

    /** Waits until a transaction log of [database] has at least [minimumBytes] unforced. */
    fun awaitUnforcedWrites(database: String, minimumBytes: Long, timeout: Duration): Boolean {
        val deadline = System.nanoTime() + timeout.toNanos()
        while (System.nanoTime() < deadline) {
            if (unforced(database) >= minimumBytes) return true
            Thread.sleep(1)
        }
        return false
    }

    private fun unforced(database: String): Long =
        logs.entries
            .filter { (path, _) ->
                path.parent?.fileName?.toString() == database &&
                    path.fileName.toString().startsWith(TRANSACTION_LOG)
            }
            .maxOfOrNull { (_, log) -> synchronized(log) { log.written - log.forced } } ?: 0

    /**
     * Loses what the log files wrote after their last force, as out-of-order writeback and torn
     * sectors leave it: everything from a random offset in that range on, and before it each 512
     * byte sector with probability one in two. Returns how many bytes it zeroed. Call after [cut],
     * once the DBMS stopped.
     */
    fun loseUnforcedWrites(random: Random): Long = damage { forced, written, zero ->
        val tail = random.nextLong(forced, written)
        var sector = forced / SECTOR * SECTOR
        while (sector < tail) {
            if (random.nextBoolean()) zero(maxOf(sector, forced), minOf(sector + SECTOR, tail))
            sector += SECTOR
        }
        zero(tail, written)
    }

    /**
     * Loses the first sector each log file wrote after its last force and keeps the rest: a hole
     * followed by data. Returns how many bytes it zeroed. Call after [cut], once the DBMS stopped.
     */
    fun loseFirstUnforcedSector(): Long = damage { forced, written, zero ->
        zero(forced, minOf(forced / SECTOR * SECTOR + SECTOR, written))
    }

    private fun damage(
        lose: (forced: Long, written: Long, zero: (Long, Long) -> Unit) -> Unit
    ): Long {
        check(cut) { "power is still on" }
        var lost = 0L
        for ((path, log) in logs) {
            if (log.written <= log.forced || !delegate.fileExists(path)) continue
            FileChannel.open(path, StandardOpenOption.WRITE).use { channel ->
                lose(log.forced, log.written) { from, to ->
                    if (from < to) {
                        channel.write(ByteBuffer.allocate((to - from).toInt()), from)
                        lost += to - from
                    }
                }
            }
        }
        return lost
    }

    private fun isLog(path: Path): Boolean {
        val name = path.fileName?.toString() ?: return false
        return name.startsWith(TRANSACTION_LOG) || name.startsWith(CHECKPOINT)
    }

    override fun write(fileName: Path): StoreChannel =
        channel(fileName) { delegate.write(fileName) }

    override fun open(fileName: Path, options: Set<OpenOption>): StoreChannel =
        if (StandardOpenOption.WRITE in options || StandardOpenOption.APPEND in options) {
            channel(fileName) { delegate.open(fileName, options) }
        } else {
            delegate.open(fileName, options)
        }

    private fun channel(path: Path, open: () -> StoreChannel): StoreChannel = lock.read {
        when {
            !cut -> Powered(path, open())
            // Writes to a file that exists are dropped; a new file is not created at all.
            delegate.fileExists(path) -> Powered(path, delegate.read(path))
            else -> Powered(path, delegate.read(EMPTY))
        }
    }

    /** Runs [action] unless the power is off. */
    private inline fun powered(action: () -> Unit) {
        lock.read { if (!cut) action() }
    }

    override fun openAsOutputStream(fileName: Path, append: Boolean): OutputStream = lock.read {
        if (cut) OutputStream.nullOutputStream() else delegate.openAsOutputStream(fileName, append)
    }

    override fun deleteFile(fileName: Path) = powered { delegate.deleteFile(fileName) }

    override fun deleteRecursively(directory: Path) = powered {
        delegate.deleteRecursively(directory)
    }

    override fun deleteRecursively(directory: Path, removeFilePredicate: Predicate<Path>) =
        powered {
            delegate.deleteRecursively(directory, removeFilePredicate)
        }

    override fun deleteFileOrThrow(file: Path) = powered { delegate.deleteFileOrThrow(file) }

    override fun renameFile(from: Path, to: Path, vararg copyOptions: CopyOption) = powered {
        delegate.renameFile(from, to, *copyOptions)
    }

    override fun moveToDirectory(file: Path, toDirectory: Path) = powered {
        delegate.moveToDirectory(file, toDirectory)
    }

    override fun copyToDirectory(file: Path, toDirectory: Path) = powered {
        delegate.copyToDirectory(file, toDirectory)
    }

    override fun copyFile(from: Path, to: Path, vararg copyOptions: CopyOption) = powered {
        delegate.copyFile(from, to, *copyOptions)
    }

    override fun copyRecursively(fromDirectory: Path, toDirectory: Path) = powered {
        delegate.copyRecursively(fromDirectory, toDirectory)
    }

    override fun truncate(file: Path, size: Long) = powered { delegate.truncate(file, size) }

    /**
     * A channel whose changes reach the file only while the power is on. Written data that never
     * reached the file is skipped over, so that positions stay as the writer expects.
     */
    private inner class Powered(private val path: Path, channel: StoreChannel) :
        DelegatingStoreChannel<StoreChannel>(channel) {
        private val log: LogFile? =
            if (isLog(path)) logs.computeIfAbsent(path) { LogFile() } else null

        private fun written(end: Long) {
            log?.let { synchronized(it) { it.written = maxOf(it.written, end) } }
        }

        private fun drop(src: ByteBuffer): Int {
            val length = src.remaining()
            src.position(src.limit())
            return length
        }

        private fun dropSequential(srcs: Array<out ByteBuffer>, offset: Int, length: Int): Long {
            var total = 0L
            for (i in offset until offset + length) total += drop(srcs[i])
            delegate.position(delegate.position() + total)
            return total
        }

        override fun write(src: ByteBuffer): Int = lock.read {
            if (cut) {
                drop(src).also { delegate.position(delegate.position() + it) }
            } else {
                super.write(src).also { written(delegate.position()) }
            }
        }

        override fun write(srcs: Array<out ByteBuffer>): Long = write(srcs, 0, srcs.size)

        override fun write(srcs: Array<out ByteBuffer>, offset: Int, length: Int): Long =
            lock.read {
                if (cut) {
                    dropSequential(srcs, offset, length)
                } else {
                    super.write(srcs, offset, length).also { written(delegate.position()) }
                }
            }

        override fun writeAll(src: ByteBuffer) {
            lock.read {
                if (cut) {
                    delegate.position(delegate.position() + drop(src))
                } else {
                    super.writeAll(src)
                    written(delegate.position())
                }
            }
        }

        override fun writeAll(src: ByteBuffer, position: Long) {
            lock.read {
                if (cut) {
                    drop(src)
                } else {
                    val end = position + src.remaining()
                    super.writeAll(src, position)
                    written(end)
                }
            }
        }

        override fun truncate(size: Long): StoreChannel {
            lock.read {
                if (!cut) {
                    super.truncate(size)
                    log?.let {
                        synchronized(it) {
                            it.written = minOf(it.written, size)
                            it.forced = minOf(it.forced, size)
                        }
                    }
                }
            }
            return this
        }

        override fun flush() = force(false)

        override fun force(metaData: Boolean) {
            lock.read {
                if (cut) return
                val covered = log?.let { synchronized(it) { it.written } }
                super.force(metaData)
                if (log != null && covered != null) {
                    synchronized(log) { log.forced = maxOf(log.forced, covered) }
                }
            }
        }
    }

    private companion object {
        const val SECTOR = 512L
        const val TRANSACTION_LOG = TransactionLogFilesHelper.DEFAULT_NAME + "."
        const val CHECKPOINT = TransactionLogFilesHelper.CHECKPOINT_FILE_PREFIX + "."

        /** Read by channels on files created after the power loss, which stay empty. */
        val EMPTY: Path =
            Files.createTempFile("dozendb-power-loss", ".empty").also { it.toFile().deleteOnExit() }
    }
}
