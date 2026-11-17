package dev.dodeca.core.edition

import dev.dodeca.core.StandaloneServer
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.Config
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import org.neo4j.io.layout.DatabaseLayout
import org.neo4j.kernel.database.DatabaseTracers
import org.neo4j.kernel.impl.transaction.CompleteBatchRepresentation
import org.neo4j.kernel.recovery.LogTailExtractor
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.storageengine.api.StorageEngineFactory
import org.neo4j.wal.LogicalTransactionStore
import org.neo4j.wal.WALService
import org.neo4j.wal.entry.LogEntryTypeCodes

/**
 * A database whose transaction log holds only the beginning of the entry after its last checkpoint,
 * the rest of it zeros, as a power loss leaves a log file allocated ahead, starts with its log cut
 * there.
 */
class TornLogTailTest {
    // Kept when a test fails, for the server's logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private lateinit var server: StandaloneServer

    @BeforeEach
    fun start() {
        server = StandaloneServer(root).also { it.start() }
    }

    @AfterEach
    fun stop() {
        server.close()
    }

    private fun films(): Long =
        server.rows("MATCH (f:Film) RETURN count(f) AS films", "films").single()["films"] as Long

    /** The ids of the transactions in the log of [database], which runs, in order. */
    private fun loggedTransactions(database: String): List<Long> {
        val resolver = server.database(database).dependencyResolver
        val logFile = resolver.resolveDependency(WALService::class.java).logFile
        val store = resolver.resolveDependency(LogicalTransactionStore::class.java)
        val lowest = logFile.matchedFiles.minOf(logFile::getLogVersion)
        val ids = mutableListOf<Long>()
        store.getCommandBatches(logFile.extractHeader(lowest).startPosition).use { cursor ->
            while (cursor.next()) {
                ids += (cursor.get() as CompleteBatchRepresentation).commitEntry().txId
            }
        }
        return ids
    }

    /**
     * Writes, at the last checkpoint of [layout], the beginning of a transaction's start entry, up
     * to the time it was written, and zeros after it.
     */
    private fun tearEntryAfterCheckpoint(layout: DatabaseLayout) {
        val fs = DefaultFileSystemAbstraction()
        val config = Config.defaults()
        val engine = StorageEngineFactory.selectStorageEngine(fs, layout).orElseThrow()
        val checkpoint =
            LogTailExtractor(fs, config, engine, DatabaseTracers.EMPTY)
                .getTailMetadata(layout, EmptyMemoryTracker.INSTANCE)
                .lastCheckPoint
                .orElseThrow()
        val position = checkpoint.transactionLogPosition()
        val log =
            layout.transactionLogsDirectory.resolve(
                "neostore.transaction.db.${position.logVersion}"
            )
        val torn =
            ByteBuffer.allocate(10 + ZEROS)
                .order(ByteOrder.LITTLE_ENDIAN)
                .put(checkpoint.kernelVersion().version())
                .put(LogEntryTypeCodes.TX_START)
                .putLong(System.currentTimeMillis())
                .position(0)
        FileChannel.open(log, StandardOpenOption.WRITE).use { it.write(torn, position.byteOffset) }
    }

    @Test
    fun `a database whose log holds only the beginning of the entry after its checkpoint starts with its log cut there`() {
        server.administer("CREATE DATABASE films")
        server.database("films").executeTransactionally("CREATE (:Film {title: 'Alien'})")
        val layout = server.database("films").databaseLayout()
        val logged = loggedTransactions("films")
        // A clean stop: the checkpoint is at the end of the log.
        server.administer("STOP DATABASE films")
        tearEntryAfterCheckpoint(layout)

        server.administer("START DATABASE films")
        server.database("films").executeTransactionally("CREATE (:Film {title: 'Heat'})")

        assertThat(films()).isEqualTo(2)
        assertThat(loggedTransactions("films"))
            .describedAs("the transactions in the log of films")
            .hasSize(logged.size + 1)
            .startsWith(*logged.toTypedArray())
        assertThat(server.debugMessages()).anyMatch {
            it.startsWith("Cut the transaction log of 'DatabaseId{") && "films" in it
        }
        assertThat(Files.list(layout.databaseDirectory()).use { files -> files.toList() })
            .describedAs("the bytes cut, kept")
            .anyMatch { it.fileName.toString().startsWith("corrupted-neostore.transaction.db") }
    }

    private companion object {
        /** Zeros after the torn beginning, as a log file allocated ahead holds them. */
        const val ZEROS = 1024
    }
}
