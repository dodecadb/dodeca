package org.dozendb.core.store

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.Optional
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread
import org.assertj.core.api.Assertions.assertThat
import org.dozendb.core.StandaloneServer
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.Config
import org.neo4j.configuration.DatabaseConfig
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.consistency.ConsistencyCheckService
import org.neo4j.io.ByteUnit
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import org.neo4j.io.layout.Neo4jLayout
import org.neo4j.io.pagecache.IOController
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory
import org.neo4j.io.pagecache.tracing.PageCacheTracer
import org.neo4j.kernel.database.Database
import org.neo4j.kernel.database.DatabaseTracers
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory
import org.neo4j.kernel.recovery.LogTailExtractor
import org.neo4j.kernel.recovery.Recovery
import org.neo4j.logging.NullLogProvider
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.storageengine.api.StorageEngineFactory
import org.neo4j.storageengine.api.TransactionId
import org.neo4j.storageengine.api.TransactionIdStore

class StoreSnapshotsTest {
    // Kept when a test fails, for the server's logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private val fs = DefaultFileSystemAbstraction()

    private class Copy(val bytes: ByteArray, val last: TransactionId)

    @Test
    fun `copies with logs cut at the last closed transaction recover strictly to it, under writes`() {
        val server =
            StandaloneServer(
                    root,
                    mapOf(
                        GraphDatabaseSettings.logical_log_rotation_threshold to
                            ByteUnit.kibiBytes(256)
                    ),
                )
                .also { it.start() }
        val copies = mutableListOf<Copy>()
        val logFilesInSource: Int
        server.use {
            val database =
                server.database().dependencyResolver.resolveDependency(Database::class.java)
            val writing = AtomicBoolean(true)
            val writers =
                (1..4).map { writer ->
                    thread {
                        var n = 0
                        while (writing.get()) {
                            server
                                .database()
                                .executeTransactionally(
                                    "UNWIND range(1, 20) AS i CREATE (:Written {writer: ${'$'}writer, n: ${'$'}n, pad: ${'$'}pad})",
                                    mapOf("writer" to writer, "n" to n++, "pad" to "x".repeat(200)),
                                )
                        }
                    }
                }
            val transactionIds =
                database.dependencyResolver.resolveDependency(TransactionIdStore::class.java)
            try {
                repeat(5) {
                    // Under way: the writers commit some transactions before each copy.
                    val before = transactionIds.lastCommittedTransactionId
                    while (transactionIds.lastCommittedTransactionId < before + 50) Thread.sleep(10)
                    val out = ByteArrayOutputStream()
                    val last =
                        StoreSnapshots.take(database, fs).use { snapshot ->
                            val archive = StoreArchive.Writer(out, fs)
                            archive.header(
                                "neo4j",
                                database.namedDatabaseId.databaseId().uuid(),
                                "test",
                            )
                            StoreSnapshots.writeStoreFiles(snapshot, database, archive)
                            // At least one transaction after the snapshot's checkpoint.
                            server.database().executeTransactionally("CREATE (:Between)")
                            StoreSnapshots.writeLogsToLastClosed(
                                    database,
                                    fs,
                                    archive,
                                    Duration.ofSeconds(10),
                                )
                                .also { archive.end() }
                        }
                    copies += Copy(out.toByteArray(), last)
                }
            } finally {
                writing.set(false)
                writers.forEach { it.join() }
            }
            logFilesInSource =
                Files.list(database.databaseLayout.transactionLogsDirectory).use { files ->
                    files
                        .filter { it.fileName.toString().startsWith("neostore.transaction.db.") }
                        .count()
                        .toInt()
                }
        }
        assertThat(logFilesInSource).describedAs("log files in the source").isGreaterThan(2)

        copies.forEachIndexed { index, copy ->
            val home = root.resolve("copy-$index")
            val config = Config.defaults(GraphDatabaseSettings.neo4j_home, home.toAbsolutePath())
            val layout = Neo4jLayout.of(config).databaseLayout("neo4j")
            Files.createDirectories(layout.databaseDirectory())
            Files.createDirectories(layout.transactionLogsDirectory)
            StoreArchive.read(
                ByteArrayInputStream(copy.bytes),
                fs,
                layout.databaseDirectory(),
                layout.transactionLogsDirectory,
            )
            assertThat(Files.list(layout.transactionLogsDirectory).use { it.count() })
                .describedAs("files in copy $index's logs")
                .isLessThan(logFilesInSource.toLong())

            recover(config, layout)

            val engine = StorageEngineFactory.selectStorageEngine(fs, layout).orElseThrow()
            val recovered =
                LogTailExtractor(fs, config, engine, DatabaseTracers.EMPTY)
                    .getTailMetadata(layout, EmptyMemoryTracker.INSTANCE)
                    .lastCommittedTransaction
            assertThat(recovered.id()).describedAs("copy $index").isEqualTo(copy.last.id())
            assertThat(recovered.checksum()).isEqualTo(copy.last.checksum())
            assertThat(
                    ConsistencyCheckService(layout)
                        .with(config)
                        .runFullConsistencyCheck()
                        .isSuccessful
                )
                .describedAs("copy $index consistent")
                .isTrue
        }
    }

    /** Neo4j's offline recovery with its strict defaults. */
    private fun recover(config: Config, layout: org.neo4j.io.layout.DatabaseLayout) {
        JobSchedulerFactory.createInitialisedScheduler().use { scheduler ->
            StandalonePageCacheFactory.createPageCache(fs, scheduler, PageCacheTracer.NULL).use {
                pageCache ->
                assertThat(
                        Recovery.isRecoveryRequired(
                            fs,
                            pageCache,
                            layout,
                            config,
                            Optional.empty(),
                            EmptyMemoryTracker.INSTANCE,
                            DatabaseTracers.EMPTY,
                        )
                    )
                    .isTrue
                val tail =
                    LogTailExtractor(
                            fs,
                            config,
                            StorageEngineFactory.selectStorageEngine(fs, layout).orElseThrow(),
                            DatabaseTracers.EMPTY,
                        )
                        .getTailMetadata(layout, EmptyMemoryTracker.INSTANCE)
                Recovery.performRecovery(
                    Recovery.context(
                            fs,
                            pageCache,
                            DatabaseTracers.EMPTY,
                            DatabaseConfig(config),
                            layout,
                            EmptyMemoryTracker.INSTANCE,
                            IOController.DISABLED,
                            NullLogProvider.getInstance(),
                            tail,
                        )
                        .awaitIndexesOnline(1, TimeUnit.MINUTES)
                )
            }
        }
    }
}
