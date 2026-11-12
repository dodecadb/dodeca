package org.dozendb.backup

import java.io.IOException
import java.nio.file.Path
import java.time.Clock
import java.util.Optional
import java.util.concurrent.TimeUnit
import org.dozendb.core.store.CommittedTransaction
import org.dozendb.core.store.RestorePoint
import org.neo4j.commandline.dbms.StoreVersionLoader
import org.neo4j.common.Subject
import org.neo4j.configuration.Config
import org.neo4j.configuration.DatabaseConfig
import org.neo4j.dbms.archive.ArchiveFormat
import org.neo4j.dbms.archive.ArchiveInput
import org.neo4j.dbms.archive.DumpFormatSelector
import org.neo4j.dbms.archive.IncorrectFormat
import org.neo4j.dbms.archive.Loader
import org.neo4j.dbms.archive.backup.BackupFormatSelector
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.io.layout.DatabaseLayout
import org.neo4j.io.pagecache.IOController
import org.neo4j.io.pagecache.PageCache
import org.neo4j.io.pagecache.context.CursorContext
import org.neo4j.io.pagecache.context.CursorContextFactory
import org.neo4j.kernel.database.DatabaseTracers
import org.neo4j.kernel.impl.api.LeaseService
import org.neo4j.kernel.lifecycle.Lifespan
import org.neo4j.kernel.recovery.LogTailExtractor
import org.neo4j.kernel.recovery.Recovery
import org.neo4j.logging.InternalLogProvider
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.storageengine.AppendIndexProvider
import org.neo4j.storageengine.api.Leases
import org.neo4j.storageengine.api.StorageEngineFactory
import org.neo4j.storageengine.api.TransactionId
import org.neo4j.storageengine.api.TransactionIdStore
import org.neo4j.wal.CompleteCommandBatch
import org.neo4j.wal.LogAppendEvent
import org.neo4j.wal.files.LogFilesBuilder

/**
 * Prepares the store of a database, offline, from a dump or a full backup and the differential
 * backups that continue it: unpacks it, appends the differential backups' transactions to its
 * transaction log, up to a point in time if given, and recovers it with every index online.
 *
 * Detached, a store whose last transaction was replicated in a Raft group gets one more, empty,
 * without a Raft index, after it: a new group starts from the store, as when a cluster forms from
 * it. A store to restore or seed a database with is detached; the store of an aggregated backup is
 * not, so that the backups after the chain still continue it.
 */
class StorePreparer(
    private val fs: FileSystemAbstraction,
    private val pageCache: PageCache,
    private val config: Config,
    private val logProvider: InternalLogProvider,
    private val clock: Clock = Clock.systemUTC(),
) {
    /**
     * A prepared store.
     *
     * @property lastTransaction its last transaction.
     * @property lastRestored the last transaction it got from the dump or backups, before the one
     *   detaching it.
     * @property restored how many transactions of differential backups it got.
     * @property previousConsensusIndex detached, the Raft index its last transaction had in the
     *   group it was replicated in; null if it had none or was not detached.
     */
    class Prepared(
        val lastTransaction: TransactionId,
        val lastRestored: TransactionId,
        val restored: Long,
        val previousConsensusIndex: Long?,
    )

    /** Prepares in [target] the store of [chain], up to [until] if given. */
    fun prepare(
        chain: ArtifactChain,
        until: RestorePoint?,
        target: DatabaseLayout,
        detach: Boolean,
    ): Prepared = prepare(chain.full.path, chain.differentials, until, target, detach)

    /**
     * Prepares in [target], which must hold no store, the store of [archive], a dump or a full
     * backup, and the transactions of [differentials] after it, up to [until] if given.
     */
    fun prepare(
        archive: Path,
        differentials: List<Artifact>,
        until: RestorePoint?,
        target: DatabaseLayout,
        detach: Boolean,
    ): Prepared {
        val unpackedRecovered = unpack(archive, target)
        checkVersion(archive, target)
        val engine = StorageEngineFactory.selectStorageEngine(fs, target).orElseThrow()
        // A full backup taken with --skip-recovery.
        if (!unpackedRecovered) recover(target, engine)
        val tail =
            LogTailExtractor(fs, config, engine, DatabaseTracers.EMPTY)
                .getTailMetadata(target, EmptyMemoryTracker.INSTANCE)
        var last = tail.lastCommittedTransaction
        if (until != null && !includes(until, last)) {
            throw IOException(
                "$archive holds transactions up to ${last.id()}, not only those $until"
            )
        }
        var restored = 0L
        var lastRestored = last
        var previousConsensusIndex: Long? = null
        if (differentials.isNotEmpty() || (detach && last.consensusIndex() >= 0)) {
            val storeId = engine.retrieveStoreId(fs, target, pageCache, CursorContext.NULL_CONTEXT)
            val logFiles =
                LogFilesBuilder.writeableBuilder(target, fs, tail, tail)
                    .withStorageEngineFactory(engine)
                    .withLogTermProvider(tail)
                    .withStoreId(storeId)
                    .withConfig(config)
                    .withLogProvider(logProvider)
                    .withExternalLogTailMetadata(tail)
                    .withAppendIndexProvider(
                        object : AppendIndexProvider {
                            override fun nextAppendIndex() =
                                throw UnsupportedOperationException("appends take their own index")

                            override fun getLastAppendIndex() = last.appendIndex()
                        }
                    )
                    .build()
            Lifespan(logFiles).use {
                val logFile = logFiles.logFile
                val writer = logFile.transactionLogWriter
                /** Appends [transaction], whose checksum must be [expected] if given. */
                fun append(
                    transaction: CommittedTransaction,
                    source: Any,
                    expected: Int?,
                ): TransactionId {
                    val batch = transaction.batch
                    if (batch.kernelVersion() != last.kernelVersion()) {
                        throw IOException(
                            "$source: $transaction is of kernel version ${batch.kernelVersion()}, " +
                                "after transactions of ${last.kernelVersion()}; restoring across a " +
                                "kernel upgrade is not supported"
                        )
                    }
                    val checksum =
                        writer.append(
                            batch,
                            transaction.transactionId,
                            TransactionIdStore.UNKNOWN_CHUNK_ID.toLong(),
                            transaction.appendIndex,
                            last.checksum(),
                            AppendIndexProvider.UNKNOWN_APPEND_INDEX,
                            LogAppendEvent.NULL,
                        )
                    if (expected != null && checksum != expected) {
                        throw IOException(
                            "$source: appending $transaction gives checksum $checksum, not the " +
                                "recorded $expected"
                        )
                    }
                    return TransactionId(
                        transaction.transactionId,
                        transaction.appendIndex,
                        batch.kernelVersion(),
                        checksum,
                        batch.timeCommitted,
                        batch.consensusIndex(),
                    )
                }

                var stopped = false
                for (differential in differentials) {
                    if (stopped) break
                    val header = differential.header
                    if (
                        !header.empty &&
                            (header.lowestAppendIndex != last.appendIndex() + 1 ||
                                header.previousChecksum != last.checksum())
                    ) {
                        throw IOException(
                            "$differential does not continue the store: it starts at transaction " +
                                "${header.lowestAppendIndex} after checksum " +
                                "${header.previousChecksum}, the store ends at ${last.appendIndex()} " +
                                "with checksum ${last.checksum()}"
                        )
                    }
                    fs.openAsInputStream(differential.path).use { input ->
                        Artifacts.readDifferential(input, engine.commandReaderFactory()) {
                            _,
                            transactions ->
                            for (transaction in transactions) {
                                if (until != null && !until.includes(transaction)) {
                                    stopped = true
                                    break
                                }
                                if (transaction.previousChecksum != last.checksum()) {
                                    throw IOException(
                                        "$differential: $transaction follows checksum " +
                                            "${transaction.previousChecksum}, not ${last.checksum()}"
                                    )
                                }
                                last = append(transaction, differential, transaction.checksum)
                                restored++
                            }
                        }
                    }
                }
                lastRestored = last
                if (detach && last.consensusIndex() >= 0) {
                    previousConsensusIndex = last.consensusIndex()
                    last = append(detachment(last), "the restore", null)
                }
                logFile.forceAfterAppend(LogAppendEvent.NULL)
            }
        }
        recover(target, engine)
        val recovered =
            LogTailExtractor(fs, config, engine, DatabaseTracers.EMPTY)
                .getTailMetadata(target, EmptyMemoryTracker.INSTANCE)
                .lastCommittedTransaction
        if (recovered.id() != last.id() || recovered.checksum() != last.checksum()) {
            throw IOException(
                "recovery ended at transaction ${recovered.id()}, not at ${last.id()} as appended"
            )
        }
        return Prepared(recovered, lastRestored, restored, previousConsensusIndex)
    }

    /**
     * Recovers in [target] a copy of a running database's store, which [source] names in errors,
     * and returns its last transaction.
     */
    fun recoverCopy(target: DatabaseLayout, source: Any): TransactionId {
        checkVersion(source, target)
        val engine =
            StorageEngineFactory.selectStorageEngine(fs, target).orElseThrow {
                IOException("$source holds no store")
            }
        val extractor = LogTailExtractor(fs, config, engine, DatabaseTracers.EMPTY)
        recover(target, engine)
        return extractor
            .getTailMetadata(target, EmptyMemoryTracker.INSTANCE)
            .lastCommittedTransaction
    }

    private fun includes(until: RestorePoint, transaction: TransactionId): Boolean =
        when (until) {
            is RestorePoint.BeforeTransaction -> transaction.id() < until.transactionId
            is RestorePoint.BeforeTime -> transaction.commitTimestamp() < until.time.toEpochMilli()
        }

    /** The empty transaction, without a Raft index, that ends the history in a Raft group. */
    private fun detachment(last: TransactionId): CommittedTransaction {
        val now = clock.millis()
        val batch =
            CompleteCommandBatch(
                emptyList(),
                TransactionIdStore.UNKNOWN_CONSENSUS_INDEX,
                now,
                last.id(),
                now,
                LeaseService.NO_LEASE,
                Leases.NO_LEASES,
                last.kernelVersion(),
                Subject.ANONYMOUS,
            )
        // Its checksum is whatever appending it gives.
        return CommittedTransaction(
            last.id() + 1,
            last.appendIndex() + 1,
            last.checksum(),
            0,
            batch,
        )
    }

    /**
     * Puts the store of [archive], a dump or a full backup, in [target]; returns whether it is
     * recovered, as a dump is.
     */
    private fun unpack(archive: Path, target: DatabaseLayout): Boolean {
        val description =
            fs.openAsInputStream(archive).use { input ->
                val magic = input.readNBytes(ArchiveFormat.MAGIC_PREFIX_LENGTH)
                BackupFormatSelector.selectReadFormat(magic)?.readMetadata(input)
            }
        if (description != null && !description.isFull) {
            throw IOException(
                "$archive is a differential backup: restore it with the full backup it continues"
            )
        }
        fs.mkdirs(target.databaseDirectory().parent)
        fs.mkdirs(target.transactionLogsDirectory.parent)
        try {
            Loader(fs)
                .load(
                    target,
                    false,
                    false,
                    { DumpFormatSelector.decompressWithBackupSupport(it) {} },
                    ArchiveInput.FileInput.of(fs, archive),
                )
        } catch (e: IncorrectFormat) {
            throw IOException("$archive is not a dump or a backup", e)
        }
        return description?.isRecovered ?: true
    }

    private fun checkVersion(archive: Any, target: DatabaseLayout) {
        val result =
            try {
                StoreVersionLoader(fs, config, CursorContextFactory.NULL_CONTEXT_FACTORY).use {
                    it.loadStoreVersionAndCheckDowngrade(target)
                }
            } catch (e: IllegalStateException) {
                throw IOException("$archive: ${e.message}", e)
            }
        if (result.migrationNeeded) {
            throw IOException(
                "$archive holds a store of format ${result.currentFormat.storeVersionUserString}, " +
                    "introduced in Neo4j ${result.currentFormatIntroductionVersion}, which this " +
                    "Neo4j opens only after 'neo4j-admin database migrate'"
            )
        }
    }

    /** Recovers the store in [target] from its transaction logs as they are now. */
    private fun recover(target: DatabaseLayout, engine: StorageEngineFactory) {
        val tail =
            LogTailExtractor(fs, config, engine, DatabaseTracers.EMPTY)
                .getTailMetadata(target, EmptyMemoryTracker.INSTANCE)
        val required =
            Recovery.isRecoveryRequired(
                fs,
                pageCache,
                target,
                config,
                Optional.of(tail),
                EmptyMemoryTracker.INSTANCE,
                DatabaseTracers.EMPTY,
            )
        if (!required) return
        val context =
            Recovery.context(
                    fs,
                    pageCache,
                    DatabaseTracers.EMPTY,
                    DatabaseConfig(config),
                    target,
                    EmptyMemoryTracker.INSTANCE,
                    IOController.DISABLED,
                    logProvider,
                    tail,
                )
                .awaitIndexesOnline(Long.MAX_VALUE, TimeUnit.MILLISECONDS)
        Recovery.performRecovery(context)
    }
}
