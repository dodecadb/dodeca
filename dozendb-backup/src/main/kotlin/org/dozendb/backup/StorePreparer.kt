package org.dozendb.backup

import java.io.IOException
import java.nio.file.Path
import java.time.Clock
import java.util.Optional
import java.util.concurrent.TimeUnit
import org.neo4j.commandline.dbms.StoreVersionLoader
import org.neo4j.common.Subject
import org.neo4j.configuration.Config
import org.neo4j.dbms.archive.ArchiveFormat
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
import org.neo4j.kernel.KernelVersionProvider
import org.neo4j.kernel.database.DatabaseTracers
import org.neo4j.kernel.impl.api.LeaseService
import org.neo4j.kernel.impl.transaction.log.CompleteCommandBatch
import org.neo4j.kernel.impl.transaction.log.LogAppendEvent
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder
import org.neo4j.kernel.lifecycle.Lifespan
import org.neo4j.kernel.recovery.LogTailExtractor
import org.neo4j.kernel.recovery.Recovery
import org.neo4j.logging.InternalLogProvider
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.storageengine.AppendIndexProvider
import org.neo4j.storageengine.api.StorageEngineFactory
import org.neo4j.storageengine.api.TransactionId
import org.neo4j.storageengine.api.TransactionIdStore

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
        unpack(archive, target)
        checkVersion(archive, target)
        val engine = StorageEngineFactory.selectStorageEngine(fs, target).orElseThrow()
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
                LogFilesBuilder.activeFilesBuilder(target, fs, tail)
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
                    transaction: ArtifactTransaction,
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
        recover(target, tail)
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

    private fun includes(until: RestorePoint, transaction: TransactionId): Boolean =
        when (until) {
            is RestorePoint.BeforeTransaction -> transaction.id() < until.transactionId
            is RestorePoint.BeforeTime -> transaction.commitTimestamp() < until.time.toEpochMilli()
        }

    /** The empty transaction, without a Raft index, that ends the history in a Raft group. */
    private fun detachment(last: TransactionId): ArtifactTransaction {
        val now = clock.millis()
        val batch =
            CompleteCommandBatch(
                emptyList(),
                TransactionIdStore.UNKNOWN_CONSENSUS_INDEX,
                now,
                last.id(),
                now,
                LeaseService.NO_LEASE,
                last.kernelVersion(),
                Subject.ANONYMOUS,
            )
        // Its checksum is whatever appending it gives.
        return ArtifactTransaction(last.id() + 1, last.appendIndex() + 1, last.checksum(), 0, batch)
    }

    private fun unpack(archive: Path, target: DatabaseLayout) {
        val description =
            fs.openAsInputStream(archive).use { input ->
                val magic =
                    String(input.readNBytes(ArchiveFormat.MAGIC_PREFIX_LENGTH), Charsets.US_ASCII)
                BackupFormatSelector.selectFormat(magic).map { it.readMetadata(input) }.orElse(null)
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
                    { fs.openAsInputStream(archive) },
                    "$archive",
                )
        } catch (e: IncorrectFormat) {
            throw IOException("$archive is not a dump or a backup", e)
        }
    }

    private fun checkVersion(archive: Path, target: DatabaseLayout) {
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

    private fun recover(target: DatabaseLayout, tail: KernelVersionProvider) {
        val required =
            Recovery.isRecoveryRequired(
                fs,
                pageCache,
                target,
                config,
                Optional.empty(),
                EmptyMemoryTracker.INSTANCE,
                DatabaseTracers.EMPTY,
            )
        if (!required) return
        val context =
            Recovery.context(
                    fs,
                    pageCache,
                    DatabaseTracers.EMPTY,
                    config,
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
