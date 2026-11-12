package org.dozendb.core.importer

import java.io.IOException
import java.io.PrintStream
import java.nio.file.Files
import java.time.Instant
import java.util.function.Supplier
import org.dozendb.core.DozenDbSettings
import org.dozendb.core.importer.IncrementState.Stage
import org.neo4j.batchimport.api.Configuration
import org.neo4j.batchimport.api.IncrementalBatchImporter
import org.neo4j.batchimport.api.IndexImporterFactory
import org.neo4j.batchimport.api.Monitor
import org.neo4j.batchimport.api.input.Collector
import org.neo4j.batchimport.api.input.IdType
import org.neo4j.batchimport.api.input.Input
import org.neo4j.batchimport.api.input.PropertySizeCalculator
import org.neo4j.cli.ExitCode
import org.neo4j.commandline.dbms.LockChecker
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.function.ThrowingSupplier
import org.neo4j.internal.batchimport.IncrementalBatchImporterFactory
import org.neo4j.internal.batchimport.input.Groups
import org.neo4j.internal.kernel.api.IndexQueryConstraints
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.internal.schema.IndexDescriptor
import org.neo4j.internal.schema.SchemaDescriptor
import org.neo4j.io.ByteUnit
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.io.layout.DatabaseLayout
import org.neo4j.io.locker.FileLockException
import org.neo4j.io.pagecache.context.CursorContextFactory
import org.neo4j.io.pagecache.tracing.PageCacheTracer
import org.neo4j.kernel.KernelVersion
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.index.IndexProvidersAccess
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.internal.LogService
import org.neo4j.memory.MemoryTracker
import org.neo4j.scheduler.JobScheduler
import org.neo4j.storageengine.api.LogFilesInitializer
import org.neo4j.storageengine.api.StorageEngineFactory
import org.neo4j.token.TokenHolders
import org.neo4j.values.storable.FloatingPointValue
import org.neo4j.values.storable.IntegralValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Value
import org.neo4j.wal.LogTailMetadata

/**
 * DozenDB's incremental importer for the record storage engine, the one Neo4j Community's
 * `neo4j-admin database import incremental` asks for and has none of (see [IncrementalImporter]).
 */
class DozenDbIncrementalImporterFactory : IncrementalBatchImporterFactory(PRIORITY) {
    override fun getName(): String = "dozendb"

    override fun instantiate(
        databaseLayout: DatabaseLayout,
        fileSystem: FileSystemAbstraction,
        cacheTracer: PageCacheTracer,
        config: Configuration,
        logService: LogService,
        progressOutput: PrintStream,
        verboseProgressOutput: Boolean,
        logTailMetadataSupplier: ThrowingSupplier<LogTailMetadata, IOException>,
        dbConfig: Config,
        monitor: Monitor,
        jobScheduler: JobScheduler,
        badCollector: Collector,
        logFilesInitializer: LogFilesInitializer,
        indexImporterFactory: IndexImporterFactory,
        memoryTracker: MemoryTracker,
        contextFactory: CursorContextFactory,
        indexProvidersAccess: Supplier<IndexProvidersAccess>,
    ): IncrementalBatchImporter =
        IncrementalImporter(
            databaseLayout,
            fileSystem,
            config,
            progressOutput,
            logTailMetadataSupplier,
            dbConfig,
            badCollector,
            memoryTracker,
        )

    private companion object {
        const val PRIORITY = 100
    }
}

/** Fails an incremental import with [message], which `neo4j-admin` prints as it is. */
internal class IncrementalImportException(
    message: String,
    val exitCode: Int = ExitCode.FAIL,
    cause: Throwable? = null,
) : RuntimeException(message, cause)

/**
 * Adds the nodes and relationships of an input to the existing database of [layout], as Neo4j
 * Enterprise's incremental import does, in its three stages:
 * - [prepare], with the database stopped: checks that every ID space of the input's nodes is
 *   identified by a label and property key with a uniqueness or key constraint, and keeps the keys
 *   of the existing nodes of those labels, with the store's position, in an [Increment];
 * - `build`, with the database stopped or online but read-only, reads the input against them;
 * - `merge`, with the database stopped, writes the increment into the database through Neo4j's own
 *   transactions, which keep its relationship chains, counts, indexes and constraints.
 *
 * The database is opened by a [ScratchDbms] of the import's own, beside its server, which may be
 * running.
 */
internal class IncrementalImporter(
    private val layout: DatabaseLayout,
    private val fs: FileSystemAbstraction,
    private val importConfig: Configuration,
    private val out: PrintStream,
    private val logTail: ThrowingSupplier<LogTailMetadata, IOException>,
    private val serverConfig: Config,
    private val collector: Collector,
    private val memoryTracker: MemoryTracker,
    private val batchSize: Int = IncrementMerger.BATCH_SIZE,
    /** Called with false before each batch of the merge commits, and with true after; for tests. */
    private val onBatch: (committed: Boolean) -> Unit = {},
) : IncrementalBatchImporter {
    private val increment = Increment(layout)
    private val name: String = layout.databaseName

    override fun prepare(input: Input) {
        refuseSystem()
        prepareIncrement(validated(input))
    }

    private fun refuseSystem() {
        if (name == GraphDatabaseSettings.SYSTEM_DATABASE_NAME) {
            throw IncrementalImportException(
                "Incremental import adds to user databases, not to system.",
                ExitCode.USAGE,
            )
        }
    }

    /**
     * [input], having read its headers: Neo4j's inputs know their id type and entities only then.
     * The estimates it gives are not needed.
     */
    private fun validated(input: Input): Input {
        input.validateAndEstimate(
            PropertySizeCalculator { _, _, _ -> 0 },
            importConfig.maxNumberOfWorkerThreads(),
        )
        return input
    }

    private fun prepareIncrement(input: Input) {
        val idType = input.idType()
        if (idType == IdType.ACTUAL) {
            throw IncrementalImportException(
                "Incremental import identifies nodes by their key properties, not by their ids in " +
                    "the store: use --id-type=string or --id-type=integer.",
                ExitCode.USAGE,
            )
        }
        increment.state()?.let { state ->
            if (state.stage > Stage.BUILT) {
                throw IncrementalImportException(
                    "The increment in ${increment.directory} is being merged into database " +
                        "'$name'. Run the merge stage again to complete it."
                )
            }
        }
        checkStopped()
        val tail = logTail.get()
        val last = tail.lastCommittedTransaction
        increment.reset()
        out.println("Preparing an increment of database '$name' in ${increment.directory}")
        val groups =
            ScratchDbms.open(
                    layout,
                    serverConfig,
                    increment.home,
                    increment.logs,
                    writable = false,
                    last.consensusIndex(),
                    tail.kernelVersion(),
                    importConfig.maxOffHeapMemory(),
                )
                .use { scratch -> keyedGroups(scratch.database, input, idType) }
        increment.record(
            IncrementState(
                database = name,
                stage = Stage.PREPARED,
                lastTransaction = last.id(),
                lastChecksum = last.checksum(),
                consensusIndex = last.consensusIndex(),
                kernelVersion = tail.kernelVersion().version(),
                keyType = if (idType == IdType.INTEGER) KeyType.INTEGER else KeyType.TEXT,
                groups = groups,
            )
        )
        for (group in groups) {
            out.println(
                "${describe(group.name)}: nodes identified by :${group.label}(${group.key}), " +
                    "${group.existing} of them in the database"
            )
        }
    }

    /** All three stages, as `--stage=all` asks, with the database stopped. */
    override fun doImport(input: Input) {
        refuseSystem()
        val validated = validated(input)
        prepareIncrement(validated)
        buildIncrement(validated)
        merge()
    }

    override fun doDryRun(input: Input, output: PrintStream) {
        throw IncrementalImportException("An incremental import runs no --dry-run.", ExitCode.USAGE)
    }

    override fun build(input: Input) = buildIncrement(validated(input))

    private fun buildIncrement(input: Input) {
        val state =
            increment.state()
                ?: throw IncrementalImportException(
                    "There is no increment of database '$name' to build: run the prepare stage " +
                        "first."
                )
        if (state.stage == Stage.MERGING) {
            throw IncrementalImportException(
                "The increment in ${increment.directory} is being merged into database '$name'. " +
                    "Run the merge stage again to complete it."
            )
        }
        val keyType = if (input.idType() == IdType.INTEGER) KeyType.INTEGER else KeyType.TEXT
        if (input.idType() == IdType.ACTUAL || keyType != state.keyType) {
            throw IncrementalImportException(
                "Prepare read the keys for --id-type=${state.keyType.idType}: build with the same " +
                    "--id-type.",
                ExitCode.USAGE,
            )
        }
        out.println("Building the increment of database '$name' in ${increment.directory}")
        increment.record(
            IncrementBuilder(increment, state, input, collector, importConfig, memoryTracker, out)
                .build()
        )
    }

    override fun merge() {
        val state =
            increment.state()
                ?: throw IncrementalImportException(
                    "There is no increment of database '$name' to merge: run the prepare and " +
                        "build stages first."
                )
        if (state.stage == Stage.PREPARED) {
            throw IncrementalImportException(
                "The increment of database '$name' is not built: run the build stage first."
            )
        }
        checkStopped()
        val last = logTail.get().lastCommittedTransaction
        MergeJournal(increment.journal, increment.mergedNodeIds, memoryTracker).use { journal ->
            val position = journal.recover(last.id(), last.checksum(), state)
            if (state.stage == Stage.BUILT) increment.record(state.copy(stage = Stage.MERGING))
            val pageCache = importConfig.maxOffHeapMemory()
            out.println(
                if (position.tokens) {
                    "Continuing the merge into database '$name' after ${position.nodes} new " +
                        "nodes and ${position.relationships} new relationships"
                } else {
                    "Merging ${state.nodes - state.skippedNodes} new nodes and " +
                        "${state.relationships} new relationships into database '$name'"
                } + ", with a page cache of ${ByteUnit.bytesToString(pageCache)}"
            )
            ScratchDbms.open(
                    layout,
                    serverConfig,
                    increment.home,
                    increment.logs,
                    writable = true,
                    state.consensusIndex,
                    KernelVersion.getForVersion(state.kernelVersion),
                    pageCache,
                )
                .use { scratch ->
                    IncrementMerger(
                            increment,
                            state,
                            scratch.database,
                            journal,
                            out,
                            batchSize,
                            onBatch,
                        )
                        .merge(position)
                }
        }
        if (serverConfig.get(DozenDbSettings.clusterEnabled)) {
            // Before the increment goes: a merge that stops in between runs again.
            Files.writeString(
                increment.imported,
                "Merged by neo4j-admin database import incremental at ${Instant.now()}\n",
            )
        }
        increment.delete()
        out.println("Merged the increment into database '$name'")
    }

    override fun close() {}

    /** Fails unless the database exists and is stopped. */
    private fun checkStopped() {
        if (StorageEngineFactory.selectStorageEngine(fs, layout).isEmpty) {
            throw IncrementalImportException(
                "Database '$name' does not exist: an incremental import adds to an existing " +
                    "database. Create it with neo4j-admin database import full, or with CREATE " +
                    "DATABASE."
            )
        }
        try {
            LockChecker.checkDatabaseLock(layout).close()
        } catch (e: FileLockException) {
            throw IncrementalImportException(
                "Database '$name' is in use. Stop it first, with STOP DATABASE $name.",
                ExitCode.FAIL,
                e,
            )
        }
    }

    /**
     * The ID spaces of the input's nodes, each identified by a label and key with a uniqueness or
     * key constraint in the database, whose existing keys are written to the increment.
     */
    private fun keyedGroups(
        database: GraphDatabaseAPI,
        input: Input,
        idType: IdType,
    ): List<KeyedGroup> {
        val tokens = database.dependencyResolver.resolveDependency(TokenHolders::class.java)
        val schemas: Map<String?, SchemaDescriptor> =
            try {
                input.referencedNodeSchema(tokens)
            } catch (e: IllegalStateException) {
                throw IncrementalImportException(
                    "Incremental import identifies the nodes of each ID space by a label and " +
                        "property key, given in the ID column of its node files' header, such as " +
                        "uuid:ID{label:Person}: ${e.message}",
                    ExitCode.DATAERR,
                    e,
                )
            }
        database
            .beginTransaction(KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED)
            .use { tx ->
                val ktx = tx.kernelTransaction()
                return schemas.entries
                    .sortedBy { it.key.orEmpty() }
                    .mapIndexed { i, (groupName, schema) ->
                        val label = tokens.labelTokens().getTokenById(schema.labelId).name()
                        val key = tokens.propertyKeyTokens().getTokenById(schema.propertyId).name()
                        val keyType = keyType(groupName, specificIdType(input, groupName), idType)
                        val index =
                            ktx.schemaRead().index(schema).asSequence().firstOrNull { it.isUnique }
                                ?: throw IncrementalImportException(
                                    "Incremental import finds the existing nodes of " +
                                        "${describe(groupName)} by :$label($key), which needs a node " +
                                        "property uniqueness or key constraint. Create one first, " +
                                        "for example: CREATE CONSTRAINT FOR (n:$label) REQUIRE " +
                                        "n.$key IS UNIQUE",
                                    ExitCode.DATAERR,
                                )
                        val existing = writeKeys(ktx, index, keyType, i)
                        KeyedGroup(groupName, label, key, keyType, existing)
                    }
            }
    }

    /** Writes the keys in [index] that [keyType] compares to the key file of group [i]. */
    private fun writeKeys(
        ktx: KernelTransaction,
        index: IndexDescriptor,
        keyType: KeyType,
        i: Int,
    ): Long {
        var count = 0L
        val session = ktx.dataRead().indexReadSession(index)
        KeyWriter(increment.keys(i)).use { writer ->
            ktx.cursors()
                .allocateNodeValueIndexCursor(ktx.cursorContext(), ktx.memoryTracker())
                .use { cursor ->
                    ktx.dataRead()
                        .nodeIndexScan(session, cursor, IndexQueryConstraints.unorderedValues())
                    while (cursor.next()) {
                        val key = key(cursor.propertyValue(0), keyType) ?: continue
                        writer.write(key, cursor.nodeReference())
                        count++
                    }
                }
        }
        return count
    }

    companion object {
        /** Neo4j's import names the global ID space by null. */
        internal fun describe(group: String?) =
            if (group == null) "the global ID space" else "ID space '$group'"

        /**
         * The ID type that the header of [group] gives its first ID column, the one incremental
         * import finds the group's nodes by; null if it gives none.
         */
        internal fun specificIdType(input: Input, group: String?): String? =
            (input.groups() as? Groups)?.getSpecificIdType(input.groups().get(group), 0)

        /**
         * How the IDs of [group] compare: as the group's own ID type, [specificIdType], if its
         * header gives one, or as [idType] says.
         */
        internal fun keyType(group: String?, specificIdType: String?, idType: IdType): KeyType {
            val specific = specificIdType?.lowercase()
            val type =
                when (specific) {
                    null -> if (idType == IdType.INTEGER) KeyType.INTEGER else KeyType.TEXT
                    "string" -> KeyType.TEXT
                    "long",
                    "int",
                    "short",
                    "byte" -> KeyType.INTEGER
                    else ->
                        throw IncrementalImportException(
                            "${describe(group)} has IDs of type $specific, which " +
                                "incremental import cannot match with existing nodes: use " +
                                "string, long, int, short or byte.",
                            ExitCode.DATAERR,
                        )
                }
            if (idType == IdType.INTEGER && type == KeyType.TEXT) {
                throw IncrementalImportException(
                    "${describe(group)} has text IDs, which --id-type=integer does " + "not take.",
                    ExitCode.DATAERR,
                )
            }
            return type
        }

        /**
         * The key [value] stands for when [keyType] compares keys, null if no input ID can equal
         * it: text as a String, a whole number as a Long.
         */
        internal fun key(value: Value, keyType: KeyType): Any? =
            when (keyType) {
                KeyType.TEXT -> (value as? TextValue)?.stringValue()
                KeyType.INTEGER ->
                    when (value) {
                        is IntegralValue -> value.longValue()
                        is FloatingPointValue -> {
                            val double = value.doubleValue()
                            val long = double.toLong()
                            if (long.toDouble() == double && double.isFinite()) long else null
                        }
                        else -> null
                    }
            }
    }
}
