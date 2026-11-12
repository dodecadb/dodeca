package org.dozendb.core.importer

import java.io.PrintStream
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.util.Properties
import org.neo4j.batchimport.api.AdditionalInitialIds
import org.neo4j.batchimport.api.BatchImporter
import org.neo4j.batchimport.api.Configuration
import org.neo4j.batchimport.api.IndexImporterFactory
import org.neo4j.batchimport.api.Monitor
import org.neo4j.batchimport.api.input.Collector
import org.neo4j.batchimport.api.input.Input
import org.neo4j.configuration.Config
import org.neo4j.internal.batchimport.DataStatistics
import org.neo4j.internal.batchimport.ImportLogic
import org.neo4j.internal.batchimport.ImporterSchemaCommandExecutor
import org.neo4j.internal.batchimport.ParallelBatchImporter
import org.neo4j.internal.batchimport.SchemaMonitors
import org.neo4j.internal.batchimport.staging.ExecutionMonitor
import org.neo4j.internal.batchimport.store.BatchingNeoStores
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.io.layout.DatabaseLayout
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout
import org.neo4j.io.pagecache.context.CursorContextFactory
import org.neo4j.io.pagecache.tracing.PageCacheTracer
import org.neo4j.kernel.DatabaseCreationOptions
import org.neo4j.kernel.api.index.IndexDirectoryStructure
import org.neo4j.kernel.impl.store.StoreType
import org.neo4j.logging.internal.LogService
import org.neo4j.memory.MemoryTracker
import org.neo4j.scheduler.JobScheduler
import org.neo4j.storageengine.api.LogFilesInitializer
import org.neo4j.storageengine.api.LogMetadataProviderImpl
import org.neo4j.wal.EmptyLogTailMetadata

/** A full import that cannot resume as asked, with the reason, which `neo4j-admin` prints. */
internal class ImportResumeException(message: String) : RuntimeException(message)

/**
 * Where a full import into [layout] stands, in a file of the database directory, from which an
 * import that stopped resumes: the stage it completed last, its input, as
 * [ImportProgress.State.input] describes it, and what relationship linking needs to know of the
 * relationships imported.
 *
 * `--overwrite-destination` deletes it with the rest of the directory.
 */
internal class ImportProgress(layout: DatabaseLayout) {
    enum class Stage(val description: String) {
        /** Nodes and relationships are being imported: an import stopped here starts over. */
        STARTED("before all its nodes and relationships were imported"),

        /** Imported, not linked: an import resumes from relationship linking. */
        IMPORTED("after its nodes and relationships were imported"),

        /** Linked: an import resumes from post-processing. */
        LINKED("after its relationships were linked"),
    }

    class State(val stage: Stage, val input: List<String>, val statistics: DataStatistics? = null)

    val file: Path = layout.databaseDirectory().resolve(".dozendb-import.properties")

    /** The state recorded, or null if there is none or it cannot be read. */
    fun state(): State? = runCatching { read() }.getOrNull()

    /** The state recorded, or null if there is none. */
    fun read(): State? {
        if (!Files.exists(file)) return null
        val properties = Properties()
        Files.newBufferedReader(file).use(properties::load)
        if (properties.getProperty("format") != FORMAT) {
            throw ImportResumeException(
                "$file records the progress of an import in a format this version does not read."
            )
        }
        val input =
            generateSequence(0) { it + 1 }
                .map { properties.getProperty("input.$it") }
                .takeWhile { it != null }
                .map { it!! }
                .toList()
        val statistics =
            properties.getProperty("types")?.let { types ->
                DataStatistics(
                    properties.getProperty("nodes").toLong(),
                    properties.getProperty("properties").toLong(),
                    types
                        .split(',')
                        .filter { it.isNotEmpty() }
                        .map { type ->
                            val (id, count) = type.split(':')
                            DataStatistics.RelationshipTypeCount(id.toInt(), count.toLong())
                        }
                        .toTypedArray(),
                )
            }
        return State(Stage.valueOf(properties.getProperty("stage")), input, statistics)
    }

    /** Records [state], whole or not at all, on disk before it returns. */
    fun record(state: State) {
        val properties = Properties()
        properties.setProperty("format", FORMAT)
        properties.setProperty("stage", state.stage.name)
        state.input.forEachIndexed { i, entry -> properties.setProperty("input.$i", entry) }
        state.statistics?.let { statistics ->
            properties.setProperty("nodes", statistics.nodeCount.toString())
            properties.setProperty("properties", statistics.propertyCount.toString())
            properties.setProperty(
                "types",
                (0 until statistics.numberOfRelationshipTypes).joinToString(",") {
                    val type = statistics.get(it)
                    "${type.typeId}:${type.count}"
                },
            )
        }
        val written = file.resolveSibling("${file.fileName}.tmp")
        Files.newBufferedWriter(written).use {
            properties.store(it, "DozenDB full import progress")
        }
        FileChannel.open(written, StandardOpenOption.WRITE).use { it.force(true) }
        Files.move(
            written,
            file,
            StandardCopyOption.ATOMIC_MOVE,
            StandardCopyOption.REPLACE_EXISTING,
        )
        FileChannel.open(file.parent, StandardOpenOption.READ).use { it.force(true) }
    }

    fun delete() {
        Files.deleteIfExists(file)
    }

    private companion object {
        const val FORMAT = "1"
    }
}

/**
 * Neo4j's batch import of a whole store, as Neo4j Community's `ParallelBatchImporter` runs it,
 * which an import that stopped resumes from relationship linking or from post-processing, as Neo4j
 * Enterprise's does, given the same input ([input]): the store is flushed at those points, and
 * [ImportProgress] records them. An import that stopped earlier, while nodes and relationships were
 * imported, starts over with `--overwrite-destination`.
 *
 * Resumed from linking, the relationships are linked anew, whatever a stopped linking wrote: the
 * temporary stores and the relationship group store go, node degrees are counted again from the
 * relationship store, and every relationship's links are written again. Post-processing is Neo4j's
 * last stage of an import, as its progress shows it: resumed from there, the relationship groups
 * that linking wrote to the temporary group store are moved to the relationship group store again,
 * which a stopped one may have left half written, and the counts store, token lookup indexes and
 * transaction log are built again.
 *
 * @param input the import's input and options, which a resumed import must have the same of.
 * @param interruption called at the points an import may stop at, for tests.
 */
internal class ResumableBatchImporter(
    databaseLayout: DatabaseLayout,
    private val fs: FileSystemAbstraction,
    private val pageCacheTracer: PageCacheTracer,
    private val config: Configuration,
    private val logService: LogService,
    private val executionMonitor: ExecutionMonitor,
    private val additionalInitialIds: AdditionalInitialIds,
    private val dbConfig: Config,
    private val monitor: Monitor,
    private val scheduler: JobScheduler,
    private val badCollector: Collector,
    private val logFilesInitializer: LogFilesInitializer,
    private val indexImporterFactory: IndexImporterFactory,
    private val memoryTracker: MemoryTracker,
    private val contextFactory: CursorContextFactory,
    private val input: List<String>,
    private val out: PrintStream,
    private val interruption: (Point) -> Unit = {},
) : BatchImporter {
    /** Points an import may stop at, where [interruption] is called. */
    enum class Point {
        NODES_IMPORTED,
        RELATIONSHIPS_IMPORTED,
        FIRST_FORWARD_LINKING,
        LINKED,
        DEFRAGMENTED,
        LOG_INITIALIZED,
        POST_PROCESSED,
    }

    private val layout = RecordDatabaseLayout.convert(databaseLayout)
    private val progress = ImportProgress(layout)
    private val name = layout.databaseName

    override fun doDryRun(input: Input, output: PrintStream) {
        ParallelBatchImporter.outputEstimates(input, config.maxNumberOfWorkerThreads(), output)
    }

    override fun doImport(input: Input) {
        // Neo4j's refuses the commands of a schema file too.
        if (
            ImporterSchemaCommandExecutor.validateSupported(input.schemaCommandSource())
                .isNotEmpty()
        ) {
            throw UnsupportedOperationException(
                "A full import into the record format does not create indexes or constraints: " +
                    "create them once the import completes."
            )
        }
        val state = progress.read()
        if (state != null) checkResumable(state)
        val logMetadata = LogMetadataProviderImpl(EmptyLogTailMetadata(dbConfig))
        ImportLogic.instantiateNeoStores(
                fs,
                layout,
                pageCacheTracer,
                config,
                logService,
                additionalInitialIds,
                logMetadata,
                dbConfig,
                scheduler,
                memoryTracker,
                contextFactory,
                DatabaseCreationOptions.EMPTY_CREATION_OPTIONS,
            )
            .use { store ->
                ImportLogic(
                        layout,
                        store,
                        config,
                        dbConfig,
                        logService,
                        executionMonitor,
                        badCollector,
                        monitor,
                        contextFactory,
                        indexImporterFactory,
                        pageCacheTracer,
                        memoryTracker,
                    )
                    .use { logic ->
                        when (state?.stage) {
                            null -> {
                                store.createNew()
                                progress.record(
                                    ImportProgress.State(ImportProgress.Stage.STARTED, this.input)
                                )
                                logic.initialize(input)
                                logic.importNodes(SchemaMonitors.NO_SCHEMA)
                                interruption(Point.NODES_IMPORTED)
                                logic.importRelationships(SchemaMonitors.NO_SCHEMA)
                                checkpoint(store)
                                progress.record(
                                    ImportProgress.State(
                                        ImportProgress.Stage.IMPORTED,
                                        this.input,
                                        logic.getState(DataStatistics::class.java),
                                    )
                                )
                                interruption(Point.RELATIONSHIPS_IMPORTED)
                                link(store, logic)
                            }
                            ImportProgress.Stage.IMPORTED -> {
                                out.println(
                                    "Resuming the import into database '$name' from " +
                                        "relationship linking."
                                )
                                // The groups that linking and defragmentation wrote go with the
                                // temporary stores; linking writes them again.
                                store.pruneAndOpenExistingStore(
                                    { it != StoreType.RELATIONSHIP_GROUP },
                                    { false },
                                )
                                deleteAuxiliaryStores()
                                logic.initialize(input)
                                logic.putState(state.statistics!!)
                                link(store, logic)
                            }
                            ImportProgress.Stage.LINKED -> {
                                out.println(
                                    "Resuming the import into database '$name' from " +
                                        "post-processing."
                                )
                                // The groups linking wrote are in the temporary group store.
                                store.pruneAndOpenExistingStore(
                                    { it != StoreType.RELATIONSHIP_GROUP },
                                    { it == StoreType.RELATIONSHIP_GROUP },
                                )
                                deleteAuxiliaryStores()
                                logic.initialize(input)
                                logic.putState(state.statistics!!)
                            }
                            ImportProgress.Stage.STARTED -> error("checked")
                        }
                        logic.defragmentRelationshipGroups()
                        interruption(Point.DEFRAGMENTED)
                        logFilesInitializer.initializeLogFiles(
                            layout,
                            store.neoStores.metaDataStore,
                            logMetadata,
                            fs,
                            CHECKPOINT,
                            dbConfig,
                        )
                        interruption(Point.LOG_INITIALIZED)
                        logic.buildAuxiliaryStores(logMetadata)
                        interruption(Point.POST_PROCESSED)
                        logic.success()
                    }
            }
        progress.delete()
    }

    /** Links the relationships, then records that they are. */
    private fun link(store: BatchingNeoStores, logic: ImportLogic) {
        logic.calculateNodeDegrees()
        logic.linkRelationshipsOfAllTypes(
            object : ImportLogic.RelationshipLinkingMonitor {
                override fun forwardLinkingCompleted(
                    fromType: Int,
                    toType: Int,
                    firstRound: Boolean,
                    lastRound: Boolean,
                ) {
                    if (firstRound) interruption(Point.FIRST_FORWARD_LINKING)
                }
            }
        )
        checkpoint(store)
        progress.record(
            ImportProgress.State(
                ImportProgress.Stage.LINKED,
                input,
                logic.getState(DataStatistics::class.java),
            )
        )
        interruption(Point.LINKED)
    }

    /**
     * Writes the stores, the temporary ones too, and their high ids, which an import resumed from
     * here starts from.
     */
    private fun checkpoint(store: BatchingNeoStores) {
        store.markHighIds()
        contextFactory.create("dozendb-import-checkpoint").use { store.flushAndForce(it) }
    }

    /**
     * Deletes what post-processing writes, which a stopped import may have left half written: the
     * counts and degrees stores, the token lookup indexes and the transaction log.
     */
    private fun deleteAuxiliaryStores() {
        fs.deleteRecursively(
            IndexDirectoryStructure.baseSchemaIndexFolder(layout.databaseDirectory())
        )
        for (file in listOf(layout.countStore(), layout.relationshipGroupDegreesStore())) {
            file.delete(fs)
        }
        fs.deleteRecursively(layout.transactionLogsDirectory)
        fs.mkdirs(layout.transactionLogsDirectory)
    }

    private fun checkResumable(state: ImportProgress.State) {
        if (state.stage == ImportProgress.Stage.STARTED) {
            throw ImportResumeException(
                "The import into database '$name' stopped ${state.stage.description}: it resumes " +
                    "only from relationship linking or post-processing. Run it again with " +
                    "--overwrite-destination to start over."
            )
        }
        if (state.input != input) {
            val gone = state.input - input.toSet()
            val added = input - state.input.toSet()
            throw ImportResumeException(
                "The import into database '$name' stopped ${state.stage.description}, with other " +
                    "input or options than these, so it does not resume: it had " +
                    gone.joinToString("; ").ifEmpty { "nothing else" } +
                    ", and now has " +
                    added.joinToString("; ").ifEmpty { "nothing else" } +
                    ". Run it with the " +
                    "input and options it had to resume it, or with --overwrite-destination to " +
                    "start over."
            )
        }
        if (state.statistics == null) {
            throw ImportResumeException(
                "${progress.file} lacks the statistics of the relationships imported."
            )
        }
    }

    private companion object {
        const val CHECKPOINT = "Batch importer checkpoint."
    }
}
