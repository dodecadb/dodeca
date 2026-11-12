package org.dozendb.core.importer

import java.io.ByteArrayOutputStream
import java.io.Closeable
import java.io.IOException
import java.io.OutputStream
import java.io.PrintStream
import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.util.Locale
import java.util.function.Supplier
import org.dozendb.core.store.BackupArtifacts
import org.dozendb.core.store.BackupTargetOptions
import org.neo4j.batchimport.api.Configuration
import org.neo4j.batchimport.api.Monitor
import org.neo4j.batchimport.api.ResumableStateAccessor
import org.neo4j.batchimport.api.input.Collector
import org.neo4j.batchimport.api.input.FileGroup
import org.neo4j.batchimport.api.input.Input
import org.neo4j.cli.CommandFailedException
import org.neo4j.cli.CommandProvider
import org.neo4j.cli.CommandType
import org.neo4j.cli.ExecutionContext
import org.neo4j.cli.ExitCode
import org.neo4j.cloud.storage.SchemeFileSystemAbstraction
import org.neo4j.commandline.dbms.LockChecker
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.importer.FileImporter
import org.neo4j.importer.ImportCommand
import org.neo4j.importer.ImportCommand.ShardingArguments
import org.neo4j.importer.ImportConverters
import org.neo4j.importer.PrintingImportLogicMonitor
import org.neo4j.importer.SchemaCommandReader
import org.neo4j.importer.SchemaCommandSource.DeferredSchemaCommands
import org.neo4j.internal.batchimport.DefaultAdditionalIds
import org.neo4j.internal.batchimport.ParallelBatchImporter
import org.neo4j.internal.batchimport.staging.ExecutionMonitors
import org.neo4j.internal.batchimport.staging.SpectrumExecutionMonitor
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.io.fs.FileSystemAbstraction.PatternStyle
import org.neo4j.io.layout.DatabaseLayout
import org.neo4j.io.layout.Neo4jLayout
import org.neo4j.io.locker.FileLockException
import org.neo4j.io.pagecache.context.CursorContextFactory
import org.neo4j.io.pagecache.tracing.PageCacheTracer
import org.neo4j.kernel.api.index.IndexProvidersAccess
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.kernel.impl.index.schema.IndexImporterFactoryImpl
import org.neo4j.logging.InternalLogProvider
import org.neo4j.logging.Level
import org.neo4j.logging.internal.LogService
import org.neo4j.logging.log4j.Log4jLogProvider
import org.neo4j.memory.MemoryTracker
import org.neo4j.scheduler.JobScheduler
import org.neo4j.storageengine.api.StorageEngineFactory
import org.neo4j.token.TokenHolders
import org.neo4j.wal.files.LogTailMetadataFactoryImpl
import org.neo4j.wal.files.TransactionLogInitializer
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.Mixin
import picocli.CommandLine.Model.CommandSpec
import picocli.CommandLine.Option
import picocli.CommandLine.Spec

/**
 * `neo4j-admin database import` with Neo4j's `incremental` subcommand next to `full`, which Neo4j
 * Community ships without registering it: DozenDB's [DozenDbIncrementalImporterFactory] does the
 * import. `full` is [FullImportCommand], Neo4j Community's, which resumes.
 */
@Command(
    name = "import",
    description = ["High-speed import of data from CSV files, optimized for fault-free data."],
    subcommands = [IncrementalImportCommand::class],
)
class DozenDbImportCommand : ImportCommand() {
    /**
     * Puts [FullImportCommand] in place of Neo4j Community's `full`: picocli calls this while it
     * builds the command's model, once the subcommands of Community's annotation are in it, and
     * would refuse a second subcommand of that name.
     */
    @Spec
    fun resumableFull(spec: CommandSpec) {
        val full = spec.subcommands()[FULL] ?: return
        if (full.getCommand<Any>() is FullImportCommand) return
        spec.removeSubcommand(FULL)
        spec.addSubcommand(FULL, CommandLine(FullImportCommand::class.java, full.factory))
    }

    private companion object {
        const val FULL = "full"
    }
}

/**
 * Neo4j Community's `neo4j-admin database import full`, which resumes an import that stopped, from
 * relationship linking or from post-processing, as Neo4j Enterprise's does: run again with the same
 * input and options, without `--overwrite-destination`, which starts over (see
 * [ResumableBatchImporter]). The report of bad entries the stopped import wrote is kept.
 */
@Command(
    name = "full",
    description =
        [
            "High-speed initial import of fault-free data from CSV files into a non-existent or " +
                "empty database."
        ],
)
class FullImportCommand(context: ExecutionContext) : ImportCommand.Full(context) {
    /** Called at the points an import may stop at, for tests. */
    internal var interruption: (ResumableBatchImporter.Point) -> Unit = {}

    @Mixin private var target = BackupTargetOptions()

    /**
     * With --target-format=backup, where the database is imported before it is written as a backup:
     * kept if the import stops, so that it resumes.
     */
    private var staging: Path? = null

    override fun createPrefilledConfigBuilder(storageTempPath: Path?): Config.Builder =
        super.createPrefilledConfigBuilder(storageTempPath).also { builder ->
            staging?.let { staged(builder, it) }
        }

    override fun execute() {
        val name = spec.positionalParameters().first().getValue<NormalizedDatabaseName>().name()
        val backup =
            BackupTarget.of(target, spec, ctx, createPrefilledConfigBuilder().build(), name)
        staging = backup?.staging
        val progress =
            ImportProgress(
                Neo4jLayout.of(createPrefilledConfigBuilder().build()).databaseLayout(name)
            )
        val overwrite = spec.findOption("--overwrite-destination").getValue<Boolean>()
        // Linking and post-processing find no bad entries: the report is the stopped import's.
        // Without
        // --report-file, each run reports in a directory of its own, which keeps the stopped one's.
        val report = spec.findOption("--report-file").getValue<Path?>()?.toAbsolutePath()
        val kept = report?.resolveSibling("${report.fileName}.dozendb-kept")
        if (report != null && !overwrite && resumable(progress) && Files.exists(report)) {
            Files.move(report, kept, StandardCopyOption.REPLACE_EXISTING)
        }
        try {
            super.execute()
            backup?.write(Neo4jLayout.of(createPrefilledConfigBuilder().build()), "an import")
        } catch (e: CommandFailedException) {
            val refused =
                generateSequence<Throwable>(e) { it.cause }.any { it is ImportResumeException }
            when {
                refused -> {}
                progress.state()?.stage == ImportProgress.Stage.IMPORTED ->
                    ctx.err().println(RESUME.format("relationship linking"))
                progress.state()?.stage == ImportProgress.Stage.LINKED ->
                    ctx.err().println(RESUME.format("post-processing"))
            }
            throw explained(e)
        } finally {
            if (kept != null && Files.exists(kept)) {
                Files.move(kept, report, StandardCopyOption.REPLACE_EXISTING)
            }
        }
    }

    override fun doImport(
        fileSystem: FileSystemAbstraction,
        databaseLayout: DatabaseLayout,
        force: Boolean,
        databaseConfig: Config,
        storageEngineFactory: StorageEngineFactory,
        jobScheduler: JobScheduler,
        logProvider: InternalLogProvider,
        pageCacheTracer: PageCacheTracer,
        contextFactory: CursorContextFactory,
        importConfig: Configuration,
        logService: LogService,
        stdOut: PrintStream,
        stdErr: PrintStream,
        verbose: Boolean,
        badCollector: Collector,
        memoryTracker: MemoryTracker,
        input: Input,
        indexProvidersAccess: Supplier<IndexProvidersAccess>,
        shardingArguments: ShardingArguments?,
        monitor: Monitor,
    ) {
        if (storageEngineFactory.name() != RECORD_ENGINE) {
            return super.doImport(
                fileSystem,
                databaseLayout,
                force,
                databaseConfig,
                storageEngineFactory,
                jobScheduler,
                logProvider,
                pageCacheTracer,
                contextFactory,
                importConfig,
                logService,
                stdOut,
                stdErr,
                verbose,
                badCollector,
                memoryTracker,
                input,
                indexProvidersAccess,
                shardingArguments,
                monitor,
            )
        }
        val printing = PrintingImportLogicMonitor(stdOut, stdErr, monitor)
        ResumableBatchImporter(
                databaseLayout,
                fileSystem,
                pageCacheTracer,
                importConfig,
                logService,
                if (verbose) SpectrumExecutionMonitor(stdOut)
                else ExecutionMonitors.defaultVisible(printing, stdOut, System.err),
                DefaultAdditionalIds.EMPTY,
                databaseConfig,
                printing,
                jobScheduler,
                badCollector,
                TransactionLogInitializer.getLogFilesInitializer(),
                IndexImporterFactoryImpl(),
                memoryTracker,
                contextFactory,
                input(fileSystem),
                stdOut,
                interruption,
            )
            .doImport(input)
    }

    /**
     * The input and options of this import, which a resumed import must have the same of: the files
     * of each group of nodes and relationships, with their sizes and modification times, and the
     * options that shape what the import writes, whether given or by default.
     */
    private fun input(fs: FileSystemAbstraction): List<String> {
        val parsed = spec.commandLine().parseResult
        val entries = mutableListOf<String>()
        entries +=
            "database " +
                spec.positionalParameters().first().getValue<NormalizedDatabaseName>().name()
        for (option in spec.options().sortedBy { it.longestName() }) {
            val name = option.longestName()
            when (name) {
                in UNSHAPING -> {}
                NODES,
                RELATIONSHIPS ->
                    for (group in parsed.matchedOption(name)?.stringValues().orEmpty()) {
                        val i = group.indexOf('=')
                        val key = if (i < 0) "" else group.substring(0, i)
                        entries +=
                            "$name $key=" + files(fs, if (i < 0) group else group.substring(i + 1))
                    }
                else -> entries += "$name=${plain(option.getValue<Any?>())}"
            }
        }
        return entries
    }

    /** The files that [list] names, as Neo4j reads it, each with its size and modification time. */
    private fun files(fs: FileSystemAbstraction, list: String): String =
        ImportConverters.parseFilesList(
                fs,
                list,
                spec.findOption("--path-pattern-style").getValue<PatternStyle>(),
            )
            .joinToString(",") { file ->
                try {
                    "$file (${fs.getFileSize(file)} bytes, modified ${fs.lastModifiedTime(file)})"
                } catch (_: IOException) {
                    file.toString()
                }
            }

    private fun plain(value: Any?): String =
        when (value) {
            is Array<*> -> value.contentToString()
            is CharArray -> value.contentToString()
            else -> value.toString()
        }

    private companion object {
        const val RECORD_ENGINE = "record"
        const val NODES = "--nodes"
        const val RELATIONSHIPS = "--relationships"

        /**
         * Options that leave what the import writes as it is, which a resumed import may change.
         */
        val UNSHAPING =
            setOf(
                "--verbose",
                "--threads",
                "--max-off-heap-memory",
                "--high-parallel-io",
                "--report-file",
                "--expand-commands",
                "--additional-config",
                "--overwrite-destination",
                "--help",
                "--temp-path",
                "--disable-instrumentation",
                "--capture-java-flight-recordings",
                "--capture-thread-dumps",
                "--profile",
                "--profile-results-path",
                "--resumable-import-checkpoint-interval",
                "--compress",
                "--split-archive-part-size",
            )

        const val RESUME =
            "The import resumes from %s: run the same command again, without " +
                "--overwrite-destination, which starts over."

        fun resumable(progress: ImportProgress): Boolean =
            progress.state()?.stage.let {
                it == ImportProgress.Stage.IMPORTED || it == ImportProgress.Stage.LINKED
            }
    }
}

/**
 * `neo4j-admin database import incremental`, as Neo4j Enterprise documents it, which adds to an
 * existing database with DozenDB's [DozenDbIncrementalImporterFactory], in the stage that `--stage`
 * names: ported from Neo4j 5.26 Community's subcommand, which Neo4j 2026 removed. It fails with the
 * reason DozenDB's importer gives, or the input's error, and without Neo4j's warning that a failed
 * import leaves the store "likely in an unusable state": no stage leaves it so. The merge stage
 * needs no `--nodes`. It takes no `--schema`. `--dry-run` checks and estimates the input, as for a
 * full import, and needs no `--force`. With `--target-format=backup` a copy of the stopped database
 * gets the import, in one go, and is written as a full backup; the database is left as it is.
 */
@Command(name = "incremental", description = ["Incremental import into an existing database."])
class IncrementalImportCommand(context: ExecutionContext) :
    ImportCommand.Base(withoutUnusableStoreWarning(context)) {

    @Option(
        names = ["--stage"],
        paramLabel = "all|prepare|build|merge",
        description =
            [
                "Stage of incremental import. For incremental import into an existing database " +
                    "use 'all' (which requires the database to be stopped). For semi-online " +
                    "incremental import run 'prepare' (on a stopped database) followed by 'build' " +
                    "(on a potentially running database) and finally 'merge' (on a stopped " +
                    "database)."
            ],
        converter = [StageConverter::class],
    )
    private var stage = Stage.ALL

    @Option(
        names = ["--force"],
        description =
            [
                "Confirm incremental import by setting this flag. Required unless --dry-run is " +
                    "given."
            ],
    )
    private var forced = false

    @Mixin private var target = BackupTargetOptions()

    /**
     * With --target-format=backup, where a copy of the database gets the import before it is
     * written as a backup; the database is left as it is.
     */
    private var staging: Path? = null

    override fun createPrefilledConfigBuilder(storageTempPath: Path?): Config.Builder =
        super.createPrefilledConfigBuilder(storageTempPath).also { builder ->
            staging?.let { staged(builder, it) }
        }

    /** The stages of an incremental import. */
    enum class Stage {
        PREPARE,
        BUILD,
        MERGE,
        ALL,
    }

    /** A stage by its name, or by its number, as Neo4j takes it. */
    class StageConverter : CommandLine.ITypeConverter<Stage> {
        override fun convert(value: String): Stage =
            when (value.lowercase(Locale.ROOT)) {
                "1",
                "prepare" -> Stage.PREPARE
                "2",
                "build" -> Stage.BUILD
                "3",
                "merge" -> Stage.MERGE
                "all" -> Stage.ALL
                else -> throw CommandLine.TypeConversionException("Invalid stage: $value")
            }
    }

    override fun execute() {
        val name = spec.positionalParameters().first().getValue<NormalizedDatabaseName>().name()
        val dryRun = spec.findOption("--dry-run").getValue<Boolean>()
        if (!forced && !dryRun) {
            throw CommandLine.ParameterException(
                spec.commandLine(),
                "Missing required option: '--force'",
            )
        }
        if (dryRun && stage == Stage.MERGE) {
            throw CommandFailedException(
                "--dry-run checks the input of an incremental import, which --stage=merge reads " +
                    "none of: check it with --stage=all, prepare or build.",
                ExitCode.USAGE,
            )
        }
        val config = createPrefilledConfigBuilder().build()
        val layout = Neo4jLayout.of(config).databaseLayout(name)
        if (StorageEngineFactory.selectStorageEngine(ctx.fs(), layout).isEmpty) {
            throw CommandFailedException(
                "Database '$name' does not exist: an incremental import adds to an existing " +
                    "database. Create it with neo4j-admin database import full, or with CREATE " +
                    "DATABASE.",
                ExitCode.FAIL,
            )
        }
        if (spec.findOption("--schema").getValue<String?>() != null) {
            throw CommandFailedException(
                "An incremental import does not apply schema commands: create the indexes and " +
                    "constraints on the database.",
                ExitCode.USAGE,
            )
        }
        if (stage == Stage.MERGE) {
            // Neo4j's import reads the input files of every stage, and of csv by default only once
            // it has some; the merge reads none.
            spec
                .findOption("--nodes")
                .takeIf { it.getValue<Any?>() == null }
                ?.setValue(ArrayList<Any>())
            spec
                .findOption("--input-type")
                .takeIf { it.getValue<Any?>() == null }
                ?.setValue(FileImporter.FileInputType.CSV)
        }
        val backup = BackupTarget.of(target, spec, ctx, config, name)
        if (backup != null) {
            if (stage != Stage.ALL) {
                throw CommandFailedException(
                    "--target-format=backup imports with --stage=all only, into a copy of the " +
                        "database that it writes as a backup.",
                    ExitCode.USAGE,
                )
            }
            stage(layout, backup)
        }
        try {
            super.execute()
            backup?.write(Neo4jLayout.of(createPrefilledConfigBuilder().build()), "an import")
        } catch (e: CommandFailedException) {
            throw explained(e)
        } finally {
            staging?.let(ctx.fs()::deleteRecursively)
        }
    }

    /** Copies the stopped database in [layout] to where [backup] imports into it. */
    private fun stage(layout: DatabaseLayout, backup: BackupTarget) {
        val fs = ctx.fs()
        fs.deleteRecursively(backup.staging)
        val staged =
            Neo4jLayout.of(staged(super.createPrefilledConfigBuilder(null), backup.staging).build())
                .databaseLayout(layout.databaseName)
        try {
            LockChecker.checkDatabaseLock(layout).use {
                fs.copyRecursively(layout.databaseDirectory(), staged.databaseDirectory())
                fs.copyRecursively(layout.transactionLogsDirectory, staged.transactionLogsDirectory)
            }
        } catch (e: FileLockException) {
            fs.deleteRecursively(backup.staging)
            throw CommandFailedException(
                "Database '${layout.databaseName}' is in use: stop it, so that the import gets " +
                    "a consistent copy of it.",
                e,
            )
        } catch (e: IOException) {
            fs.deleteRecursively(backup.staging)
            throw e
        }
        staging = backup.staging
    }

    override fun importType() = "Incremental import"

    override fun isDistributedGraphShard() = false

    override fun isDistributedPropShard() = false

    override fun includeUpdatesInProgress() = true

    override fun requiresNodeParameter() = stage != Stage.MERGE

    /** The database exists, in its own format. */
    override fun importFormat(): String? = null

    override fun configureFileImporterBuilder(builder: FileImporter.Builder): FileImporter.Builder =
        builder.withStorageEngineFactory(
            StorageEngineFactory.selectStorageEngine(builder.fileSystem, builder.databaseLayout)
                .orElseThrow()
        )

    /** Each stage locks the database itself, as far as it needs to. */
    override fun maybeLockChecker() = MaybeLocker { Closeable {} }

    override fun doDryRun(
        input: Input,
        fileSystem: FileSystemAbstraction,
        databaseLayout: DatabaseLayout,
        databaseConfig: Config,
        storageEngineFactory: StorageEngineFactory,
        jobScheduler: JobScheduler,
        contextFactory: CursorContextFactory,
        importConfig: Configuration,
        indexProvidersAccess: Supplier<IndexProvidersAccess>,
        stdOut: PrintStream,
        verbose: Boolean,
        shardingArguments: ShardingArguments?,
        monitor: Monitor,
    ) {
        // Validates the input and estimates it, as Neo4j's full import does: writes nothing.
        ParallelBatchImporter.outputEstimates(
            input,
            importConfig.maxNumberOfWorkerThreads(),
            stdOut,
        )
    }

    override fun doImport(
        fileSystem: FileSystemAbstraction,
        databaseLayout: DatabaseLayout,
        force: Boolean,
        databaseConfig: Config,
        storageEngineFactory: StorageEngineFactory,
        jobScheduler: JobScheduler,
        logProvider: InternalLogProvider,
        pageCacheTracer: PageCacheTracer,
        contextFactory: CursorContextFactory,
        importConfig: Configuration,
        logService: LogService,
        stdOut: PrintStream,
        stdErr: PrintStream,
        verbose: Boolean,
        badCollector: Collector,
        memoryTracker: MemoryTracker,
        input: Input,
        indexProvidersAccess: Supplier<IndexProvidersAccess>,
        shardingArguments: ShardingArguments?,
        monitor: Monitor,
    ) {
        val importer =
            storageEngineFactory.incrementalBatchImporter(
                databaseLayout,
                fileSystem,
                pageCacheTracer,
                importConfig,
                logService,
                stdOut,
                verbose,
                LogTailMetadataFactoryImpl(fileSystem),
                databaseConfig,
                PrintingImportLogicMonitor(stdOut, stdErr, monitor),
                jobScheduler,
                badCollector,
                TransactionLogInitializer.getLogFilesInitializer(),
                IndexImporterFactoryImpl(),
                memoryTracker,
                contextFactory,
                indexProvidersAccess,
                0,
                null,
            )
        importer.use {
            when (stage) {
                Stage.PREPARE -> it.prepare(input)
                Stage.BUILD -> it.build(input)
                Stage.MERGE -> it.merge()
                Stage.ALL -> it.doImport(input)
            }
        }
    }

    override fun doResumableImport(
        fileSystem: FileSystemAbstraction,
        databaseLayout: DatabaseLayout,
        force: Boolean,
        databaseConfig: Config,
        storageEngineFactory: StorageEngineFactory,
        jobScheduler: JobScheduler,
        logProvider: InternalLogProvider,
        pageCacheTracer: PageCacheTracer,
        contextFactory: CursorContextFactory,
        importConfig: Configuration,
        logService: LogService,
        stdOut: PrintStream,
        stdErr: PrintStream,
        verbose: Boolean,
        badCollector: Collector,
        memoryTracker: MemoryTracker,
        input: Input,
        encoding: Charset,
        nodeFileGroupsByAdditionalLabels: Map<Set<String>, List<FileGroup>>,
        indexProvidersAccess: Supplier<IndexProvidersAccess>,
        shardingArguments: ShardingArguments?,
        monitor: Monitor,
        resumableStateAccessor: ResumableStateAccessor,
        resume: Boolean,
    ): Unit =
        throw CommandFailedException(
            "An incremental import runs only with Neo4j's legacy importer, without --legacy=false.",
            ExitCode.USAGE,
        )

    /** Never asked for: [execute] refuses `--schema`. */
    override fun schemaCommandReader(
        fileSystem: SchemeFileSystemAbstraction,
        config: Config,
        adaptor: DeferredSchemaCommands.Adaptor,
        tokenHolders: TokenHolders,
    ): SchemaCommandReader = throw UnsupportedOperationException("no schema commands")

    private companion object {
        /** The line Neo4j's file importer prints when any import fails. */
        const val UNUSABLE_STORE_WARNING = "WARNING Import failed. The store files in "

        fun withoutUnusableStoreWarning(context: ExecutionContext): ExecutionContext =
            ExecutionContext(
                context.homeDir(),
                context.confDir(),
                context.out(),
                PrintStream(LineFilter(context.err()) { !it.startsWith(UNUSABLE_STORE_WARNING) }),
                context.`in`(),
                context.fs(),
            )
    }

    /** Passes on to [target] the lines that [keep] keeps. */
    private class LineFilter(
        private val target: PrintStream,
        private val keep: (String) -> Boolean,
    ) : OutputStream() {
        private val line = ByteArrayOutputStream()

        override fun write(b: Int) {
            line.write(b)
            if (b == '\n'.code) emit()
        }

        override fun flush() {
            target.flush()
        }

        override fun close() {
            if (line.size() > 0) emit()
            target.flush()
        }

        private fun emit() {
            val bytes = line.toByteArray()
            line.reset()
            if (keep(String(bytes))) target.write(bytes)
        }
    }
}

/**
 * [e], a failed import, with the reason DozenDB's importers gave in place of Neo4j's "Error
 * importing csv file."; Neo4j's console output adds the message of the cause, which says it once.
 * Other failures as Neo4j gives them, with the input's error, such as a duplicate key or a missing
 * node.
 */
private fun explained(e: CommandFailedException): CommandFailedException {
    val causes = generateSequence<Throwable>(e) { it.cause }.toList()
    causes.filterIsInstance<IncrementalImportException>().firstOrNull()?.let {
        return CommandFailedException(it.message, it, it.exitCode)
    }
    causes.filterIsInstance<ImportResumeException>().firstOrNull()?.let {
        return CommandFailedException(it.message, it, ExitCode.FAIL)
    }
    return e
}

/** Provides [DozenDbImportCommand] in place of Neo4j Community's `import`. */
class DozenDbImportCommandProvider : CommandProvider {
    override fun createCommand(ctx: ExecutionContext) = DozenDbImportCommand()

    override fun commandType() = CommandType.IMPORT

    // Lower than Neo4j Community's, so that it takes precedence.
    override fun getPriority() = Int.MAX_VALUE - 1
}

/**
 * An import with `--target-format=backup`: it is made in [staging], under `--temp-path` or the
 * local `--target-location`, and then written as a full backup of [database] in
 * `--target-location`.
 */
internal class BackupTarget
private constructor(
    private val options: BackupTargetOptions,
    private val ctx: ExecutionContext,
    private val config: Config,
    private val database: String,
    private val partSize: Long,
    private val artifacts: BackupArtifacts,
    val staging: Path,
) {
    /**
     * Writes the database imported into [layout] as a full backup, which [source] says came from,
     * and deletes [staging].
     */
    fun write(layout: Neo4jLayout, source: String) {
        try {
            Log4jLogProvider(ctx.out(), Level.WARN).use { logProvider ->
                val written =
                    artifacts.write(
                        layout.databaseLayout(database),
                        database,
                        source,
                        checkNotNull(options.location),
                        "--target-location",
                        options.compress,
                        partSize,
                        config,
                        ctx.fs(),
                        logProvider,
                    )
                ctx.out()
                    .println(
                        "Wrote the imported database '$database' as a full backup to " +
                            "${written.file}, up to transaction ${written.lastTransaction}."
                    )
            }
        } catch (e: IOException) {
            throw CommandFailedException(e.message, e)
        } catch (e: IllegalArgumentException) {
            throw CommandFailedException(e.message, e)
        } finally {
            ctx.fs().deleteRecursively(staging)
        }
    }

    companion object {
        /**
         * The backup target that [options] give the import of [database]; null for a database, and
         * for a dry run, which writes nothing.
         */
        fun of(
            options: BackupTargetOptions,
            spec: CommandSpec,
            ctx: ExecutionContext,
            config: Config,
            database: String,
        ): BackupTarget? {
            if (!options.backup()) return null
            val partSize = options.partSize(config)
            if (spec.findOption("--dry-run").getValue<Boolean>()) return null
            val root = options.staging(spec.findOption("--temp-path").getValue<Path?>(), ctx.fs())
            return BackupTarget(
                options,
                ctx,
                config,
                database,
                partSize,
                options.artifacts(),
                root.toAbsolutePath().resolve(".dozendb-import-$database"),
            )
        }
    }
}

/** [builder] with the databases and their transaction logs in [staging]. */
private fun staged(builder: Config.Builder, staging: Path): Config.Builder =
    builder
        .set(GraphDatabaseInternalSettings.databases_root_path, staging.resolve("databases"))
        .set(GraphDatabaseSettings.transaction_logs_root_path, staging.resolve("transactions"))
