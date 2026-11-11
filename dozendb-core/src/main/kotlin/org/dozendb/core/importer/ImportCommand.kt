package org.dozendb.core.importer

import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.OutputStream
import java.io.PrintStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import org.neo4j.batchimport.api.Configuration
import org.neo4j.batchimport.api.input.Collector
import org.neo4j.batchimport.api.input.Input
import org.neo4j.cli.CommandFailedException
import org.neo4j.cli.CommandProvider
import org.neo4j.cli.CommandType
import org.neo4j.cli.ExecutionContext
import org.neo4j.cli.ExitCode
import org.neo4j.configuration.Config
import org.neo4j.importer.ImportCommand
import org.neo4j.importer.PrintingImportLogicMonitor
import org.neo4j.internal.batchimport.DefaultAdditionalIds
import org.neo4j.internal.batchimport.staging.ExecutionMonitors
import org.neo4j.internal.batchimport.staging.SpectrumExecutionMonitor
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.io.layout.DatabaseLayout
import org.neo4j.io.layout.Neo4jLayout
import org.neo4j.io.pagecache.context.CursorContextFactory
import org.neo4j.io.pagecache.tracing.PageCacheTracer
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.kernel.impl.index.schema.IndexImporterFactoryImpl
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogInitializer
import org.neo4j.kernel.impl.util.Converters
import org.neo4j.logging.InternalLogProvider
import org.neo4j.logging.internal.LogService
import org.neo4j.memory.MemoryTracker
import org.neo4j.scheduler.JobScheduler
import org.neo4j.storageengine.api.StorageEngineFactory
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.Model.CommandSpec
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

    override fun execute() {
        val name = spec.positionalParameters().first().getValue<NormalizedDatabaseName>().name()
        val progress =
            ImportProgress(
                Neo4jLayout.of(createPrefilledConfigBuilder().build()).databaseLayout(name)
            )
        val overwrite = spec.findOption("--overwrite-destination").getValue<Boolean>()
        // Linking and post-processing find no bad entries: the report is the stopped import's.
        val report = spec.findOption("--report-file").getValue<Path>().toAbsolutePath()
        val kept = report.resolveSibling("${report.fileName}.dozendb-kept")
        if (!overwrite && resumable(progress) && Files.exists(report)) {
            Files.move(report, kept, StandardCopyOption.REPLACE_EXISTING)
        }
        try {
            super.execute()
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
            if (Files.exists(kept)) Files.move(kept, report, StandardCopyOption.REPLACE_EXISTING)
        }
    }

    override fun doImport(
        fileSystem: FileSystemAbstraction,
        databaseLayout: DatabaseLayout,
        databaseConfig: Config,
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
    ) {
        if (StorageEngineFactory.selectStorageEngine(databaseConfig).name() != RECORD_ENGINE) {
            return super.doImport(
                fileSystem,
                databaseLayout,
                databaseConfig,
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
            )
        }
        val monitor = PrintingImportLogicMonitor(stdOut, stdErr)
        ResumableBatchImporter(
                databaseLayout,
                fileSystem,
                pageCacheTracer,
                importConfig,
                logService,
                if (verbose) SpectrumExecutionMonitor(stdOut)
                else ExecutionMonitors.defaultVisible(monitor, stdOut, System.err),
                DefaultAdditionalIds.EMPTY,
                databaseConfig,
                monitor,
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
        Converters.toFiles(",", Converters.regexFiles(fs, true)).apply(list).joinToString(",") {
            file ->
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
 * Neo4j's `neo4j-admin database import incremental`, which fails with the reason DozenDB's importer
 * gives, or the input's error, rather than only "Error importing csv file.", and without Neo4j's
 * warning that a failed import leaves the store "likely in an unusable state": no stage leaves it
 * so. As Neo4j Enterprise documents, the merge stage needs no `--nodes`, which Neo4j Community's
 * subcommand requires for every stage.
 */
@Command(name = "incremental", description = ["Incremental import into an existing database."])
class IncrementalImportCommand(context: ExecutionContext) :
    ImportCommand.Incremental(withoutUnusableStoreWarning(context)) {

    /**
     * Makes `--nodes` optional, for the merge stage: picocli calls this while it builds the
     * command's model, once Neo4j's options are in it.
     */
    @Spec
    fun optionalNodes(spec: CommandSpec) {
        val nodes = spec.findOption(NODES) ?: return
        if (!nodes.required()) return
        spec.remove(nodes)
        spec.addOption(nodes.toBuilder().required(false).build())
    }

    override fun execute() {
        val name = spec.positionalParameters().first().getValue<NormalizedDatabaseName>().name()
        val layout = Neo4jLayout.of(createPrefilledConfigBuilder().build()).databaseLayout(name)
        if (StorageEngineFactory.selectStorageEngine(ctx.fs(), layout).isEmpty) {
            throw CommandFailedException(
                "Database '$name' does not exist: an incremental import adds to an existing " +
                    "database. Create it with neo4j-admin database import full, or with CREATE " +
                    "DATABASE.",
                ExitCode.FAIL,
            )
        }
        val nodes = spec.findOption(NODES)
        if (nodes.getValue<List<*>?>() == null) {
            if (spec.findOption("--stage").getValue<Any>().toString() != "merge") {
                throw CommandFailedException(
                    "Missing required option: '${nodes.paramLabel()}', which every stage but " +
                        "merge needs.",
                    ExitCode.USAGE,
                )
            }
            nodes.setValue(emptyList<Any>())
        }
        try {
            super.execute()
        } catch (e: CommandFailedException) {
            throw explained(e)
        }
    }

    private companion object {
        const val NODES = "--nodes"

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
 * [e], a failed import, with the reason DozenDB's importers gave, or else the input's error, such
 * as a duplicate key or a missing node, which Neo4j leaves to `--verbose`.
 */
private fun explained(e: CommandFailedException): CommandFailedException {
    val causes = generateSequence<Throwable>(e) { it.cause }.toList()
    causes.filterIsInstance<IncrementalImportException>().firstOrNull()?.let {
        return CommandFailedException(it.message, e, it.exitCode)
    }
    causes.filterIsInstance<ImportResumeException>().firstOrNull()?.let {
        return CommandFailedException(it.message, e, ExitCode.FAIL)
    }
    val detail = causes.drop(1).lastOrNull { !it.message.isNullOrBlank() }?.message
    if (detail == null || e.message.isNullOrBlank()) return e
    return CommandFailedException("${e.message?.removeSuffix(".")}: $detail", e, e.exitCode)
}

/** Provides [DozenDbImportCommand] in place of Neo4j Community's `import`. */
class DozenDbImportCommandProvider : CommandProvider {
    override fun createCommand(ctx: ExecutionContext) = DozenDbImportCommand()

    override fun commandType() = CommandType.IMPORT

    // Lower than Neo4j Community's, so that it takes precedence.
    override fun getPriority() = Int.MAX_VALUE - 1
}
