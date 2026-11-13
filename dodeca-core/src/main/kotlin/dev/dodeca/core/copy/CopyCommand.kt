package dev.dodeca.core.copy

import dev.dodeca.core.store.BackupTargetOptions
import java.io.IOException
import java.nio.file.Path
import java.util.Optional
import org.neo4j.cli.AbstractAdminCommand
import org.neo4j.cli.CommandFailedException
import org.neo4j.cli.CommandProvider
import org.neo4j.cli.CommandType
import org.neo4j.cli.Converters.ByteUnitConverter
import org.neo4j.cli.Converters.DatabaseNameConverter
import org.neo4j.cli.ExecutionContext
import org.neo4j.cli.ExitCode
import org.neo4j.commandline.Util
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.io.layout.DatabaseLayout
import org.neo4j.io.layout.Neo4jLayout
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.logging.Level
import org.neo4j.logging.log4j.Log4jLogProvider
import picocli.CommandLine.Command
import picocli.CommandLine.Help.Visibility.ALWAYS
import picocli.CommandLine.Mixin
import picocli.CommandLine.Option
import picocli.CommandLine.Parameters

/**
 * `neo4j-admin database copy`, with the options of Neo4j Enterprise's, which Neo4j Community
 * declares and does not ship (see [StoreCopier]).
 */
@Command(
    name = "copy",
    header = ["Copy a database and optionally apply filters."],
    description =
        [
            "This command creates a copy of a database. If your labels, properties, or relationships contain dots or commas you can use ` to escape them, e.g. `My,label`, `My.property`. A file named <database-name>-schema.cypher, containing the schema commands needed to recreate indexes/constraints on the copy, will be created."
        ],
)
class CopyCommand(context: ExecutionContext) : AbstractAdminCommand(context) {
    @Parameters(
        index = "0",
        paramLabel = "<fromDatabase>",
        converter = [DatabaseNameConverter::class],
        description = ["Name of database to copy from."],
    )
    private lateinit var fromDatabase: NormalizedDatabaseName

    @Parameters(
        index = "1",
        paramLabel = "<toDatabase>",
        converter = [DatabaseNameConverter::class],
        description = ["Name of database to copy to."],
    )
    private lateinit var toDatabase: NormalizedDatabaseName

    @Option(
        names = ["--force"],
        description =
            ["Force the command to run even if the integrity of the database cannot be verified."],
    )
    private var force = false

    @Option(
        names = ["--compact-node-store"],
        arity = "0..1",
        fallbackValue = "true",
        showDefaultValue = ALWAYS,
        paramLabel = "true|false",
        description =
            [
                "By default node store is not compacted on copy since that changes node ids. Please use this option to enforce the compaction."
            ],
    )
    private var compactNodeStore = false

    @Option(
        names = ["--to-format"],
        paramLabel = "<format>",
        defaultValue = "same",
        showDefaultValue = ALWAYS,
        description =
            [
                "Set the format for the new database. Must be one of same, standard, aligned. 'same' will use the same format as the source."
            ],
    )
    private var toFormat = "same"

    @Option(
        names = ["--from-pagecache"],
        paramLabel = "<size>",
        defaultValue = "8m",
        showDefaultValue = ALWAYS,
        converter = [ByteUnitConverter::class],
        description = ["The size of the page cache to use for reading."],
    )
    private var fromPageCache = 0L

    @Option(
        names = ["--copy-schema"],
        description =
            [
                "Copy the schema instead of generating schema statements. The indexes are built by the copy."
            ],
    )
    private var copySchema = false

    @Option(
        names = ["--to-path-schema"],
        paramLabel = "<path>",
        description =
            [
                "Path to directory to create the schema commands file in. Default is the current directory."
            ],
    )
    private var toPathSchema: Path? = null

    @Option(
        names = ["--temp-path"],
        paramLabel = "<path>",
        description =
            [
                "Path to a directory to be used as a staging area when the source and target databases are the same, and for the stores of --source-format=backup and --target-format=backup. Default is next to the database, on the same file system, and for --target-format=backup --target-location, which must be local then."
            ],
    )
    private var tempPath: Path? = null

    @Option(
        names = ["--source-format"],
        paramLabel = "<format>",
        defaultValue = "database",
        showDefaultValue = ALWAYS,
        description =
            [
                "Source format: database, a stopped database, or backup, the backup --source-location names, restored first with the backups before it."
            ],
    )
    private var sourceFormat = "database"

    @Option(
        names = ["--source-location"],
        paramLabel = "<path>",
        description =
            [
                "Backup file to copy from with --source-format=backup, local or in cloud storage, as s3://<bucket>/<folder>/<file>; the backups it continues must be in its folder. Cannot be combined with --from-path-data and --from-path-txn."
            ],
    )
    private var sourceLocation: String? = null

    @Mixin private var target = BackupTargetOptions()

    @Option(
        names = ["--from-path-data"],
        paramLabel = "<path>",
        description =
            ["Path to the databases directory, containing the database directory to source from."],
    )
    private var fromPathData: Path? = null

    @Option(
        names = ["--from-path-txn"],
        paramLabel = "<path>",
        description =
            [
                "Path to the transactions directory, containing the transaction directory for the database to source from."
            ],
    )
    private var fromPathTxn: Path? = null

    @Option(
        names = ["--to-path-data"],
        paramLabel = "<path>",
        description =
            ["Path to the databases directory, containing the database directory to target."],
    )
    private var toPathData: Path? = null

    @Option(
        names = ["--to-path-txn"],
        paramLabel = "<path>",
        description =
            [
                "Path to the transactions directory containing the transaction directory for the database to target."
            ],
    )
    private var toPathTxn: Path? = null

    @Option(
        names = ["--copy-only-nodes-with-labels"],
        paramLabel = "<label>[,<label>...]",
        description =
            [
                "A comma separated list of labels. All nodes that have ANY of the specified labels will be included. Cannot be combined with --ignore-nodes-with-labels."
            ],
    )
    private var copyOnlyNodesWithLabels: String? = null

    @Option(
        names = ["--ignore-nodes-with-labels"],
        paramLabel = "<label>[,<label>...]",
        description =
            [
                "A comma separated list of labels. All nodes that have ANY of the specified labels will be ignored. Cannot be combined with --copy-only-nodes-with-labels."
            ],
    )
    private var ignoreNodesWithLabels: String? = null

    @Option(
        names = ["--copy-only-relationships-with-types"],
        paramLabel = "<type>[,<type>...]",
        description =
            [
                "A comma separated list of relationship types. All relationships with any of the specified types will be included. Cannot be combined with --ignore-relationships-with-types."
            ],
    )
    private var copyOnlyRelationshipsWithTypes: String? = null

    @Option(
        names = ["--ignore-relationships-with-types"],
        paramLabel = "<type>[,<type>...]",
        description =
            [
                "A comma separated list of relationship types. All relationships with any of the specified types will be ignored. Cannot be combined with --copy-only-relationships-with-types."
            ],
    )
    private var ignoreRelationshipsWithTypes: String? = null

    @Option(
        names = ["--skip-labels"],
        paramLabel = "<label>[,<label>...]",
        description = ["A comma separated list of labels to ignore."],
    )
    private var skipLabels: String? = null

    @Option(
        names = ["--skip-properties"],
        paramLabel = "<property>[,<property>...]",
        description =
            [
                "A comma separated list of property keys to ignore. Cannot be combined with --skip-node-properties, --copy-only-node-properties, --skip-relationship-properties or --copy-only-relationship-properties."
            ],
    )
    private var skipProperties: String? = null

    @Option(
        names = ["--skip-node-properties"],
        paramLabel = "<label.property>[,<label.property>...]",
        description =
            [
                "A comma separated list of property keys to ignore for nodes with the specified label. Cannot be combined with --skip-properties or --copy-only-node-properties."
            ],
    )
    private var skipNodeProperties: String? = null

    @Option(
        names = ["--skip-relationship-properties"],
        paramLabel = "<relationship.property>[,<relationship.property>...]",
        description =
            [
                "A comma separated list of property keys to ignore for relationships with the specified type. Cannot be combined with --skip-properties or --copy-only-relationship-properties."
            ],
    )
    private var skipRelationshipProperties: String? = null

    @Option(
        names = ["--copy-only-node-properties"],
        paramLabel = "<label.property>[,<label.property>...]",
        description =
            [
                "A comma separated list of property keys to include for nodes with the specified label. Any labels not explicitly mentioned will include all their properties. Cannot be combined with --skip-properties or --skip-node-properties."
            ],
    )
    private var copyOnlyNodeProperties: String? = null

    @Option(
        names = ["--copy-only-relationship-properties"],
        paramLabel = "<relationship.property>[,<relationship.property>...]",
        description =
            [
                "A comma separated list of property keys to include for relationships with the specified type. Any relationship types not explicitly mentioned will include all their properties. Cannot be combined with --skip-properties or --skip-relationship-properties."
            ],
    )
    private var copyOnlyRelationshipProperties: String? = null

    override fun commandConfigName(): Optional<String> = Optional.of("database-copy")

    override fun execute() {
        val filters =
            try {
                filters()
            } catch (e: IllegalArgumentException) {
                throw CommandFailedException(e.message, ExitCode.USAGE)
            }
        if ((fromPathData == null) != (fromPathTxn == null)) {
            throw CommandFailedException(
                "--from-path-data and --from-path-txn go together.",
                ExitCode.USAGE,
            )
        }
        if ((toPathData == null) != (toPathTxn == null)) {
            throw CommandFailedException(
                "--to-path-data and --to-path-txn go together.",
                ExitCode.USAGE,
            )
        }
        val fromBackup = format("--source-format", sourceFormat)
        val toBackup = target.backup()
        checkFormats(fromBackup, toBackup)
        val config = createPrefilledConfigBuilder().build()
        val partSize = if (toBackup) target.partSize(config) else 0L
        val artifacts = if (fromBackup || toBackup) target.artifacts() else null
        val from = fromDatabase.name()
        val to = layout(config, toPathData, toPathTxn, toDatabase.name())
        val staging =
            staging(toBackup, to)?.let { ctx.fs().createTempDirectory(it, ".dodeca-copy-") }
        try {
            (if (verbose) Util.configuredLogProvider(ctx.out(), true)
                else Log4jLogProvider(ctx.out(), Level.WARN))
                .use { logProvider ->
                    val source =
                        if (artifacts == null || !fromBackup)
                            layout(config, fromPathData, fromPathTxn, from)
                        else {
                            val restored =
                                Neo4jLayout.ofFlat(staging!!.resolve("source")).databaseLayout(from)
                            val how =
                                artifacts.restore(
                                    sourceLocation!!,
                                    "--source-location",
                                    from,
                                    restored,
                                    config,
                                    ctx.fs(),
                                    logProvider,
                                )
                            ctx.out().println("Restored database '$from' $how, to copy it.")
                            restored
                        }
                    val copied =
                        if (toBackup)
                            Neo4jLayout.ofFlat(staging!!.resolve("target"))
                                .databaseLayout(toDatabase.name())
                        else to
                    copy(config, filters, source, copied)
                    if (artifacts != null && toBackup) {
                        val written =
                            artifacts.write(
                                copied,
                                toDatabase.name(),
                                "a copy of database '$from'",
                                target.location!!,
                                "--target-location",
                                target.compress,
                                partSize,
                                config,
                                ctx.fs(),
                                logProvider,
                            )
                        ctx.out()
                            .println(
                                "Wrote the copy of database '$from' as a full backup of " +
                                    "'${toDatabase.name()}' to ${written.file}, up to transaction " +
                                    "${written.lastTransaction}."
                            )
                    }
                }
        } catch (e: IOException) {
            throw CommandFailedException(e.message, e)
        } catch (e: IllegalArgumentException) {
            throw CommandFailedException(e.message, e)
        } finally {
            staging?.let(ctx.fs()::deleteRecursively)
        }
    }

    private fun copy(
        config: Config,
        filters: CopyFilters,
        from: DatabaseLayout,
        to: DatabaseLayout,
    ) {
        try {
            StoreCopier(ctx.fs(), config, ctx.out(), ctx.err(), verbose)
                .copy(
                    StoreCopier.Request(
                        from = from,
                        to = to,
                        filters = filters,
                        compact = compactNodeStore,
                        toFormat = toFormat,
                        fromPageCache = fromPageCache,
                        copySchema = copySchema,
                        schemaDirectory = toPathSchema ?: Path.of("").toAbsolutePath(),
                        tempPath = tempPath,
                        force = force,
                    )
                )
        } catch (e: StoreCopyException) {
            throw CommandFailedException(e.message, e, e.exitCode)
        }
    }

    /** Whether [value] of [option] is `backup`, not `database`. */
    private fun format(option: String, value: String): Boolean =
        when (value.lowercase()) {
            "database" -> false
            "backup" -> true
            else ->
                throw CommandFailedException(
                    "Invalid $option $value: give database or backup.",
                    ExitCode.USAGE,
                )
        }

    private fun checkFormats(fromBackup: Boolean, toBackup: Boolean) {
        fun usage(message: String): Nothing = throw CommandFailedException(message, ExitCode.USAGE)
        if (fromBackup != (sourceLocation != null)) {
            usage("--source-location and --source-format=backup go together.")
        }
        if (fromBackup && (fromPathData != null || fromPathTxn != null)) {
            usage(
                "--source-format=backup cannot be combined with --from-path-data and " +
                    "--from-path-txn: the source is the backup --source-location names."
            )
        }
        if (toBackup && (toPathData != null || toPathTxn != null)) {
            usage(
                "--target-format=backup cannot be combined with --to-path-data and " +
                    "--to-path-txn: the target is the folder --target-location names."
            )
        }
    }

    /**
     * Where the stores of a backup source and a backup target are staged: --temp-path, else for a
     * backup target its local --target-location, else next to the target database [to]; null when
     * neither is a backup.
     */
    private fun staging(toBackup: Boolean, to: DatabaseLayout): Path? =
        when {
            toBackup -> target.staging(tempPath, ctx.fs())
            sourceLocation == null -> null
            tempPath != null -> target.staging(tempPath, ctx.fs())
            else -> to.neo4jLayout.databasesDirectory().also { ctx.fs().mkdirs(it) }
        }

    private fun filters(): CopyFilters {
        fun exclusive(vararg options: Pair<String, Any?>) {
            val given = options.filter { it.second != null }.map { it.first }
            if (given.size > 1) {
                throw IllegalArgumentException("${given.joinToString(" and ")} cannot be combined.")
            }
        }
        exclusive(
            "--copy-only-nodes-with-labels" to copyOnlyNodesWithLabels,
            "--ignore-nodes-with-labels" to ignoreNodesWithLabels,
        )
        exclusive(
            "--copy-only-relationships-with-types" to copyOnlyRelationshipsWithTypes,
            "--ignore-relationships-with-types" to ignoreRelationshipsWithTypes,
        )
        exclusive(
            "--skip-properties" to skipProperties,
            "--skip-node-properties" to skipNodeProperties,
        )
        exclusive(
            "--skip-properties" to skipProperties,
            "--copy-only-node-properties" to copyOnlyNodeProperties,
        )
        exclusive(
            "--skip-properties" to skipProperties,
            "--skip-relationship-properties" to skipRelationshipProperties,
        )
        exclusive(
            "--skip-properties" to skipProperties,
            "--copy-only-relationship-properties" to copyOnlyRelationshipProperties,
        )
        exclusive(
            "--skip-node-properties" to skipNodeProperties,
            "--copy-only-node-properties" to copyOnlyNodeProperties,
        )
        exclusive(
            "--skip-relationship-properties" to skipRelationshipProperties,
            "--copy-only-relationship-properties" to copyOnlyRelationshipProperties,
        )
        return CopyFilters(
            onlyNodesWithLabels = copyOnlyNodesWithLabels?.let { CopyFilters.names(it).toSet() },
            ignoreNodesWithLabels =
                ignoreNodesWithLabels?.let { CopyFilters.names(it).toSet() }.orEmpty(),
            onlyRelationshipsWithTypes =
                copyOnlyRelationshipsWithTypes?.let { CopyFilters.names(it).toSet() },
            ignoreRelationshipsWithTypes =
                ignoreRelationshipsWithTypes?.let { CopyFilters.names(it).toSet() }.orEmpty(),
            skipLabels = skipLabels?.let { CopyFilters.names(it).toSet() }.orEmpty(),
            skipProperties = skipProperties?.let { CopyFilters.names(it).toSet() }.orEmpty(),
            skipNodeProperties =
                skipNodeProperties
                    ?.let { CopyFilters.pairs(it, "--skip-node-properties") }
                    .orEmpty(),
            skipRelationshipProperties =
                skipRelationshipProperties
                    ?.let { CopyFilters.pairs(it, "--skip-relationship-properties") }
                    .orEmpty(),
            onlyNodeProperties =
                copyOnlyNodeProperties?.let {
                    CopyFilters.pairs(it, "--copy-only-node-properties")
                },
            onlyRelationshipProperties =
                copyOnlyRelationshipProperties?.let {
                    CopyFilters.pairs(it, "--copy-only-relationship-properties")
                },
        )
    }

    /** The layout of database [name], under [data] and [txn] if given, else as configured. */
    private fun layout(config: Config, data: Path?, txn: Path?, name: String): DatabaseLayout {
        if (data == null || txn == null) return Neo4jLayout.of(config).databaseLayout(name)
        return Neo4jLayout.of(
                Config.newBuilder()
                    .fromConfig(config)
                    .set(GraphDatabaseInternalSettings.databases_root_path, data.toAbsolutePath())
                    .set(GraphDatabaseSettings.transaction_logs_root_path, txn.toAbsolutePath())
                    .build()
            )
            .databaseLayout(name)
    }
}

/** Provides [CopyCommand], which Neo4j Community declares the type of and does not ship. */
class CopyCommandProvider : CommandProvider {
    override fun createCommand(ctx: ExecutionContext) = CopyCommand(ctx)

    override fun commandType() = CommandType.COPY
}
