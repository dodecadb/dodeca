package org.dozendb.backup.cli

import java.io.IOException
import java.nio.file.Path
import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter
import java.util.Optional
import org.dozendb.backup.Artifact
import org.dozendb.backup.ArtifactChains
import org.neo4j.cli.AbstractAdminCommand
import org.neo4j.cli.CommandFailedException
import org.neo4j.cli.Converters.DatabaseNameConverter
import org.neo4j.cli.ExecutionContext
import org.neo4j.cloud.storage.SchemeFileSystemAbstraction
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.kernel.database.NormalizedDatabaseName
import picocli.CommandLine.Command
import picocli.CommandLine.Help.Visibility.ALWAYS
import picocli.CommandLine.Option
import picocli.CommandLine.Parameters

@Command(
    name = "inspect",
    header = ["Command to read the backup metadata."],
    description = ["Lists backups and, with --show-metadata, what their headers tell."],
)
class InspectBackupCommand(context: ExecutionContext) : AbstractAdminCommand(context) {
    enum class Format {
        JSON,
        TABULAR,
    }

    @Parameters(
        index = "0",
        paramLabel = "<backup-path>",
        description =
            [
                "Path denoting either a directory where backups are stored or a single backup to " +
                    "inspect, local or in cloud storage, as s3://<bucket>/<folder>/, " +
                    "gs://<bucket>/<folder>/ or azb://<storage account>/<container>/<folder>/."
            ],
    )
    private lateinit var pathOption: String

    private lateinit var path: Path
    private lateinit var fs: FileSystemAbstraction

    @Option(
        names = ["--latest-backup"],
        showDefaultValue = ALWAYS,
        description = ["Show only the latest backup."],
    )
    private var latestBackup = false

    @Option(
        names = ["--latest-chain"],
        showDefaultValue = ALWAYS,
        description = ["List the full backup chain ending with the latest downloaded backup."],
    )
    private var latestChain = false

    @Option(
        names = ["--show-metadata"],
        showDefaultValue = ALWAYS,
        description = ["Show the backup metadata."],
    )
    private var showMetadata = false

    @Option(
        names = ["--database"],
        paramLabel = "<database>",
        converter = [DatabaseNameConverter::class],
        description = ["Name of the database to inspect."],
    )
    private var database: NormalizedDatabaseName? = null

    @Option(
        names = ["--format"],
        paramLabel = "<value>",
        showDefaultValue = ALWAYS,
        description =
            ["Format of the output of the command. Possible values are: 'JSON, TABULAR'."],
    )
    private var format = Format.TABULAR

    @Option(
        names = ["--empty"],
        showDefaultValue = ALWAYS,
        description = ["Include empty backups."],
    )
    private var empty = false

    override fun commandConfigName(): Optional<String> = Optional.of("backup-inspect")

    override fun execute() {
        val config = createPrefilledConfigBuilder().build()
        commandLogProvider(ctx.out(), verbose).use { logProvider ->
            SchemeFileSystemAbstraction(ctx.fs(), config, logProvider).use { fs ->
                this.fs = fs
                path = CommandPaths.resolve(fs, pathOption, "<backup-path>")
                inspect()
            }
        }
    }

    private fun inspect() {
        val found =
            try {
                ArtifactChains.find(listOf(path), fs)
            } catch (e: IOException) {
                throw CommandFailedException("${e.message}", e)
            }
        found.unreadable.forEach { (file, why) ->
            ctx.err().println("Skipping ${CommandPaths.display(file)}: $why")
        }
        val name = database?.name()
        val named = found.artifacts.filter { name == null || it.header.databaseName == name }
        val rows =
            when {
                latestChain -> chain(named)
                else -> {
                    val shown = named.filter { empty || !it.header.empty }
                    val latest =
                        if (latestBackup) {
                            shown
                                .groupBy { it.header.databaseName }
                                .values
                                .map { it.maxWith(LATEST) }
                        } else {
                            shown
                        }
                    latest.sortedWith(
                        compareByDescending<Artifact> { it.header.databaseName }
                            .then(LATEST.reversed())
                    )
                }
            }
        when (format) {
            Format.TABULAR -> printTable(rows)
            Format.JSON -> printJson(rows)
        }
    }

    /**
     * The chain ending with the backup named, or with the latest backup of the one database, latest
     * first. A backup named alone continues the backups in its folder.
     */
    private fun chain(artifacts: List<Artifact>): List<Artifact> {
        val names = artifacts.map { it.header.databaseName }.toSet()
        if (names.size > 1) {
            throw CommandFailedException(
                "${CommandPaths.display(path)} holds backups of several databases: name one with " +
                    "--database"
            )
        }
        return try {
            if (fs.isDirectory(path)) {
                val holding = artifacts.filterNot { it.header.empty }
                val end = holding.maxWithOrNull(LATEST) ?: return emptyList()
                ArtifactChains.select(holding, null, end).artifacts.reversed()
            } else {
                val end = artifacts.singleOrNull() ?: return emptyList()
                val folder = ArtifactChains.find(listOfNotNull(path.toAbsolutePath().parent), fs)
                val candidates =
                    folder.artifacts.filter {
                        it.header.databaseName == end.header.databaseName && !it.header.empty
                    }
                ArtifactChains.select(candidates + end, null, end).artifacts.reversed()
            }
        } catch (e: IOException) {
            throw CommandFailedException("${e.message}", e)
        }
    }

    private fun columns(artifact: Artifact): List<String> {
        val header = artifact.header
        val file = artifact.path.toAbsolutePath().toUri().toString()
        if (!showMetadata) return listOf(file)
        return listOf(
            file,
            header.databaseName,
            header.databaseId.uuid().toString(),
            TIME.format(header.time.atOffset(UTC)),
            header.full.toString(),
            header.compressed.toString(),
            header.lowestAppendIndex.toString(),
            header.highestAppendIndex.toString(),
        )
    }

    private fun printTable(rows: List<Artifact>) {
        val titles = if (showMetadata) TITLES else TITLES.take(1)
        val lines = listOf(titles) + rows.map(::columns)
        val widths = titles.indices.map { column -> lines.maxOf { it[column].length } }
        for (line in lines) {
            ctx.out()
                .println(
                    line
                        .mapIndexed { column, cell -> cell.padStart(widths[column]) }
                        .joinToString(" | ", "| ", " |")
                )
        }
    }

    private fun printJson(rows: List<Artifact>) {
        val objects = rows.map { artifact ->
            val values = columns(artifact)
            val header = artifact.header
            val fields =
                if (!showMetadata) {
                    listOf("file" to json(values[0]))
                } else {
                    listOf(
                        "file" to json(values[0]),
                        "database" to json(header.databaseName),
                        "databaseId" to json(values[2]),
                        "time" to json(values[3]),
                        "full" to values[4],
                        "compressed" to values[5],
                        "lowestTx" to values[6],
                        "highestTx" to values[7],
                    )
                }
            fields.joinToString(",", "{", "}") { (key, value) -> "\"$key\":$value" }
        }
        ctx.out().println(objects.joinToString(",", "[", "]"))
    }

    private fun json(text: String): String = buildString {
        append('"')
        for (c in text) {
            when {
                c == '"' || c == '\\' -> append('\\').append(c)
                c < ' ' -> append("\\u%04x".format(c.code))
                else -> append(c)
            }
        }
        append('"')
    }

    companion object {
        /**
         * Lists the backups at [path], a folder or a file, with their metadata, as `backup inspect
         * --show-metadata` does: for `backup --inspect-path`.
         */
        internal fun run(context: ExecutionContext, path: String, verbose: Boolean) {
            val command = InspectBackupCommand(context)
            command.pathOption = path
            command.showMetadata = true
            command.verbose = verbose
            command.execute()
        }

        private val TITLES =
            listOf(
                "FILE",
                "DATABASE",
                "DATABASE ID",
                "TIME (UTC)",
                "FULL",
                "COMPRESSED",
                "LOWEST TX",
                "HIGHEST TX",
            )
        private val TIME: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
        private val LATEST: Comparator<Artifact> =
            compareBy({ it.header.time }, { it.header.highestAppendIndex })
    }
}
