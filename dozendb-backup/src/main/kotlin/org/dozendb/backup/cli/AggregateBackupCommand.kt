package org.dozendb.backup.cli

import java.io.IOException
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.time.Instant
import java.util.Optional
import org.dozendb.backup.ArtifactChain
import org.dozendb.backup.ArtifactChains
import org.dozendb.backup.ArtifactHeader
import org.dozendb.backup.Artifacts
import org.neo4j.cli.AbstractAdminCommand
import org.neo4j.cli.CommandFailedException
import org.neo4j.cli.Converters.DatabaseNamePatternConverter
import org.neo4j.cli.ExecutionContext
import org.neo4j.cloud.storage.SchemeFileSystemAbstraction
import org.neo4j.configuration.helpers.DatabaseNamePattern
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.io.layout.Neo4jLayout
import picocli.CommandLine.Command
import picocli.CommandLine.Help.Visibility.ALWAYS
import picocli.CommandLine.Option
import picocli.CommandLine.Parameters

/** `neo4j-admin backup aggregate`, Neo4j Enterprise's name for it since 2025.01. */
@Command(
    name = "aggregate",
    header = ["Aggregates a chain of backup artifacts into a single artifact."],
    description = [AggregateCommand.DESCRIPTION],
)
class BackupAggregateCommand(context: ExecutionContext) : AggregateCommand(context) {
    override fun commandConfigName(): Optional<String> = Optional.of("backup-aggregate")
}

/** `neo4j-admin database aggregate-backup`, deprecated in Neo4j Enterprise 2025.01. */
@Command(
    name = "aggregate-backup",
    header =
        [
            "Deprecated: use neo4j-admin backup aggregate. Aggregates a chain of backup artifacts " +
                "into a single artifact."
        ],
    description = [AggregateCommand.DESCRIPTION],
)
class AggregateBackupCommand(context: ExecutionContext) : AggregateCommand(context) {
    override fun commandConfigName(): Optional<String> = Optional.of("database-aggregate-backup")
}

/** Aggregates the latest chain of backups into one full backup, by either name of the command. */
abstract class AggregateCommand(context: ExecutionContext) : AbstractAdminCommand(context) {
    @Parameters(
        arity = "0..1",
        paramLabel = "<database>",
        converter = [DatabaseNamePatternConverter::class],
        description =
            [
                "Name of the database for which to aggregate the artifacts. Can contain * and ? " +
                    "for globbing."
            ],
    )
    private var database: DatabaseNamePattern? = null

    @Option(
        names = ["--from-path"],
        required = true,
        paramLabel = "<path>",
        description =
            [
                "Accepts either a path to a single artifact file or a folder containing backup " +
                    "artifacts, local or in cloud storage, as s3://<bucket>/<folder>/, " +
                    "gs://<bucket>/<folder>/ or azb://<storage account>/<container>/<folder>/, " +
                    "which needs --temp-path. When a file is supplied, the <database> parameter " +
                    "should be omitted."
            ],
    )
    private lateinit var fromPathOption: String

    private lateinit var fromPath: Path
    private lateinit var fs: FileSystemAbstraction

    @Option(
        names = ["--keep-old-backup"],
        arity = "0..1",
        paramLabel = "true|false",
        fallbackValue = "true",
        showDefaultValue = ALWAYS,
        description = ["If set to true, the old backup chain is not removed."],
    )
    private var keepOld = false

    @Option(
        names = ["--temp-path"],
        paramLabel = "<path>",
        description =
            [
                "Provide a path to a local temporary empty directory for storing backup files " +
                    "until the command is completed. The files will be deleted once the command is " +
                    "finished. The default is the backups' folder, which must be local then."
            ],
    )
    private var tempPath: Path? = null

    @Option(
        names = ["--parallel-recovery"],
        arity = "0..1",
        paramLabel = "true|false",
        fallbackValue = "true",
        showDefaultValue = ALWAYS,
        description =
            [
                "Accepted as by Neo4j Enterprise, where it is experimental. DozenDB recovers the " +
                    "aggregated backup as it always does."
            ],
    )
    private var parallelRecovery = false

    override fun execute() {
        val config = createPrefilledConfigBuilder().build()
        commandLogProvider(ctx.out(), verbose).use { logProvider ->
            SchemeFileSystemAbstraction(ctx.fs(), config, logProvider).use { fs ->
                this.fs = fs
                fromPath = CommandPaths.resolve(fs, fromPathOption, "--from-path")
                if (CommandPaths.isCloud(fromPath) && tempPath == null) {
                    throw CommandFailedException(
                        "--from-path ${CommandPaths.display(fromPath)} is in cloud storage: give " +
                            "--temp-path, a local directory for the aggregated database before " +
                            "it is uploaded"
                    )
                }
                AdminResources(fs, config, logProvider).use { resources ->
                    for ((chain, directory) in chains()) aggregate(chain, directory, resources)
                }
            }
        }
    }

    /** The chains to aggregate, and the folder each is in. */
    private fun chains(): List<Pair<ArtifactChain, Path>> {
        val directory = if (fs.isDirectory(fromPath)) fromPath else fromPath.toAbsolutePath().parent
        val chains =
            try {
                val found = ArtifactChains.find(listOf(directory), fs)
                found.unreadable.forEach { (file, why) ->
                    ctx.err().println("Skipping ${CommandPaths.display(file)}: $why")
                }
                if (fs.isDirectory(fromPath)) {
                    val pattern =
                        database
                            ?: throw CommandFailedException(
                                "Name the database to aggregate the backups of, or a pattern " +
                                    "such as \"*\""
                            )
                    val byName =
                        found.artifacts
                            .groupBy { it.header.databaseName }
                            .filterKeys(pattern::matches)
                    if (byName.isEmpty()) {
                        throw CommandFailedException(
                            "No backups of '${pattern.databaseName}' in " +
                                CommandPaths.display(fromPath)
                        )
                    }
                    byName.values.map { ArtifactChains.select(it, null) }
                } else {
                    if (database != null) {
                        throw CommandFailedException(
                            "Omit the database name when --from-path names a backup file"
                        )
                    }
                    val end = ArtifactChains.find(listOf(fromPath), fs).artifacts.single()
                    val candidates =
                        found.artifacts.filter {
                            it.header.databaseName == end.header.databaseName
                        }
                    listOf(ArtifactChains.select(candidates + end, null, end))
                }
            } catch (e: IOException) {
                throw CommandFailedException("${e.message}", e)
            }
        return chains.map { it to directory }
    }

    private fun aggregate(chain: ArtifactChain, directory: Path, resources: AdminResources) {
        val name = chain.full.header.databaseName
        if (chain.differentials.isEmpty()) {
            ctx.out()
                .println(
                    "The latest backup of '$name' is a full backup already: " +
                        CommandPaths.display(chain.full.path)
                )
            return
        }
        val temporary = fs.createTempDirectory(tempPath ?: directory, ".dozendb-aggregate-")
        try {
            val staged = Neo4jLayout.ofFlat(temporary).databaseLayout(name)
            val prepared = resources.preparer().prepare(chain, null, staged, detach = false)
            val last = prepared.lastTransaction
            val expected = chain.last.header
            if (
                last.appendIndex() != expected.highestAppendIndex ||
                    last.checksum() != expected.lastChecksum
            ) {
                throw IOException(
                    "the aggregated store ends at transaction ${last.appendIndex()} with checksum " +
                        "${last.checksum()}, not where ${chain.last} ends"
                )
            }
            val header =
                ArtifactHeader(
                    databaseName = name,
                    databaseId = chain.full.header.databaseId,
                    storeId = chain.full.header.storeId,
                    time = expected.time,
                    full = true,
                    compressed = chain.full.header.compressed,
                    lowestAppendIndex = 1,
                    highestAppendIndex = last.appendIndex(),
                    previousChecksum = null,
                    lastChecksum = last.checksum(),
                    lastCommitTime = Instant.ofEpochMilli(last.commitTimestamp()),
                    source = expected.source,
                    metadataScript = chain.metadataScript,
                )
            // In place under a new name before the chain goes, so that a crash loses neither.
            var aggregate =
                Artifacts.writeNew(directory, name, header.time, fs) {
                    Artifacts.writeFull(it, header, staged, fs)
                }
            if (!keepOld) {
                chain.artifacts.forEach { fs.deleteFile(it.path) }
                // The name the chain's last backup most likely had.
                val first = directory.resolve(Artifacts.fileName(name, header.time))
                if (!fs.fileExists(first)) {
                    fs.renameFile(aggregate, first, StandardCopyOption.ATOMIC_MOVE)
                    aggregate = first
                }
            }
            ctx.out()
                .println(
                    "Aggregated the chain of ${chain.artifacts.size} backups of '$name' into " +
                        CommandPaths.display(aggregate) +
                        "${if (keepOld) "" else "; the chain was removed"}."
                )
        } finally {
            fs.deleteRecursively(temporary)
        }
    }

    internal companion object {
        const val DESCRIPTION =
            "Turns the latest chain of a full backup and the differential backups after it into " +
                "one full backup, which the backups after the chain still continue."
    }
}
