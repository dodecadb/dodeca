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
import org.neo4j.configuration.helpers.DatabaseNamePattern
import org.neo4j.io.layout.Neo4jLayout
import picocli.CommandLine.Command
import picocli.CommandLine.Help.Visibility.ALWAYS
import picocli.CommandLine.Option
import picocli.CommandLine.Parameters

@Command(
    name = "aggregate-backup",
    header = ["Aggregates a chain of backup artifacts into a single artifact."],
    description =
        [
            "Turns the latest chain of a full backup and the differential backups after it into " +
                "one full backup, which the backups after the chain still continue."
        ],
)
class AggregateBackupCommand(context: ExecutionContext) : AbstractAdminCommand(context) {
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
                    "artifacts. When a file is supplied, the <database> parameter should be omitted."
            ],
    )
    private lateinit var fromPath: Path

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
                "Provide a path to a temporary empty directory for storing backup files until the " +
                    "command is completed. The files will be deleted once the command is finished."
            ],
    )
    private var tempPath: Path? = null

    override fun commandConfigName(): Optional<String> = Optional.of("database-aggregate-backup")

    override fun execute() {
        val fs = ctx.fs()
        val directory = if (fs.isDirectory(fromPath)) fromPath else fromPath.toAbsolutePath().parent
        val chains =
            try {
                val found = ArtifactChains.find(listOf(directory), fs)
                found.unreadable.forEach { (file, why) ->
                    ctx.err().println("Skipping $file: $why")
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
                            "No backups of '${pattern.databaseName}' in $fromPath"
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
        val config = createPrefilledConfigBuilder().build()
        commandLogProvider(ctx.out(), verbose).use { logProvider ->
            AdminResources(fs, config, logProvider).use { resources ->
                for (chain in chains) aggregate(chain, directory, resources)
            }
        }
    }

    private fun aggregate(chain: ArtifactChain, directory: Path, resources: AdminResources) {
        val fs = ctx.fs()
        val name = chain.full.header.databaseName
        if (chain.differentials.isEmpty()) {
            ctx.out()
                .println(
                    "The latest backup of '$name' is a full backup already: ${chain.full.path}"
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
                        "$aggregate${if (keepOld) "" else "; the chain was removed"}."
                )
        } finally {
            fs.deleteRecursively(temporary)
        }
    }
}
