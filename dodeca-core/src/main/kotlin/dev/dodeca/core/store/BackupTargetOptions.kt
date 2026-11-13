package dev.dodeca.core.store

import java.nio.file.Path
import org.neo4j.cli.CommandFailedException
import org.neo4j.cli.CommandOptionDescriptions.SplitArchiveOption
import org.neo4j.cli.Converters.ByteUnitConverter
import org.neo4j.cli.ExitCode
import org.neo4j.configuration.Config
import org.neo4j.dbms.archive.Dumper.SplitFileOutput
import org.neo4j.io.fs.FileSystemAbstraction
import picocli.CommandLine.Help.Visibility.ALWAYS
import picocli.CommandLine.Option

/**
 * The options with which `neo4j-admin database copy` and `import` write their store as a full
 * backup instead of a database, as Neo4j Enterprise's do: `--target-format=backup` with
 * `--target-location`, `--compress` and `--split-archive-part-size`. The store is made in a local
 * directory first, and then written with the backup module's [BackupArtifacts].
 */
class BackupTargetOptions {
    @Option(
        names = ["--target-format"],
        paramLabel = "<format>",
        defaultValue = "database",
        showDefaultValue = ALWAYS,
        description =
            [
                "Target format: database, a database's directories, or backup, a full backup of " +
                    "the database in --target-location."
            ],
    )
    private var format = "database"

    @Option(
        names = ["--target-location"],
        paramLabel = "<path>",
        description =
            [
                "Folder to write the backup to with --target-format=backup: a local one, which " +
                    "must exist, or one in cloud storage, as s3://<bucket>/<folder>/, which needs " +
                    "--temp-path."
            ],
    )
    var location: String? = null
        private set

    @Option(
        names = ["--compress"],
        arity = "0..1",
        paramLabel = "true|false",
        fallbackValue = "true",
        showDefaultValue = ALWAYS,
        description = ["Request backup artifact to be compressed, with --target-format=backup."],
    )
    var compress = false
        private set

    @Option(
        names = [SplitArchiveOption.OPTION_NAME],
        paramLabel = "<splitsize>",
        converter = [ByteUnitConverter::class],
        description =
            [
                SplitArchiveOption.BACKUP_DESCRIPTION +
                    " Only with --target-format=backup, for which server.split_archive.part_size " +
                    "gives the default."
            ],
    )
    private var splitArchivePartSize = 0L

    /** Whether the target is a backup; checks that the options given go together. */
    fun backup(): Boolean {
        val backup =
            when (format.lowercase()) {
                "database" -> false
                "backup" -> true
                else -> usage("Invalid --target-format $format: give database or backup.")
            }
        if (backup != (location != null)) {
            usage("--target-location and --target-format=backup go together.")
        }
        if (!backup && compress) usage("--compress goes with --target-format=backup.")
        if (!backup && splitArchivePartSize != 0L) {
            usage("${SplitArchiveOption.OPTION_NAME} goes with --target-format=backup.")
        }
        return backup
    }

    /** The size of the backup's parts, 0 for none, as the option or else [config] gives it. */
    fun partSize(config: Config): Long =
        try {
            SplitFileOutput.determineSplitArtifactSize(config, splitArchivePartSize)
        } catch (e: IllegalArgumentException) {
            throw CommandFailedException(
                "Invalid split archive part size, as --split-archive-part-size or " +
                    "server.split_archive.part_size give it: ${e.message}",
                e,
            )
        }

    /** The backup module's [BackupArtifacts], which reading and writing backups needs. */
    fun artifacts(): BackupArtifacts =
        BackupArtifacts.load()
            ?: throw CommandFailedException(
                "Reading and writing backups needs Dodeca's backup module, which is not on the " +
                    "classpath."
            )

    /**
     * The local directory the store is made in before it is written as a backup: [tempPath] if
     * given, else --target-location, which must be local then.
     */
    fun staging(tempPath: Path?, fs: FileSystemAbstraction): Path {
        if (tempPath != null) {
            if (!fs.isDirectory(tempPath)) {
                throw CommandFailedException("--temp-path $tempPath is not an existing directory.")
            }
            return tempPath
        }
        val location = checkNotNull(location)
        if (location.contains("://")) {
            throw CommandFailedException(
                "--target-location $location is in cloud storage: give --temp-path, a local " +
                    "directory for the store before it is uploaded."
            )
        }
        val local = Path.of(location)
        if (!fs.isDirectory(local)) {
            throw CommandFailedException(
                "--target-location $location is not an existing directory."
            )
        }
        return local
    }

    private fun usage(message: String): Nothing =
        throw CommandFailedException(message, ExitCode.USAGE)
}
