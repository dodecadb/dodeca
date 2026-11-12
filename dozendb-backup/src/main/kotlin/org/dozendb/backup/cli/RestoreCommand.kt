package org.dozendb.backup.cli

import java.io.IOException
import java.nio.file.Path
import java.time.Instant
import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter
import java.util.Optional
import org.dozendb.backup.ArtifactChain
import org.dozendb.backup.ArtifactChains
import org.dozendb.backup.StorePreparer
import org.dozendb.core.DozenDbSettings
import org.dozendb.core.store.RestorePoint
import org.dozendb.core.store.StorePlacement
import org.neo4j.cli.AbstractAdminCommand
import org.neo4j.cli.CommandFailedException
import org.neo4j.cli.Converters.DatabaseNameConverter
import org.neo4j.cli.ExecutionContext
import org.neo4j.cloud.storage.SchemeFileSystemAbstraction
import org.neo4j.commandline.dbms.LockChecker
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.dbms.identity.AbstractIdentityModule
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.io.layout.Neo4jLayout
import org.neo4j.io.locker.FileLockException
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.logging.InternalLogProvider
import org.neo4j.storageengine.api.StorageEngineFactory
import picocli.CommandLine.Command
import picocli.CommandLine.Help.Visibility.ALWAYS
import picocli.CommandLine.Option
import picocli.CommandLine.Parameters

@Command(
    name = "restore",
    header = ["Restore a backed up database."],
    description =
        [
            "Restores databases from backups: a full backup, or a full backup and the " +
                "differential backups that continue it. The database must not be running. With " +
                "clustering enabled, the database is prepared on this server, and CREATE DATABASE " +
                "creates it from there, as the command prints."
        ],
)
class RestoreCommand(context: ExecutionContext) : AbstractAdminCommand(context) {
    @Parameters(
        arity = "0..1",
        paramLabel = "<database>",
        converter = [DatabaseNameConverter::class],
        description =
            [
                "Name of the database after restore. Usage of this parameter is only allowed if " +
                    "the --from-path option points to a path to a single artifact."
            ],
    )
    private var database: NormalizedDatabaseName? = null

    @Option(
        names = ["--from-path"],
        required = true,
        paramLabel = "<path>[,<path>...]",
        description =
            [
                "A single path or a comma-separated list of paths pointing to a backup artifact " +
                    "file or a folder of them, local or in cloud storage, as " +
                    "s3://<bucket>/<folder>/, gs://<bucket>/<folder>/ or azb://<storage " +
                    "account>/<container>/<folder>/. A folder restores the latest chain of each " +
                    "database in it. A file restores the chain ending at it, found in its folder."
            ],
    )
    private lateinit var fromPath: String

    private lateinit var fromPaths: List<Path>
    private lateinit var fs: FileSystemAbstraction

    @Option(
        names = ["--source-database"],
        paramLabel = "<source-database-name>",
        converter = [DatabaseNameConverter::class],
        description =
            [
                "Restore only this database's backups: the latest chain of its backups in each " +
                    "folder, and only files that are its backups. Neo4j Enterprise requires it " +
                    "when a folder holds backups of several databases; without it, the latest " +
                    "chain of each database in the folder is restored."
            ],
    )
    private var sourceDatabase: NormalizedDatabaseName? = null

    @Option(
        names = ["--restore-until"],
        paramLabel = "<recovery-criteria>",
        description =
            [
                "Restore up to, but not including, a transaction id, as --restore-until=123, or " +
                    "the transactions committed before a time in UTC, as " +
                    "--restore-until=\"2021-09-11 10:15:30\"."
            ],
    )
    private var restoreUntil: String? = null

    @Option(
        names = ["--overwrite-destination"],
        arity = "0..1",
        paramLabel = "true|false",
        fallbackValue = "true",
        showDefaultValue = ALWAYS,
        description =
            [
                "If an existing database should be replaced. Not supported with clustering " +
                    "enabled: restore to a new database there."
            ],
    )
    private var overwrite = false

    @Option(
        names = ["--to-path-data"],
        paramLabel = "<path>",
        description =
            [
                "Base directory for databases. Usage of this option is only allowed if the " +
                    "--from-path parameter points to exactly one path."
            ],
    )
    private var toPathData: Path? = null

    @Option(
        names = ["--to-path-txn"],
        paramLabel = "<path>",
        description =
            [
                "Base directory for transaction logs. Usage of this option is only allowed if " +
                    "the --from-path parameter points to exactly one path."
            ],
    )
    private var toPathTxn: Path? = null

    @Option(
        names = ["--temp-path"],
        paramLabel = "<path>",
        description =
            [
                "Accepted as by Neo4j Enterprise, and not used: the database is prepared next " +
                    "to its final directories, and backups in cloud storage are read as they are " +
                    "restored."
            ],
    )
    private var tempPath: Path? = null

    override fun commandConfigName(): Optional<String> = Optional.of("database-restore")

    override fun execute() {
        val until = restoreUntil?.let {
            try {
                RestorePoint.parse(it)
            } catch (e: IllegalArgumentException) {
                throw CommandFailedException("Invalid --restore-until: ${e.message}", e)
            }
        }
        val config = config()
        commandLogProvider(ctx.out(), verbose).use { logProvider ->
            SchemeFileSystemAbstraction(ctx.fs(), config, logProvider).use { fs ->
                this.fs = fs
                fromPaths =
                    fromPath
                        .split(',')
                        .filter { it.isNotBlank() }
                        .map { CommandPaths.resolve(fs, it, "--from-path") }
                restoreAll(until, config, logProvider)
            }
        }
    }

    private fun restoreAll(until: RestorePoint?, config: Config, logProvider: InternalLogProvider) {
        val singleFile = fromPaths.size == 1 && !fs.isDirectory(fromPaths[0])
        if (database != null && !singleFile) {
            throw CommandFailedException(
                "A database name can be given only when --from-path names a single backup file"
            )
        }
        if ((toPathData != null || toPathTxn != null) && fromPaths.size != 1) {
            throw CommandFailedException(
                "--to-path-data and --to-path-txn can be given only with a single --from-path"
            )
        }
        val clustered = config.get(DozenDbSettings.clusterEnabled)
        if (clustered && overwrite) {
            throw CommandFailedException(
                "--overwrite-destination is not supported with clustering enabled: restore to a " +
                    "new database, or drop the database first"
            )
        }
        val restores = restores(until)
        val failures = mutableListOf<Pair<String, Exception>>()
        AdminResources(fs, config, logProvider).use { resources ->
            for ((name, chain) in restores) {
                try {
                    if (clustered) {
                        stage(name, chain, until, config, resources.preparer())
                    } else {
                        restore(name, chain, until, config, resources.preparer())
                    }
                } catch (e: Exception) {
                    ctx.err().println("Failed to restore database '$name': ${e.message}")
                    failures += name to e
                }
            }
        }
        if (failures.isNotEmpty()) {
            throw CommandFailedException(
                "Restore failed for databases: ${failures.joinToString { "'${it.first}'" }}",
                failures.first().second,
            )
        }
    }

    private fun config(): Config {
        val builder = createPrefilledConfigBuilder()
        toPathData?.let {
            builder.set(GraphDatabaseInternalSettings.databases_root_path, it.toAbsolutePath())
        }
        toPathTxn?.let {
            builder.set(GraphDatabaseSettings.transaction_logs_root_path, it.toAbsolutePath())
        }
        return builder.build()
    }

    /** The database each chain restores, by its name after the restore. */
    private fun restores(until: RestorePoint?): List<Pair<String, ArtifactChain>> {
        val restores = mutableListOf<Pair<String, ArtifactChain>>()
        for (path in fromPaths) {
            val chains =
                try {
                    if (fs.isDirectory(path)) {
                        find(listOf(path))
                            .filter { sourceDatabase == null || isSource(it.header.databaseName) }
                            .groupBy { it.header.databaseName }
                            .values
                            .map {
                                ArtifactChains.select(it, until)
                            }
                    } else {
                        val end = ArtifactChains.find(listOf(path), fs).artifacts.single()
                        val source = sourceDatabase
                        if (source != null && !isSource(end.header.databaseName)) {
                            throw CommandFailedException(
                                "${CommandPaths.display(path)} is a backup of database " +
                                    "'${end.header.databaseName}', not of '${source.name()}' as " +
                                    "--source-database names"
                            )
                        }
                        val candidates =
                            find(listOfNotNull(path.toAbsolutePath().parent)).filter {
                                it.header.databaseName == end.header.databaseName
                            }
                        listOf(ArtifactChains.select(candidates + end, until, end))
                    }
                } catch (e: IOException) {
                    throw CommandFailedException("${e.message}", e)
                }
            for (chain in chains) {
                val name = database?.name() ?: chain.full.header.databaseName
                if (restores.any { it.first == name }) {
                    throw CommandFailedException("Several backups of database '$name' were given")
                }
                restores += name to chain
            }
        }
        if (restores.isEmpty()) {
            val of = sourceDatabase?.let { " of database '${it.name()}'" } ?: ""
            throw CommandFailedException(
                "No backups$of found in ${fromPaths.joinToString { CommandPaths.display(it) }}"
            )
        }
        return restores
    }

    private fun isSource(databaseName: String) =
        NormalizedDatabaseName(databaseName).name() == sourceDatabase?.name()

    private fun find(paths: List<Path>) =
        ArtifactChains.find(paths, fs)
            .also { found ->
                found.unreadable.forEach { (path, why) ->
                    ctx.err().println("Skipping ${CommandPaths.display(path)}: $why")
                }
            }
            .artifacts

    private fun restore(
        name: String,
        chain: ArtifactChain,
        until: RestorePoint?,
        config: Config,
        preparer: StorePreparer,
    ) {
        val layout = Neo4jLayout.of(config)
        val target = layout.databaseLayout(name)
        if (StorageEngineFactory.selectStorageEngine(fs, target).isPresent) {
            if (!overwrite) {
                throw IOException(
                    "database '$name' exists at ${target.databaseDirectory()}: stop it and " +
                        "restore with --overwrite-destination=true to replace it"
                )
            }
            try {
                LockChecker.checkDatabaseLock(target).close()
            } catch (e: FileLockException) {
                throw IOException("the database is in use: stop database '$name' and try again", e)
            }
        }
        val placement = StorePlacement(fs, layout)
        val staged = placement.stage(name, "restore")
        try {
            val prepared = preparer.prepare(chain, until, staged, detach = true)
            placement.moveIntoPlace(staged, name)
            ctx.out().println("Restored database '$name' ${describe(chain, prepared)}.")
        } finally {
            placement.unstage(name, "restore")
        }
        ctx.out()
            .println(
                if (name == NamedDatabaseId.SYSTEM_DATABASE_NAME) "Start the server to use it."
                else
                    "To use it, run CREATE DATABASE $name, or START DATABASE $name if it " +
                        "exists, on the system database."
            )
        writeMetadata(name, chain, config)
    }

    /**
     * Writes the latest users, roles and privileges of [chain] to `restore_metadata.cypher`, as
     * Enterprise's restore does, and tells how to run it once the database exists.
     */
    private fun writeMetadata(name: String, chain: ArtifactChain, config: Config) {
        if (name == NamedDatabaseId.SYSTEM_DATABASE_NAME) return
        val script = chain.metadataScript ?: return
        val file =
            config
                .get(GraphDatabaseSettings.data_directory)
                .resolve("scripts")
                .resolve(name)
                .resolve(METADATA_SCRIPT)
        fs.mkdirs(file.parent)
        fs.openAsOutputStream(file, false).use { it.write(script.toByteArray(Charsets.UTF_8)) }
        ctx.out()
            .println(
                "Wrote the users, roles and privileges of database '$name' to $file. Once the " +
                    "database exists, run it on the system database to give them back:\n  cat " +
                    "$file | bin/cypher-shell -d system --param \"database => '$name'\""
            )
    }

    /** Prepares the database on this clustered server, where CREATE DATABASE finds it. */
    private fun stage(
        name: String,
        chain: ArtifactChain,
        until: RestorePoint?,
        config: Config,
        preparer: StorePreparer,
    ) {
        if (name == NamedDatabaseId.SYSTEM_DATABASE_NAME) {
            throw IOException(
                "restore the system database with clustering disabled, then form a new cluster " +
                    "from this server"
            )
        }
        val layout = Neo4jLayout.of(config)
        val placement = StorePlacement(fs, layout)
        val staged = placement.stage(name, StorePlacement.RESTORED)
        val prepared =
            try {
                preparer.prepare(chain, until, staged, detach = true)
            } catch (e: Exception) {
                placement.unstage(name, StorePlacement.RESTORED)
                throw e
            }
        ctx.out().println("Prepared database '$name' on this server ${describe(chain, prepared)}.")
        writeMetadata(name, chain, config)
        val storage = AbstractIdentityModule.createServerIdStorage(fs, layout.serverIdFile())
        if (storage.exists()) {
            ctx.out()
                .println(
                    "To create it, run on the system database:\n  CREATE DATABASE $name OPTIONS " +
                        "{existingData: 'use', existingDataSeedServer: " +
                        "'${storage.readState().uuid()}'}"
                )
        } else {
            ctx.out()
                .println(
                    "This server has no id yet. Start it, then create the database with " +
                        "CREATE DATABASE $name OPTIONS {existingData: 'use', " +
                        "existingDataSeedServer: '<id>'}, where <id> is its memberId in CALL " +
                        "dozendb.cluster.members()."
                )
        }
    }

    private fun describe(chain: ArtifactChain, prepared: StorePreparer.Prepared): String {
        val last = prepared.lastRestored
        val differentials =
            when (val count = chain.differentials.size) {
                0 -> ""
                1 -> " and 1 differential backup"
                else -> " and $count differential backups"
            }
        val committed = TIME.format(Instant.ofEpochMilli(last.commitTimestamp()).atOffset(UTC))
        return "from ${CommandPaths.display(chain.full.path)}$differentials, up to transaction " +
            "${last.id()}, committed " +
            "at $committed UTC"
    }

    private companion object {
        val TIME: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        const val METADATA_SCRIPT = "restore_metadata.cypher"
    }
}
