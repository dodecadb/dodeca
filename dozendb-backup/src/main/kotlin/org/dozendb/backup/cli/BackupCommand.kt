package org.dozendb.backup.cli

import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter
import java.util.Optional
import java.util.UUID
import org.dozendb.backup.Artifact
import org.dozendb.backup.ArtifactChains
import org.dozendb.backup.ArtifactHeader
import org.dozendb.backup.Artifacts
import org.dozendb.backup.BackupProtocol.Refused
import org.dozendb.backup.BackupProtocol.Status
import org.dozendb.backup.BackupSettings
import org.dozendb.backup.BackupTls
import org.dozendb.backup.client.BackupClient
import org.dozendb.consensus.HostPort
import org.dozendb.core.security.MetadataScript
import org.neo4j.cli.AbstractAdminCommand
import org.neo4j.cli.CommandFailedException
import org.neo4j.cli.Converters.DatabaseNamePatternConverter
import org.neo4j.cli.ExecutionContext
import org.neo4j.cloud.storage.SchemeFileSystemAbstraction
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.helpers.DatabaseNamePattern
import org.neo4j.io.ByteUnit
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.io.layout.Neo4jLayout
import org.neo4j.io.pagecache.context.CursorContext
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.logging.InternalLogProvider
import org.neo4j.ssl.config.DefaultSslPolicyProvider
import org.neo4j.storageengine.api.StorageEngineFactory
import org.neo4j.storageengine.api.StoreId
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.Help.Visibility.ALWAYS
import picocli.CommandLine.Option
import picocli.CommandLine.Parameters

@Command(
    name = "backup",
    header = ["Perform an online backup from a running DozenDB server."],
    description =
        [
            "Backs up databases of a running server, clustered or not, into backup artifacts: a " +
                "full backup holds a copy of the database, a differential backup the transactions " +
                "after the latest backup of the database in --to-path. The server serves backups " +
                "on server.backup.listen_address, over TLS with the dbms.ssl.policy.backup policy, " +
                "which this command uses too."
        ],
)
class BackupCommand(context: ExecutionContext) : AbstractAdminCommand(context) {
    enum class Type {
        FULL,
        DIFF,
        AUTO,
    }

    @Parameters(
        arity = "0..*",
        paramLabel = "<database>",
        converter = [DatabaseNamePatternConverter::class],
        defaultValue = GraphDatabaseSettings.DEFAULT_DATABASE_NAME,
        description =
            [
                "Name of the remote database to backup. Supports globbing inside of double quotes, " +
                    "for example \"data*\"."
            ],
    )
    private var databases: List<DatabaseNamePattern> = emptyList()

    @Option(
        names = ["--from"],
        paramLabel = "<host:port>[,<host:port>...]",
        defaultValue = "localhost:${BackupSettings.DEFAULT_PORT}",
        showDefaultValue = ALWAYS,
        description =
            [
                "Comma-separated list of the servers to back up from, as host:port, tried in " +
                    "order until one serves the backup of a database."
            ],
    )
    private lateinit var from: String

    @Option(
        names = ["--to-path"],
        required = true,
        paramLabel = "<path>",
        description =
            [
                "Directory to place the backups in: a local one, which must exist, or a folder " +
                    "in cloud storage, as s3://<bucket>/<folder>/, gs://<bucket>/<folder>/ or " +
                    "azb://<storage account>/<container>/<folder>/, which needs --temp-path. A " +
                    "differential backup continues the latest backup of the database there."
            ],
    )
    private lateinit var toPathOption: String

    private lateinit var toPath: Path
    private lateinit var fs: FileSystemAbstraction

    @Option(
        names = ["--type"],
        paramLabel = "<type>",
        defaultValue = "AUTO",
        showDefaultValue = ALWAYS,
        description =
            [
                "Type of backup to perform. Possible values are: FULL, DIFF, AUTO. If none is " +
                    "specified, the type is automatically determined based on the existing " +
                    "backups. A differential backup falls back to a full one when the server no " +
                    "longer has the transactions it needs, or when the database is not the one " +
                    "the existing backups are of."
            ],
    )
    private var type = Type.AUTO

    @Option(
        names = ["--include-metadata"],
        paramLabel = "none|all|users|roles",
        defaultValue = "all",
        showDefaultValue = ALWAYS,
        converter = [IncludeConverter::class],
        description =
            [
                "Include in the backup the Cypher that gives a DBMS back the database's users, " +
                    "roles and privileges, which a restore writes to " +
                    "data/scripts/<database>/restore_metadata.cypher. roles: the roles with " +
                    "privileges on the database or every database, and those privileges. users: " +
                    "the users who can access the database, and their roles among those. all: " +
                    "both. none: nothing. Never for the system database."
            ],
    )
    private var includeMetadata = MetadataScript.Include.ALL

    /** `none|all|users|roles`, as Enterprise writes them. */
    class IncludeConverter : CommandLine.ITypeConverter<MetadataScript.Include> {
        override fun convert(value: String): MetadataScript.Include =
            MetadataScript.Include.entries.find { it.name.equals(value, ignoreCase = true) }
                ?: throw CommandLine.TypeConversionException(
                    "'$value' is not one of none, all, users and roles"
                )
    }

    @Option(
        names = ["--compress"],
        arity = "0..1",
        paramLabel = "true|false",
        fallbackValue = "true",
        defaultValue = "true",
        showDefaultValue = ALWAYS,
        description = ["Request backup artifact to be compressed."],
    )
    private var compress = true

    @Option(
        names = ["--keep-failed"],
        arity = "0..1",
        paramLabel = "true|false",
        fallbackValue = "true",
        showDefaultValue = ALWAYS,
        description =
            [
                "Request to keep a failed backup: the copy of the database a full backup failed " +
                    "with is kept in the temporary directory. If false a failed backup is removed."
            ],
    )
    private var keepFailed = false

    @Option(
        names = ["--pagecache"],
        paramLabel = "<size>",
        description =
            [
                "The size of the page cache to use for the backup process, which recovers a full " +
                    "backup's copy of the database."
            ],
    )
    private var pagecache: String? = null

    @Option(
        names = ["--temp-path"],
        paramLabel = "<path>",
        description =
            [
                "Local directory for temporary files, such as the copy of a database before it " +
                    "is recovered. The default is --to-path, which must be local then."
            ],
    )
    private var tempPath: Path? = null

    override fun commandConfigName(): Optional<String> = Optional.of("database-backup")

    /** Why a database's backup did not continue its latest backup. */
    private class Discontinued(message: String) : Exception(message)

    override fun execute() {
        if (tempPath?.let(ctx.fs()::isDirectory) == false) {
            throw CommandFailedException("--temp-path $tempPath is not an existing directory")
        }
        val servers =
            try {
                from.split(',').filter { it.isNotBlank() }.map(HostPort::parse)
            } catch (e: IllegalArgumentException) {
                throw CommandFailedException("Invalid --from: ${e.message}", e)
            }
        if (servers.isEmpty()) throw CommandFailedException("--from names no server")
        val config = config()
        var serversFailed = false
        val failed = mutableListOf<String>()
        commandLogProvider(ctx.out(), verbose).use { logProvider ->
            SchemeFileSystemAbstraction(ctx.fs(), config, logProvider).use { fs ->
                this.fs = fs
                toPath = CommandPaths.resolve(fs, toPathOption, "--to-path")
                checkToPath()
                val client =
                    try {
                        BackupClient(fs, BackupTls.client(config, policies(config, logProvider)))
                    } catch (e: Exception) {
                        throw CommandFailedException("Invalid TLS configuration: ${e.message}", e)
                    }
                val names = names(client, servers) { serversFailed = true }
                AdminResources(fs, config, logProvider).use { resources ->
                    for (name in names) {
                        if (!backUp(name, client, servers, resources) { serversFailed = true }) {
                            failed += name
                        }
                    }
                }
            }
        }
        if (failed.isNotEmpty()) {
            throw CommandFailedException(
                "Backup failed for databases: ${failed.joinToString { "'$it'" }}"
            )
        }
        if (serversFailed) {
            throw CommandFailedException(
                "Every database was backed up, but some servers failed, as reported above"
            )
        }
    }

    /**
     * A local --to-path must be an existing directory. In cloud storage, folders always exist, so
     * the folder's listing is opened instead, which reads its first entries: that checks the
     * credentials and the bucket before a backup is taken. The copy of a database is then made in
     * --temp-path.
     */
    private fun checkToPath() {
        if (!CommandPaths.isCloud(toPath)) {
            if (!fs.isDirectory(toPath)) {
                throw CommandFailedException("--to-path $toPath is not an existing directory")
            }
            return
        }
        if (tempPath == null) {
            throw CommandFailedException(
                "--to-path ${CommandPaths.display(toPath)} is in cloud storage: give --temp-path, " +
                    "a local directory for the copy of a database before it is uploaded"
            )
        }
        try {
            Files.newDirectoryStream(toPath).close()
        } catch (e: IOException) {
            throw CommandFailedException(
                "Cannot list --to-path ${CommandPaths.display(toPath)}: ${e.message}",
                e,
            )
        }
    }

    private fun config(): Config {
        val builder = createPrefilledConfigBuilder()
        pagecache?.let {
            val size =
                try {
                    ByteUnit.parse(it)
                } catch (e: IllegalArgumentException) {
                    throw CommandFailedException("Invalid --pagecache: ${e.message}", e)
                }
            builder.set(GraphDatabaseSettings.pagecache_memory, size)
        }
        return builder.build()
    }

    private fun policies(config: Config, logProvider: InternalLogProvider) =
        DefaultSslPolicyProvider(ctx.fs(), config, false, false, logProvider)

    /** The databases to back up: the names given, and those of the first server for patterns. */
    private fun names(
        client: BackupClient,
        servers: List<HostPort>,
        onServerFailed: () -> Unit,
    ): List<String> {
        val names = linkedSetOf<String>()
        for (pattern in databases) {
            if (!pattern.containsPattern()) {
                names += NormalizedDatabaseName(pattern.databaseName).name()
                continue
            }
            val matching =
                servers.firstNotNullOfOrNull { server ->
                    try {
                        client.list(server, pattern.databaseName)
                    } catch (e: Exception) {
                        ctx.err().println("Could not list the databases of $server: ${e.message}")
                        if (e !is Refused) onServerFailed()
                        null
                    }
                }
                    ?: throw CommandFailedException(
                        "No server could list the databases matching '${pattern.databaseName}'"
                    )
            if (matching.isEmpty()) {
                throw CommandFailedException("No database matches '${pattern.databaseName}'")
            }
            names += matching
        }
        return names.toList()
    }

    /** Backs up [name] from the first of [servers] that serves it; whether one did. */
    private fun backUp(
        name: String,
        client: BackupClient,
        servers: List<HostPort>,
        resources: AdminResources,
        onServerFailed: () -> Unit,
    ): Boolean {
        val latest =
            if (type == Type.FULL) null
            else
                try {
                    latest(name)
                } catch (e: IOException) {
                    ctx.err().println("Could not back up database '$name': ${e.message}")
                    return false
                }
        if (type == Type.DIFF && latest == null) {
            ctx.err()
                .println(
                    "Could not back up database '$name': there is no backup of it in " +
                        "${CommandPaths.display(toPath)} to " +
                        "continue; take a full backup first"
                )
            return false
        }
        for (server in servers) {
            try {
                val script = metadata(name, client, server)
                if (latest != null) {
                    try {
                        differential(name, latest, client, server, script)
                        return true
                    } catch (e: Discontinued) {
                        ctx.out()
                            .println(
                                "Taking a full backup of database '$name' instead of a " +
                                    "differential one: ${e.message}."
                            )
                    }
                }
                full(name, client, server, resources, script)
                return true
            } catch (e: Refused) {
                ctx.err().println("Could not back up database '$name' from $server: ${e.message}")
            } catch (e: IOException) {
                ctx.err().println("Could not back up database '$name' from $server: ${e.message}")
                onServerFailed()
            }
        }
        return false
    }

    /** The latest backup of [name] in --to-path, which a differential one continues. */
    private fun latest(name: String): Artifact? {
        val found = ArtifactChains.find(listOf(toPath), fs)
        found.unreadable.forEach { (file, why) ->
            ctx.err().println("Skipping ${CommandPaths.display(file)}: $why")
        }
        val artifacts = found.artifacts.filter { it.header.databaseName == name }
        if (artifacts.none { !it.header.empty }) return null
        return ArtifactChains.select(artifacts, null).last
    }

    /**
     * The users, roles and privileges of [name] that --include-metadata asks [server] for, taken
     * before the backup; none for `system`.
     */
    private fun metadata(name: String, client: BackupClient, server: HostPort): String? {
        if (name == GraphDatabaseSettings.SYSTEM_DATABASE_NAME) return null
        if (includeMetadata == MetadataScript.Include.NONE) return null
        return client.metadata(server, name, includeMetadata)
    }

    private fun differential(
        name: String,
        latest: Artifact,
        client: BackupClient,
        server: HostPort,
        script: String?,
    ) {
        val base = latest.header
        val readers = StorageEngineFactory.defaultStorageEngine().commandReaderFactory()
        val written =
            try {
                client.differential(
                    server,
                    name,
                    base.highestAppendIndex,
                    base.lastChecksum!!,
                    readers,
                ) { header, transactions ->
                    if (header.databaseId != base.databaseId || header.storeId != base.storeId) {
                        throw Discontinued(
                            "the database on $server is not the one $latest is of: it was " +
                                "re-created or replaced"
                        )
                    }
                    val artifact = header.copy(compressed = compress, metadataScript = script)
                    val file =
                        Artifacts.writeNew(toPath, header.databaseName, header.time, fs) {
                            Artifacts.writeDifferential(
                                it,
                                artifact,
                                transactions,
                                temporaryDirectory()
                                    .resolve(
                                        ".${header.databaseName}-${UUID.randomUUID()}.records"
                                    ),
                                fs,
                            )
                        }
                    file to artifact
                }
            } catch (e: Refused) {
                if (e.status == Status.PRUNED || e.status == Status.DIVERGED) {
                    throw Discontinued("${e.message}")
                }
                throw e
            }
        val (file, header) = written
        val what =
            if (header.empty) "an empty differential backup: no transactions since $latest"
            else {
                val transactions =
                    if (header.lowestAppendIndex == header.highestAppendIndex)
                        "transaction ${header.lowestAppendIndex}, committed at"
                    else
                        "transactions ${header.lowestAppendIndex} to " +
                            "${header.highestAppendIndex}, the last committed at"
                "a differential backup of $transactions ${format(header.lastCommitTime!!)}"
            }
        ctx.out()
            .println(
                "Backed up database '$name' from $server to ${CommandPaths.display(file)}: $what."
            )
    }

    private fun full(
        name: String,
        client: BackupClient,
        server: HostPort,
        resources: AdminResources,
        script: String?,
    ) {
        val staging = fs.createTempDirectory(temporaryDirectory(), ".dozendb-backup-")
        var succeeded = false
        try {
            val layout = Neo4jLayout.ofFlat(staging).databaseLayout(name)
            val copy = client.full(server, name, layout)
            val last = resources.preparer().recoverCopy(layout, "the copy of '$name' from $server")
            if (
                last.id() != copy.last.transactionId ||
                    last.appendIndex() != copy.last.appendIndex ||
                    last.checksum() != copy.last.checksum
            ) {
                throw IOException(
                    "the copy of '$name' from $server recovered up to transaction ${last.id()} " +
                        "with checksum ${last.checksum()}, not to ${copy.last.transactionId} with " +
                        "${copy.last.checksum} as the server sent it"
                )
            }
            val header =
                ArtifactHeader(
                    databaseName = copy.database,
                    databaseId = copy.databaseId,
                    storeId =
                        StoreId.retrieveFromStore(
                            fs,
                            layout,
                            resources.pageCache,
                            CursorContext.NULL_CONTEXT,
                        ),
                    time = copy.metadata.time,
                    full = true,
                    compressed = compress,
                    lowestAppendIndex = 1,
                    highestAppendIndex = last.appendIndex(),
                    previousChecksum = null,
                    lastChecksum = last.checksum(),
                    lastCommitTime = Instant.ofEpochMilli(last.commitTimestamp()),
                    source = copy.metadata.source,
                    metadataScript = script,
                )
            val file =
                Artifacts.writeNew(toPath, header.databaseName, header.time, fs) {
                    Artifacts.writeFull(it, header, layout, fs)
                }
            succeeded = true
            ctx.out()
                .println(
                    "Backed up database '$name' from $server to ${CommandPaths.display(file)}: " +
                        "a full backup up to " +
                        "transaction ${last.id()}, committed at ${format(header.lastCommitTime!!)}."
                )
        } finally {
            if (succeeded || !keepFailed) {
                fs.deleteRecursively(staging)
            } else {
                ctx.err().println("Kept the failed backup of '$name' in $staging")
            }
        }
    }

    private fun temporaryDirectory(): Path = tempPath ?: toPath

    private fun format(time: Instant): String = "${TIME.format(time.atOffset(UTC))} UTC"

    private companion object {
        val TIME: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    }
}
