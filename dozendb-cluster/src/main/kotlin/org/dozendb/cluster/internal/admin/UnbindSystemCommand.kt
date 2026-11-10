package org.dozendb.cluster.internal.admin

import java.io.IOException
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.time.Instant
import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter
import java.util.Optional
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream
import org.dozendb.cluster.DatabaseGroups
import org.dozendb.cluster.MembershipFile
import org.dozendb.cluster.internal.ClusterSettings
import org.neo4j.cli.AbstractAdminCommand
import org.neo4j.cli.CommandFailedException
import org.neo4j.cli.CommandProvider
import org.neo4j.cli.CommandType
import org.neo4j.cli.ExecutionContext
import org.neo4j.commandline.dbms.CannotWriteException
import org.neo4j.commandline.dbms.LockChecker
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import org.neo4j.io.layout.Neo4jLayout
import org.neo4j.io.locker.FileLockException
import org.neo4j.kernel.database.DatabaseTracers
import org.neo4j.kernel.recovery.LogTailExtractor
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.storageengine.api.StorageEngineFactory
import picocli.CommandLine.Command
import picocli.CommandLine.Help.Visibility.ALWAYS
import picocli.CommandLine.Option

/**
 * `neo4j-admin dbms unbind-system-db`, as Neo4j Enterprise documents it: removes this server's Raft
 * state of `system`, archived if asked, so that `system` can be re-formed from the servers that
 * survived the loss of most of its primaries. It records `systemUnbound` in the membership record,
 * first, so that a run cut short is run again; the server's id, the store of `system` and the Raft
 * state of every other database stay. It prints the last transaction of the store of `system`, as
 * `neo4j-admin database info system` does: the server with the latest one is the copy the others
 * take when `system` is re-formed.
 */
@Command(
    name = "unbind-system-db",
    header = ["Removes and archives cluster state for the system database."],
    description =
        [
            "Removes the cluster state of the system database on this server, archived with --archive-cluster-state, and keeps its store. Set dbms.dozendb.cluster.initial_members to the servers that form the system database again, and start them: the one with the latest store is the copy the others take."
        ],
)
class UnbindSystemCommand(context: ExecutionContext) : AbstractAdminCommand(context) {
    @Option(
        names = ["--archive-cluster-state"],
        arity = "0..1",
        paramLabel = "true|false",
        fallbackValue = "true",
        showDefaultValue = ALWAYS,
        description = ["Enable or disable the cluster state archiving."],
    )
    private var archiveClusterState = false

    @Option(
        names = ["--archive-path"],
        paramLabel = "<path>",
        description = ["Destination (file or folder) of the cluster state archive."],
    )
    private var archivePath: Path? = null

    override fun commandConfigName(): Optional<String> = Optional.of("dbms-unbind-system-db")

    override fun execute() {
        if (archiveClusterState && archivePath == null) {
            throw CommandFailedException("--archive-cluster-state needs --archive-path.")
        }
        if (!archiveClusterState && archivePath != null) {
            throw CommandFailedException("--archive-path needs --archive-cluster-state=true.")
        }
        val config = createPrefilledConfigBuilder().build()
        val layout = Neo4jLayout.of(config)
        val raft = config.get(ClusterSettings.raftDirectory)
        if (!Files.exists(raft.resolve(MembershipFile.FILE_NAME))) {
            throw CommandFailedException(
                "The server has no cluster state in $raft: there is nothing to unbind."
            )
        }
        try {
            LockChecker.checkDbmsLock(layout).use { unbind(config, layout, raft) }
        } catch (e: CannotWriteException) {
            throw CommandFailedException("Cannot write to ${layout.dataDirectory()}.", e)
        } catch (e: FileLockException) {
            throw CommandFailedException("The server is running: shut it down first.", e)
        } catch (e: IOException) {
            throw CommandFailedException("Unbinding the system database failed: ${e.message}", e)
        }
    }

    private fun unbind(config: Config, layout: Neo4jLayout, raft: Path) {
        val membership = MembershipFile(raft.resolve(MembershipFile.FILE_NAME))
        val record =
            membership.read()
                ?: throw CommandFailedException(
                    "The server has no cluster state in $raft: there is nothing to unbind."
                )
        val group = raft.resolve((record.systemGroup ?: DatabaseGroups.SYSTEM_DATABASE).toString())
        val archive = archivePath?.let {
            archive(raft, listOf(group, raft.resolve(MembershipFile.FILE_NAME)), it)
        }
        if (!record.systemUnbound) membership.write(record.copy(systemUnbound = true))
        if (Files.exists(group)) deleteRecursively(group)
        val last = lastTransaction(config, layout)
        ctx.out()
            .println(
                buildString {
                    append("Removed the cluster state of the system database on server ")
                    append("${record.memberId ?: "unknown"}: $group")
                    if (archive != null) append(", archived in $archive")
                    append(". ")
                    if (last != null) {
                        append("Its store is kept, with lastCommittedTransaction $last. ")
                    } else {
                        append("The server holds no store of the system database. ")
                    }
                    append("Set ${ClusterSettings.initialMembers.name()} to the servers that ")
                    append("form the system database again and start them: the one with the ")
                    append("latest store is the copy the others take.")
                }
            )
    }

    /** The id of the last transaction of the store of `system`, or null if there is none. */
    private fun lastTransaction(config: Config, layout: Neo4jLayout): Long? {
        val fs = DefaultFileSystemAbstraction()
        val system = layout.databaseLayout(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
        val engine =
            StorageEngineFactory.selectStorageEngine(fs, system).orElse(null) ?: return null
        return LogTailExtractor(fs, config, engine, DatabaseTracers.EMPTY)
            .getTailMetadata(system, EmptyMemoryTracker.INSTANCE)
            .lastCommittedTransaction
            .id()
    }

    /**
     * Writes [paths] of the Raft directory [raft] that exist to a zip archive; returns its path.
     */
    private fun archive(raft: Path, paths: List<Path>, destination: Path): Path {
        val time = TIME.format(Instant.now())
        val file =
            if (Files.isDirectory(destination))
                destination.resolve("unbound_system_cluster_state.$time.zip")
            else destination
        if (Files.exists(file)) throw IOException("the archive $file exists already")
        ZipOutputStream(Files.newOutputStream(file, StandardOpenOption.CREATE_NEW)).use { zip ->
            for (root in paths.filter(Files::exists)) {
                Files.walk(root).use { walked ->
                    for (path in walked.filter(Files::isRegularFile)) {
                        zip.putNextEntry(
                            ZipEntry("raft/${raft.relativize(path).joinToString("/")}")
                        )
                        Files.copy(path, zip)
                        zip.closeEntry()
                    }
                }
            }
        }
        FileChannel.open(file, StandardOpenOption.WRITE).use { it.force(true) }
        return file
    }

    private fun deleteRecursively(path: Path) {
        Files.walk(path).use { paths ->
            paths.sorted(Comparator.reverseOrder()).forEach { Files.delete(it) }
        }
    }

    private companion object {
        val TIME: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmm").withZone(UTC)
    }
}

/** Registers `neo4j-admin dbms unbind-system-db`, which Neo4j Community declares but lacks. */
class UnbindSystemCommandProvider : CommandProvider {
    override fun createCommand(ctx: ExecutionContext) = UnbindSystemCommand(ctx)

    override fun commandType() = CommandType.UNBIND_SYSTEM

    override fun getPriority() = Int.MAX_VALUE - 1
}
