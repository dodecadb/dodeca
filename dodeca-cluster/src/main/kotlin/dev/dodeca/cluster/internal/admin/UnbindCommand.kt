package dev.dodeca.cluster.internal.admin

import dev.dodeca.cluster.MembershipFile
import dev.dodeca.cluster.internal.ClusterSettings
import dev.dodeca.core.edition.Quarantines
import java.io.IOException
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.time.Instant
import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter
import java.util.Optional
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream
import org.neo4j.cli.AbstractAdminCommand
import org.neo4j.cli.CommandFailedException
import org.neo4j.cli.CommandProvider
import org.neo4j.cli.CommandType
import org.neo4j.cli.ExecutionContext
import org.neo4j.commandline.dbms.CannotWriteException
import org.neo4j.commandline.dbms.LockChecker
import org.neo4j.io.layout.Neo4jLayout
import org.neo4j.io.locker.FileLockException
import picocli.CommandLine.Command
import picocli.CommandLine.Help.Visibility.ALWAYS
import picocli.CommandLine.Option

/**
 * `neo4j-admin server unbind`, as Neo4j Enterprise documents it, in place of Neo4j Community's,
 * which removes only the server's id: it removes the server's cluster state, its Raft directory, id
 * and quarantines, archived if asked, so that the server starts as a new server of its cluster. The
 * files of its databases are set aside in `unbound/<time>/` next to their directories, kept but no
 * longer served: as a new server it copies the databases placed on it from the others, and would
 * delete files it no longer counts as its own.
 *
 * Each step can be run again: a run cut short leaves the server id removed and the Raft directory's
 * membership record, which makes the server refuse to start, in place until the last step.
 */
@Command(
    name = "unbind",
    header =
        [
            "Removes and archives the cluster state of a cluster server so that it can rebind to a cluster."
        ],
    description =
        [
            "Removes the server's id and its cluster state, archived with --archive-cluster-state, and sets the files of its databases aside. When it starts, it is a new server, which waits to be added to its cluster."
        ],
)
class UnbindCommand(context: ExecutionContext) : AbstractAdminCommand(context) {
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

    override fun commandConfigName(): Optional<String> = Optional.of("server-unbind")

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
        try {
            LockChecker.checkDbmsLock(layout).use { unbind(layout, raft) }
        } catch (e: CannotWriteException) {
            throw CommandFailedException("Cannot write to ${layout.dataDirectory()}.", e)
        } catch (e: FileLockException) {
            throw CommandFailedException("The server is running: shut it down first.", e)
        } catch (e: IOException) {
            throw CommandFailedException("Unbinding the server failed: ${e.message}", e)
        }
    }

    private fun unbind(layout: Neo4jLayout, raft: Path) {
        val out = ctx.out()
        val serverId = layout.serverIdFile()
        if (!Files.exists(raft)) {
            Files.deleteIfExists(serverId)
            out.println(
                "The server has no cluster state in $raft: removed its id, $serverId, only. It " +
                    "starts with a new id."
            )
            return
        }
        val owner = MembershipFile(raft.resolve(MembershipFile.FILE_NAME)).read()?.memberId
        // In the names of the archive and of the directory the databases' files go to.
        val time = TIME.format(Instant.now())
        // The quarantines of copies that are set aside, which the server copies anew.
        val quarantines = layout.dataDirectory().resolve(Quarantines.FILE_NAME)
        val archive = archivePath?.let { archive(raft, listOf(serverId, quarantines), it, time) }
        Files.deleteIfExists(serverId)
        Files.deleteIfExists(quarantines)
        val aside = setAside(layout, time)
        deleteClusterState(raft)
        out.println(
            buildString {
                append("Removed the cluster state of server ${owner ?: "unknown"}: $raft and ")
                append("$serverId")
                if (archive != null) append(", archived in $archive")
                append(". ")
                if (aside.isNotEmpty()) {
                    append("The files of its databases are in ${aside.joinToString(" and ")}: ")
                    append("the server no longer serves them; delete them once not needed. ")
                }
                append("When the server starts, it is a new server, which waits to be added: ")
                append("run CALL dodeca.cluster.addMember('<its Raft address>') on a member, ")
                append("enable it, then deallocate and drop ")
                append(if (owner != null) "server $owner." else "the server under its old id.")
            }
        )
    }

    /**
     * Writes the Raft directory and the [files] of the data directory that exist, such as the
     * server id, to a zip archive; returns its path.
     */
    private fun archive(raft: Path, files: List<Path>, destination: Path, time: String): Path {
        val file =
            if (Files.isDirectory(destination))
                destination.resolve("unbound_cluster_state.$time.zip")
            else destination
        if (Files.exists(file)) throw IOException("the archive $file exists already")
        ZipOutputStream(Files.newOutputStream(file, StandardOpenOption.CREATE_NEW)).use { zip ->
            Files.walk(raft).use { paths ->
                for (path in paths.filter(Files::isRegularFile)) {
                    zip.putNextEntry(ZipEntry("raft/${raft.relativize(path).joinToString("/")}"))
                    Files.copy(path, zip)
                    zip.closeEntry()
                }
            }
            for (file in files.filter(Files::exists)) {
                zip.putNextEntry(ZipEntry(file.fileName.toString()))
                Files.copy(file, zip)
                zip.closeEntry()
            }
        }
        FileChannel.open(file, StandardOpenOption.WRITE).use { it.force(true) }
        return file
    }

    /**
     * Moves the directory of each database, and of its transaction logs, to `unbound/<time>/` next
     * to the directory it is in; returns the directories they went to.
     */
    private fun setAside(layout: Neo4jLayout, time: String): List<Path> =
        listOf(layout.databasesDirectory(), layout.transactionLogsRootDirectory())
            .distinct()
            .mapNotNull { root ->
                if (!Files.isDirectory(root)) return@mapNotNull null
                val stores =
                    Files.list(root).use { list -> list.filter(Files::isDirectory).toList() }
                if (stores.isEmpty()) return@mapNotNull null
                val target =
                    root.resolveSibling(UNBOUND).resolve(time).resolve(root.fileName.toString())
                Files.createDirectories(target)
                for (store in stores) {
                    Files.move(
                        store,
                        target.resolve(store.fileName.toString()),
                        StandardCopyOption.ATOMIC_MOVE,
                    )
                }
                target
            }

    /** Deletes the Raft directory, its membership record last. */
    private fun deleteClusterState(raft: Path) {
        val membership = raft.resolve(MembershipFile.FILE_NAME)
        Files.list(raft)
            .use { list -> list.filter { it != membership }.toList() }
            .forEach { deleteRecursively(it) }
        Files.deleteIfExists(membership)
        Files.delete(raft)
    }

    private fun deleteRecursively(path: Path) {
        Files.walk(path).use { paths ->
            paths.sorted(Comparator.reverseOrder()).forEach { Files.delete(it) }
        }
    }

    private companion object {
        const val UNBOUND = "unbound"
        val TIME: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmm").withZone(UTC)
    }
}

/**
 * Registered in META-INF/services. `neo4j-admin` keeps the provider of a command type with the
 * lowest priority: below that of Neo4j Community's, the default.
 */
class UnbindCommandProvider : CommandProvider {
    override fun createCommand(ctx: ExecutionContext) = UnbindCommand(ctx)

    override fun commandType() = CommandType.UNBIND

    override fun getPriority() = Int.MAX_VALUE - 1
}
