package dev.dodeca.backup.source

import dev.dodeca.backup.ArtifactHeader
import dev.dodeca.backup.Artifacts
import dev.dodeca.backup.BackupProtocol
import dev.dodeca.backup.BackupProtocol.Request
import dev.dodeca.backup.BackupProtocol.Status
import dev.dodeca.consensus.BulkSource
import dev.dodeca.core.SystemGraphReads
import dev.dodeca.core.edition.BackupSources
import dev.dodeca.core.edition.LatestWrites
import dev.dodeca.core.security.MetadataScript
import dev.dodeca.core.store.CommittedTransactions
import dev.dodeca.core.store.CommittedTransactions.Behind
import dev.dodeca.core.store.CommittedTransactions.Continued
import dev.dodeca.core.store.CommittedTransactions.Diverged
import dev.dodeca.core.store.CommittedTransactions.Pruned
import dev.dodeca.core.store.StoreArchive
import dev.dodeca.core.store.StoreSnapshots
import java.io.IOException
import java.io.OutputStream
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.helpers.DatabaseNamePattern
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.kernel.database.Database
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.logging.InternalLogProvider

/**
 * Serves the requests of [BackupProtocol] from the databases of this server. A backup holds every
 * write acknowledged before it was asked for: in a cluster, the server first waits until it holds
 * them ([LatestWrites]).
 */
internal class BackupSource(
    private val databases: DatabaseContextProvider<*>,
    private val latestWrites: LatestWrites,
    private val fs: FileSystemAbstraction,
    private val source: String,
    logProvider: InternalLogProvider,
    private val clock: Clock = Clock.systemUTC(),
    private val sources: () -> BackupSources? = { null },
) : BulkSource {
    private val log = logProvider.getLog(BackupSource::class.java)

    override fun serve(request: ByteArray, out: OutputStream) {
        val decoded =
            try {
                BackupProtocol.decode(request)
            } catch (e: IOException) {
                BackupProtocol.writeStatus(out, Status.FAILED, "${e.message}")
                return
            }
        try {
            when (decoded) {
                is Request.List -> list(decoded, out)
                is Request.Sources -> sources(decoded, out)
                is Request.Full -> database(decoded.database, out)?.let { full(it, out) }
                is Request.Differential ->
                    database(decoded.database, out)?.let { differential(it, decoded, out) }
                is Request.Metadata -> database(SYSTEM, out)?.let { metadata(it, decoded, out) }
            }
        } catch (e: IOException) {
            // Typically the client went away; the transport logs nothing that a server shows.
            val what =
                when (decoded) {
                    is Request.List -> "the list of databases"
                    is Request.Sources -> "the servers that back up ${decoded.database}"
                    is Request.Full -> "a full backup of ${decoded.database}"
                    is Request.Differential -> "a differential backup of ${decoded.database}"
                    is Request.Metadata -> "the metadata of ${decoded.database}"
                }
            log.warn("Serving %s failed: %s", what, e.message)
            throw e
        }
    }

    private fun list(request: Request.List, out: OutputStream) {
        val pattern = DatabaseNamePattern(request.pattern)
        val names = known().filter(pattern::matches).sorted()
        BackupProtocol.writeStatus(out, Status.OK)
        BackupProtocol.writeNames(out, names)
    }

    /** The backup addresses of the servers that host the database, in the order to try them. */
    private fun sources(request: Request.Sources, out: OutputStream) {
        val name = NormalizedDatabaseName(request.database).name()
        val lookup = sources()
        if (lookup == null) {
            BackupProtocol.writeStatus(
                out,
                Status.FAILED,
                "this server cannot tell which servers back up a database",
            )
            return
        }
        if (name !in known()) {
            BackupProtocol.writeStatus(out, Status.NO_DATABASE, "there is no database '$name'")
            return
        }
        BackupProtocol.writeStatus(out, Status.OK)
        BackupProtocol.writeNames(out, lookup.of(name))
    }

    /**
     * The databases of the DBMS: those registered here, and those the system graph has, which in a
     * cluster other servers may host instead of this one.
     */
    private fun known(): Set<String> {
        val registered = databases.registeredDatabases().keys.map { it.name() }
        val system =
            databases.getDatabaseContext(SYSTEM).orElse(null)?.databaseFacade()
                ?: return registered.toSet()
        return SystemGraphReads.read(system) { tx ->
            tx.findNodes(TopologyGraphDbmsModel.DATABASE_LABEL).use { nodes ->
                nodes
                    .asSequence()
                    .mapNotNull {
                        it.getProperty(TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY, null)
                            as? String
                    }
                    .toSet()
            }
        } + registered
    }

    /**
     * The started database [name], once this server holds the writes acknowledged before; null
     * after refusing the request in [out] if there is none.
     */
    private fun database(name: String, out: OutputStream): Database? {
        val context = databases.getDatabaseContext(name).orElse(null)
        if (context == null) {
            BackupProtocol.writeStatus(
                out,
                Status.NO_DATABASE,
                if (name in known()) "database '$name' is not hosted on this server"
                else "there is no database '$name'",
            )
            return null
        }
        val database = context.database() as? Database
        if (
            database == null ||
                !database.isStarted ||
                !database.databaseAvailabilityGuard.isAvailable
        ) {
            BackupProtocol.writeStatus(
                out,
                Status.UNAVAILABLE,
                "database '$name' is not available: it must be running to be backed up",
            )
            return null
        }
        try {
            latestWrites.await(database.namedDatabaseId, LATEST_WRITES_TIMEOUT)
        } catch (e: Exception) {
            log.warn("Backup of %s refused: %s", name, e.toString())
            BackupProtocol.writeStatus(
                out,
                Status.FAILED,
                "this server could not tell it holds the latest writes to '$name': ${e.message}",
            )
            return null
        }
        return database
    }

    private fun full(database: Database, out: OutputStream) {
        val name = database.namedDatabaseId.name()
        val time = now()
        StoreSnapshots.take(database, fs).use { snapshot ->
            BackupProtocol.writeStatus(out, Status.OK)
            val archive = StoreArchive.Writer(out, fs)
            archive.header(
                name,
                database.namedDatabaseId.databaseId().uuid(),
                "backup",
                BackupProtocol.encode(BackupProtocol.FullMetadata(time, source)),
            )
            StoreSnapshots.writeStoreFiles(snapshot, database, archive)
            val last = StoreSnapshots.writeLogsToLastClosed(database, fs, archive, CLOSE_TIMEOUT)
            archive.end(
                BackupProtocol.encode(
                    BackupProtocol.Last(
                        last.id(),
                        last.appendIndex(),
                        last.checksum(),
                        Instant.ofEpochMilli(last.commitTimestamp()),
                    )
                )
            )
            log.info("Served a full backup of %s, up to transaction %d", name, last.id())
        }
    }

    private fun differential(database: Database, request: Request.Differential, out: OutputStream) {
        val name = database.namedDatabaseId.name()
        CommittedTransactions.read(database, request.after, request.checksum) { range ->
            when (range) {
                is Pruned ->
                    BackupProtocol.writeStatus(
                        out,
                        Status.PRUNED,
                        "the transaction logs of '$name' no longer hold the transactions after " +
                            "append index ${request.after}",
                    )
                is Diverged ->
                    BackupProtocol.writeStatus(
                        out,
                        Status.DIVERGED,
                        "database '$name' does not continue the backup: ${range.reason}",
                    )
                is Behind ->
                    BackupProtocol.writeStatus(
                        out,
                        Status.DIVERGED,
                        "database '$name' does not continue the backup: its last transaction " +
                            "has append index ${range.last.appendIndex()}, before the backup's last",
                    )
                is Continued -> {
                    val last = range.last
                    val empty = last.appendIndex() == request.after
                    val header =
                        ArtifactHeader(
                            databaseName = name,
                            databaseId = database.namedDatabaseId.databaseId(),
                            storeId = database.storeId,
                            time = now(),
                            full = false,
                            compressed = false,
                            lowestAppendIndex = if (empty) 0 else request.after + 1,
                            highestAppendIndex = if (empty) 0 else last.appendIndex(),
                            previousChecksum = if (empty) null else request.checksum,
                            lastChecksum = if (empty) null else last.checksum(),
                            lastCommitTime =
                                if (empty) null else Instant.ofEpochMilli(last.commitTimestamp()),
                            source = source,
                        )
                    BackupProtocol.writeStatus(out, Status.OK)
                    Artifacts.writeUncompressedDifferential(out, header, range.transactions)
                    log.info(
                        "Served a differential backup of %s, after append index %d up to %d",
                        name,
                        request.after,
                        last.appendIndex(),
                    )
                }
            }
        }
    }

    /** The users, roles and privileges of the database, from [system] once it holds the latest. */
    private fun metadata(system: Database, request: Request.Metadata, out: OutputStream) {
        val script =
            SystemGraphReads.read(
                databases.getDatabaseContext(system.namedDatabaseId).orElseThrow().databaseFacade()
            ) {
                MetadataScript.generate(
                    it,
                    request.database,
                    request.include,
                    log,
                    request.users?.toSet(),
                )
            }
        BackupProtocol.writeStatus(out, Status.OK)
        BackupProtocol.writeScript(out, script)
    }

    private fun now(): Instant = clock.instant().truncatedTo(ChronoUnit.SECONDS)

    private companion object {
        val LATEST_WRITES_TIMEOUT: Duration = Duration.ofMinutes(1)
        val CLOSE_TIMEOUT: Duration = Duration.ofMinutes(1)
        val SYSTEM: String = GraphDatabaseSettings.SYSTEM_DATABASE_NAME
    }
}
