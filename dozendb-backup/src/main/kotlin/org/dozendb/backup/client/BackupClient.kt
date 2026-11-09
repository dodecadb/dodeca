package org.dozendb.backup.client

import java.io.InputStream
import java.util.logging.Level
import java.util.logging.Logger
import org.dozendb.backup.ArtifactHeader
import org.dozendb.backup.Artifacts
import org.dozendb.backup.BackupProtocol
import org.dozendb.backup.BackupProtocol.Request
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.ratis.BulkServer
import org.dozendb.consensus.ratis.TlsConfig
import org.dozendb.core.security.MetadataScript
import org.dozendb.core.store.CommittedTransaction
import org.dozendb.core.store.StoreArchive
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.io.layout.DatabaseLayout
import org.neo4j.kernel.database.DatabaseId
import org.neo4j.kernel.database.DatabaseIdFactory
import org.neo4j.storageengine.api.CommandReaderFactory

/**
 * Takes backups from the servers that serve them, over TLS with [tls] or in plaintext if it is
 * null.
 *
 * Every request throws [BackupProtocol.Refused] if the server refuses it, and an
 * [java.io.IOException] if it cannot be reached or fails.
 */
internal class BackupClient(private val fs: FileSystemAbstraction, private val tls: TlsConfig?) {
    /** The databases of [server] that match the glob [pattern]. */
    fun list(server: HostPort, pattern: String): List<String> =
        fetch(server, Request.List(pattern), BackupProtocol::readNames)

    /**
     * A copy of a running database as a server sent it, which recovery completes up to [last].
     *
     * @property database its name.
     */
    class FullCopy(
        val database: String,
        val databaseId: DatabaseId,
        val metadata: BackupProtocol.FullMetadata,
        val last: BackupProtocol.Last,
    )

    /** Copies [database] from [server] into [target], whose directories must hold nothing. */
    fun full(server: HostPort, database: String, target: DatabaseLayout): FullCopy =
        fetch(server, Request.Full(database)) { input ->
            fs.mkdirs(target.databaseDirectory())
            fs.mkdirs(target.transactionLogsDirectory)
            val summary =
                StoreArchive.read(
                    input,
                    fs,
                    target.databaseDirectory(),
                    target.transactionLogsDirectory,
                )
            FullCopy(
                summary.database,
                DatabaseIdFactory.from(summary.databaseId),
                BackupProtocol.decodeFullMetadata(summary.metadata),
                BackupProtocol.decodeLast(summary.trailer),
            )
        }

    /**
     * Passes [block] the header and transactions of a differential backup of [database] from
     * [server], after the transaction at append index [after] with [checksum]. The transactions are
     * read and checked as [block] takes them.
     */
    fun <T> differential(
        server: HostPort,
        database: String,
        after: Long,
        checksum: Int,
        readers: CommandReaderFactory,
        block: (ArtifactHeader, Sequence<CommittedTransaction>) -> T,
    ): T =
        fetch(server, Request.Differential(database, after, checksum)) { input ->
            Artifacts.readDifferential(input, readers, block)
        }

    /** What [include] asks for of the users, roles and privileges of [database] on [server]. */
    fun metadata(server: HostPort, database: String, include: MetadataScript.Include): String? =
        fetch(server, Request.Metadata(database, include), BackupProtocol::readScript)

    private companion object {
        // gRPC logs to java.util.logging, which a command prints; its INFO lines are noise there.
        // Held, as java.util.logging keeps loggers weakly.
        val GRPC_LOG: Logger =
            Logger.getLogger("org.apache.ratis.thirdparty.io.grpc").apply { level = Level.WARNING }
    }

    private fun <T> fetch(server: HostPort, request: Request, consume: (InputStream) -> T): T =
        BulkServer.fetch(server, tls, BackupProtocol.NAME, BackupProtocol.encode(request)) {
            BackupProtocol.readStatus(it)
            consume(it)
        }
}
