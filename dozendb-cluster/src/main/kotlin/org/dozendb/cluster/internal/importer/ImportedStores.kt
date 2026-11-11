package org.dozendb.cluster.internal.importer

import java.nio.file.Files
import org.dozendb.core.importer.importedMarker
import org.dozendb.core.servers.CopyMode
import org.neo4j.io.layout.Neo4jLayout
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.logging.InternalLog

/**
 * The stores that `neo4j-admin database import incremental` merged into on this server, which the
 * cluster starts as Neo4j Enterprise's procedure for clusters has them: the database recreated with
 * this server as its only primary before the import, so that the servers added afterwards copy the
 * store, with what the import merged. The merge leaves a marker next to the store
 * ([importedMarker]); before the database starts, it is checked here, and removed once the store is
 * taken.
 *
 * Refused, so that the database fails to start here:
 * - a secondary's store: a secondary applies its primaries' transactions, which do not include the
 *   import;
 * - a primary's store, while other primaries host the database and this server rejoins their Raft
 *   group: their copies do not have the import. Recreated from this server's store, under a new
 *   group, the database starts from it everywhere, and is taken.
 *
 * @property serverId this server's id, for the refusal's advice.
 * @property primaries how many primaries the system graph places the database named so on.
 * @property joinsGroup whether this server holds the Raft group of the database already.
 */
internal class ImportedStores(
    private val layout: Neo4jLayout,
    private val serverId: String,
    private val primaries: (String) -> Int,
    private val joinsGroup: (NamedDatabaseId) -> Boolean,
    private val log: InternalLog,
) {
    /** Fails if the store of [id], which this server hosts in [mode], cannot be taken. */
    fun check(id: NamedDatabaseId, mode: CopyMode) {
        val name = id.name()
        val database = layout.databaseLayout(name)
        val marker = importedMarker(database)
        if (!Files.exists(marker)) return
        val dropImport =
            "To drop the import here instead, stop this server and delete " +
                "${database.databaseDirectory()}, ${database.transactionLogsDirectory} and " +
                "$marker: it copies the database from the others again."
        if (mode == CopyMode.SECONDARY) {
            throw ImportedStoreException(
                "An incremental import merged into database '$name' on this server, which hosts " +
                    "a secondary of it: a secondary takes its primaries' transactions, which do " +
                    "not include the import. Import on the server that hosts the database as its " +
                    "only primary. $dropImport"
            )
        }
        val primaries = primaries(name)
        if (primaries > 1 && joinsGroup(id)) {
            throw ImportedStoreException(
                "An incremental import merged into database '$name' on this server while " +
                    "$primaries servers host it as primaries, which do not have what it merged. " +
                    "Recreate the database from this server's store, so that every server " +
                    "copies it: CALL dbms.cluster.recreateDatabase('$name', {seedingServers: " +
                    "['$serverId']}). $dropImport"
            )
        }
        log.info(
            "Database %s holds what an incremental import merged; the servers that copy it from " +
                "now on have it",
            name,
        )
        Files.delete(marker)
    }
}

/** A database that an incremental import merged into, which this server cannot start. */
internal class ImportedStoreException(message: String) : IllegalStateException(message)
