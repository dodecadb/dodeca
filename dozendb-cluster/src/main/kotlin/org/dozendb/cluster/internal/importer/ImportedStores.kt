package org.dozendb.cluster.internal.importer

import java.nio.file.Files
import org.dozendb.core.copy.copiedMarker
import org.dozendb.core.importer.importedMarker
import org.dozendb.core.servers.CopyMode
import org.neo4j.io.layout.Neo4jLayout
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.logging.InternalLog

/**
 * The stores that `neo4j-admin` changed on this server while their databases were stopped, which
 * leaves a marker next to each; before the database starts, it is checked here, and removed once
 * the store is taken.
 *
 * `neo4j-admin database import incremental` merges into a store and keeps its Raft position; the
 * cluster starts it as Neo4j Enterprise's procedure for clusters has it: the database recreated
 * with this server as its only primary before the import, so that the servers added afterwards copy
 * the store, with what the import merged ([importedMarker]). Refused, so that the database fails to
 * start here:
 * - a secondary's store: a secondary applies its primaries' transactions, which do not include the
 *   import;
 * - a primary's store, while other primaries host the database and this server rejoins their Raft
 *   group: their copies do not have the import. Recreated from this server's store, under a new
 *   group, the database starts from it everywhere, and is taken.
 *
 * `neo4j-admin database copy` writes a store of its own, without a Raft position, which starts only
 * as the seed of a database ([copiedMarker]): one created from it, or recreated from it. Refused:
 * - a secondary's store, which its primaries' transactions do not apply to;
 * - a primary's store while this server holds the Raft group of the database, whose log the copy
 *   does not follow, with as many primaries as there may be.
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
        checkCopied(id, mode)
        checkImported(id, mode)
    }

    private fun checkCopied(id: NamedDatabaseId, mode: CopyMode) {
        val name = id.name()
        val database = layout.databaseLayout(name)
        val marker = copiedMarker(database)
        if (!Files.exists(marker)) return
        val replaced =
            "neo4j-admin database copy replaced the store of database '$name' on this server"
        val dropCopy =
            "To drop the copy here instead, stop this server and delete " +
                "${database.databaseDirectory()}, ${database.transactionLogsDirectory} and " +
                "$marker: it copies the database from the others that host it again."
        if (mode == CopyMode.SECONDARY) {
            throw ImportedStoreException(
                "$replaced, which hosts a secondary of it: a secondary takes its primaries' " +
                    "transactions, which do not apply to the copy. Copy on a server that hosts " +
                    "the database as a primary, and recreate it from that server's store with " +
                    "dbms.recreateDatabase. $dropCopy"
            )
        }
        if (joinsGroup(id)) {
            throw ImportedStoreException(
                "$replaced: the copy is a store of its own, which the Raft group of the database " +
                    "did not write. Recreate the database from this server's store, so that " +
                    "every server copies it: CALL dbms.recreateDatabase('$name', " +
                    "{seedingServers: ['$serverId']}). $dropCopy"
            )
        }
        log.info(
            "Database %s starts from a store neo4j-admin database copy made; the servers that " +
                "copy it from now on have it",
            name,
        )
        Files.delete(marker)
    }

    private fun checkImported(id: NamedDatabaseId, mode: CopyMode) {
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
                    "copies it: CALL dbms.recreateDatabase('$name', {seedingServers: " +
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

/** A database that `neo4j-admin` changed the store of, which this server cannot start. */
internal class ImportedStoreException(message: String) : IllegalStateException(message)
