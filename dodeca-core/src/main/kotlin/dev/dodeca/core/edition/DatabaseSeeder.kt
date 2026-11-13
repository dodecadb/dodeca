package dev.dodeca.core.edition

import dev.dodeca.core.store.RestorePoint
import java.net.URI
import java.net.URISyntaxException
import java.nio.file.Path
import java.time.ZoneOffset
import java.time.ZonedDateTime
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel
import org.neo4j.graphdb.Node
import org.neo4j.kernel.database.NamedDatabaseId

/**
 * Creates the store of a database from a seed: a backup or a dump at a URI, as `CREATE DATABASE ...
 * OPTIONS {seedURI: ...}` names. Dodeca's backup module provides one as a global dependency.
 */
interface DatabaseSeeder {
    /**
     * Checks that this server reads [seed]: its URI, by its scheme, and its options.
     *
     * @throws IllegalArgumentException telling why not.
     */
    fun check(seed: UriSeed)

    /**
     * Puts the store of [seed] in place as the store of [database], which has none, detached from
     * any Raft group it was replicated in.
     */
    fun seed(database: NamedDatabaseId, seed: UriSeed)
}

/**
 * A seed at a URI, as the options of `CREATE DATABASE` give it: the backup or dump at [uri]
 * (`seedURI`), restored up to [restoreUntil] if given (`seedRestoreUntil`), and, of the backups
 * that [uri] finds, those of the database [sourceDatabase] if given (`seedSourceDatabase`).
 */
data class UriSeed(
    val uri: String,
    val restoreUntil: RestorePoint? = null,
    val sourceDatabase: String? = null,
) {
    override fun toString() =
        listOfNotNull(
                uri,
                restoreUntil?.let { "restored $it" },
                sourceDatabase?.let { "of database '$it'" },
            )
            .joinToString(", ")

    /** Keeps it on [database], a `Database` node, as Neo4j Enterprise does. */
    fun write(database: Node) {
        database.setProperty(TopologyGraphDbmsModel.DATABASE_SEED_URI_PROPERTY, uri)
        setOrRemove(database, RESTORE_UNTIL, restoreUntil?.let(::storedValue))
        setOrRemove(database, SOURCE_DATABASE, sourceDatabase)
    }

    companion object {
        private const val RESTORE_UNTIL =
            TopologyGraphDbmsModel.DATABASE_SEED_RESTORE_UNTIL_PROPERTY
        private const val SOURCE_DATABASE =
            TopologyGraphDbmsModel.DATABASE_SEED_SOURCE_DATABASE_PROPERTY

        /** The seed that [database], a `Database` node, keeps; null for none. */
        fun of(database: Node): UriSeed? {
            val uri =
                database.getProperty(TopologyGraphDbmsModel.DATABASE_SEED_URI_PROPERTY, null)
                    as? String ?: return null
            return UriSeed(
                uri,
                restorePoint(database.getProperty(RESTORE_UNTIL, null)),
                database.getProperty(SOURCE_DATABASE, null) as? String,
            )
        }

        /** Removes the seed that [database], a `Database` node, keeps. */
        fun remove(database: Node) {
            for (property in
                listOf(
                    TopologyGraphDbmsModel.DATABASE_SEED_URI_PROPERTY,
                    RESTORE_UNTIL,
                    SOURCE_DATABASE,
                )) {
                database.removeProperty(property)
            }
        }

        /**
         * `seedRestoreUntil` as Neo4j keeps it, a transaction id or a time, as a restore point;
         * null for none.
         */
        fun restorePoint(value: Any?): RestorePoint? =
            when (value) {
                is Number -> RestorePoint.BeforeTransaction(value.toLong())
                is ZonedDateTime -> RestorePoint.BeforeTime(value.toInstant())
                else -> null
            }

        /** [point] as Neo4j keeps `seedRestoreUntil`: a transaction id, or a time. */
        fun storedValue(point: RestorePoint): Any =
            when (point) {
                is RestorePoint.BeforeTransaction -> point.transactionId
                is RestorePoint.BeforeTime -> ZonedDateTime.ofInstant(point.time, ZoneOffset.UTC)
            }

        private fun setOrRemove(node: Node, property: String, value: Any?) {
            if (value == null) node.removeProperty(property) else node.setProperty(property, value)
        }
    }
}

/**
 * The URI of a seed in the seeds directory of a server, `server.directories.seeds`, as Neo4j
 * Enterprise's ServerSeedProvider reads it: `server://<server id or name>/<artifact>`, or
 * `server://<server id or name>/` for the directory itself.
 *
 * @property server the server, by id or name.
 * @property artifact the file in its seeds directory; null for the directory.
 */
data class ServerSeedUri(val server: String, val artifact: String?) {
    /** The URI, with [server] in place of the one it named. */
    fun on(server: String): String = "$SCHEME://$server/${artifact ?: ""}"

    companion object {
        const val SCHEME = "server"

        /**
         * The server and artifact of [uri]; null if it is no `server:` URI.
         *
         * @throws IllegalArgumentException if it names no server, or a file outside the seeds
         *   directory.
         */
        fun parse(uri: String): ServerSeedUri? {
            val parsed =
                try {
                    URI(uri)
                } catch (_: URISyntaxException) {
                    return null
                }
            if (!SCHEME.equals(parsed.scheme, ignoreCase = true)) return null
            val server =
                parsed.authority?.takeIf { it.isNotBlank() }
                    ?: throw IllegalArgumentException(
                        "seedURI '$uri' names no server: give $SCHEME://<server id or " +
                            "name>/<artifact>, or $SCHEME://<server id or name>/ for the seeds " +
                            "directory"
                    )
            val artifact = parsed.path.orEmpty().trimStart('/').takeIf { it.isNotEmpty() }
            if (artifact != null) {
                val base = Path.of("seeds")
                val resolved = base.resolve(artifact).normalize()
                require(resolved.startsWith(base) && resolved != base) {
                    "seedURI '$uri' names a file outside server.directories.seeds"
                }
            }
            return ServerSeedUri(server, artifact)
        }
    }
}
