package org.dozendb.core.edition

import java.time.ZoneOffset
import java.time.ZonedDateTime
import org.dozendb.core.store.RestorePoint
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel
import org.neo4j.graphdb.Node
import org.neo4j.kernel.database.NamedDatabaseId

/**
 * Creates the store of a database from a seed: a backup or a dump at a URI, as `CREATE DATABASE ...
 * OPTIONS {seedURI: ...}` names. DozenDB's backup module provides one as a global dependency.
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
