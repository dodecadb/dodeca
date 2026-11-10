package org.dozendb.core.edition

import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.database.NamedDatabaseId

/** The store formats `CREATE DATABASE ... OPTIONS {storeFormat: ...}` asked for. */
internal object StoreFormats {
    /**
     * The format the system graph of [system] keeps for the store of [database], if a store is
     * created for it, as `CREATE DATABASE` asked; null for the configured `db.format`.
     */
    fun requested(system: () -> GraphDatabaseService, database: NamedDatabaseId): String? {
        if (database.isSystemDatabase) return null
        return system().beginTx().use { tx ->
            tx.findNode(
                    TopologyGraphDbmsModel.DATABASE_LABEL,
                    TopologyGraphDbmsModel.DATABASE_UUID_PROPERTY,
                    database.databaseId().uuid().toString(),
                )
                ?.getProperty(TopologyGraphDbmsModel.DATABASE_STORE_FORMAT_NEW_DB_PROPERTY, null)
                as? String
        }
    }
}
