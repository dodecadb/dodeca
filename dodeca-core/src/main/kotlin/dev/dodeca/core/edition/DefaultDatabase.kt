package dev.dodeca.core.edition

import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Transaction

/**
 * The default database as the system graph names it: the database `initial.dbms.default_database`
 * named at the first start, or the one `dbms.setDefaultDatabase` named since.
 */
object DefaultDatabase {
    /**
     * The property of the `Database` node of the default database, true there. Neo4j 2026
     * deprecates it, as Community's default database is the configured one alone; Dodeca's is the
     * system graph's, as Neo4j Enterprise's.
     */
    @Suppress("DEPRECATION") const val PROPERTY = TopologyGraphDbmsModel.DATABASE_DEFAULT_PROPERTY

    /** The name of the default database that [system]'s graph names; null if none. */
    fun name(system: GraphDatabaseService): String? = system.beginTx().use(::name)

    /** The name of the default database that the system graph of [tx] names; null if none. */
    fun name(tx: Transaction): String? =
        tx.findNodes(
                TopologyGraphDbmsModel.DATABASE_LABEL,
                PROPERTY,
                true,
            )
            .use { nodes ->
                nodes.asSequence().firstNotNullOfOrNull {
                    it.getProperty(TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY, null) as? String
                }
            }
}
