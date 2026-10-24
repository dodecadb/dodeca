package org.dozendb.core.edition

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.dbms.database.SystemGraphComponent
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Transaction
import org.neo4j.kernel.database.NormalizedDatabaseName

/**
 * Refuses to start with a default database, `initial.dbms.default_database`, that a database alias
 * is named like: Neo4j Community creates the default database of that name at every start if there
 * is none, which an alias's name stops with a constraint violation that names neither the setting
 * nor the alias. Neo4j does not support aliases there. Registered before Community's components, so
 * that its failure is the one the start reports.
 */
internal class DefaultDatabaseCheck(private val config: Config) : SystemGraphComponent {
    override fun componentName(): SystemGraphComponent.Name = NAME

    // It keeps nothing in the system graph.
    override fun detect(tx: Transaction): SystemGraphComponent.Status =
        SystemGraphComponent.Status.CURRENT

    override fun initializeSystemGraph(system: GraphDatabaseService, firstInitialization: Boolean) {
        val name =
            NormalizedDatabaseName(config.get(GraphDatabaseSettings.initial_default_database))
                .name()
        val alias =
            system.beginTx().use { tx ->
                tx.findNodes(
                        TopologyGraphDbmsModel.DATABASE_NAME_LABEL,
                        TopologyGraphDbmsModel.NAME_PROPERTY,
                        name,
                        TopologyGraphDbmsModel.NAMESPACE_PROPERTY,
                        TopologyGraphDbmsModel.DEFAULT_NAMESPACE,
                    )
                    .use { nodes ->
                        nodes.asSequence().any {
                            it.getProperty(TopologyGraphDbmsModel.PRIMARY_PROPERTY, true) == false
                        }
                    }
            }
        check(!alias) {
            "The default database '$name' that ${GraphDatabaseSettings.initial_default_database.name()} " +
                "names is a database alias, which cannot be the default database: drop the alias or " +
                "choose another default database."
        }
    }

    override fun upgradeToCurrent(system: GraphDatabaseService) {}

    private companion object {
        val NAME = SystemGraphComponent.Name("dozendb-default-database")
    }
}
