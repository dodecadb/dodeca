package org.dozendb.core.edition

import java.time.Clock
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.dbms.database.DefaultSystemGraphComponent
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.database.NormalizedDatabaseName

/**
 * Neo4j Community's system graph component for databases, which takes the default database from
 * `initial.dbms.default_database` only while the system graph names none, as Neo4j Enterprise does.
 * Community's makes that setting's database the default at every start: it stops every other
 * database, and creates the setting's again if it was dropped. The first start marks the default
 * database in the system graph (see [DefaultDatabase]), which Neo4j 2026 no longer does.
 */
internal class FirstStartDefaultDatabase(config: Config, clock: Clock) :
    DefaultSystemGraphComponent(config, clock) {
    override fun initializeSystemGraphModel(system: GraphDatabaseService) {
        super.initializeSystemGraphModel(system)
        system.beginTx().use { tx ->
            tx.findNode(
                    TopologyGraphDbmsModel.DATABASE_LABEL,
                    TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY,
                    NormalizedDatabaseName(
                            config.get(GraphDatabaseSettings.initial_default_database)
                        )
                        .name(),
                )
                ?.setProperty(DefaultDatabase.PROPERTY, true)
            tx.commit()
        }
    }

    override fun verifySystemGraph(system: GraphDatabaseService) {
        if (DefaultDatabase.name(system) == null) super.verifySystemGraph(system)
    }
}
