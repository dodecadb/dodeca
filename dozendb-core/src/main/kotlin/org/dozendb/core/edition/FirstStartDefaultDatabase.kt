package org.dozendb.core.edition

import java.time.Clock
import org.neo4j.configuration.Config
import org.neo4j.dbms.database.DefaultSystemGraphComponent
import org.neo4j.graphdb.GraphDatabaseService

/**
 * Neo4j Community's system graph component for databases, which takes the default database from
 * `initial.dbms.default_database` only while the system graph names none, as Neo4j Enterprise does.
 * Community's makes that setting's database the default at every start: it stops the one
 * `dbms.setDefaultDatabase` made the default and creates the setting's again if it was dropped.
 */
internal class FirstStartDefaultDatabase(config: Config, clock: Clock) :
    DefaultSystemGraphComponent(config, clock) {
    override fun verifySystemGraph(system: GraphDatabaseService) {
        if (DefaultDatabase.name(system) == null) super.verifySystemGraph(system)
    }
}
