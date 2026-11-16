package dev.dodeca.core.edition

import java.time.Clock
import java.time.ZonedDateTime
import java.util.UUID
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
 * database, and creates the setting's again if it was dropped.
 *
 * Dodeca marks the default database in the system graph (see [DefaultDatabase]), which Neo4j 2026
 * no longer does: a system graph that Neo4j Community 2026 made, or DozerDB on it, marks none. The
 * first start of Dodeca on such a system graph marks the setting's database, and creates it if the
 * system graph has none of that name, as Community would; unlike Community, it leaves every other
 * database as it was.
 */
internal class FirstStartDefaultDatabase(config: Config, private val clock: Clock) :
    DefaultSystemGraphComponent(config, clock) {
    override fun initializeSystemGraphModel(system: GraphDatabaseService) {
        super.initializeSystemGraphModel(system)
        markDefault(system)
    }

    override fun verifySystemGraph(system: GraphDatabaseService) {
        if (DefaultDatabase.name(system) == null) markDefault(system)
    }

    /** Marks the database `initial.dbms.default_database` names, created if it does not exist. */
    private fun markDefault(system: GraphDatabaseService) {
        val name =
            NormalizedDatabaseName(config.get(GraphDatabaseSettings.initial_default_database))
        system.beginTx().use { tx ->
            val database =
                tx.findNode(
                    TopologyGraphDbmsModel.DATABASE_LABEL,
                    TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY,
                    name.name(),
                )
                    ?: createDatabaseNode(
                        tx,
                        name.name(),
                        UUID.randomUUID(),
                        ZonedDateTime.ofInstant(clock.instant(), clock.zone),
                        cypherVersionFromConfig(config.get(GraphDatabaseSettings.default_language)),
                    )
            database.setProperty(DefaultDatabase.PROPERTY, true)
            tx.commit()
        }
    }
}
