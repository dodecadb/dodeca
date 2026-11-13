package dev.dodeca.plugin

import dev.dodeca.cluster.internal.ClusteredDbms
import dev.dodeca.core.Dodeca
import dev.dodeca.core.DodecaSettings
import dev.dodeca.core.edition.DodecaEditionModule
import java.nio.file.Path
import java.time.Instant
import org.neo4j.configuration.Config
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.config.Configuration
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory
import org.neo4j.graphdb.facade.GraphDatabaseDependencies
import org.neo4j.kernel.impl.factory.DbmsInfo
import org.neo4j.server.NeoBootstrapper

/**
 * Starts Dodeca, with clustering if `dbms.dodeca.cluster.enabled` is set.
 *
 * Reports the Enterprise edition, as DozerDB did, so that tools offer what Dodeca supports beyond
 * Neo4j Community, such as many databases.
 */
class DodecaBootstrapper : NeoBootstrapper() {
    override fun createNeo(
        config: Config,
        daemonMode: Boolean,
        dependencies: GraphDatabaseDependencies,
    ): DatabaseManagementService {
        if (!config.get(DodecaSettings.clusterEnabled)) {
            log.info("Dodeca %s: clustering is disabled", Dodeca.version)
            return DatabaseManagementServiceFactory(DbmsInfo.ENTERPRISE, ::DodecaEditionModule)
                .build(config, daemonMode, dependencies)
        }
        log.info(
            "Dodeca %s: clustering is enabled, built for Neo4j %s",
            Dodeca.version,
            Dodeca.builtForNeo4j,
        )
        return ClusteredDbms.build(config, daemonMode, dependencies, log)
    }

    /** Neo4j Community's, which needs no license agreement. */
    override fun checkLicenseAgreement(
        homeDir: Path,
        config: Configuration,
        daemonMode: Boolean,
    ): Boolean = true

    /** Neo4j Community's, which has no license evaluation period to report. */
    override fun logOnLicenseEvaluation(
        homeDir: Path,
        config: Configuration,
        databaseCreationDate: Instant,
    ) {}
}
