package org.dozendb.plugin

import java.nio.file.Path
import org.dozendb.cluster.internal.ClusteredDbms
import org.dozendb.core.DozenDb
import org.dozendb.core.DozenDbSettings
import org.dozendb.core.edition.DozenDbEditionModule
import org.neo4j.configuration.Config
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.config.Configuration
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory
import org.neo4j.graphdb.facade.GraphDatabaseDependencies
import org.neo4j.kernel.impl.factory.DbmsInfo
import org.neo4j.server.NeoBootstrapper

/**
 * Starts DozenDB, with clustering if `dbms.dozendb.cluster.enabled` is set.
 *
 * Reports the Enterprise edition, as DozerDB did, so that tools offer what DozenDB supports beyond
 * Neo4j Community, such as many databases.
 */
class DozenDbBootstrapper : NeoBootstrapper() {
    override fun createNeo(
        config: Config,
        daemonMode: Boolean,
        dependencies: GraphDatabaseDependencies,
    ): DatabaseManagementService {
        if (!config.get(DozenDbSettings.clusterEnabled)) {
            log.info("DozenDB %s: clustering is disabled", DozenDb.version)
            return DatabaseManagementServiceFactory(DbmsInfo.ENTERPRISE, ::DozenDbEditionModule)
                .build(config, daemonMode, dependencies)
        }
        log.info(
            "DozenDB %s: clustering is enabled, built for Neo4j %s",
            DozenDb.version,
            DozenDb.builtForNeo4j,
        )
        return ClusteredDbms.build(config, daemonMode, dependencies, log)
    }

    /** Neo4j Community's, which needs no license agreement. */
    override fun checkLicenseAgreement(
        homeDir: Path,
        config: Configuration,
        daemonMode: Boolean,
    ): Boolean = true
}
