package org.dozendb.cluster.core.internal

import java.nio.file.Path
import org.dozendb.cluster.core.DozenDbMetadata
import org.dozendb.cluster.core.internal.durability.withSingleFsync
import org.dozendb.cluster.core.internal.edition.DozenDbEditionModule
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
class ClusterBootstrapper : NeoBootstrapper() {
    override fun createNeo(
        config: Config,
        daemonMode: Boolean,
        dependencies: GraphDatabaseDependencies,
    ): DatabaseManagementService {
        if (!config.get(DozenDbSettings.clusterEnabled)) {
            log.info("DozenDB %s: clustering is disabled", DozenDbMetadata.version)
            return DatabaseManagementServiceFactory(DbmsInfo.ENTERPRISE, ::DozenDbEditionModule)
                .build(config, daemonMode, dependencies)
        }
        log.info(
            "DozenDB %s: clustering is enabled, built for Neo4j %s",
            DozenDbMetadata.version,
            DozenDbMetadata.builtForNeo4j,
        )
        Slf4jLogRouting.install(dependencies.userLogProvider(), log)
        return DatabaseManagementServiceFactory(DbmsInfo.ENTERPRISE, ::ClusterEditionModule)
            .build(config, daemonMode, withSingleFsync(config, dependencies))
    }

    /** Neo4j Community's, which needs no license agreement. */
    override fun checkLicenseAgreement(
        homeDir: Path,
        config: Configuration,
        daemonMode: Boolean,
    ): Boolean = true
}
