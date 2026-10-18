package org.dozendb.cluster.core.internal

import org.dozendb.cluster.core.DozenDbMetadata
import org.dozendb.cluster.core.internal.durability.withSingleFsync
import org.neo4j.configuration.Config
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory
import org.neo4j.graphdb.facade.GraphDatabaseDependencies
import org.neo4j.kernel.impl.factory.DbmsInfo
import org.neo4j.server.DozerDbBootstrapper

/**
 * Starts DozerDB, with clustering if `dbms.dozendb.cluster.enabled` is set.
 *
 * With clustering disabled it runs DozerDB's own bootstrap code unchanged, which is what makes the
 * plugin behave exactly like stock DozerDB.
 */
class ClusterBootstrapper : DozerDbBootstrapper() {
    override fun createNeo(
        config: Config,
        daemonMode: Boolean,
        dependencies: GraphDatabaseDependencies,
    ): DatabaseManagementService {
        if (!config.get(DozenDbSettings.clusterEnabled)) {
            log.info("DozenDB %s: clustering is disabled", DozenDbMetadata.version)
            return super.createNeo(config, daemonMode, dependencies)
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
}
