package org.dozendb.cluster.internal

import org.dozendb.cluster.internal.durability.withSingleFsync
import org.neo4j.configuration.Config
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory
import org.neo4j.graphdb.facade.GraphDatabaseDependencies
import org.neo4j.kernel.impl.factory.DbmsInfo
import org.neo4j.logging.InternalLog

/** Starts DozenDB with clustering, as the bootstrapper does when clustering is enabled. */
object ClusteredDbms {
    /** Routes the logging of Ratis to Neo4j's logs through [log], then builds the DBMS. */
    fun build(
        config: Config,
        daemonMode: Boolean,
        dependencies: GraphDatabaseDependencies,
        log: InternalLog,
    ): DatabaseManagementService {
        Slf4jLogRouting.install(dependencies.userLogProvider(), log)
        return DatabaseManagementServiceFactory(DbmsInfo.ENTERPRISE, ::ClusterEditionModule)
            .build(config, daemonMode, withSingleFsync(config, dependencies))
    }
}
