package org.dozendb.cluster.core.internal

import org.dozendb.cluster.api.DatabaseRef
import org.dozendb.cluster.api.HostPort
import org.dozendb.cluster.api.MemberId
import org.dozendb.cluster.core.ClusterFormation
import org.dozendb.cluster.core.ClusterService
import org.dozendb.cluster.core.DozenDbMetadata
import org.dozendb.cluster.ratis.RatisConsensusConfig
import org.dozendb.cluster.ratis.RatisConsensusLog
import org.neo4j.configuration.Config
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.graphdb.factory.module.GlobalModule
import org.neo4j.graphdb.factory.module.edition.DozerDbEditionModule
import org.neo4j.kernel.api.procedure.GlobalProcedures
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.internal.Version
import org.neo4j.kernel.lifecycle.LifecycleAdapter

/**
 * DozerDB's edition module with clustering added. Only used when clustering is enabled.
 *
 * The cluster service is registered in the global lifecycle from the constructor, before DozerDB
 * registers its database starters, so consensus starts before and stops after every database.
 */
class ClusterEditionModule(globalModule: GlobalModule) : DozerDbEditionModule(globalModule) {
    private val cluster: ClusterService

    init {
        val config = globalModule.globalConfig
        val logService = globalModule.logService
        // Before anything creates a Ratis or DozenDB SLF4J logger.
        Slf4jLogRouting.useDebugLogProvider(logService.internalLogProvider)
        val userLog = logService.getUserLog(ClusterService::class.java)
        val neo4jVersion = Version.getNeo4jVersion()
        val consensus =
            RatisConsensusLog(
                RatisConsensusConfig(
                    memberId = MemberId(identityModule.serverId().uuid()),
                    listenHost = config.get(DozenDbSettings.listenAddress).hostname,
                    listenPort = config.get(DozenDbSettings.listenAddress).port,
                    advertisedAddress = config.get(DozenDbSettings.advertisedAddress).toHostPort(),
                    storageDirectory = config.get(DozenDbSettings.raftDirectory),
                    electionTimeoutMin = config.get(DozenDbSettings.electionTimeoutMin),
                    electionTimeoutMax = config.get(DozenDbSettings.electionTimeoutMax),
                    metadata = {
                        mapOf(
                            DozenDbMetadata.NEO4J_VERSION to neo4jVersion,
                            DozenDbMetadata.DOZENDB_VERSION to DozenDbMetadata.version,
                        )
                    },
                )
            )
        val systemDatabase =
            DatabaseRef(
                NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID.name(),
                NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID.databaseId().uuid(),
            )
        val formation = ClusterFormation(consensus, initialMembers(config), neo4jVersion, userLog)
        cluster = ClusterService(consensus, systemDatabase, formation, userLog)
        globalModule.globalDependencies.satisfyDependency(cluster)
        globalModule.globalLife.add(
            object : LifecycleAdapter() {
                override fun start() = cluster.start()

                override fun stop() = cluster.stop()
            }
        )
    }

    override fun registerEditionSpecificProcedures(
        globalProcedures: GlobalProcedures,
        databaseContextProvider: DatabaseContextProvider<*>,
    ) {
        super.registerEditionSpecificProcedures(globalProcedures, databaseContextProvider)
        globalProcedures.registerComponent(ClusterService::class.java, { cluster }, true)
        globalProcedures.registerProcedure(ClusterProcedures::class.java)
    }

    private companion object {
        fun SocketAddress.toHostPort(): HostPort = HostPort(hostname, port)

        fun initialMembers(config: Config): List<HostPort> =
            config.get(DozenDbSettings.initialMembers).map { it.toHostPort() }
    }
}
