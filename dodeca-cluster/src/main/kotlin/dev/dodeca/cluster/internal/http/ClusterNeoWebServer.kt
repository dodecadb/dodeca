package dev.dodeca.cluster.internal.http

import dev.dodeca.cluster.internal.ClusterSettings
import dev.dodeca.core.http.DodecaNeoWebServer
import java.util.regex.Pattern
import org.neo4j.bolt.tx.TransactionManager
import org.neo4j.collection.Dependencies
import org.neo4j.configuration.Config
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.kernel.impl.factory.DbmsInfo
import org.neo4j.logging.InternalLogProvider
import org.neo4j.memory.MemoryPools
import org.neo4j.monitoring.Monitors
import org.neo4j.server.configuration.ConfigurableServerModules
import org.neo4j.server.configuration.ServerSettings
import org.neo4j.server.modules.ServerModule
import org.neo4j.server.rest.discovery.DiscoverableURIs
import org.neo4j.time.SystemNanoClock

/**
 * Dodeca's web server with Neo4j Enterprise's cluster status endpoints, which
 * `server.http_enabled_modules` turns on with `ENTERPRISE_MANAGEMENT_ENDPOINTS`, as in Enterprise.
 * They need a login unless `dbms.security.cluster_status_auth_enabled` is false.
 */
internal class ClusterNeoWebServer(
    managementService: DatabaseManagementService,
    transactionManager: TransactionManager,
    dependencies: Dependencies,
    config: Config,
    userLogProvider: InternalLogProvider,
    dbmsInfo: DbmsInfo,
    memoryPools: MemoryPools,
    monitors: Monitors,
    clock: SystemNanoClock,
    private val statuses: ClusterStatuses,
) :
    DodecaNeoWebServer(
        managementService,
        transactionManager,
        dependencies,
        config,
        userLogProvider,
        dbmsInfo,
        memoryPools,
        monitors,
        clock,
    ) {
    override fun extendModules(discoverableURIs: DiscoverableURIs.Builder): Iterable<ServerModule> {
        val enabled = config.get(ServerSettings.http_enabled_modules)
        return if (ConfigurableServerModules.ENTERPRISE_MANAGEMENT_ENDPOINTS in enabled) {
            listOf(ClusterStatusModule(webServer, statuses))
        } else {
            emptyList()
        }
    }

    override fun getUriWhitelist(): List<Pattern> =
        if (config.get(ClusterSettings.clusterStatusAuthEnabled)) {
            super.getUriWhitelist()
        } else {
            super.getUriWhitelist() + UNAUTHENTICATED
        }

    private companion object {
        /** The cluster status endpoints, as Neo4j matches the paths that need no login. */
        val UNAUTHENTICATED: List<Pattern> =
            listOf(Pattern.compile("/db/[^/]+/cluster/.*"), Pattern.compile("/dbms/cluster/status"))
    }
}
