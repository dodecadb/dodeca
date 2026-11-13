package dev.dodeca.cluster.internal.http

import org.neo4j.server.modules.ServerModule
import org.neo4j.server.web.Injectable
import org.neo4j.server.web.WebServer

/**
 * Mounts the cluster status endpoints on Neo4j's [webServer]: those of a database at `/db`, next to
 * Neo4j's transactional endpoint, and those of every database at `/dbms`.
 */
internal class ClusterStatusModule(
    private val webServer: WebServer,
    private val statuses: ClusterStatuses,
) : ServerModule {
    override fun start() {
        val injectables = listOf(Injectable.injectable(ClusterStatuses::class.java, statuses))
        webServer.addJAXRSClasses(listOf(DatabaseClusterEndpoints::class.java), DB, injectables)
        webServer.addJAXRSClasses(listOf(DbmsClusterEndpoints::class.java), DBMS, injectables)
    }

    override fun stop() {
        webServer.removeJAXRSClasses(listOf(DatabaseClusterEndpoints::class.java), DB)
        webServer.removeJAXRSClasses(listOf(DbmsClusterEndpoints::class.java), DBMS)
    }

    companion object {
        const val DB = "/db"
        const val DBMS = "/dbms"
    }
}
