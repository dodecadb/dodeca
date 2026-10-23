package org.dozendb.cluster.internal.forwarding

import org.neo4j.dbms.database.DatabaseContext
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.fabric.executor.Location
import org.neo4j.graphdb.factory.module.GlobalModule
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog
import org.neo4j.kernel.database.DatabaseReferenceRepository
import org.neo4j.router.CommunityQueryRouterBootstrap
import org.neo4j.router.location.LocationService
import org.neo4j.router.transaction.DatabaseTransactionFactory
import org.neo4j.router.transaction.RoutingInfo

/**
 * Neo4j Community's query router, whose transactions run where [locations] says: here, or on
 * another member through [remote].
 */
internal class ForwardingQueryRouterBootstrap(
    globalModule: GlobalModule,
    databaseProvider: DatabaseContextProvider<out DatabaseContext>,
    databaseReferenceRepo: DatabaseReferenceRepository,
    securityLog: AbstractSecurityLog,
    private val locations: (RoutingInfo) -> LocationService,
    private val remote: DatabaseTransactionFactory<Location.Remote>,
) :
    CommunityQueryRouterBootstrap(
        globalModule.globalLife,
        globalModule.globalDependencies,
        globalModule.logService,
        databaseProvider,
        databaseReferenceRepo,
        securityLog,
    ) {
    override fun createLocationService(routingInfo: RoutingInfo): LocationService =
        locations(routingInfo)

    override fun createRemoteDatabaseTransactionFactory():
        DatabaseTransactionFactory<Location.Remote> = remote
}
