package org.dozendb.cluster.internal.routing

import org.dozendb.cluster.internal.ReadConsistency
import org.dozendb.cluster.internal.reads.LinearizableLocalTransactions
import org.dozendb.cluster.internal.reads.LinearizableWaits
import org.dozendb.core.edition.DozenDbQueryRouterBootstrap
import org.neo4j.dbms.database.DatabaseContext
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.fabric.bookmark.LocalGraphTransactionIdTracker
import org.neo4j.fabric.executor.Location
import org.neo4j.graphdb.factory.module.GlobalModule
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog
import org.neo4j.kernel.database.DatabaseReferenceRepository
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.router.impl.transaction.database.LocalDatabaseTransactionFactory
import org.neo4j.router.location.LocationService
import org.neo4j.router.transaction.DatabaseTransactionFactory
import org.neo4j.router.transaction.RoutingInfo

/**
 * Neo4j Community's query router on a member. Transactions that run here wait for linearizable
 * reads ([LinearizableLocalTransactions]); with [forwarding], write transactions run on the leader
 * of their database when this member does not lead it.
 */
internal class ClusterQueryRouterBootstrap(
    globalModule: GlobalModule,
    databaseProvider: DatabaseContextProvider<out DatabaseContext>,
    databaseReferenceRepo: DatabaseReferenceRepository,
    securityLog: AbstractSecurityLog,
    private val defaultConsistency: ReadConsistency,
    private val awaitLinearizable: (NamedDatabaseId) -> Unit,
    private val forwarding: Forwarding?,
    remoteAliases: DatabaseTransactionFactory<Location.Remote>,
) :
    DozenDbQueryRouterBootstrap(
        globalModule.globalLife,
        globalModule.globalDependencies,
        globalModule.logService,
        databaseProvider,
        databaseReferenceRepo,
        securityLog,
        remoteAliases,
    ) {
    /** Where the transactions run ([locations]), and how they run on other members ([remote]). */
    class Forwarding(
        val locations: (RoutingInfo) -> LocationService,
        val remote: DatabaseTransactionFactory<Location.Remote>,
    )

    override fun internalLocations(routingInfo: RoutingInfo): LocationService =
        forwarding?.locations?.invoke(routingInfo) ?: super.internalLocations(routingInfo)

    override fun internalRemoteTransactions(): DatabaseTransactionFactory<Location.Remote> =
        forwarding?.remote ?: super.internalRemoteTransactions()

    override fun getLocalDatabaseTransactionFactory(
        databaseProvider: DatabaseContextProvider<out DatabaseContext>,
        transactionIdTracker: LocalGraphTransactionIdTracker,
    ): LocalDatabaseTransactionFactory =
        LinearizableLocalTransactions(
            databaseProvider,
            transactionIdTracker,
            LinearizableWaits(defaultConsistency, awaitLinearizable),
        )
}
