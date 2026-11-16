package dev.dodeca.core.edition

import org.neo4j.collection.Dependencies
import org.neo4j.dbms.database.DatabaseContext
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.fabric.executor.Location
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog
import org.neo4j.kernel.database.DatabaseReferenceImpl
import org.neo4j.kernel.database.DatabaseReferenceRepository
import org.neo4j.kernel.lifecycle.LifeSupport
import org.neo4j.logging.internal.LogService
import org.neo4j.router.CommunityQueryRouterBootstrap
import org.neo4j.router.location.LocationService
import org.neo4j.router.transaction.DatabaseTransactionFactory
import org.neo4j.router.transaction.RoutingInfo

/**
 * Neo4j Community's query router, which runs transactions in remote aliases on the DBMS each names,
 * with [remoteAliases], and every other transaction where [internalLocations] puts it: on this
 * server, unless an edition runs some elsewhere with [internalRemoteTransactions]. A transaction
 * that names a database while `system` is unavailable gets a transient error.
 */
open class DodecaQueryRouterBootstrap(
    lifeSupport: LifeSupport,
    dependencies: Dependencies,
    logService: LogService,
    databaseProvider: DatabaseContextProvider<out DatabaseContext>,
    databaseReferenceRepo: DatabaseReferenceRepository,
    securityLog: AbstractSecurityLog,
    private val remoteAliases: DatabaseTransactionFactory<Location.Remote>,
) :
    CommunityQueryRouterBootstrap(
        lifeSupport,
        dependencies,
        logService,
        databaseProvider,
        TransientWhileSystemUnavailable(databaseReferenceRepo),
        securityLog,
    ) {
    /** Where the databases of this DBMS run a transaction's part: here, as in Community. */
    protected open fun internalLocations(routingInfo: RoutingInfo): LocationService =
        super.createLocationService(routingInfo)

    /**
     * How parts of transactions that [internalLocations] puts elsewhere run: Community has none.
     */
    protected open fun internalRemoteTransactions(): DatabaseTransactionFactory<Location.Remote> =
        super.createRemoteDatabaseTransactionFactory()

    final override fun createLocationService(routingInfo: RoutingInfo): LocationService {
        val internal = internalLocations(routingInfo)
        return LocationService { reference ->
            if (reference is DatabaseReferenceImpl.External) {
                Location.Remote.External(GRAPH_ID, reference)
            } else {
                internal.locationOf(reference)
            }
        }
    }

    final override fun createRemoteDatabaseTransactionFactory():
        DatabaseTransactionFactory<Location.Remote> {
        val internal = internalRemoteTransactions()
        return DatabaseTransactionFactory { location, info, bookmarks, terminated, constituents ->
            val factory = if (location is Location.Remote.External) remoteAliases else internal
            factory.beginTransaction(location, info, bookmarks, terminated, constituents)
        }
    }

    private companion object {
        /** No graph of a composite database: as Community's locations of local databases. */
        const val GRAPH_ID = -1L
    }
}
