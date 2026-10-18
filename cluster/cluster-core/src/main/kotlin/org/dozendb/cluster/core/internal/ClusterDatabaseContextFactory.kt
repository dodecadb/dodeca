package org.dozendb.cluster.core.internal

import java.util.Optional
import org.neo4j.configuration.DatabaseConfig
import org.neo4j.cypher.internal.javacompat.DozerDbCypherEngineProvider
import org.neo4j.dbms.database.AbstractDatabaseContextFactory
import org.neo4j.dbms.database.DefaultDatabaseContextFactoryComponents
import org.neo4j.dbms.database.StandaloneDatabaseContext
import org.neo4j.dbms.identity.ServerIdentity
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.HostedOnMode
import org.neo4j.graphdb.factory.module.GlobalModule
import org.neo4j.graphdb.factory.module.ModularDatabaseCreationContext
import org.neo4j.graphdb.factory.module.id.IdContextFactory
import org.neo4j.io.device.DeviceMapper
import org.neo4j.kernel.api.Kernel
import org.neo4j.kernel.database.Database
import org.neo4j.kernel.database.DatabaseCreationContext
import org.neo4j.kernel.database.DatabaseTracers
import org.neo4j.kernel.database.GlobalAvailabilityGuardController
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.impl.api.ExternalIdReuseConditionProvider
import org.neo4j.kernel.impl.api.LeaseService
import org.neo4j.kernel.impl.api.TransactionalProcessFactory
import org.neo4j.kernel.impl.api.TransactionsFactory
import org.neo4j.kernel.impl.factory.AccessCapabilityFactory
import org.neo4j.kernel.impl.index.DatabaseIndexStats
import org.neo4j.kernel.impl.pagecache.CommunityVersionStorageFactory
import org.neo4j.kernel.impl.pagecache.IOControllerService
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats

/**
 * A port of `DozerDbDatabaseContextFactory` that gives every database [DozenDbConstraintSemantics].
 */
internal class ClusterDatabaseContextFactory(
    globalModule: GlobalModule,
    private val serverIdentity: ServerIdentity,
    private val transactionStatsFactory: DatabaseTransactionStats.Factory,
    private val indexStatsFactory: DatabaseIndexStats.Factory,
    idContextFactory: IdContextFactory,
    private val deviceMapper: DeviceMapper,
    private val controllerService: IOControllerService,
    private val commitProcessFactory: TransactionalProcessFactory,
    private val components: DefaultDatabaseContextFactoryComponents,
) :
    AbstractDatabaseContextFactory<StandaloneDatabaseContext, Optional<*>>(
        globalModule,
        idContextFactory,
    ) {

    override fun create(
        namedDatabaseId: NamedDatabaseId,
        ignored: Optional<*>,
    ): StandaloneDatabaseContext {
        val databaseConfig = DatabaseConfig(globalModule.globalConfig)
        val contextFactory = createContextFactory(databaseConfig, namedDatabaseId)
        lateinit var database: Database
        val creationContext =
            ModularDatabaseCreationContext(
                HostedOnMode.SINGLE,
                serverIdentity,
                namedDatabaseId,
                globalModule,
                globalModule.globalDependencies,
                contextFactory,
                deviceMapper,
                CommunityVersionStorageFactory(),
                databaseConfig,
                globalModule.globalMonitors,
                LeaseService.NO_LEASES,
                {
                    DatabaseCreationContext.selectStorageEngine(
                        globalModule.fileSystem,
                        globalModule.neo4jLayout,
                        databaseConfig,
                        namedDatabaseId,
                    )
                },
                DozenDbConstraintSemantics(),
                DozerDbCypherEngineProvider(),
                transactionStatsFactory.create(),
                indexStatsFactory.create(),
                ModularDatabaseCreationContext.defaultFileWatcherFilter(),
                AccessCapabilityFactory.configDependent(),
                ExternalIdReuseConditionProvider.NONE,
                idContextFactory.createIdContext(
                    namedDatabaseId,
                    contextFactory,
                    databaseConfig,
                    true,
                ),
                commitProcessFactory,
                createTokenHolderProvider {
                    database.dependencyResolver.resolveDependency(Kernel::class.java)
                },
                GlobalAvailabilityGuardController(globalModule.globalAvailabilityGuard),
                components.readOnlyDatabases(),
                controllerService,
                DatabaseTracers(globalModule.tracers, namedDatabaseId),
                globalModule.defaultCommandCommitListeners,
                TransactionsFactory.DEFAULT,
            )
        database = Database(creationContext)
        return StandaloneDatabaseContext(database)
    }
}
