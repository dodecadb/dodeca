package org.dozendb.cluster.core.internal

import org.dozendb.cluster.api.ConsensusLog
import org.dozendb.cluster.api.DatabaseRef
import org.dozendb.cluster.api.HostPort
import org.dozendb.cluster.api.MemberId
import org.dozendb.cluster.core.ClusterFormation
import org.dozendb.cluster.core.ClusterService
import org.dozendb.cluster.core.DozenDbMetadata
import org.dozendb.cluster.core.MemberDirectory
import org.dozendb.cluster.core.internal.durability.FsyncPolicy
import org.dozendb.cluster.core.internal.durability.SingleFsyncFileSystem
import org.dozendb.cluster.core.internal.reads.ConsistentBoltDatabases
import org.dozendb.cluster.core.internal.replication.CatchUp
import org.dozendb.cluster.core.internal.replication.DatabaseControl
import org.dozendb.cluster.core.internal.replication.DatabaseReconciler
import org.dozendb.cluster.core.internal.replication.ReplicationService
import org.dozendb.cluster.core.internal.routing.ClusterRoutingTableProvider
import org.dozendb.cluster.core.internal.storecopy.Neo4jLocalStores
import org.dozendb.cluster.core.internal.storecopy.StoreCopySource
import org.dozendb.cluster.ratis.RatisConsensusConfig
import org.dozendb.cluster.ratis.RatisConsensusLog
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.configuration.connectors.ConnectorType
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.dbms.CommunityDatabaseStateService
import org.neo4j.dbms.CommunityKernelPanicListener
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.dbms.database.DatabaseContextProviderDelegate
import org.neo4j.dbms.database.DatabaseReferenceCacheClearingListener
import org.neo4j.dbms.database.DatabaseRepository
import org.neo4j.dbms.database.DefaultSystemGraphInitializer
import org.neo4j.dbms.database.MultiDatabaseLifecycleService
import org.neo4j.dbms.database.MultiDatabaseManager
import org.neo4j.dbms.database.StandaloneDatabaseContext
import org.neo4j.dbms.database.SystemGraphComponents
import org.neo4j.dbms.database.SystemGraphInitializer
import org.neo4j.dbms.routing.ClientRoutingDomainChecker
import org.neo4j.dbms.routing.DefaultDatabaseAvailabilityChecker
import org.neo4j.dbms.routing.DefaultRoutingService
import org.neo4j.dbms.routing.LocalRoutingTableServiceValidator
import org.neo4j.dbms.routing.RoutingOption
import org.neo4j.dbms.routing.RoutingService
import org.neo4j.dbms.routing.RoutingTableTTLProvider
import org.neo4j.dbms.routing.SingleAddressRoutingTableProvider
import org.neo4j.dbms.systemgraph.SystemDatabaseProvider
import org.neo4j.graphdb.factory.module.GlobalModule
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule
import org.neo4j.graphdb.factory.module.edition.DozerDbEditionModule
import org.neo4j.io.device.DeviceMapper
import org.neo4j.kernel.api.procedure.GlobalProcedures
import org.neo4j.kernel.database.DatabaseId
import org.neo4j.kernel.database.DatabaseIdRepository
import org.neo4j.kernel.database.DatabaseReferenceRepository
import org.neo4j.kernel.database.MapCachingDatabaseIdRepository
import org.neo4j.kernel.database.MapCachingDatabaseReferenceRepository
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.database.SystemGraphDatabaseIdRepository
import org.neo4j.kernel.database.SystemGraphDatabaseReferenceRepository
import org.neo4j.kernel.impl.pagecache.CommunityIOControllerService
import org.neo4j.kernel.internal.Version
import org.neo4j.kernel.lifecycle.Lifecycle
import org.neo4j.kernel.lifecycle.LifecycleAdapter
import org.neo4j.server.security.systemgraph.CommunityDefaultDatabaseResolver

/**
 * DozerDB's edition module with clustering added. Only used when clustering is enabled.
 *
 * The cluster service is registered in the global lifecycle from the constructor, before DozerDB
 * registers its database starters, so consensus starts (and the databases are copied from another
 * member if needed) before any database starts, and stops after every database.
 *
 * Databases other than `system` and the default database are created, started, stopped and dropped
 * by the [DatabaseReconciler], which replaces DozerDB's system graph listener and its starter of
 * those databases.
 */
class ClusterEditionModule(globalModule: GlobalModule) : DozerDbEditionModule(globalModule) {
    private val cluster: ClusterService
    private val replication: ReplicationService
    private val directory: MemberDirectory
    private val consensus: ConsensusLog
    private val memberId = MemberId(identityModule.serverId().uuid())
    private val systemDatabase =
        DatabaseRef(
            NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID.name(),
            NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID.databaseId().uuid(),
        )
    private val localStores: Neo4jLocalStores

    private var clusterDefaultDatabaseInitializer: Lifecycle = LifecycleAdapter()

    /** DozerDB's database manager and databases, created with the database context provider. */
    private lateinit var databaseManager: MultiDatabaseManager
    private lateinit var databaseContexts: DatabaseRepository<StandaloneDatabaseContext>
    private lateinit var reconciler: DatabaseReconciler

    init {
        val config = globalModule.globalConfig
        val logService = globalModule.logService
        // Before anything creates a Ratis or DozenDB SLF4J logger.
        Slf4jLogRouting.useDebugLogProvider(logService.internalLogProvider)
        DozenDbConstraintSemantics.enableForClustering()
        val userLog = logService.getUserLog(ClusterService::class.java)
        val neo4jVersion = Version.getNeo4jVersion()
        val defaultDatabase = config.get(GraphDatabaseSettings.initial_default_database)
        lateinit var stores: Neo4jLocalStores
        lateinit var replicationService: ReplicationService
        val tuning =
            AbstractEditionModule.tryResolveOrCreate(
                ConsensusTuning::class.java,
                globalModule.externalDependencyResolver,
            ) {
                ConsensusTuning { it }
            }
        consensus =
            RatisConsensusLog(
                tuning.adjust(
                    RatisConsensusConfig(
                        memberId = memberId,
                        listenHost = config.get(DozenDbSettings.listenAddress).hostname,
                        listenPort = config.get(DozenDbSettings.listenAddress).port,
                        advertisedAddress =
                            config.get(DozenDbSettings.advertisedAddress).toHostPort(),
                        storageDirectory = config.get(DozenDbSettings.raftDirectory),
                        electionTimeoutMin = config.get(DozenDbSettings.electionTimeoutMin),
                        electionTimeoutMax = config.get(DozenDbSettings.electionTimeoutMax),
                        metadata = {
                            mapOf(
                                DozenDbMetadata.NEO4J_VERSION to neo4jVersion,
                                DozenDbMetadata.DOZENDB_VERSION to DozenDbMetadata.version,
                                DozenDbMetadata.HAS_STORES to stores.existed.toString(),
                                DozenDbMetadata.STORES_READY to
                                    replicationService.defaultDatabaseStarted.toString(),
                            ) +
                                listOfNotNull(
                                    boltAddress()?.let {
                                        DozenDbMetadata.BOLT_ADDRESS to it.toString()
                                    }
                                )
                        },
                    )
                )
            )
        stores =
            Neo4jLocalStores(
                globalModule.fileSystem,
                globalModule.neo4jLayout,
                listOf(systemDatabase.name, defaultDatabase),
                consensus,
                logService.getInternalLog(Neo4jLocalStores::class.java),
            )
        localStores = stores
        val catchUp =
            CatchUp(
                consensus,
                systemDatabase,
                { databaseControl() },
                stores::copyReplicated,
                logService.getInternalLog(CatchUp::class.java),
            )
        replicationService =
            ReplicationService(
                consensus,
                systemDatabase,
                defaultDatabase,
                catchUp,
                logService.internalLogProvider,
            )
        replication = replicationService
        val fileSystem = globalModule.fileSystem
        if (fileSystem is SingleFsyncFileSystem) {
            fileSystem.policy = FsyncPolicy(replicationService::singleFsync)
            userLog.info("Commits to replicated databases force only the Raft log (single fsync)")
        } else {
            userLog.info("Commits to replicated databases force the Raft log and Neo4j's log")
        }
        consensus.registerBulkSource(
            StoreCopySource.NAME,
            StoreCopySource(
                {
                    globalModule.globalDependencies.resolveDependency(
                        DatabaseContextProvider::class.java
                    )
                },
                globalModule.fileSystem,
                replicating = { id ->
                    replicationService.running().any { it.namedDatabaseId == id }
                },
                snapshots = { name ->
                    replicationService
                        .running()
                        .firstOrNull { it.ref.name == name }
                        ?.let { consensus.currentSnapshot(it.ref) }
                },
                log = logService.getInternalLog(StoreCopySource::class.java),
            ),
        )
        globalModule.databaseEventListeners.registerDatabaseEventListener(replication)
        val formation = ClusterFormation(consensus, initialMembers(config), neo4jVersion, userLog)
        directory =
            MemberDirectory(
                consensus,
                systemDatabase,
                ::boltAddress,
                logService.getInternalLog(MemberDirectory::class.java),
            )
        cluster =
            ClusterService(
                consensus,
                systemDatabase,
                formation,
                stores,
                userLog,
                { replication.running().map { it.ref } },
                directory,
            )
        globalModule.globalDependencies.satisfyDependency(cluster)
        globalModule.globalLife.add(
            object : LifecycleAdapter() {
                override fun start() = cluster.start()

                override fun stop() {
                    try {
                        cluster.stop()
                    } finally {
                        replication.close()
                    }
                }
            }
        )
    }

    /**
     * A port of DozerDB's wiring that differs only in the database context factory, which hosts
     * replicated databases on Raft. DozerDB keeps the default database starter in a private field,
     * hence [registerDefaultDatabaseInitializer].
     */
    override fun createDatabaseContextProvider(
        globalModule: GlobalModule
    ): DatabaseContextProvider<*> {
        val databaseContextFactory =
            ClusterDatabaseContextFactory(
                globalModule,
                identityModule,
                transactionMonitorFactory,
                indexMonitorFactory,
                AbstractEditionModule.createIdContextFactory(globalModule),
                DeviceMapper.UNKNOWN_MAPPER,
                CommunityIOControllerService(),
                createCommitProcessFactory(),
                this,
                replication,
            )
        val databaseIdRepository = MapCachingDatabaseIdRepository()
        val databaseRepository = DatabaseRepository<StandaloneDatabaseContext>(databaseIdRepository)
        val rootDatabaseIdRepository =
            AbstractEditionModule.tryResolveOrCreate(
                DatabaseIdRepository::class.java,
                globalModule.externalDependencyResolver,
            ) {
                SystemGraphDatabaseIdRepository(
                    { databaseRepository.getDatabaseContext(DatabaseId.SYSTEM_DATABASE_ID) },
                    globalModule.logService.internalLogProvider,
                )
            }
        val rootDatabaseReferenceRepository =
            AbstractEditionModule.tryResolveOrCreate(
                DatabaseReferenceRepository::class.java,
                globalModule.externalDependencyResolver,
            ) {
                SystemGraphDatabaseReferenceRepository(databaseRepository::getSystemDatabaseContext)
            }
        // DozerDB's constructor creates this repository; its getter only exposes the interface.
        val databaseReferenceRepository =
            databaseReferenceRepo as MapCachingDatabaseReferenceRepository
        databaseIdRepository.setDelegate(rootDatabaseIdRepository)
        databaseReferenceRepository.setDelegate(rootDatabaseReferenceRepository)
        val databaseIdCacheCleaner =
            DatabaseReferenceCacheClearingListener(
                databaseIdRepository,
                databaseReferenceRepository,
            )
        globalModule.globalLife.add(
            CommunityKernelPanicListener(globalModule.databaseEventListeners, databaseRepository)
        )
        val multiDatabaseManager =
            MultiDatabaseManager(globalModule, databaseRepository, databaseContextFactory)
        databaseManager = multiDatabaseManager
        databaseContexts = databaseRepository
        val defaultDatabase =
            globalModule.globalConfig.get(GraphDatabaseSettings.initial_default_database)
        reconciler =
            DatabaseReconciler(
                Neo4jSystemGraph(
                    { databaseRepository.getSystemDatabaseContext().databaseFacade() },
                    replication,
                ),
                Neo4jLocalDatabases(
                    multiDatabaseManager,
                    databaseRepository,
                    localStores,
                    replication,
                ),
                memberId,
                defaultDatabase,
                ::copySources,
                globalModule.logService.getInternalLog(DatabaseReconciler::class.java),
            )
        val databaseLifecycles =
            MultiDatabaseLifecycleService(
                multiDatabaseManager,
                databaseRepository,
                defaultDatabase,
                databaseContextFactory,
                globalModule.logService.internalLogProvider,
            )
        databaseStateService = CommunityDatabaseStateService(databaseRepository)
        globalModule.globalLife.add(databaseLifecycles.systemDatabaseStarter())
        globalModule.globalLife.add(databaseLifecycles.allDatabaseShutdown())
        globalModule.globalDependencies.satisfyDependency(
            DatabaseContextProviderDelegate.delegate(databaseRepository)
        )
        globalModule.globalDependencies.satisfyDependency(databaseStateService)
        globalModule.transactionEventListeners.registerTransactionEventListener(
            GraphDatabaseSettings.SYSTEM_DATABASE_NAME,
            databaseIdCacheCleaner,
        )
        clusterDefaultDatabaseInitializer = databaseLifecycles.defaultDatabaseStarter()
        globalModule.globalDependencies.satisfyDependency<SystemGraphComponents.UpgradeChecker>(
            SystemGraphComponents.UpgradeChecker.UPGRADE_ALWAYS_ALLOWED
        )
        // Transactions applied from Raft fire no transaction event listeners: refresh what the
        // listeners refresh after local commits.
        replication.afterSystemApply = {
            databaseIdRepository.invalidateAll()
            databaseReferenceRepository.invalidateAll()
            (defaultDatabaseResolver as? CommunityDefaultDatabaseResolver)?.clearCache()
            globalReadOnlyChecker?.refresh()
            reconciler.request()
        }
        return databaseRepository
    }

    /** The members to copy a new database from, [creator] first. */
    private fun copySources(creator: MemberId): List<HostPort> =
        consensus
            .members(systemDatabase)
            .filter { it.id != memberId }
            .sortedBy { it.id != creator }
            .map { it.raftAddress }

    /**
     * Neo4j's system graph initialiser, run only by the member that founds the cluster: it writes
     * the first system graph with local transactions, after which the system database is
     * replicated. Every other member copies that system graph, and changes to it come through Raft.
     */
    override fun registerDatabaseInitializers(
        globalModule: GlobalModule,
        systemDatabaseProvider: SystemDatabaseProvider,
    ) {
        val initializer =
            DefaultSystemGraphInitializer(systemDatabaseProvider::database, systemGraphComponents)
        val clusterInitializer =
            object : SystemGraphInitializer() {
                override fun initializeSystemGraph() {
                    if (!replication.systemBootstrapping) return
                    initializer.start()
                    replication.systemGraphInitialized()
                }
            }
        globalModule.globalDependencies.satisfyDependency(clusterInitializer)
        globalModule.globalLife.add(clusterInitializer)
        registerDefaultDatabaseInitializer(globalModule)
        // Once the system and default databases run; stops before the databases stop.
        globalModule.globalLife.add(
            object : LifecycleAdapter() {
                override fun start() = reconciler.start()

                override fun stop() = reconciler.close()
            }
        )
    }

    override fun registerDefaultDatabaseInitializer(globalModule: GlobalModule) {
        globalModule.globalLife.add(clusterDefaultDatabaseInitializer)
    }

    /** Stops and starts databases through DozerDB's database manager. */
    private fun databaseControl(): DatabaseControl =
        object : DatabaseControl {
            override fun stop(id: NamedDatabaseId) = databaseManager.stopDatabase(id)

            override fun start(id: NamedDatabaseId): Boolean {
                databaseManager.startDatabase(id)
                return databaseContexts
                    .getDatabaseContext(id)
                    .map { it.database().isStarted }
                    .orElse(false)
            }
        }

    /** This member's advertised Bolt address, with the bound port if the configured one is 0. */
    private fun boltAddress(): HostPort? {
        if (!globalModule.globalConfig.get(BoltConnector.enabled)) return null
        val advertised = globalModule.globalConfig.get(BoltConnector.advertised_address)
        if (advertised.port > 0) return advertised.toHostPort()
        val bound =
            globalModule.connectorPortRegister.getLocalAddress(ConnectorType.BOLT) ?: return null
        return HostPort(advertised.hostname, bound.port)
    }

    /**
     * DozerDB's routing service, port of its wiring, with routing tables of replicated databases
     * taken from their Raft groups. The member is alone, and drivers get DozerDB's single-address
     * tables, only in a one-member cluster.
     */
    override fun createRoutingService(
        databaseContextProvider: DatabaseContextProvider<*>,
        clientRoutingDomainChecker: ClientRoutingDomainChecker,
    ): RoutingService {
        val config = globalModule.globalConfig
        val logProvider = globalModule.logService.internalLogProvider
        val ttl = RoutingTableTTLProvider.ttlFromConfig(config)
        val singleAddress =
            SingleAddressRoutingTableProvider(
                globalModule.connectorPortRegister,
                RoutingOption.ROUTE_WRITE_AND_READ,
                config,
                logProvider,
                ttl,
            )
        val routing =
            ClusterRoutingTableProvider(
                replication,
                consensus,
                directory,
                config.get(DozenDbSettings.readsOnLeader),
                ttl,
                singleAddress,
                singleAddress,
            )
        return DefaultRoutingService(
            logProvider,
            LocalRoutingTableServiceValidator(
                DefaultDatabaseAvailabilityChecker(databaseContextProvider)
            ),
            routing,
            routing,
            clientRoutingDomainChecker,
            config,
            { cluster.members().size <= 1 },
            defaultDatabaseResolver,
            databaseReferenceRepo,
            true,
            globalModule.globalClock,
        )
    }

    override fun createBoltDatabaseManagementServiceProvider():
        BoltGraphDatabaseManagementServiceSPI =
        ConsistentBoltDatabases(
            super.createBoltDatabaseManagementServiceProvider(),
            replication,
            globalModule.globalConfig.get(DozenDbSettings.defaultReadConsistency),
        )

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
