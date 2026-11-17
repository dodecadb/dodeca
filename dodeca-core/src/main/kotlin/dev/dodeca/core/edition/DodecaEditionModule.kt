/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Modifications Copyright (c) DozerDB
 * https://dozerdb.org
 *
 * Modifications Copyright (c) The Dodeca Authors
 * 2026-10-19: ported to Kotlin from DozerDB's DozerDbEditionModule, now a subclass of Neo4j's
 * CommunityEditionModule that keeps Community's user security component as a dependency, which
 * ALTER USER needs.
 * 2026-11-11: Neo4j 2026's fleet discovery is off unless the operator turns it on.
 * 2026-11-12: a web server whose HTTPS takes TLS reloads on the connector it has.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */
package dev.dodeca.core.edition

import dev.dodeca.core.DodecaSettings
import dev.dodeca.core.SettingsProcedures
import dev.dodeca.core.checkpoint.CheckpointIoControllerService
import dev.dodeca.core.http.DodecaNeoWebServer
import dev.dodeca.core.logging.QueryLog
import dev.dodeca.core.logging.SecurityLog
import dev.dodeca.core.monitoring.MetricsService
import dev.dodeca.core.monitoring.MonitoringProcedures
import dev.dodeca.core.remote.RemoteAliasTransactions
import dev.dodeca.core.routing.DodecaRoutingService
import dev.dodeca.core.routing.LocalRoutingTables
import dev.dodeca.core.routing.SingleAddressRoutingTable
import dev.dodeca.core.security.AliasPasswords
import dev.dodeca.core.security.DodecaDefaultDatabaseResolver
import dev.dodeca.core.security.DodecaSecurityComponent
import dev.dodeca.core.security.DodecaSecurityModule
import dev.dodeca.core.security.MemberLogins
import dev.dodeca.core.security.MemberRealm
import dev.dodeca.core.security.RefusingMemberRealm
import dev.dodeca.core.security.SystemGraphSecurity
import dev.dodeca.core.servers.DatabasePlacement
import dev.dodeca.core.servers.Placements
import dev.dodeca.core.servers.ServerOptions
import dev.dodeca.core.servers.Servers
import dev.dodeca.core.servers.SingleServerPlacement
import dev.dodeca.core.servers.SingleServerTopology
import dev.dodeca.core.ssl.TlsProcedures
import dev.dodeca.core.store.StorePlacement
import java.util.UUID
import org.neo4j.bolt.tx.TransactionManager
import org.neo4j.collection.Dependencies
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.configuration.connectors.ConnectorType
import org.neo4j.configuration.connectors.HttpConnector
import org.neo4j.configuration.connectors.HttpsConnector
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.dbms.CommunityKernelPanicListener
import org.neo4j.dbms.DatabaseStateService
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.dbms.database.CommunityDatabaseObjectRepositoryModelProvider
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.dbms.database.DatabaseContextProviderDelegate
import org.neo4j.dbms.database.DatabaseOperationCounts
import org.neo4j.dbms.database.DatabaseReferenceCacheClearingListener
import org.neo4j.dbms.database.DatabaseRepository
import org.neo4j.dbms.database.DefaultDatabaseDetailsExtrasProvider
import org.neo4j.dbms.database.DefaultSystemGraphComponent
import org.neo4j.dbms.database.DefaultTopologyInfoService
import org.neo4j.dbms.database.StandaloneDatabaseContext
import org.neo4j.dbms.database.SystemGraphComponent
import org.neo4j.dbms.database.SystemGraphComponents
import org.neo4j.dbms.database.TopologyInfoService
import org.neo4j.dbms.routing.ClientRoutingDomainChecker
import org.neo4j.dbms.routing.RoutingService
import org.neo4j.dbms.systemgraph.ContextBasedSystemDatabaseProvider
import org.neo4j.dbms.systemgraph.SystemDatabaseProvider
import org.neo4j.fabric.executor.Location
import org.neo4j.graphdb.Transaction
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.module.GlobalModule
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog
import org.neo4j.io.device.DeviceMapper
import org.neo4j.kernel.api.procedure.GlobalProcedures
import org.neo4j.kernel.api.security.AuthManager
import org.neo4j.kernel.api.security.provider.NoAuthSecurityProvider
import org.neo4j.kernel.api.security.provider.SecurityProvider
import org.neo4j.kernel.database.DatabaseIdRepository
import org.neo4j.kernel.database.DatabaseReferenceRepository
import org.neo4j.kernel.database.MapCachingDatabaseIdRepository
import org.neo4j.kernel.database.MapCachingDatabaseReferenceRepository
import org.neo4j.kernel.database.ModelBasedDatabaseIdRepository
import org.neo4j.kernel.database.ModelBasedDatabaseReferenceRepository
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.impl.factory.DbmsInfo
import org.neo4j.kernel.lifecycle.Lifecycle
import org.neo4j.kernel.lifecycle.LifecycleAdapter
import org.neo4j.logging.InternalLogProvider
import org.neo4j.logging.log4j.LoggerTarget
import org.neo4j.router.transaction.DatabaseTransactionFactory

/**
 * Neo4j Community's edition module with many databases, which follow the `Database` nodes of the
 * system graph, and with Dodeca's constraint semantics and administration commands.
 *
 * Without clustering, [StandaloneDatabases] makes the databases follow the system graph. The
 * cluster's edition module hosts the databases on Raft through [hosting] and makes them follow the
 * replicated system graph instead.
 */
open class DodecaEditionModule(globalModule: GlobalModule) : CommunityEditionModule(globalModule) {
    init {
        // Neo4j sends usage data to udc.neo4j.com unless told not to, and would report this server
        // as Neo4j Enterprise: Dodeca sends it only when the operator turns it on.
        globalModule.globalConfig.setIfNotSet(GraphDatabaseSettings.udc_enabled, false)
        // Neo4j 2026 announces each server on the local network for Neo4j's fleet tools, as Neo4j
        // Enterprise here: Dodeca announces it only when the operator turns it on.
        globalModule.globalConfig.setIfNotSet(BoltConnector.enable_discovery, false)
        // Refuses initial.server.* settings that no server could be enabled with.
        ServerOptions.initial(globalModule.globalConfig)
    }

    protected lateinit var databaseManager: DatabaseManager
        private set

    protected lateinit var databaseRepository: DatabaseRepository<StandaloneDatabaseContext>
        private set

    /** Caches the ids of the databases the system graph names. */
    protected lateinit var databaseIdCache: MapCachingDatabaseIdRepository
        private set

    /** Creates and starts the default database. */
    protected var defaultDatabaseStarter: Lifecycle = LifecycleAdapter()
        private set

    /** The copies of databases in quarantine on this server. */
    protected lateinit var quarantines: Quarantines
        private set

    /** Puts copies of databases in quarantine and lifts it. */
    protected lateinit var quarantine: DatabaseQuarantine
        private set

    /** The databases registered on this server. */
    private lateinit var databases: DatabaseRepository<StandaloneDatabaseContext>

    // Once called: the cluster's servers and members are known only once it started.
    private val unquarantines by lazy {
        Unquarantines(
            { databases.getSystemDatabaseContext().databaseFacade() },
            servers(),
            identityModule.serverId().uuid(),
            quarantine,
            remoteQuarantines(),
        )
    }

    /** Makes the databases follow the system graph without clustering, once created. */
    private var standaloneDatabases: StandaloneDatabases? = null

    /** Hosts the databases together with other servers; null when this server alone does. */
    protected open val hosting: DatabaseHosting?
        get() = null

    protected val defaultDatabase: String
        get() = globalModule.globalConfig.get(GraphDatabaseSettings.initial_default_database)

    /** Neo4j Community's wiring with Dodeca's database manager and database context factory. */
    override fun createDatabaseContextProvider(
        globalModule: GlobalModule
    ): DatabaseContextProvider<*> {
        val idCache = MapCachingDatabaseIdRepository()
        val repository = DatabaseRepository<StandaloneDatabaseContext>(idCache)
        databases = repository
        val contextFactory =
            DodecaDatabaseContextFactory(
                globalModule,
                identityModule,
                transactionMonitorFactory,
                indexMonitorFactory,
                AbstractEditionModule.createIdContextFactory(globalModule),
                DeviceMapper.UNKNOWN_MAPPER,
                CheckpointIoControllerService(globalModule.jobScheduler),
                createCommitProcessFactory(),
                this,
                hosting,
                { securityLog },
            ) {
                StoreFormats.requested(
                    { repository.getSystemDatabaseContext().databaseFacade() },
                    it,
                )
            }
        val systemDatabase =
            ContextBasedSystemDatabaseProvider(repository, globalModule.databaseEventListeners)
        // The databases and aliases of the system graph, as Neo4j Community reads them.
        val model = CommunityDatabaseObjectRepositoryModelProvider(systemDatabase)
        idCache.setDelegate(
            AbstractEditionModule.tryResolveOrCreate(
                DatabaseIdRepository::class.java,
                globalModule.externalDependencyResolver,
            ) {
                ModelBasedDatabaseIdRepository(model)
            }
        )
        referenceCache =
            globalModule.globalDependencies.satisfyDependency(
                MapCachingDatabaseReferenceRepository(
                    AbstractEditionModule.tryResolveOrCreate(
                        DatabaseReferenceRepository::class.java,
                        globalModule.externalDependencyResolver,
                    ) {
                        ModelBasedDatabaseReferenceRepository(model)
                    }
                )
            )
        globalModule.globalLife.add(
            CommunityKernelPanicListener(globalModule.databaseEventListeners, repository)
        )
        quarantines =
            Quarantines(globalModule.neo4jLayout.dataDirectory().resolve(Quarantines.FILE_NAME))
        quarantine =
            DatabaseQuarantine(
                quarantines,
                repository,
                quarantineCopies(),
                identityModule.serverId().uuid().toString(),
                globalModule.globalClock,
                globalModule.logService.getUserLog(DatabaseQuarantine::class.java),
            )
        // Before ShutdownOnPanic, so that a copy that panics is in quarantine when the server ends.
        globalModule.databaseEventListeners.registerDatabaseEventListener(quarantine)
        globalModule.databaseEventListeners.registerDatabaseEventListener(
            ShutdownOnPanic(
                globalModule.globalConfig,
                globalModule.logService.getUserLog(ShutdownOnPanic::class.java),
                AbstractEditionModule.tryResolveOrCreate(
                    ServerExit::class.java,
                    globalModule.externalDependencyResolver,
                ) {
                    ServerExit.PROCESS
                },
            )
        )
        val manager =
            DatabaseManager(
                repository,
                contextFactory,
                { DefaultDatabase.name(repository.getSystemDatabaseContext().databaseFacade()) },
                globalModule.globalDependencies.resolveDependency(
                    DatabaseOperationCounts.Counter::class.java
                ),
                StorePlacement(globalModule.fileSystem, globalModule.neo4jLayout),
                quarantines,
                globalModule.fileSystem,
                globalModule.logService.internalLogProvider,
                beforeStart = { id -> hosting?.beforeStart(id) },
            )
        stateService = DodecaDatabaseStateService(repository, quarantines)
        // Before the databases start, so that it sees them start.
        MetricsService.create(globalModule, repository)?.let {
            globalModule.globalLife.add(it)
            globalModule.globalDependencies.satisfyDependency(it)
        }
        globalModule.globalLife.add(
            QueryLog(
                globalModule.globalConfig,
                globalModule.globalMonitors,
                globalModule.logService.internalLogProvider.getLog(LoggerTarget.QUERY_LOGGER),
            )
        )
        globalModule.globalLife.add(manager.systemDatabaseStarter())
        globalModule.globalLife.add(manager.allDatabaseStopper())
        globalModule.globalDependencies.satisfyDependency(
            DatabaseContextProviderDelegate.delegate(repository)
        )
        globalModule.globalDependencies.satisfyDependency(stateService)
        globalModule.transactionEventListeners.registerTransactionEventListener(
            GraphDatabaseSettings.SYSTEM_DATABASE_NAME,
            DatabaseReferenceCacheClearingListener(idCache, referenceCache),
        )
        globalModule.globalDependencies.satisfyDependency<SystemGraphComponents.UpgradeChecker>(
            SystemGraphComponents.UpgradeChecker.UPGRADE_ALWAYS_ALLOWED
        )
        // The default Cypher version of each database and remote alias, which the query router
        // asks for.
        val languages =
            DefaultLanguages(
                systemDatabase,
                globalModule.jobScheduler,
                globalModule.logService.internalLogProvider,
            )
        globalModule.globalDependencies.satisfyDependency(languages)
        globalModule.globalLife.add(languages.life)
        globalModule.transactionEventListeners.registerTransactionEventListener(
            GraphDatabaseSettings.SYSTEM_DATABASE_NAME,
            languages.listener,
        )
        defaultLanguages = languages
        databaseManager = manager
        databaseRepository = repository
        databaseIdCache = idCache
        defaultDatabaseStarter = manager.defaultDatabaseStarter()
        // For WAIT; asks the servers only when a command waits.
        globalModule.globalDependencies.satisfyDependency(DatabaseProgress(::hostingServers))
        // For backups taken from this server.
        globalModule.globalDependencies.satisfyDependency(latestWrites())
        seedServers()?.let { globalModule.globalDependencies.satisfyDependency(it) }
        // For the server commands.
        globalModule.globalDependencies.satisfyDependency(servers())
        // For backups with --remote-address-resolution.
        globalModule.globalDependencies.satisfyDependency(backupSources())
        // For the database commands.
        globalModule.globalDependencies.satisfyDependency(placement())
        return repository
    }

    /**
     * How quarantines stop and bring back copies of databases: without clustering, through the
     * database manager, and by following the system graph.
     */
    protected open fun quarantineCopies(): DatabaseQuarantine.Copies =
        object : DatabaseQuarantine.Copies {
            override fun stop(id: NamedDatabaseId) = databaseManager.stopDatabase(id)

            override fun reconcile(id: NamedDatabaseId) {
                if (id.isSystemDatabase) databaseManager.startDatabase(id)
                else standaloneDatabases?.follow()
            }

            override fun replace(id: NamedDatabaseId, keepStore: Boolean) =
                throw IllegalStateException(
                    "Database '${id.name()}': a server without clustering has no cluster state " +
                        "to replace. Lift its quarantine with " +
                        "${UnquarantineOperation.KEEP_STATE_KEEP_STORE.label}."
                )
        }

    /** How quarantines are lifted on other servers: without clustering, there are none. */
    protected open fun remoteQuarantines(): Unquarantines.Remote? = null

    /**
     * The servers a database can be created from with existingDataSeedServer: without clustering,
     * none.
     */
    protected open fun seedServers(): SeedServers? = null

    /** Where the copies of databases go: without clustering, on this server alone. */
    protected open fun placement(): DatabasePlacement = SingleServerPlacement

    /** The servers the server commands act on: without clustering, this one. */
    protected open fun servers(): Servers =
        object : Servers {
            private val self = identityModule.serverId().uuid()

            override fun ids(): Set<UUID> = setOf(self)

            override fun initialOptions(id: UUID): ServerOptions? =
                if (id == self) {
                    ServerOptions.initial(globalModule.globalConfig)
                } else {
                    null
                }

            override fun address(id: UUID): String? = null

            override fun holding(tx: Transaction, id: UUID): Set<String> =
                if (id == self) Placements.placed(tx).mapTo(HashSet()) { it.name } else emptySet()
        }

    /** What creates databases from seeds: Dodeca's backup module, once Neo4j created it. */
    protected fun seeder(): DatabaseSeeder? =
        globalModule.globalDependencies
            .resolveTypeDependencies(DatabaseSeeder::class.java)
            .firstOrNull()

    /** Waits until this server holds the writes acknowledged before: without clustering, none. */
    protected open fun latestWrites(): LatestWrites = LatestWrites.LOCAL

    /** The servers that host [wanted], which WAIT waits for: without clustering, this one. */
    protected open fun hostingServers(wanted: WantedDatabase): List<HostingServer> =
        listOf(
            object : HostingServer {
                override val id: UUID = identityModule.serverId().uuid()
                override val address: String = boltAddress()?.toString() ?: "localhost"

                override fun databases(): List<DatabaseState> = databaseStates()
            }
        )

    /** The databases of this server; none before they are created. */
    protected fun databaseStates(): List<DatabaseState> =
        if (::databaseRepository.isInitialized) databaseRepository.databaseStates(quarantines)
        else emptyList()

    /** This server's advertised Bolt address, with the bound port if the configured one is 0. */
    protected fun boltAddress(): SocketAddress? {
        val config = globalModule.globalConfig
        if (!config.get(BoltConnector.enabled)) return null
        val advertised = config.get(BoltConnector.advertised_address)
        if (advertised.port > 0) return advertised
        val bound =
            globalModule.connectorPortRegister.getLocalAddress(ConnectorType.BOLT) ?: return null
        return SocketAddress(advertised.hostname, bound.port)
    }

    /**
     * This server's advertised backup address, `server.backup.advertised_address` with the bound
     * port of `server.backup.listen_address` for port 0; null while it serves no backups.
     */
    protected fun backupAddress(): SocketAddress? {
        val bound =
            globalModule.connectorPortRegister.getLocalAddress(ConnectorType.BACKUP) ?: return null
        val advertised = globalModule.globalConfig.get(DodecaSettings.backupAdvertisedAddress)
        return SocketAddress(advertised.hostname, advertised.port.takeIf { it > 0 } ?: bound.port)
    }

    /** The servers that serve backups of a database: without clustering, this one. */
    protected open fun backupSources(): BackupSources = BackupSources {
        listOfNotNull(backupAddress()?.toString())
    }

    /** This server's advertised HTTP address, with the bound port if the configured one is 0. */
    protected fun httpAddress(): SocketAddress? =
        advertised(HttpConnector.enabled, HttpConnector.advertised_address, ConnectorType.HTTP)

    /** This server's advertised HTTPS address, with the bound port if the configured one is 0. */
    protected fun httpsAddress(): SocketAddress? =
        advertised(HttpsConnector.enabled, HttpsConnector.advertised_address, ConnectorType.HTTPS)

    private fun advertised(
        enabled: Setting<Boolean>,
        address: Setting<SocketAddress>,
        type: ConnectorType,
    ): SocketAddress? {
        val config = globalModule.globalConfig
        if (!config.get(enabled)) return null
        val advertised = config.get(address)
        if (advertised.port > 0) return advertised
        val bound = globalModule.connectorPortRegister.getLocalAddress(type) ?: return null
        return SocketAddress(advertised.hostname, bound.port)
    }

    /**
     * The states of the databases on this server, as SHOW DATABASES shows them: Community's, with
     * quarantines (see [DodecaDatabaseStateService]).
     */
    protected lateinit var stateService: DatabaseStateService<org.neo4j.dbms.DatabaseState>
        private set

    /**
     * SHOW SERVERS and SHOW DATABASES of this server, with its name and options (see
     * [SingleServerTopology]), and the states of [stateService].
     */
    override fun createTopologyInfoService(
        databaseContextProvider: DatabaseContextProvider<*>
    ): TopologyInfoService =
        SingleServerTopology(
            DefaultTopologyInfoService(
                identityModule.serverId(),
                globalModule.globalConfig,
                stateService,
                globalReadOnlyChecker,
                DefaultDatabaseDetailsExtrasProvider(databaseContextProvider),
            ),
            globalModule.globalConfig,
        )

    /**
     * Routes by database or alias, as Neo4j 5.26 did (see [DodecaRoutingService]), with tables that
     * name only this server.
     */
    override fun createRoutingService(
        databaseContextProvider: DatabaseContextProvider<*>,
        clientRoutingDomainChecker: ClientRoutingDomainChecker,
    ): RoutingService {
        val singleAddress =
            SingleAddressRoutingTable(
                globalModule.connectorPortRegister,
                globalModule.globalConfig,
            )
        return DodecaRoutingService(
            referenceCache,
            defaultDatabaseResolver,
            LocalRoutingTables(databaseContextProvider, singleAddress),
            singleAddress,
            clientRoutingDomainChecker,
            globalModule.globalConfig,
        ) {
            true
        }
    }

    /** Caches the references to the databases and aliases the system graph names. */
    protected lateinit var referenceCache: MapCachingDatabaseReferenceRepository
        private set

    /**
     * The default Cypher version of each database and remote alias, as the system graph gives it.
     */
    private lateinit var defaultLanguages: DefaultLanguages

    /** Has the default Cypher versions read again, as after a commit to `system` here. */
    protected fun refreshDefaultLanguages() {
        defaultLanguages.refresh()
    }

    /**
     * The users, roles and privileges of the system graph, which the login realm and the default
     * database resolver read.
     */
    private lateinit var security: SystemGraphSecurity

    /** The system graph component of roles and privileges. */
    protected lateinit var securityComponent: DodecaSecurityComponent
        private set

    /**
     * Neo4j Community's system graph components, then Dodeca's roles and privileges. Community's
     * component for databases takes the default database from the configuration only while there is
     * none.
     */
    override fun registerSystemGraphComponents(
        builder: SystemGraphComponents.Builder,
        globalModule: GlobalModule,
    ) {
        val roles =
            DodecaSecurityComponent(
                    globalModule.globalConfig,
                    globalModule.fileSystem,
                    globalModule.logService.internalLogProvider,
                )
                .also { securityComponent = it }
        // After Community's users, which the roles are given to.
        super.registerSystemGraphComponents(
            object : SystemGraphComponents.Builder() {
                override fun register(component: SystemGraphComponent) =
                    builder.register(
                        if (component is DefaultSystemGraphComponent) {
                            FirstStartDefaultDatabase(
                                globalModule.globalConfig,
                                globalModule.globalClock,
                            )
                        } else {
                            component
                        }
                    )

                override fun build(): SystemGraphComponents {
                    builder.register(roles)
                    return builder.build()
                }
            },
            globalModule,
        )
    }

    /** Resolves a user's home database before the DBMS default database. */
    override fun createDefaultDatabaseResolver(systemDatabaseProvider: SystemDatabaseProvider) {
        security =
            SystemGraphSecurity(
                systemDatabaseProvider,
                globalModule.logService.internalLogProvider,
            )
        val resolver =
            DodecaDefaultDatabaseResolver(
                globalModule.globalConfig,
                systemDatabaseProvider,
                security,
            )
        globalModule.transactionEventListeners.registerTransactionEventListener(
            GraphDatabaseSettings.SYSTEM_DATABASE_NAME,
            resolver,
        )
        defaultDatabaseResolver = resolver
    }

    /** The security log, with authentication enabled. */
    private val securityLog: SecurityLog? by lazy {
        val config = globalModule.globalConfig
        if (config.get(GraphDatabaseSettings.auth_enabled)) {
            SecurityLog(
                globalModule.logService.internalLogProvider.getLog(LoggerTarget.SECURITY_LOGGER),
                config.get(DodecaSettings.logSuccessfulAuthentication),
            )
        } else {
            null
        }
    }

    /** Community's web server, whose HTTPS takes TLS reloads without closing its connections. */
    override fun createWebServer(
        managementService: DatabaseManagementService,
        transactionManager: TransactionManager,
        globalDependencies: Dependencies,
        config: Config,
        userLogProvider: InternalLogProvider,
        dbmsInfo: DbmsInfo,
    ): Lifecycle =
        DodecaNeoWebServer(
            managementService,
            transactionManager,
            globalDependencies,
            config,
            userLogProvider,
            dbmsInfo,
            globalModule.memoryPools,
            globalModule.globalMonitors,
            globalModule.globalClock,
        )

    /**
     * Neo4j Community's, with logins that have the privileges of their users' roles. The databases'
     * authorization failures go to the security log through their own dependencies (see
     * [DodecaDatabaseContextFactory]): Community registers its own, which writes to `debug.log`,
     * for the whole DBMS.
     */
    override fun createSecurityModule(
        globalModule: GlobalModule,
        systemDatabaseProvider: SystemDatabaseProvider,
    ) {
        val log = securityLog
        if (log == null) {
            setSecurityProvider(NoAuthProvider(memberLogins()))
            return
        }
        globalModule.databaseEventListeners.registerDatabaseEventListener(
            log.followSystemDatabase(databaseRepository)
        )
        val module =
            DodecaSecurityModule(
                globalModule.logService,
                globalModule.globalConfig,
                globalModule.globalDependencies,
                log,
                security,
                memberLogins(),
                { defaultDatabaseResolver.defaultDatabase(it) },
                ::awaitingSystem,
            )
        module.setup()
        setSecurityProvider(module)
    }

    /**
     * Runs before a member's login waits until `system` here has the transaction [transactionId],
     * as a server whose `system` pulls it asks for it at once.
     */
    protected open fun awaitingSystem(transactionId: Long) {}

    /**
     * Whether other members of a cluster may log in on this server's routing connector as the users
     * whose transactions they forward; a server outside a cluster refuses them.
     */
    protected open fun memberLogins(): MemberLogins =
        MemberLogins.Refused("it is not a member of a cluster")

    /**
     * Neo4j's security provider with authentication disabled, with logins on the routing connector
     * that [memberLogins] allows or refuses.
     */
    private class NoAuthProvider(memberLogins: MemberLogins) :
        LifecycleAdapter(), SecurityProvider {
        private val members =
            when (memberLogins) {
                MemberLogins.Allowed -> MemberRealm(null)
                is MemberLogins.Refused -> RefusingMemberRealm(memberLogins.reason)
            }

        override fun authManager(): AuthManager = NoAuthSecurityProvider.INSTANCE.authManager()

        override fun inClusterAuthManager(): AuthManager = members

        override fun domainSocketAuthManager(): AuthManager =
            NoAuthSecurityProvider.INSTANCE.domainSocketAuthManager()
    }

    /**
     * Neo4j Community's query router, whose authorization failures go to the security log, and
     * which runs transactions in remote aliases on the DBMS each names.
     */
    override fun bootstrapQueryRouterServices(
        databaseManagementService: DatabaseManagementService
    ) {
        val config = globalModule.globalConfig
        val remoteAliases =
            RemoteAliasTransactions(
                config,
                { databaseManagementService.database(GraphDatabaseSettings.SYSTEM_DATABASE_NAME) },
                AliasPasswords(config),
                globalModule.logService.internalLogProvider,
            )
        globalModule.globalDependencies.satisfyDependency(remoteAliases)
        globalModule.globalLife.add(
            object : LifecycleAdapter() {
                override fun stop() = remoteAliases.close()
            }
        )
        val bootstrap =
            createQueryRouterBootstrap(
                globalModule.globalDependencies.resolveDependency(
                    DatabaseContextProvider::class.java
                ),
                securityLog
                    ?: globalModule.globalDependencies.resolveDependency(
                        AbstractSecurityLog::class.java
                    ),
                remoteAliases,
            )
        globalModule.globalDependencies.satisfyDependency(
            bootstrap.bootstrapServices(databaseManagementService)
        )
    }

    /**
     * Neo4j Community's query router, whose transactions run on this server but those in remote
     * aliases, which [remoteAliases] runs.
     */
    protected open fun createQueryRouterBootstrap(
        databaseProvider: DatabaseContextProvider<*>,
        securityLog: AbstractSecurityLog,
        remoteAliases: DatabaseTransactionFactory<Location.Remote>,
    ): DodecaQueryRouterBootstrap =
        DodecaQueryRouterBootstrap(
            globalModule.globalLife,
            globalModule.globalDependencies,
            globalModule.logService,
            databaseProvider,
            referenceCache,
            securityLog,
            remoteAliases,
        )

    /**
     * Neo4j Community's procedures, `dbms.setConfigValue`, `dbms.setDefaultDatabase`,
     * `dbms.quarantineDatabase`, `dbms.unquarantineDatabase` and `dbms.security.reloadTLS`.
     */
    override fun registerEditionSpecificProcedures(
        globalProcedures: GlobalProcedures,
        databaseContextProvider: DatabaseContextProvider<*>,
    ) {
        super.registerEditionSpecificProcedures(globalProcedures, databaseContextProvider)
        globalProcedures.registerComponent(DatabaseQuarantine::class.java, { quarantine }, true)
        globalProcedures.registerComponent(Unquarantines::class.java, { unquarantines }, true)
        globalProcedures.registerProcedure(SettingsProcedures::class.java)
        globalProcedures.registerProcedure(DefaultDatabaseProcedures::class.java)
        globalProcedures.registerProcedure(MonitoringProcedures::class.java)
        globalProcedures.registerProcedure(QuarantineProcedures::class.java)
        globalProcedures.registerProcedure(TlsProcedures::class.java)
    }

    /** Starts the default database, then follows the system graph for the other databases. */
    override fun registerDefaultDatabaseInitializer(globalModule: GlobalModule) {
        globalModule.globalLife.add(defaultDatabaseStarter)
        val databases =
            StandaloneDatabases(
                databaseManager,
                databaseRepository,
                ::seeder,
                quarantines,
                globalModule.logService.internalLogProvider,
            )
        standaloneDatabases = databases
        globalModule.transactionEventListeners.registerTransactionEventListener(
            GraphDatabaseSettings.SYSTEM_DATABASE_NAME,
            databases,
        )
        globalModule.globalLife.add(databases.starter())
    }
}
