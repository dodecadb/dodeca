/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Modifications Copyright (c) DozerDB
 * https://dozerdb.org
 *
 * Modifications Copyright (c) DozenDB
 * 2026-10-19: ported to Kotlin from DozerDB's DozerDbEditionModule, now a subclass of Neo4j's
 * CommunityEditionModule that keeps Community's user security component as a dependency, which
 * ALTER USER needs.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */
package org.dozendb.core.edition

import java.util.UUID
import org.dozendb.core.DozenDbSettings
import org.dozendb.core.SettingsProcedures
import org.dozendb.core.logging.QueryLog
import org.dozendb.core.logging.SecurityLog
import org.dozendb.core.monitoring.MetricsService
import org.dozendb.core.remote.RemoteAliasTransactions
import org.dozendb.core.security.AliasPasswords
import org.dozendb.core.security.DozenDbDefaultDatabaseResolver
import org.dozendb.core.security.DozenDbSecurityComponent
import org.dozendb.core.security.DozenDbSecurityModule
import org.dozendb.core.security.MemberLogins
import org.dozendb.core.security.MemberRealm
import org.dozendb.core.security.RefusingMemberRealm
import org.dozendb.core.security.SystemGraphSecurity
import org.dozendb.core.servers.ServerOptions
import org.dozendb.core.servers.Servers
import org.dozendb.core.servers.SingleServerTopology
import org.dozendb.core.store.StorePlacement
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.configuration.connectors.ConnectorType
import org.neo4j.configuration.connectors.HttpConnector
import org.neo4j.configuration.connectors.HttpsConnector
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.dbms.CommunityKernelPanicListener
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.dbms.database.DatabaseContextProviderDelegate
import org.neo4j.dbms.database.DatabaseOperationCounts
import org.neo4j.dbms.database.DatabaseReferenceCacheClearingListener
import org.neo4j.dbms.database.DatabaseRepository
import org.neo4j.dbms.database.StandaloneDatabaseContext
import org.neo4j.dbms.database.SystemGraphComponent
import org.neo4j.dbms.database.SystemGraphComponents
import org.neo4j.dbms.database.TopologyInfoService
import org.neo4j.dbms.systemgraph.SystemDatabaseProvider
import org.neo4j.fabric.executor.Location
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.module.GlobalModule
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog
import org.neo4j.internal.kernel.api.security.CommunitySecurityLog
import org.neo4j.io.device.DeviceMapper
import org.neo4j.kernel.api.procedure.GlobalProcedures
import org.neo4j.kernel.api.security.AuthManager
import org.neo4j.kernel.api.security.provider.NoAuthSecurityProvider
import org.neo4j.kernel.api.security.provider.SecurityProvider
import org.neo4j.kernel.database.DatabaseId
import org.neo4j.kernel.database.DatabaseIdRepository
import org.neo4j.kernel.database.DatabaseReferenceRepository
import org.neo4j.kernel.database.MapCachingDatabaseIdRepository
import org.neo4j.kernel.database.MapCachingDatabaseReferenceRepository
import org.neo4j.kernel.database.SystemGraphDatabaseIdRepository
import org.neo4j.kernel.database.SystemGraphDatabaseReferenceRepository
import org.neo4j.kernel.impl.pagecache.CommunityIOControllerService
import org.neo4j.kernel.lifecycle.Lifecycle
import org.neo4j.kernel.lifecycle.LifecycleAdapter
import org.neo4j.logging.log4j.LoggerTarget
import org.neo4j.router.transaction.DatabaseTransactionFactory

/**
 * Neo4j Community's edition module with many databases, which follow the `Database` nodes of the
 * system graph, and with DozenDB's constraint semantics and administration commands.
 *
 * Without clustering, [StandaloneDatabases] makes the databases follow the system graph. The
 * cluster's edition module hosts the databases on Raft through [hosting] and makes them follow the
 * replicated system graph instead.
 */
open class DozenDbEditionModule(globalModule: GlobalModule) : CommunityEditionModule(globalModule) {
    init {
        // Neo4j sends usage data to udc.neo4j.com unless told not to, and would report this server
        // as Neo4j Enterprise: DozenDB sends it only when the operator turns it on.
        globalModule.globalConfig.setIfNotSet(GraphDatabaseSettings.udc_enabled, false)
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

    /** Hosts the databases together with other servers; null when this server alone does. */
    protected open val hosting: DatabaseHosting?
        get() = null

    protected val defaultDatabase: String
        get() = globalModule.globalConfig.get(GraphDatabaseSettings.initial_default_database)

    /** Neo4j Community's wiring with DozenDB's database manager and database context factory. */
    override fun createDatabaseContextProvider(
        globalModule: GlobalModule
    ): DatabaseContextProvider<*> {
        val contextFactory =
            DozenDbDatabaseContextFactory(
                globalModule,
                identityModule,
                transactionMonitorFactory,
                indexMonitorFactory,
                AbstractEditionModule.createIdContextFactory(globalModule),
                DeviceMapper.UNKNOWN_MAPPER,
                CommunityIOControllerService(),
                createCommitProcessFactory(),
                this,
                hosting,
            )
        val idCache = MapCachingDatabaseIdRepository()
        val repository = DatabaseRepository<StandaloneDatabaseContext>(idCache)
        val rootDatabaseIdRepository =
            AbstractEditionModule.tryResolveOrCreate(
                DatabaseIdRepository::class.java,
                globalModule.externalDependencyResolver,
            ) {
                SystemGraphDatabaseIdRepository(
                    { repository.getDatabaseContext(DatabaseId.SYSTEM_DATABASE_ID) },
                    globalModule.logService.internalLogProvider,
                )
            }
        val rootDatabaseReferenceRepository =
            AbstractEditionModule.tryResolveOrCreate(
                DatabaseReferenceRepository::class.java,
                globalModule.externalDependencyResolver,
            ) {
                SystemGraphDatabaseReferenceRepository(repository::getSystemDatabaseContext)
            }
        idCache.setDelegate(rootDatabaseIdRepository)
        referenceCache.setDelegate(rootDatabaseReferenceRepository)
        globalModule.globalLife.add(
            CommunityKernelPanicListener(globalModule.databaseEventListeners, repository)
        )
        val manager =
            DatabaseManager(
                repository,
                contextFactory,
                defaultDatabase,
                globalModule.globalDependencies.resolveDependency(
                    DatabaseOperationCounts.Counter::class.java
                ),
                StorePlacement(globalModule.fileSystem, globalModule.neo4jLayout),
                globalModule.logService.internalLogProvider,
            )
        databaseStateService = DozenDbDatabaseStateService(repository)
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
        globalModule.globalDependencies.satisfyDependency(databaseStateService)
        globalModule.transactionEventListeners.registerTransactionEventListener(
            GraphDatabaseSettings.SYSTEM_DATABASE_NAME,
            DatabaseReferenceCacheClearingListener(idCache, referenceCache),
        )
        globalModule.globalDependencies.satisfyDependency<SystemGraphComponents.UpgradeChecker>(
            SystemGraphComponents.UpgradeChecker.UPGRADE_ALWAYS_ALLOWED
        )
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
        return repository
    }

    /**
     * The servers a database can be created from with existingDataSeedServer: without clustering,
     * none.
     */
    protected open fun seedServers(): SeedServers? = null

    /** The servers the server commands act on: without clustering, this one. */
    protected open fun servers(): Servers =
        object : Servers {
            private val self = identityModule.serverId().uuid()

            override fun ids(): Set<UUID> = setOf(self)

            override fun initialOptions(id: UUID): ServerOptions? =
                if (id == self) {
                    ServerOptions(globalModule.globalConfig.get(DozenDbSettings.initialServerTags))
                } else {
                    null
                }
        }

    /** What creates databases from seeds: DozenDB's backup module, once Neo4j created it. */
    protected fun seeder(): DatabaseSeeder? =
        globalModule.globalDependencies
            .resolveTypeDependencies(DatabaseSeeder::class.java)
            .firstOrNull()

    /** Waits until this server holds the writes acknowledged before: without clustering, none. */
    protected open fun latestWrites(): LatestWrites = LatestWrites.LOCAL

    /** The servers that host the databases, which WAIT waits for: without clustering, this one. */
    protected open fun hostingServers(): List<HostingServer> =
        listOf(
            object : HostingServer {
                override val address: String = boltAddress()?.toString() ?: "localhost"

                override fun databases(): List<DatabaseState> = databaseStates()
            }
        )

    /** The databases of this server; none before they are created. */
    protected fun databaseStates(): List<DatabaseState> =
        if (::databaseRepository.isInitialized) databaseRepository.databaseStates() else emptyList()

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

    /** SHOW SERVERS of this server, with its name and options (see [SingleServerTopology]). */
    override fun createTopologyInfoService(
        databaseContextProvider: DatabaseContextProvider<*>
    ): TopologyInfoService =
        SingleServerTopology(
            super.createTopologyInfoService(databaseContextProvider),
            globalModule.globalConfig,
        )

    /** Caches the references to the databases the system graph names. */
    protected val referenceCache: MapCachingDatabaseReferenceRepository
        // Community creates it and exposes only the interface.
        get() = databaseReferenceRepo as MapCachingDatabaseReferenceRepository

    /**
     * The users, roles and privileges of the system graph, which the login realm and the default
     * database resolver read.
     */
    private lateinit var security: SystemGraphSecurity

    /** The system graph component of roles and privileges. */
    protected lateinit var securityComponent: DozenDbSecurityComponent
        private set

    /**
     * Neo4j Community's system graph components, then DozenDB's roles and privileges; before them,
     * the check that the default database is no alias.
     */
    override fun registerSystemGraphComponents(
        builder: SystemGraphComponents.Builder,
        globalModule: GlobalModule,
    ) {
        builder.register(DefaultDatabaseCheck(globalModule.globalConfig))
        val roles =
            DozenDbSecurityComponent(
                    globalModule.globalConfig,
                    globalModule.fileSystem,
                    globalModule.logService.internalLogProvider,
                )
                .also { securityComponent = it }
        // After Community's users, which the roles are given to.
        super.registerSystemGraphComponents(
            object : SystemGraphComponents.Builder() {
                override fun register(component: SystemGraphComponent) = builder.register(component)

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
            DozenDbDefaultDatabaseResolver(
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
                config.get(DozenDbSettings.logSuccessfulAuthentication),
            )
        } else {
            null
        }
    }

    /** Neo4j Community's, with logins that have the privileges of their users' roles. */
    override fun createSecurityModule(globalModule: GlobalModule) {
        val log = securityLog
        // For authorization failures; Community gives a log that writes nothing.
        globalModule.globalDependencies.satisfyDependency(log ?: CommunitySecurityLog.NULL_LOG)
        if (log == null) {
            setSecurityProvider(NoAuthProvider(memberLogins()))
            return
        }
        globalModule.databaseEventListeners.registerDatabaseEventListener(
            log.followSystemDatabase(databaseRepository)
        )
        val module =
            DozenDbSecurityModule(
                globalModule.logService,
                globalModule.globalConfig,
                globalModule.globalDependencies,
                log,
                security,
                memberLogins(),
            ) {
                defaultDatabaseResolver.defaultDatabase(it)
            }
        module.setup()
        setSecurityProvider(module)
    }

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

        override fun loopbackAuthManager(): AuthManager =
            NoAuthSecurityProvider.INSTANCE.loopbackAuthManager()
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
                securityLog ?: CommunitySecurityLog.NULL_LOG,
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
    ): DozenDbQueryRouterBootstrap =
        DozenDbQueryRouterBootstrap(
            globalModule.globalLife,
            globalModule.globalDependencies,
            globalModule.logService,
            databaseProvider,
            databaseReferenceRepo,
            securityLog,
            remoteAliases,
        )

    /** Neo4j Community's procedures, and `dbms.setConfigValue`. */
    override fun registerEditionSpecificProcedures(
        globalProcedures: GlobalProcedures,
        databaseContextProvider: DatabaseContextProvider<*>,
    ) {
        super.registerEditionSpecificProcedures(globalProcedures, databaseContextProvider)
        globalProcedures.registerProcedure(SettingsProcedures::class.java)
    }

    /** Starts the default database, then follows the system graph for the other databases. */
    override fun registerDefaultDatabaseInitializer(globalModule: GlobalModule) {
        globalModule.globalLife.add(defaultDatabaseStarter)
        val databases =
            StandaloneDatabases(
                databaseManager,
                databaseRepository,
                defaultDatabase,
                ::seeder,
                globalModule.logService.internalLogProvider,
            )
        globalModule.transactionEventListeners.registerTransactionEventListener(
            GraphDatabaseSettings.SYSTEM_DATABASE_NAME,
            databases,
        )
        globalModule.globalLife.add(databases.starter())
    }
}
