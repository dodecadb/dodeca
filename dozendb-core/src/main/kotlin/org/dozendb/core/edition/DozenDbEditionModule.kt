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

import org.dozendb.core.security.DozenDbDefaultDatabaseResolver
import org.dozendb.core.security.DozenDbSecurityModule
import org.dozendb.core.security.SystemGraphUsers
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.dbms.CommunityDatabaseStateService
import org.neo4j.dbms.CommunityKernelPanicListener
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.dbms.database.DatabaseContextProviderDelegate
import org.neo4j.dbms.database.DatabaseOperationCounts
import org.neo4j.dbms.database.DatabaseReferenceCacheClearingListener
import org.neo4j.dbms.database.DatabaseRepository
import org.neo4j.dbms.database.StandaloneDatabaseContext
import org.neo4j.dbms.database.SystemGraphComponents
import org.neo4j.dbms.systemgraph.SystemDatabaseProvider
import org.neo4j.graphdb.factory.module.GlobalModule
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule
import org.neo4j.internal.kernel.api.security.CommunitySecurityLog
import org.neo4j.io.device.DeviceMapper
import org.neo4j.kernel.api.security.provider.NoAuthSecurityProvider
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
import org.neo4j.server.security.auth.CommunitySecurityModule

/**
 * Neo4j Community's edition module with many databases, which follow the `Database` nodes of the
 * system graph, and with DozenDB's constraint semantics and administration commands.
 *
 * Without clustering, [StandaloneDatabases] makes the databases follow the system graph. The
 * cluster's edition module hosts the databases on Raft through [hosting] and makes them follow the
 * replicated system graph instead.
 */
open class DozenDbEditionModule(globalModule: GlobalModule) : CommunityEditionModule(globalModule) {
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
                globalModule.logService.internalLogProvider,
            )
        databaseStateService = CommunityDatabaseStateService(repository)
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
        return repository
    }

    /** Caches the references to the databases the system graph names. */
    protected val referenceCache: MapCachingDatabaseReferenceRepository
        // Community creates it and exposes only the interface.
        get() = databaseReferenceRepo as MapCachingDatabaseReferenceRepository

    /**
     * Which users are suspended and their home databases, which the login realm and the default
     * database resolver read.
     */
    private lateinit var users: SystemGraphUsers

    /** Resolves a user's home database before the DBMS default database. */
    override fun createDefaultDatabaseResolver(systemDatabaseProvider: SystemDatabaseProvider) {
        users = SystemGraphUsers(systemDatabaseProvider)
        val resolver =
            DozenDbDefaultDatabaseResolver(
                globalModule.globalConfig,
                systemDatabaseProvider,
                users,
            )
        globalModule.transactionEventListeners.registerTransactionEventListener(
            GraphDatabaseSettings.SYSTEM_DATABASE_NAME,
            resolver,
        )
        defaultDatabaseResolver = resolver
    }

    /** Neo4j Community's, with a realm that refuses suspended users. */
    override fun createSecurityModule(globalModule: GlobalModule) {
        globalModule.globalDependencies.satisfyDependency(CommunitySecurityLog.NULL_LOG)
        if (!globalModule.globalConfig.get(GraphDatabaseSettings.auth_enabled)) {
            setSecurityProvider(NoAuthSecurityProvider.INSTANCE)
            return
        }
        val module =
            DozenDbSecurityModule(
                globalModule.logService,
                globalModule.globalConfig,
                globalModule.globalDependencies,
                CommunitySecurityLog(
                    globalModule.logService.internalLogProvider.getLog(
                        CommunitySecurityModule::class.java
                    )
                ),
                users,
            )
        module.setup()
        setSecurityProvider(module)
    }

    /** Starts the default database, then follows the system graph for the other databases. */
    override fun registerDefaultDatabaseInitializer(globalModule: GlobalModule) {
        globalModule.globalLife.add(defaultDatabaseStarter)
        val databases =
            StandaloneDatabases(
                databaseManager,
                databaseRepository,
                defaultDatabase,
                globalModule.logService.internalLogProvider,
            )
        globalModule.transactionEventListeners.registerTransactionEventListener(
            GraphDatabaseSettings.SYSTEM_DATABASE_NAME,
            databases,
        )
        globalModule.globalLife.add(databases.starter())
    }
}
