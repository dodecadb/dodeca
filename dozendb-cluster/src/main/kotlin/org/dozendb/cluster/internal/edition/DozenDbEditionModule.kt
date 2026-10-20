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
package org.dozendb.cluster.internal.edition

import org.dozendb.cluster.internal.replication.ReplicationService
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
import org.neo4j.graphdb.factory.module.GlobalModule
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule
import org.neo4j.io.device.DeviceMapper
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

/**
 * Neo4j Community's edition module with many databases, which follow the `Database` nodes of the
 * system graph, and with DozenDB's constraint semantics and administration commands.
 *
 * Without clustering, [StandaloneDatabases] makes the databases follow the system graph. The
 * cluster's edition module hosts the databases on Raft through [replication] and makes them follow
 * the replicated system graph instead.
 */
open class DozenDbEditionModule(globalModule: GlobalModule) : CommunityEditionModule(globalModule) {
    internal lateinit var databaseManager: DatabaseManager
        private set

    internal lateinit var databaseRepository: DatabaseRepository<StandaloneDatabaseContext>
        private set

    /** Caches the ids of the databases the system graph names. */
    protected lateinit var databaseIdCache: MapCachingDatabaseIdRepository
        private set

    /** Creates and starts the default database. */
    protected var defaultDatabaseStarter: Lifecycle = LifecycleAdapter()
        private set

    /** Hosts the databases on Raft; null without clustering. */
    internal open val replication: ReplicationService?
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
                replication,
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
