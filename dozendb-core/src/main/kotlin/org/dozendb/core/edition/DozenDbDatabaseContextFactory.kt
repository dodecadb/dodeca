/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Modifications Copyright (c) DozerDB
 * https://dozerdb.org
 *
 * Modifications Copyright (c) DozenDB
 * 2026-10-19: ported to Kotlin from DozerDB's DozerDbDatabaseContextFactory, with optional
 * hosting on Raft.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */
package org.dozendb.core.edition

import java.util.Optional
import org.dozendb.core.constraints.DozenDbConstraintSemantics
import org.dozendb.core.cypher.DozenDbCypherEngineProvider
import org.neo4j.configuration.DatabaseConfig
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
import org.neo4j.storageengine.api.StorageEngineFactory

/**
 * Neo4j Community's database context factory with DozenDB's constraint semantics and Cypher engine.
 *
 * With [hosting], every database gets the hosting's mode, lease service and commit process, and ID
 * generators that reuse freed IDs only if the hosting allows it from the start. Without it, every
 * value is Neo4j Community's.
 */
internal class DozenDbDatabaseContextFactory(
    globalModule: GlobalModule,
    private val serverIdentity: ServerIdentity,
    private val transactionStatsFactory: DatabaseTransactionStats.Factory,
    private val indexStatsFactory: DatabaseIndexStats.Factory,
    idContextFactory: IdContextFactory,
    private val deviceMapper: DeviceMapper,
    private val controllerService: IOControllerService,
    private val commitProcessFactory: TransactionalProcessFactory,
    private val components: DefaultDatabaseContextFactoryComponents,
    private val hosting: DatabaseHosting?,
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
        val idContext =
            idContextFactory.createIdContext(
                namedDatabaseId,
                contextFactory,
                databaseConfig,
                hosting?.reusesFreedIdsAtStart ?: true,
            )
        val hosted =
            hosting?.prepare(
                namedDatabaseId,
                idContext.idGeneratorFactory,
                storeExists(namedDatabaseId),
            )
        lateinit var database: Database
        val creationContext =
            ModularDatabaseCreationContext(
                hosted?.hostedOnMode ?: HostedOnMode.SINGLE,
                serverIdentity,
                namedDatabaseId,
                globalModule,
                globalModule.globalDependencies,
                contextFactory,
                deviceMapper,
                CommunityVersionStorageFactory(),
                databaseConfig,
                globalModule.globalMonitors,
                hosted?.leaseService ?: LeaseService.NO_LEASES,
                {
                    DatabaseCreationContext.selectStorageEngine(
                        globalModule.fileSystem,
                        globalModule.neo4jLayout,
                        databaseConfig,
                        namedDatabaseId,
                    )
                },
                DozenDbConstraintSemantics(),
                DozenDbCypherEngineProvider(),
                transactionStatsFactory.create(),
                indexStatsFactory.create(),
                ModularDatabaseCreationContext.defaultFileWatcherFilter(),
                AccessCapabilityFactory.configDependent(),
                ExternalIdReuseConditionProvider.NONE,
                idContext,
                hosted?.commitProcessFactory(commitProcessFactory) ?: commitProcessFactory,
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
        hosted?.attach(database)
        return StandaloneDatabaseContext(database)
    }

    private fun storeExists(namedDatabaseId: NamedDatabaseId): Boolean =
        StorageEngineFactory.allAvailableStorageEngines().any {
            it.storageExists(
                globalModule.fileSystem,
                globalModule.neo4jLayout,
                namedDatabaseId.name(),
            )
        }
}
