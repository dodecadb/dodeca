/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Modifications Copyright (c) DozerDB
 * https://dozerdb.org
 *
 * Modifications Copyright (c) The Dodeca Authors
 * 2026-10-19: ported to Kotlin from DozerDB's DozerDbDatabaseContextFactory, with optional
 * hosting on Raft.
 * 2026-11-09: creates a new store in the format CREATE DATABASE asked for.
 * 2026-11-10: gives each database's checkpoints a flush event of their own.
 * 2026-11-11: watches each database's directories again when an entry went meanwhile.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */
package dev.dodeca.core.edition

import dev.dodeca.core.constraints.DodecaConstraintSemantics
import dev.dodeca.core.cypher.DodecaCypherEngineProvider
import java.util.concurrent.atomic.LongAdder
import java.util.function.Function
import org.neo4j.collection.Dependencies
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.dbms.database.AbstractDatabaseContextFactory
import org.neo4j.dbms.database.DefaultDatabaseContextFactoryComponents
import org.neo4j.dbms.database.StandaloneDatabaseContext
import org.neo4j.dbms.identity.ServerIdentity
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.HostedOnMode
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.module.GlobalModule
import org.neo4j.graphdb.factory.module.ModularDatabaseCreationContext
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog
import org.neo4j.io.device.DeviceMapper
import org.neo4j.io.fs.watcher.DatabaseLayoutWatcher
import org.neo4j.io.layout.DatabaseLayout
import org.neo4j.io.pagecache.tracing.DatabaseFlushEvent
import org.neo4j.io.pagecache.tracing.PageCacheTracer
import org.neo4j.io.pagecache.tracing.async.AsyncFlushCompletion
import org.neo4j.io.pagecache.tracing.async.AsyncFlushFailure
import org.neo4j.kernel.DatabaseCreationOptions
import org.neo4j.kernel.api.Kernel
import org.neo4j.kernel.database.Database
import org.neo4j.kernel.database.DatabaseCreationContext
import org.neo4j.kernel.database.DatabaseMonitors
import org.neo4j.kernel.database.DatabaseTracers
import org.neo4j.kernel.database.DefaultDatabaseMonitorsFactory
import org.neo4j.kernel.database.GlobalAvailabilityGuardController
import org.neo4j.kernel.database.IdContextFactory
import org.neo4j.kernel.database.IdGeneratorSettings
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.impl.api.ExternalIdReuseConditionProvider
import org.neo4j.kernel.impl.api.LeaseService
import org.neo4j.kernel.impl.api.TransactionalProcessFactory
import org.neo4j.kernel.impl.api.TransactionsFactory
import org.neo4j.kernel.impl.api.tracer.DefaultDatabaseTracer
import org.neo4j.kernel.impl.core.IsolatedTransactionVectorStoreCreator
import org.neo4j.kernel.impl.factory.AccessCapabilityFactory
import org.neo4j.kernel.impl.index.DatabaseIndexStats
import org.neo4j.kernel.impl.pagecache.CommunityVersionStorageFactory
import org.neo4j.kernel.impl.pagecache.IOControllerService
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats
import org.neo4j.kernel.impl.util.watcher.DefaultFileDeletionListenerFactory
import org.neo4j.logging.internal.DatabaseLogIdentifier
import org.neo4j.logging.internal.DatabaseLogProvider
import org.neo4j.storageengine.api.StorageEngineFactory
import org.neo4j.wal.pruning.LogPruneStrategyFactory

/**
 * Neo4j Community's database context factory with Dodeca's constraint semantics and Cypher engine.
 *
 * With [hosting], every database gets the hosting's mode, lease service and commit process, and ID
 * generators that reuse freed IDs only if the hosting allows it from the start. Without it, every
 * value is Neo4j Community's.
 *
 * A database without a store yet gets one in the format [newStoreFormat] names for it, if any,
 * instead of `db.format`.
 */
internal class DodecaDatabaseContextFactory(
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
    private val securityLog: () -> AbstractSecurityLog?,
    private val newStoreFormat: (NamedDatabaseId) -> String?,
) :
    AbstractDatabaseContextFactory<StandaloneDatabaseContext, NamedDatabaseId>(
        globalModule,
        idContextFactory,
    ) {

    /**
     * Neo4j's tracers for [id], but for the flush event its checkpoints count their pages, I/Os,
     * I/O limit and pauses in: Neo4j's page cache tracer hands every database the same one, so that
     * the log lines and metrics of databases checkpointing at once counted, and reset, each
     * other's. Tracers other than Neo4j's default ones stay as they are.
     */
    private fun tracers(id: NamedDatabaseId): DatabaseTracers {
        val tracers = globalModule.tracers
        if (tracers.getDatabaseTracer(id) !is DefaultDatabaseTracer) {
            return DatabaseTracers(tracers, id)
        }
        val pageCacheTracer = tracers.pageCacheTracer
        // With server.memory.pagecache.async, the pages a checkpoint flushes complete in these.
        val shared = pageCacheTracer.beginDatabaseFlush()
        val flush =
            DatabaseFlushEvent(
                pageCacheTracer.beginFileFlush(),
                OwnAsyncFlushCompletion(shared.asyncFlushCompletion()),
                OwnAsyncFlushFailure(shared.asyncFlushFailure()),
            )
        val own =
            object : PageCacheTracer by pageCacheTracer {
                override fun beginDatabaseFlush() = flush
            }
        return DatabaseTracers(
            DefaultDatabaseTracer(own),
            tracers.lockTracer,
            pageCacheTracer,
            tracers.getVersionStorageTracer(id),
        )
    }

    override fun create(namedDatabaseId: NamedDatabaseId): StandaloneDatabaseContext {
        val storeExists = storeExists(namedDatabaseId)
        val format = if (storeExists) null else newStoreFormat(namedDatabaseId)
        val databaseConfig =
            createDatabaseConfig(
                namedDatabaseId,
                format?.let { mapOf<Setting<*>, Any>(GraphDatabaseSettings.db_format to it) }
                    ?: emptyMap(),
            )
        val contextFactory = createContextFactorySupplier(databaseConfig, namedDatabaseId)
        val hosted = hosting?.prepare(namedDatabaseId, storeExists)
        val logIdentifier = DatabaseLogIdentifier.create(namedDatabaseId)
        lateinit var database: Database
        // The databases log their authorization failures to Dodeca's security log: Neo4j Community
        // registers its own for the whole DBMS, which a dependency cannot replace.
        val dependencies =
            securityLog()?.let { log ->
                Dependencies(globalModule.globalDependencies).apply { satisfyDependency(log) }
            } ?: globalModule.globalDependencies
        val kernel = { database.dependencyResolver.resolveDependency(Kernel::class.java) }
        val creationContext =
            object :
                ModularDatabaseCreationContext(
                    hosted?.hostedOnMode ?: HostedOnMode.SINGLE,
                    serverIdentity,
                    namedDatabaseId,
                    logIdentifier,
                    globalModule,
                    dependencies,
                    contextFactory,
                    deviceMapper,
                    CommunityVersionStorageFactory(),
                    databaseConfig,
                    hosted?.leaseService ?: LeaseService.NO_LEASES,
                    {
                        DatabaseCreationContext.selectStorageEngine(
                            globalModule.fileSystem,
                            globalModule.neo4jLayout,
                            databaseConfig,
                            namedDatabaseId,
                        )
                    },
                    DodecaConstraintSemantics(),
                    DodecaCypherEngineProvider(),
                    transactionStatsFactory.create(),
                    indexStatsFactory.create(),
                    ModularDatabaseCreationContext.defaultFileWatcherFilter(),
                    AccessCapabilityFactory.configDependent(),
                    ExternalIdReuseConditionProvider.NONE,
                    idContextFactory,
                    // A hosted database's freed IDs go through its ID files, as followers and new
                    // leaders rebuild their caches from them; a standalone one caches them
                    // directly, as
                    // Community does.
                    IdGeneratorSettings(hosting?.reusesFreedIdsAtStart ?: true, hosting == null),
                    hosted?.commitProcessFactory(commitProcessFactory) ?: commitProcessFactory,
                    createTokenHolderProvider(kernel),
                    IsolatedTransactionVectorStoreCreator(kernel),
                    GlobalAvailabilityGuardController(globalModule.globalAvailabilityGuard),
                    components.readOnlyDatabases(),
                    controllerService,
                    tracers(namedDatabaseId),
                    globalModule.defaultCommandCommitListeners,
                    TransactionsFactory.DEFAULT,
                    DefaultDatabaseMonitorsFactory(
                        DatabaseMonitors(
                            globalModule.globalMonitors,
                            DatabaseLogProvider(
                                logIdentifier,
                                globalModule.logService.internalLogProvider,
                            ),
                        )
                    ),
                    globalModule.exceptionHandlerService,
                    DatabaseCreationOptions.EMPTY_CREATION_OPTIONS,
                    LogPruneStrategyFactory(),
                    false,
                    false,
                ) {
                override fun getWatcherServiceFactory() = watchers
            }
        database = Database(creationContext)
        hosted?.attach(database)
        return StandaloneDatabaseContext(database)
    }

    /**
     * Neo4j's watchers of the database directories, which watch a directory again when an entry of
     * it went while they began to.
     */
    private val watchers =
        Function<DatabaseLayout, DatabaseLayoutWatcher> { layout ->
            DatabaseLayoutWatcher(
                RetryingFileWatcher(
                    globalModule.fileWatcher,
                    globalModule.logService.getInternalLog(RetryingFileWatcher::class.java),
                ),
                layout,
                DefaultFileDeletionListenerFactory(
                    layout,
                    globalModule.logService,
                    ModularDatabaseCreationContext.defaultFileWatcherFilter(),
                ),
            )
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

/**
 * The asynchronous page writes of one database's checkpoints, counted for that database alone, and
 * in Neo4j's page cache counters through [shared].
 */
private class OwnAsyncFlushCompletion(private val shared: AsyncFlushCompletion) :
    AsyncFlushCompletion {
    private var pages = 0L
    private var ios = 0L
    private val bytes = LongAdder()

    override fun addBytesWritten(bytes: Int) {
        shared.addBytesWritten(bytes)
        this.bytes.add(bytes.toLong())
    }

    override fun addPagesCompleted(pageCount: Int) {
        shared.addPagesCompleted(pageCount)
        pages += pageCount
    }

    override fun reportIO(completedIOs: Int) {
        shared.reportIO(completedIOs)
        ios += completedIOs
    }

    override fun reset() {
        pages = 0
        ios = 0
        bytes.reset()
    }

    override fun pagesFlushed() = pages

    override fun ioPerformed() = ios

    override fun getLocalBytesWritten() = bytes.sum()

    override fun close() {}
}

/**
 * The failed asynchronous page writes of one database's checkpoints, as [OwnAsyncFlushCompletion].
 */
private class OwnAsyncFlushFailure(private val shared: AsyncFlushFailure) : AsyncFlushFailure {
    private var ios = 0L

    override fun addPagesFailed(pageCount: Int) = shared.addPagesFailed(pageCount)

    override fun reportIO(completedIOs: Int) {
        shared.reportIO(completedIOs)
        ios += completedIOs
    }

    override fun reset() {
        ios = 0
    }

    override fun ioPerformed() = ios

    override fun close() {}
}
