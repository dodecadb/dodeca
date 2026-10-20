/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Modifications Copyright (c) DozerDB
 * https://dozerdb.org
 *
 * Modifications Copyright (c) DozenDB
 * 2026-10-19: ported to Kotlin from DozerDB's MultiDatabaseManager and
 * MultiDatabaseLifecycleService, which extend Neo4j's DatabaseLifecycles to many databases. Stops
 * the system database last and drops databases without a checkpoint.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */
package org.dozendb.core.edition

import java.util.Optional
import org.neo4j.dbms.api.DatabaseManagementException
import org.neo4j.dbms.api.DatabaseNotFoundHelper
import org.neo4j.dbms.database.DatabaseContextFactory
import org.neo4j.dbms.database.DatabaseOperationCounts
import org.neo4j.dbms.database.DatabaseRepository
import org.neo4j.dbms.database.StandaloneDatabaseContext
import org.neo4j.dbms.database.UnableToStartDatabaseException
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.lifecycle.Lifecycle
import org.neo4j.kernel.lifecycle.LifecycleAdapter
import org.neo4j.logging.InternalLogProvider

/**
 * This server's databases: creates, starts, stops and drops them, and keeps the created ones in
 * [repository]. A database that fails to start or stop is marked failed rather than throwing.
 *
 * Like Neo4j's own database lifecycles, it throws database management exceptions without a GQL
 * status, which Neo4j 5.26 has none for.
 */
@Suppress("DEPRECATION")
class DatabaseManager(
    private val repository: DatabaseRepository<StandaloneDatabaseContext>,
    private val contextFactory: DatabaseContextFactory<StandaloneDatabaseContext, Optional<*>>,
    private val defaultDatabase: String,
    private val counts: DatabaseOperationCounts.Counter,
    logProvider: InternalLogProvider,
) {
    private val log = logProvider.getLog(javaClass)

    /** Registers [id]; starting it creates its store if there is none. */
    @Synchronized
    fun createDatabase(id: NamedDatabaseId): StandaloneDatabaseContext {
        log.info("Creating '%s'.", id)
        val context = contextFactory.create(id, Optional.empty<Any>())
        repository.add(id, context)
        counts.increaseCreateCount()
        return context
    }

    fun startDatabase(id: NamedDatabaseId) {
        val context = repository.getDatabaseContext(id)
        if (context.isEmpty) {
            log.warn("Cannot start '%s': it does not exist.", id)
            return
        }
        startDatabase(context.get())
    }

    fun startDatabase(context: StandaloneDatabaseContext) {
        val id = context.database().namedDatabaseId
        try {
            log.info("Starting '%s'.", id)
            context.database().start()
            counts.increaseStartCount()
        } catch (e: Throwable) {
            log.error("Failed to start '$id'.", e)
            context.fail(UnableToStartDatabaseException.unableToStartDb(id, e))
            counts.increaseFailedCount()
        }
    }

    fun stopDatabase(id: NamedDatabaseId) {
        val context = repository.getDatabaseContext(id)
        if (context.isEmpty) {
            log.warn("Cannot stop '%s': it does not exist.", id)
            return
        }
        stopDatabase(context.get())
    }

    fun stopDatabase(context: StandaloneDatabaseContext) {
        val id = context.database().namedDatabaseId
        // A failure until now, typically a panic, is not a failure to stop.
        context.clearFailure()
        try {
            log.info("Stopping '%s'.", id)
            context.database().stop()
            log.info("Stopped '%s' successfully.", id)
            counts.increaseStopCount()
        } catch (e: Throwable) {
            log.error("Failed to stop '$id'.", e)
            context.fail(DatabaseManagementException("An error occurred! Unable to stop `$id`.", e))
        }
    }

    /** Stops [id] without a checkpoint, unregisters it and deletes its files. */
    @Synchronized
    fun dropDatabase(id: NamedDatabaseId) {
        val context = repository.getDatabaseContext(id)
        if (context.isEmpty) {
            log.warn("Cannot drop '%s': it does not exist.", id)
            return
        }
        log.info("Dropping '%s'.", id)
        repository.remove(id)
        try {
            context.get().database().drop()
        } catch (e: Throwable) {
            log.error("Failed to drop '$id'.", e)
            throw DatabaseManagementException("Failed to drop `$id`.", e)
        }
        counts.increaseDropCount()
        log.info("Dropped '%s' successfully.", id)
    }

    /** Creates `system` when initialised and starts it when started. */
    fun systemDatabaseStarter(): Lifecycle =
        object : LifecycleAdapter() {
            override fun init() {
                createDatabase(NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID)
            }

            override fun start() = startDatabase(systemContext())
        }

    /** Creates and starts the default database, which `system` must name. */
    fun defaultDatabaseStarter(): Lifecycle =
        object : LifecycleAdapter() {
            override fun start() {
                synchronized(this@DatabaseManager) {
                    val id =
                        repository.databaseIdRepository().getByName(defaultDatabase).orElseThrow {
                            DatabaseNotFoundHelper.defaultDatabaseNotFound(defaultDatabase)
                        }
                    if (repository.getDatabaseContext(id).isPresent) {
                        throw DatabaseManagementException(
                            "Cannot initialize $id because it already exists"
                        )
                    }
                    startDatabase(createDatabase(id))
                }
            }
        }

    /**
     * Stops every database when stopped, `system` last. Throws if the default database or `system`
     * failed to stop, unless they had failed to start.
     */
    fun allDatabaseStopper(): Lifecycle =
        object : LifecycleAdapter() {
            override fun stop() {
                val system = systemContext()
                val others =
                    repository.registeredDatabases().filterKeys { !it.isSystemDatabase }.values
                others.forEach(::stopDatabase)
                stopDatabase(system)
                listOfNotNull(repository.getDatabaseContext(defaultDatabase).orElse(null), system)
                    .firstOrNull {
                        it.isFailed && it.failureCause() !is UnableToStartDatabaseException
                    }
                    ?.let {
                        throw DatabaseManagementException(
                            "Failed to stop ${it.database().namedDatabaseId.name()} database.",
                            it.failureCause(),
                        )
                    }
            }
        }

    private fun systemContext(): StandaloneDatabaseContext =
        repository.getDatabaseContext(NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID).orElseThrow {
            DatabaseNotFoundHelper.databaseNotFound(NamedDatabaseId.SYSTEM_DATABASE_NAME)
        }
}
