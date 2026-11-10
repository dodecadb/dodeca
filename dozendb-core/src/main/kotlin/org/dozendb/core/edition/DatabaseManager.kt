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
 * 2026-11-09: starts a database again when Neo4j's file watcher failed to start it because a
 * directory entry went meanwhile.
 * 2026-11-10: starts no database in quarantine; stops databases without the checkpoint I/O limit.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */
package org.dozendb.core.edition

import java.nio.file.NoSuchFileException
import java.util.Optional
import org.dozendb.core.checkpoint.CheckpointIoController
import org.dozendb.core.store.StorePlacement
import org.neo4j.dbms.api.DatabaseManagementException
import org.neo4j.dbms.api.DatabaseNotFoundHelper
import org.neo4j.dbms.database.DatabaseContextFactory
import org.neo4j.dbms.database.DatabaseOperationCounts
import org.neo4j.dbms.database.DatabaseRepository
import org.neo4j.dbms.database.StandaloneDatabaseContext
import org.neo4j.dbms.database.UnableToStartDatabaseException
import org.neo4j.io.fs.watcher.DatabaseLayoutWatcher
import org.neo4j.kernel.database.Database
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.lifecycle.Lifecycle
import org.neo4j.kernel.lifecycle.LifecycleAdapter
import org.neo4j.logging.InternalLogProvider

/**
 * This server's databases: creates, starts, stops and drops them, and keeps the created ones in
 * [repository]. A database that fails to start or stop is marked failed rather than throwing. A
 * database whose files [placement] left half-moved, as by an interrupted restore, fails to start. A
 * database in [quarantines] is not started at all.
 *
 * Like Neo4j's own database lifecycles, it throws database management exceptions without a GQL
 * status, which Neo4j 5.26 has none for.
 */
@Suppress("DEPRECATION")
class DatabaseManager(
    private val repository: DatabaseRepository<StandaloneDatabaseContext>,
    private val contextFactory: DatabaseContextFactory<StandaloneDatabaseContext, Optional<*>>,
    private val defaultDatabase: () -> String?,
    private val counts: DatabaseOperationCounts.Counter,
    private val placement: StorePlacement,
    private val quarantines: Quarantines,
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

    fun startDatabase(id: NamedDatabaseId, seed: Seed? = null) {
        val context = repository.getDatabaseContext(id)
        if (context.isEmpty) {
            log.warn("Cannot start '%s': it does not exist.", id)
            return
        }
        startDatabase(context.get(), seed)
    }

    /** What a database without a store is created from. */
    sealed interface Seed {
        /** A dump or full backup at [uri], which [seeder] reads. */
        class Uri(val uri: String, val seeder: () -> DatabaseSeeder?) : Seed {
            override fun toString() = "its seed $uri"
        }

        /** The store that `neo4j-admin database restore` prepared on this server. */
        data object Restored : Seed {
            override fun toString() = "the store restored on this server"
        }
    }

    /**
     * Starts the database of [context]; first, if it has no store and a [seed], puts the seed's
     * store in place. A seed that fails fails the database.
     */
    /**
     * Starts [database], again while it fails because an entry of a directory went while Neo4j's
     * file watcher began to watch the directory: the JDK's polling watch service (macOS and AIX)
     * reads each entry as it registers a directory, and fails if one goes meanwhile, as when
     * another database's copy moves into place or a database is dropped. Returns false, starting
     * nothing, if the database is in quarantine: checked under the database's monitor, which
     * starting and stopping it hold, and under which [DatabaseQuarantine] puts it in quarantine.
     */
    private fun start(database: Database): Boolean {
        var attempt = 1
        while (true) {
            try {
                synchronized(database) {
                    if (quarantined(database.namedDatabaseId)) return false
                    database.start()
                }
                return true
            } catch (e: Throwable) {
                if (attempt == WATCH_ATTEMPTS || !goneWhileWatched(e)) throw e
                log.warn(
                    "Starting '%s' failed as a file went while Neo4j began to watch its " +
                        "directory; starting it again",
                    database.namedDatabaseId,
                )
                attempt++
            }
        }
    }

    private fun goneWhileWatched(failure: Throwable): Boolean =
        generateSequence(failure) { it.cause }
            .any { cause ->
                cause is NoSuchFileException &&
                    cause.stackTrace.any { it.className == DatabaseLayoutWatcher::class.java.name }
            }

    fun startDatabase(context: StandaloneDatabaseContext, seed: Seed? = null) {
        val id = context.database().namedDatabaseId
        if (quarantined(id)) {
            log.info("Not starting '%s': it is in quarantine on this server.", id)
            return
        }
        if (seed != null && !placement.holdsStore(id.name()) && !placement.incomplete(id.name())) {
            try {
                log.info("Creating the store of '%s' from %s.", id, seed)
                when (seed) {
                    is Seed.Uri -> {
                        val seeder =
                            seed.seeder()
                                ?: throw IllegalStateException(
                                    "this server cannot create databases from seeds"
                                )
                        seeder.seed(id, seed.uri)
                    }
                    Seed.Restored -> {
                        val staged =
                            placement.staged(id.name(), StorePlacement.RESTORED)
                                ?: throw IllegalStateException(
                                    "there is none. Restore it here with 'neo4j-admin database " +
                                        "restore' first"
                                )
                        placement.moveIntoPlace(staged, id.name())
                    }
                }
            } catch (e: Exception) {
                val message = "Unable to create `${id.name()}` from $seed: ${e.message}"
                log.error(message, e)
                // Without the cause, whose message the message holds; the log has its trace.
                context.fail(DatabaseManagementException(message))
                counts.increaseFailedCount()
                return
            }
        }
        if (placement.incomplete(id.name())) {
            val message =
                "Unable to start `${id.name()}`: its files were left half-moved into place, as " +
                    "by an interrupted 'neo4j-admin database restore'. Restore it again."
            log.error(message)
            context.fail(DatabaseManagementException(message))
            counts.increaseFailedCount()
            return
        }
        try {
            log.info("Starting '%s'.", id)
            if (start(context.database())) {
                counts.increaseStartCount()
            } else {
                log.info("Not starting '%s': it was put in quarantine on this server.", id)
            }
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
            // Stopping waits for the background checkpoint under way, if any.
            val database = context.database()
            CheckpointIoController.unlimited(database.ioController) { database.stop() }
            log.info("Stopped '%s' successfully.", id)
            counts.increaseStopCount()
        } catch (e: Throwable) {
            log.error("Failed to stop '$id'.", e)
            context.fail(DatabaseManagementException("An error occurred! Unable to stop `$id`.", e))
        }
    }

    /** Stops [id] and unregisters it, keeping its files, so that it can be created anew. */
    @Synchronized
    fun releaseDatabase(id: NamedDatabaseId) {
        val context = repository.getDatabaseContext(id)
        if (context.isEmpty) {
            log.warn("Cannot release '%s': it does not exist.", id)
            return
        }
        stopDatabase(context.get())
        repository.remove(id)
        log.info("Released '%s', keeping its files.", id)
    }

    /**
     * Stops [id] without a checkpoint, unregisters it and deletes its files; a quarantine of it
     * here goes with it.
     */
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
        quarantines.remove(id.databaseId().uuid())
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

    /** Creates and starts the default database, which `system` must name, once `system` runs. */
    fun defaultDatabaseStarter(): Lifecycle =
        object : LifecycleAdapter() {
            override fun start() {
                synchronized(this@DatabaseManager) {
                    val name =
                        defaultDatabase()
                            ?: throw DatabaseManagementException(
                                "The system graph names no default database."
                            )
                    val id =
                        repository.databaseIdRepository().getByName(name).orElseThrow {
                            DatabaseNotFoundHelper.defaultDatabaseNotFound(name)
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
                val default = runCatching { defaultDatabase() }.getOrNull()
                val system = systemContext()
                val others =
                    repository.registeredDatabases().filterKeys { !it.isSystemDatabase }.values
                others.forEach(::stopDatabase)
                stopDatabase(system)
                listOfNotNull(
                        default?.let { repository.getDatabaseContext(it).orElse(null) },
                        system,
                    )
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

    private fun quarantined(id: NamedDatabaseId): Boolean =
        quarantines.of(id.databaseId().uuid()) != null

    private fun systemContext(): StandaloneDatabaseContext =
        repository.getDatabaseContext(NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID).orElseThrow {
            DatabaseNotFoundHelper.databaseNotFound(NamedDatabaseId.SYSTEM_DATABASE_NAME)
        }

    private companion object {
        /** How often a database is started while directory entries go as it begins to watch. */
        const val WATCH_ATTEMPTS = 3
    }
}
