/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Modifications Copyright (c) DozenDB
 * 2026-11-11: the default database of the system graph, ported to Kotlin from Neo4j 5.26's
 * CommunityDefaultDatabaseResolver, which Neo4j 2026 replaced with the configured default alone.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */
package org.dozendb.core.security

import java.util.Optional
import java.util.concurrent.atomic.AtomicReference
import org.dozendb.core.edition.DefaultDatabase
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.dbms.systemgraph.SystemDatabaseProvider
import org.neo4j.dbms.systemgraph.SystemDatabaseProvider.SystemDatabaseUnavailableException
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel
import org.neo4j.graphdb.DatabaseShutdownException
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.event.TransactionData
import org.neo4j.graphdb.event.TransactionEventListenerAdapter
import org.neo4j.kernel.database.DefaultDatabaseResolver

/**
 * The default database: a user's home database, set with `SET HOME DATABASE`, before the DBMS
 * default database, the one the system graph marks default (`dbms.setDefaultDatabase` moves the
 * mark), or `initial.dbms.default_database` while none is marked. A home database that does not
 * exist is still resolved, as Neo4j Enterprise does: the user's queries then fail until it exists.
 *
 * Listens to the transactions of `system`, after each of which it reads the system graph anew.
 * While `system` is unavailable, as while the server stops, a default database it has to read fails
 * with the transient error of an unavailable database, which drivers retry elsewhere, where Neo4j's
 * own error has no status: Bolt would log it as unexpected and not send it back.
 */
class DozenDbDefaultDatabaseResolver(
    private val config: Config,
    private val systemDatabase: SystemDatabaseProvider,
    private val security: SystemGraphSecurity,
) : TransactionEventListenerAdapter<Any?>(), DefaultDatabaseResolver {
    private val cached = AtomicReference<String?>()

    override fun defaultDatabase(username: String?): String =
        try {
            username?.takeIf { it.isNotEmpty() }?.let(security::homeDatabase) ?: dbmsDefault()
        } catch (e: SystemDatabaseUnavailableException) {
            throw DatabaseShutdownException.databaseUnavailable(
                GraphDatabaseSettings.SYSTEM_DATABASE_NAME,
                e,
            )
        }

    private fun dbmsDefault(): String =
        cached.get()
            ?: systemDatabase
                .query { tx ->
                    // An Optional: the provider does not take null for an answer.
                    Optional.ofNullable(
                        tx.findNode(
                                TopologyGraphDbmsModel.DATABASE_LABEL,
                                DefaultDatabase.PROPERTY,
                                true,
                            )
                            ?.getProperty(TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY, null)
                            as String?
                    )
                }
                .orElseGet { config.get(GraphDatabaseSettings.initial_default_database) }
                .also { cached.set(it) }

    /** Forgets the default database, and the users, roles and privileges. */
    fun clearCache() {
        cached.set(null)
        security.clear()
    }

    override fun afterCommit(
        data: TransactionData,
        state: Any?,
        databaseService: GraphDatabaseService,
    ) {
        clearCache()
    }
}
