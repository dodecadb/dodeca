/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Modifications Copyright (c) DozenDB
 * 2026-11-11: ported to Kotlin from Neo4j 5.26's DefaultRoutingService, which Neo4j 2026 replaced
 * with one that routes only the databases hosted here, found by name; without routers provided by
 * clients, and without panics.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */
package org.dozendb.core.routing

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.dbms.routing.ClientRoutingDomainChecker
import org.neo4j.dbms.routing.RoutingException
import org.neo4j.dbms.routing.RoutingResult
import org.neo4j.dbms.routing.RoutingService
import org.neo4j.dbms.routing.RoutingTableServiceHelpers
import org.neo4j.kernel.database.DatabaseReferenceImpl
import org.neo4j.kernel.database.DatabaseReferenceRepository
import org.neo4j.kernel.database.DefaultDatabaseResolver
import org.neo4j.values.virtual.MapValue

/**
 * The routing tables of drivers that route (`neo4j://`), as Neo4j 5.26 answered them, by database
 * or alias: Neo4j 2026 Community's know neither aliases nor databases hosted elsewhere.
 *
 * The database or alias of the name, or else the user's home database, must be one of [references].
 * A remote alias gets [singleAddress]'s table, of this server, which forwards its queries; it
 * refuses if another alias forwarded the request, as alias chains are not allowed. A database, or a
 * local alias of one, must pass [tables]' check; it then gets the table of [tables] if the driver
 * routes itself, else [singleAddress]'s. A driver routes itself unless this server is [alone], with
 * `dbms.routing.default_router` set to `CLIENT`, or set to `SERVER` and the address the driver
 * connected to is in one of `dbms.routing.client_side.enforce_for_domains` ([domains]).
 */
class DozenDbRoutingService(
    private val references: DatabaseReferenceRepository,
    private val defaultDatabase: DefaultDatabaseResolver,
    private val tables: RoutingTables,
    private val singleAddress: SingleAddressRoutingTable,
    private val domains: ClientRoutingDomainChecker,
    private val config: Config,
    private val alone: () -> Boolean,
) : RoutingService {
    override fun route(
        databaseName: String?,
        user: String?,
        routingContext: MapValue,
        isDefaultDatabase: Boolean,
    ): RoutingResult {
        val name = databaseName?.takeIf { it.isNotBlank() } ?: defaultDatabase.defaultDatabase(user)
        val reference =
            references.getByAlias(name).orElseThrow {
                RoutingTableServiceHelpers.databaseNotFoundException(name)
            }
        val alias = reference.alias().name()
        if (!config.get(BoltConnector.enabled)) throw RoutingException.boltNotEnabled(alias)
        val result =
            if (reference is DatabaseReferenceImpl.Internal) {
                tables.check(reference)
                if (routesItself(routingContext)) tables.table(reference, routingContext)
                else singleAddress.table(routingContext)
            } else {
                RoutingTableServiceHelpers.findClientProvidedAliasChain(routingContext)?.let {
                    throw RoutingException.aliasChainsNotPermitted(alias, it)
                }
                singleAddress.table(routingContext)
            }
        if (
            result.routeEndpoints().isEmpty() &&
                result.writeEndpoints().isEmpty() &&
                result.readEndpoints().isEmpty()
        ) {
            throw RoutingException.routingTableIsEmpty(alias)
        }
        return result
    }

    private fun routesItself(routingContext: MapValue): Boolean {
        if (alone()) return false
        return when (config.get(GraphDatabaseSettings.routing_default_router)!!) {
            GraphDatabaseSettings.RoutingMode.CLIENT -> true
            GraphDatabaseSettings.RoutingMode.SERVER -> {
                val address = RoutingTableServiceHelpers.findClientProvidedAddress(routingContext)
                address == null || domains.shouldGetClientRouting(address)
            }
        }
    }
}

/** The routing tables of the databases that drivers route themselves to. */
interface RoutingTables {
    /** Refuses [reference] if it cannot be routed, as it does not exist or is not available. */
    fun check(reference: DatabaseReferenceImpl.Internal)

    /** The routing table of [reference], which passed [check], for [routingContext]. */
    fun table(reference: DatabaseReferenceImpl.Internal, routingContext: MapValue): RoutingResult
}
