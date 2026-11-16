/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Modifications Copyright (c) The Dodeca Authors
 * 2026-11-11: ported to Kotlin from Neo4j 5.26's LocalRoutingTableServiceValidator,
 * DefaultDatabaseAvailabilityChecker and SingleAddressRoutingTableProvider, which Neo4j 2026
 * removed.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */
package dev.dodeca.core.routing

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.configuration.connectors.ConnectorPortRegister
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.dbms.routing.RoutingResult
import org.neo4j.dbms.routing.RoutingTableServiceHelpers
import org.neo4j.kernel.database.DatabaseReferenceImpl
import org.neo4j.values.virtual.MapValue

/**
 * The routing tables of the databases hosted on this server, which name only this server
 * ([singleAddress]). A database that is not hosted here does not exist; one that is, but does not
 * run, is not available.
 */
class LocalRoutingTables(
    private val databases: DatabaseContextProvider<*>,
    private val singleAddress: SingleAddressRoutingTable,
) : RoutingTables {
    /** Whether [reference]'s database is hosted here. */
    fun hosts(reference: DatabaseReferenceImpl.Internal): Boolean =
        databases.getDatabaseContext(reference.databaseId()).isPresent

    override fun check(reference: DatabaseReferenceImpl.Internal) {
        val name = reference.alias().name()
        val context =
            databases.getDatabaseContext(reference.databaseId()).orElseThrow {
                RoutingTableServiceHelpers.databaseNotFoundException(name)
            }
        if (!context.database().databaseAvailabilityGuard.isAvailable) {
            throw RoutingTableServiceHelpers.databaseNotAvailableException(name)
        }
    }

    override fun table(
        reference: DatabaseReferenceImpl.Internal,
        routingContext: MapValue,
    ): RoutingResult = singleAddress.table(routingContext)
}

/**
 * The routing table that names only this server, as a router, writer and reader: by the address the
 * driver connected to, if it says, else by `server.bolt.advertised_address`, with the port Bolt
 * listens on where that has none. It lasts `dbms.routing_ttl`.
 */
class SingleAddressRoutingTable(
    private val ports: ConnectorPortRegister,
    private val config: Config,
) {
    fun table(routingContext: MapValue): RoutingResult =
        RoutingResult.single(
            RoutingTableServiceHelpers.ensureBoltAddressIsUsable(
                RoutingTableServiceHelpers.findClientProvidedAddress(routingContext),
                ports,
                config.get(BoltConnector.advertised_address),
            ),
            config.get(GraphDatabaseSettings.routing_ttl).toMillis(),
        )
}
