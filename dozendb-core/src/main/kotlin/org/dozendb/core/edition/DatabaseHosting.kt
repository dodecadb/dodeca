package org.dozendb.core.edition

import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.HostedOnMode
import org.neo4j.internal.id.IdGeneratorFactory
import org.neo4j.kernel.database.Database
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.impl.api.LeaseService
import org.neo4j.kernel.impl.api.TransactionalProcessFactory

/**
 * Hosts this server's databases together with other servers, as clustering does. Without one, this
 * server alone hosts each database, as in Neo4j Community.
 */
interface DatabaseHosting {
    /**
     * Whether a database's ID generators reuse freed IDs from the start. A hosting that must first
     * learn which IDs are free enables reuse itself later.
     */
    val reusesFreedIdsAtStart: Boolean

    /**
     * Prepares to host the database [namedDatabaseId], before Neo4j creates it. [storeExists] tells
     * whether the database's files exist or Neo4j is about to create them.
     */
    fun prepare(
        namedDatabaseId: NamedDatabaseId,
        idGeneratorFactory: IdGeneratorFactory,
        storeExists: Boolean,
    ): HostedDatabase

    /**
     * Called before the database [namedDatabaseId] starts, each time, with its store in place; an
     * exception fails the start, with its message.
     */
    fun beforeStart(namedDatabaseId: NamedDatabaseId) {}
}

/** A database as a [DatabaseHosting] hosts it. */
interface HostedDatabase {
    /** How Neo4j reports that the database is hosted. */
    val hostedOnMode: HostedOnMode

    /** The leases the database's transactions take. */
    val leaseService: LeaseService

    /** The database's commit process, built around Neo4j's [local] one. */
    fun commitProcessFactory(local: TransactionalProcessFactory): TransactionalProcessFactory

    /** Called with the database once Neo4j has created it. */
    fun attach(database: Database)
}
