package org.dozendb.cluster.internal

import org.dozendb.cluster.DatabaseGroups
import org.dozendb.consensus.DatabaseRef
import org.neo4j.kernel.database.NamedDatabaseId

/** The group of the database [id] (see [DatabaseGroups]). */
internal fun DatabaseGroups.of(id: NamedDatabaseId): DatabaseRef =
    of(id.name(), id.databaseId().uuid())
