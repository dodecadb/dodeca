package org.dozendb.core.edition

import org.neo4j.kernel.database.NamedDatabaseId

/**
 * Creates the store of a database from a seed: a backup or a dump at a URI, as `CREATE DATABASE ...
 * OPTIONS {seedURI: ...}` names. DozenDB's backup module provides one as a global dependency.
 */
interface DatabaseSeeder {
    /**
     * Checks that this server reads seeds at [uri], by its scheme.
     *
     * @throws IllegalArgumentException telling why not.
     */
    fun check(uri: String)

    /**
     * Puts the store of the seed at [uri] in place as the store of [database], which has none,
     * detached from any Raft group it was replicated in.
     */
    fun seed(database: NamedDatabaseId, uri: String)
}
