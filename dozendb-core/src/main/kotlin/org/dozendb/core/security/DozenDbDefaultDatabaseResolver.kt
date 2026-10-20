package org.dozendb.core.security

import org.neo4j.configuration.Config
import org.neo4j.dbms.systemgraph.SystemDatabaseProvider
import org.neo4j.server.security.systemgraph.CommunityDefaultDatabaseResolver

/**
 * Neo4j Community's default database resolver, which resolves a user's home database, set with `SET
 * HOME DATABASE`, before the DBMS default database. A home database that does not exist is still
 * resolved, as Neo4j Enterprise does: the user's queries then fail until it exists.
 */
class DozenDbDefaultDatabaseResolver(
    config: Config,
    systemDatabase: SystemDatabaseProvider,
    private val users: SystemGraphUsers,
) : CommunityDefaultDatabaseResolver(config, systemDatabase) {
    override fun defaultDatabase(username: String?): String =
        username?.takeIf { it.isNotEmpty() }?.let(users::homeDatabase)
            ?: super.defaultDatabase(username)

    /** Also forgets the users' home databases and status: the system graph changed. */
    override fun clearCache() {
        super.clearCache()
        users.clear()
    }
}
