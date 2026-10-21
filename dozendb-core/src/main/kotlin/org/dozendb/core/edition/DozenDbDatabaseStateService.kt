package org.dozendb.core.edition

import java.util.Optional
import org.neo4j.dbms.CommunityDatabaseState
import org.neo4j.dbms.DatabaseStateService
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.dbms.database.StandaloneDatabaseContext
import org.neo4j.kernel.database.NamedDatabaseId

/**
 * The states `SHOW DATABASES` shows, as Neo4j Community reports them, except that a database that
 * failed while running, such as one that panicked, is offline. Community shows it online, as it is
 * still started, although it serves nothing; Neo4j documents offline with a status message as a
 * database that is not running because of a problem.
 */
internal class DozenDbDatabaseStateService(
    private val databases: DatabaseContextProvider<StandaloneDatabaseContext>
) : DatabaseStateService {
    override fun stateOfAllDatabases(): Map<NamedDatabaseId, org.neo4j.dbms.DatabaseState> =
        databases.registeredDatabases().mapValues { (_, context) -> stateOf(context) }

    override fun stateOfDatabase(namedDatabaseId: NamedDatabaseId): org.neo4j.dbms.DatabaseState =
        databases
            .getDatabaseContext(namedDatabaseId)
            .map(::stateOf)
            .orElse(CommunityDatabaseState.unknown(namedDatabaseId))

    override fun causeOfFailure(namedDatabaseId: NamedDatabaseId): Optional<Throwable> =
        databases.getDatabaseContext(namedDatabaseId).map { it.failureCause() }

    private fun stateOf(context: StandaloneDatabaseContext): org.neo4j.dbms.DatabaseState =
        CommunityDatabaseState(
            context.database().namedDatabaseId,
            context.database().isStarted && !context.isFailed,
            context.isFailed,
            context.failureCause(),
        )
}
