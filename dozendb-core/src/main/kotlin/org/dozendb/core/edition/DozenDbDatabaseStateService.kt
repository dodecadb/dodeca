package org.dozendb.core.edition

import java.util.Optional
import org.neo4j.dbms.CommunityDatabaseState
import org.neo4j.dbms.DatabaseStateService
import org.neo4j.dbms.OperatorState
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.dbms.database.StandaloneDatabaseContext
import org.neo4j.kernel.database.NamedDatabaseId

/**
 * The states `SHOW DATABASES` shows, as Neo4j Community reports them, except that a database that
 * failed while running, such as one that panicked, is offline, and one in [quarantines] is
 * `quarantined`, with the quarantine's message, as in Neo4j Enterprise. Community shows a failed
 * one online, as it is still started, although it serves nothing; Neo4j documents offline with a
 * status message as a database that is not running because of a problem.
 */
internal class DozenDbDatabaseStateService(
    private val databases: DatabaseContextProvider<StandaloneDatabaseContext>,
    private val quarantines: Quarantines,
) : DatabaseStateService {
    override fun stateOfAllDatabases(): Map<NamedDatabaseId, org.neo4j.dbms.DatabaseState> =
        databases.registeredDatabases().mapValues { (_, context) -> stateOf(context) }

    override fun stateOfDatabase(namedDatabaseId: NamedDatabaseId): org.neo4j.dbms.DatabaseState =
        databases
            .getDatabaseContext(namedDatabaseId)
            .map(::stateOf)
            .orElse(CommunityDatabaseState.unknown(namedDatabaseId))

    /** The failure, or the quarantine as a failure whose message is the quarantine's. */
    override fun causeOfFailure(namedDatabaseId: NamedDatabaseId): Optional<Throwable> {
        quarantineOf(namedDatabaseId)?.let {
            return Optional.of(QuarantineMessage(it.message))
        }
        return databases.getDatabaseContext(namedDatabaseId).map { it.failureCause() }
    }

    private fun stateOf(context: StandaloneDatabaseContext): org.neo4j.dbms.DatabaseState {
        val id = context.database().namedDatabaseId
        quarantineOf(id)?.let {
            return QuarantinedState(id, it)
        }
        return CommunityDatabaseState(
            id,
            context.database().isStarted && !context.isFailed,
            context.isFailed,
            context.failureCause(),
        )
    }

    private fun quarantineOf(id: NamedDatabaseId): Quarantine? =
        quarantines.of(id.databaseId().uuid())

    /** A database in quarantine on this server: stopped, not failed, with its message. */
    private class QuarantinedState(
        private val id: NamedDatabaseId,
        private val quarantine: Quarantine,
    ) : org.neo4j.dbms.DatabaseState {
        override fun databaseId(): NamedDatabaseId = id

        override fun operatorState(): OperatorState = Quarantined

        override fun hasFailed(): Boolean = false

        override fun failure(): Optional<Throwable> = Optional.empty()

        override fun statusMessage(): Optional<String> = Optional.of(quarantine.message)
    }

    /** The status Neo4j Enterprise shows a copy in quarantine with. */
    object Quarantined : OperatorState {
        const val DESCRIPTION = "quarantined"

        override fun description(): String = DESCRIPTION

        // After Neo4j Community's STOPPED, STARTED and UNKNOWN.
        override fun ordinal(): Int = 3
    }

    /** Carries the message of a quarantine where Neo4j Community reads a failure's message. */
    private class QuarantineMessage(message: String) : Exception(message, null, false, false)
}
