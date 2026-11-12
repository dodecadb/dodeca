package org.dozendb.core.edition

import java.time.Clock
import java.time.Duration
import org.dozendb.core.Gql
import org.neo4j.dbms.api.DatabaseNotFoundHelper
import org.neo4j.dbms.database.DatabaseRepository
import org.neo4j.dbms.database.StandaloneDatabaseContext
import org.neo4j.graphdb.event.DatabaseEventContext
import org.neo4j.graphdb.event.DatabaseEventListenerAdapter
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.monitoring.ExceptionalDatabaseEvent
import org.neo4j.logging.InternalLog

/**
 * Puts the copies of databases on this server in quarantine and lifts it, for
 * `dbms.quarantineDatabase`, and puts a copy that fails while running, as by a panic, in
 * quarantine, as Neo4j Enterprise does.
 *
 * A copy in quarantine is stopped here and stays stopped: the [DatabaseManager] starts none, so
 * neither START DATABASE nor a restart starts it until the quarantine is lifted, while the other
 * servers follow commands as usual. Lifting the quarantine brings the copy to the state the system
 * graph asks of it. The quarantines are kept in [quarantines].
 */
class DatabaseQuarantine(
    private val quarantines: Quarantines,
    private val repository: DatabaseRepository<StandaloneDatabaseContext>,
    private val copies: Copies,
    private val clock: Clock,
    private val log: InternalLog,
) : DatabaseEventListenerAdapter() {
    /** The copies of databases on this server, as quarantines stop them and bring them back. */
    interface Copies {
        /** Stops the copy of [id] on this server only: one that leads hands its leadership over. */
        fun stop(id: NamedDatabaseId)

        /** Brings the copy of [id] on this server to the state the system graph asks of it. */
        fun reconcile(id: NamedDatabaseId)
    }

    /** What a call of `dbms.quarantineDatabase` did, in its columns. */
    data class Outcome(val databaseName: String, val quarantined: Boolean, val result: String)

    /** A call of `dbms.quarantineDatabase`: on the database [database], while [open]. */
    class Caller(val database: String, val open: () -> Boolean)

    /**
     * Puts the copy of the database [name] on this server in quarantine, by [user] for [reason], or
     * lifts its quarantine, as [quarantined] says. A copy in quarantine already gets the new
     * reason. Lifting the quarantine of a copy that is in none changes nothing.
     *
     * The copy is stopped before this returns, unless the [caller] runs on that database: stopping
     * it waits for the caller's transaction, so it then stops in the background once that is
     * closed.
     */
    fun set(
        name: String,
        quarantined: Boolean,
        user: String,
        reason: String,
        caller: Caller? = null,
    ): Outcome {
        val id =
            repository.databaseIdRepository().getByName(name).orElseThrow {
                DatabaseNotFoundHelper.databaseDoesNotExist(name)
            }
        val uuid = id.databaseId().uuid()
        if (!quarantined) {
            val lifted =
                quarantines.remove(uuid)
                    ?: return Outcome(id.name(), false, "The database is in no quarantine here")
            log.info(
                "Lifted the quarantine of '%s' on this server (%s), as %s asked",
                id.name(),
                lifted.message,
                user,
            )
            // A failure from before, such as the panic that put it in quarantine, which Neo4j may
            // record after the copy stopped, is no reason to keep it stopped: it is tried anew.
            repository.getDatabaseContext(id).ifPresent { context ->
                if (!context.database().isStarted) context.clearFailure()
            }
            copies.reconcile(id)
            return Outcome(id.name(), false, Quarantine(user, clock.instant(), LIFTED).message)
        }
        val context =
            repository.getDatabaseContext(id).orElseThrow {
                Gql.refused(
                    "Database '${id.name()}' is not hosted on this server: put the copy of a " +
                        "server in quarantine on that server, connected to it with bolt://."
                )
            }
        val quarantine = Quarantine(user, clock.instant(), reason)
        // As the database manager checks for a quarantine before it starts a database.
        synchronized(context.database()) { quarantines.put(uuid, quarantine) }
        log.warn("Put '%s' in quarantine on this server: %s", id.name(), quarantine.message)
        if (caller?.database == id.name()) {
            inBackground(id) {
                val deadline = System.nanoTime() + CALLER_WAIT.toNanos()
                while (caller.open() && System.nanoTime() - deadline < 0) Thread.sleep(10)
                copies.stop(id)
            }
        } else {
            copies.stop(id)
        }
        return Outcome(id.name(), true, quarantine.message)
    }

    /**
     * Puts the copy of a database that panicked in quarantine, before any other listener can end
     * the server (`server.panic.shutdown_on_panic`), and stops it in the background: the panic may
     * come from a thread that stopping waits for.
     */
    override fun databasePanic(eventContext: DatabaseEventContext) {
        val context =
            repository.getDatabaseContext(eventContext.databaseName).orElse(null) ?: return
        val id = context.database().namedDatabaseId
        val cause = (eventContext as? ExceptionalDatabaseEvent)?.cause
        val quarantine =
            Quarantine(
                Quarantine.SERVER,
                clock.instant(),
                "Database '${id.name()}' failed while running" +
                    (cause?.let { ": ${describe(it)}" } ?: "") +
                    ". Once its cause is dealt with, lift the quarantine on this server with " +
                    "CALL dbms.quarantineDatabase('${id.name()}', false)",
            )
        try {
            quarantines.put(id.databaseId().uuid(), quarantine)
        } catch (e: Exception) {
            log.error("Putting '${id.name()}', which panicked, in quarantine failed", e)
            return
        }
        log.error("Put '%s' in quarantine on this server: %s", id.name(), quarantine.message)
        inBackground(id) { copies.stop(id) }
    }

    /** Runs [stop], which stops the copy of [id], on a thread of its own. */
    private fun inBackground(id: NamedDatabaseId, stop: () -> Unit) {
        Thread(
                {
                    try {
                        stop()
                    } catch (e: Exception) {
                        log.error("Stopping '${id.name()}', in quarantine, failed", e)
                    }
                },
                "dozendb-quarantine-${id.name()}",
            )
            .apply { isDaemon = true }
            .start()
    }

    private companion object {
        const val LIFTED = "Lifted the quarantine"

        /** How long a copy put in quarantine by a call on it waits for the call to end. */
        val CALLER_WAIT: Duration = Duration.ofMinutes(1)
    }
}
