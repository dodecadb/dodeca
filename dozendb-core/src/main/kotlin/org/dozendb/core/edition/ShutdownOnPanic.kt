package org.dozendb.core.edition

import java.util.concurrent.atomic.AtomicBoolean
import kotlin.system.exitProcess
import org.dozendb.core.DozenDbSettings
import org.neo4j.configuration.Config
import org.neo4j.graphdb.event.DatabaseEventContext
import org.neo4j.graphdb.event.DatabaseEventListenerAdapter
import org.neo4j.logging.InternalLog

/** Ends the server's process with a status, as `server.panic.shutdown_on_panic` does. */
fun interface ServerExit {
    fun exit(status: Int)

    companion object {
        /**
         * Exits the JVM, on a thread of its own: Neo4j's shutdown hook then stops the server, which
         * a panicking database's thread must not wait for.
         */
        val PROCESS = ServerExit { status ->
            Thread({ exitProcess(status) }, "dozendb-shutdown-on-panic").start()
        }
    }
}

/**
 * Ends the server's process, with status 1, once any database panics, when
 * `server.panic.shutdown_on_panic` is true, as in Neo4j Enterprise: so that a process manager
 * starts it again. Either way [DatabaseQuarantine] puts the database's copy in quarantine first, so
 * that the restart keeps it stopped.
 */
internal class ShutdownOnPanic(
    private val config: Config,
    private val log: InternalLog,
    private val exit: ServerExit,
) : DatabaseEventListenerAdapter() {
    private val exiting = AtomicBoolean()

    override fun databasePanic(eventContext: DatabaseEventContext) {
        if (!config.get(DozenDbSettings.shutdownOnPanic)) return
        if (!exiting.compareAndSet(false, true)) return
        log.error(
            "Database %s panicked: shutting the server down, as %s is true.",
            eventContext.databaseName,
            DozenDbSettings.shutdownOnPanic.name(),
        )
        exit.exit(1)
    }
}
