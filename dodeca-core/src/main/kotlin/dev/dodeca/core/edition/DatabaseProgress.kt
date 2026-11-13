package dev.dodeca.core.edition

import dev.dodeca.core.servers.CopyMode
import java.time.Duration
import java.util.UUID
import org.neo4j.dbms.database.DatabaseRepository
import org.neo4j.dbms.database.StandaloneDatabaseContext

/**
 * What the system graph asks of the database [name] after an administration command: to run as [id]
 * if [online], to be stopped if not, or, without an [id], to be dropped.
 */
data class WantedDatabase(val name: String, val id: UUID?, val online: Boolean)

/**
 * A database on one server: running, stopped, being copied from another server, failed with
 * [failure], or in quarantine there, with the quarantine's message as [failure]. [leaderKnown]
 * tells whether the server knows the leader of its copies, which takes its writes: in a cluster, of
 * its Raft group; a server without clustering leads its own. The server hosts it in [mode]: a
 * server without clustering hosts primaries. A secondary may be [paused]: it pulls nothing until it
 * is resumed.
 */
data class DatabaseState(
    val name: String,
    val id: UUID,
    val status: Status,
    val failure: String? = null,
    val leaderKnown: Boolean = true,
    val mode: CopyMode = CopyMode.PRIMARY,
    val paused: Boolean = false,
) {
    enum class Status {
        ONLINE,
        /** Started, but not yet replicated: a new database's Raft group starts after it. */
        STARTING,
        OFFLINE,
        /** Copied from another server of a cluster, before it runs here or to catch up. */
        COPYING,
        FAILED,
        /** Stopped and kept stopped on the server until its quarantine is lifted. */
        QUARANTINED,
    }
}

/** The databases of this server, as [DatabaseState]s, those in [quarantines] as quarantined. */
fun DatabaseRepository<StandaloneDatabaseContext>.databaseStates(
    quarantines: Quarantines
): List<DatabaseState> =
    registeredDatabases().map { (id, context) ->
        val quarantine = quarantines.of(id.databaseId().uuid())
        DatabaseState(
            id.name(),
            id.databaseId().uuid(),
            when {
                quarantine != null -> DatabaseState.Status.QUARANTINED
                context.isFailed -> DatabaseState.Status.FAILED
                context.database().isStarted -> DatabaseState.Status.ONLINE
                else -> DatabaseState.Status.OFFLINE
            },
            quarantine?.message ?: context.failureCause()?.let(::describe),
        )
    }

/** The message of [failure], with its root cause, which Neo4j's own messages leave out. */
internal fun describe(failure: Throwable): String {
    val root = generateSequence(failure) { it.cause }.last()
    return if (root === failure) failure.message ?: failure.toString()
    else "${failure.message} Cause: $root"
}

/**
 * Where one server, [server] at [address], stands with a [WantedDatabase]: a row of the result of
 * WAIT. A server that timed out because it could not be asked is not [available].
 */
data class ServerProgress(
    val server: UUID,
    val address: String,
    val state: State,
    val message: String,
    val available: Boolean = true,
) {
    /** The `state` column, named as in Neo4j Enterprise where it shows `CaughtUp`. */
    enum class State {
        CaughtUp,
        Failed,
        TimedOut,
    }
}

/** A server that hosts databases, as WAIT asks it about them. */
interface HostingServer {
    /** The server's id. */
    val id: UUID

    /** The server's advertised Bolt address. */
    val address: String

    /** The databases the server hosts; throws if it cannot be asked. */
    fun databases(): List<DatabaseState>
}

/**
 * Waits until every server that hosts a database did what the system graph asks of it, for WAIT,
 * asking them every [pollInterval]. A server that fails to do it, as when a database fails to
 * start, is done too: WAIT reports it.
 *
 * @property servers the servers that are to do what is wanted of a database: those it is placed on,
 *   or, for one dropped, those that may still hold it.
 */
class DatabaseProgress(
    private val servers: (WantedDatabase) -> List<HostingServer>,
    private val pollInterval: Duration = Duration.ofMillis(100),
) {
    /**
     * Waits until every server did what [wanted] asks, failed to, or [timeout] passed, and tells
     * where each server stands. Calls [check] between attempts, which throws to stop waiting.
     */
    fun await(wanted: WantedDatabase, timeout: Duration, check: () -> Unit): List<ServerProgress> {
        val deadline = System.nanoTime() + timeout.toNanos()
        val done = mutableMapOf<String, ServerProgress>()
        val unreachable = mutableMapOf<String, String>()
        while (true) {
            val current = servers(wanted)
            for (server in current.filter { it.address !in done }) {
                val databases =
                    try {
                        server.databases()
                    } catch (e: Exception) {
                        unreachable[server.address] = e.message ?: e.javaClass.simpleName
                        continue
                    }
                unreachable.remove(server.address)
                progressOf(databases, wanted)?.let { done[server.address] = it(server) }
            }
            if (current.all { it.address in done }) return current.map { done.getValue(it.address) }
            if (System.nanoTime() - deadline >= 0) {
                return current.map { server ->
                    val failure = unreachable[server.address]
                    done[server.address]
                        ?: ServerProgress(
                            server.id,
                            server.address,
                            ServerProgress.State.TimedOut,
                            failure?.let { "could not be asked: $it" }
                                ?: "not caught up within ${timeout.toSeconds()} seconds",
                            available = failure == null,
                        )
                }
            }
            check()
            Thread.sleep(pollInterval.toMillis())
        }
    }

    companion object {
        private val RUNNING = setOf(DatabaseState.Status.ONLINE, DatabaseState.Status.STARTING)

        /**
         * Where a server hosting [databases] stands with [wanted]: caught up, failed, or, as null,
         * not there yet. A database that failed to start is not running, as a stopped one is. One
         * wanted online is caught up once it runs and the server knows its leader, so that it takes
         * writes at once; a secondary, which follows no leader, once it runs. One in quarantine
         * there, which stays stopped, failed.
         */
        fun progressOf(
            databases: List<DatabaseState>,
            wanted: WantedDatabase,
        ): ((HostingServer) -> ServerProgress)? {
            val hosted = databases.firstOrNull { it.name == wanted.name }
            val caughtUp = { server: HostingServer ->
                ServerProgress(
                    server.id,
                    server.address,
                    ServerProgress.State.CaughtUp,
                    "caught up",
                )
            }
            if (wanted.id == null) return caughtUp.takeIf { hosted == null }
            val same = hosted?.takeIf { it.id == wanted.id }
            if (!wanted.online) {
                return caughtUp.takeIf { same == null || same.status !in RUNNING }
            }
            return when (same?.status) {
                DatabaseState.Status.ONLINE ->
                    caughtUp.takeIf { same.leaderKnown || same.mode == CopyMode.SECONDARY }
                DatabaseState.Status.FAILED,
                DatabaseState.Status.QUARANTINED -> { server ->
                    ServerProgress(
                        server.id,
                        server.address,
                        ServerProgress.State.Failed,
                        same.failure ?: "failed to start",
                    )
                }
                else -> null
            }
        }
    }
}
