package org.dozendb.core.logging

import java.util.concurrent.ConcurrentHashMap
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.graphdb.event.DatabaseEventContext
import org.neo4j.graphdb.event.DatabaseEventListener
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.impl.query.TransactionExecutionMonitor
import org.neo4j.logging.InternalLog
import org.neo4j.logging.log4j.Neo4jMapMessage

/**
 * Neo4j Enterprise's security log, `security.log`, with authentication enabled: logins,
 * administration commands and authorization failures. Community has its file, logger and line
 * format, but writes its few security events to `debug.log` and its authorization failures nowhere.
 *
 * An administration command is logged once its transaction commits: in a cluster, a follower
 * refuses changes to the system graph only when committing.
 */
class SecurityLog(inner: InternalLog, private val logSuccessfulLogins: Boolean) :
    AbstractSecurityLog(inner), TransactionExecutionMonitor {
    private class Command(val sequence: Long, val context: SecurityContext, val text: String)

    private val commands = ConcurrentHashMap<KernelTransaction, Command>()

    /** [login] logged in, unless `dbms.security.log_successful_authentication` is false. */
    fun loggedIn(login: LoginContext) {
        if (logSuccessfulLogins) info(login, "logged in")
    }

    /** [user] failed to log in from [connection], for [reason]. */
    fun loginFailed(user: String, connection: ClientConnectionInfo, reason: String) {
        inner.error(Line(connection, user, "failed to log in: $reason"))
    }

    /** [login] started acting as [user]. */
    fun impersonating(login: LoginContext, user: String) {
        info(login, "impersonating user $user logged in")
    }

    /** Logs [command], which [transaction] ran as [context], when the transaction commits. */
    fun command(transaction: KernelTransaction, context: SecurityContext, command: String) {
        commands[transaction] = Command(transaction.transactionSequenceNumber, context, command)
    }

    override fun start(tx: KernelTransaction) {}

    override fun commit(tx: KernelTransaction) {
        val command = commands.remove(tx) ?: return
        // Kernel transactions are reused; this one is still the one that ran the command.
        if (command.sequence == tx.transactionSequenceNumber) info(command.context, command.text)
    }

    override fun rollback(tx: KernelTransaction, failure: Throwable?) {
        commands.remove(tx)
    }

    /**
     * Follows the transactions of the `system` database, which administration commands run in,
     * while it runs: only its own monitors, so other databases' transactions cost nothing.
     */
    fun followSystemDatabase(databases: DatabaseContextProvider<*>): DatabaseEventListener =
        object : DatabaseEventListener {
            override fun databaseStart(eventContext: DatabaseEventContext) {
                system(eventContext)?.addMonitorListener(this@SecurityLog)
            }

            override fun databaseShutdown(eventContext: DatabaseEventContext) {
                system(eventContext)?.removeMonitorListener(this@SecurityLog)
            }

            override fun databasePanic(eventContext: DatabaseEventContext) {}

            override fun databaseCreate(eventContext: DatabaseEventContext) {}

            override fun databaseDrop(eventContext: DatabaseEventContext) {}

            private fun system(eventContext: DatabaseEventContext) =
                if (eventContext.databaseName == NamedDatabaseId.SYSTEM_DATABASE_NAME) {
                    databases
                        .getDatabaseContext(NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID)
                        .orElse(null)
                        ?.database()
                        ?.monitors
                } else {
                    null
                }
        }

    /**
     * A line about a user who has no login, as one that failed: in the format of Community's lines,
     * which it does not let others write.
     */
    private class Line(
        connection: ClientConnectionInfo,
        private val user: String,
        message: String,
    ) : Neo4jMapMessage(6) {
        private val message = message.replace(Regex("\\R+"), " ")

        init {
            with("type", "security")
            @Suppress("DEPRECATION") // The documented source format, as Community writes it.
            with("source", connection.asConnectionDetails())
            if (user.isNotEmpty()) {
                with("executingUser", user)
                with("authenticatedUser", user)
            }
            with("message", this.message)
        }

        override fun formatAsString(sb: StringBuilder) {
            if (user.isNotEmpty()) sb.append('[').append(user).append("]: ")
            sb.append(message)
        }
    }
}
