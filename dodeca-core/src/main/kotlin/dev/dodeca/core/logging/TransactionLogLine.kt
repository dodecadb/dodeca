package dev.dodeca.core.logging

import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.logging.log4j.Neo4jMapMessage

/**
 * A line of the query log about a transaction, with the JSON fields Neo4j documents. Neo4j does not
 * document the text form; Dodeca's is in `doc/COMPATIBILITY.md`.
 */
internal class TransactionLogLine
private constructor(
    private val event: Event,
    private val id: Long,
    private val database: String,
    private val executingUser: String,
    private val authenticatedUser: String,
    private val elapsedMillis: Long?,
    private val failure: String?,
) : Neo4jMapMessage(8) {
    enum class Event(val field: String, val text: String) {
        START("start", "Transaction started"),
        COMMIT("commit", "Transaction committed"),
        ROLLBACK("rollback", "Transaction rolled back"),
    }

    init {
        with("type", "transaction")
        with("event", event.field)
        with("database", database)
        with("executingUser", executingUser)
        with("authenticatedUser", authenticatedUser)
        with("transactionId", id)
        elapsedMillis?.let { with("elapsedTimeMs", it) }
        failure?.let { with("failureReason", it) }
    }

    override fun formatAsString(sb: StringBuilder) {
        sb.append(event.text).append(": transaction id:").append(id).append(" - ")
        elapsedMillis?.let { sb.append(it).append(" ms - ") }
        sb.append(database).append(" - ")
        if (authenticatedUser.isNotEmpty() && authenticatedUser != executingUser) {
            sb.append(authenticatedUser).append(':')
        }
        sb.append(executingUser)
        failure?.let { sb.append(" - ").append(it) }
    }

    companion object {
        fun started(tx: KernelTransaction) = of(tx, Event.START, null, null)

        fun ended(tx: KernelTransaction, event: Event, elapsedMillis: Long, failure: String?) =
            of(tx, event, elapsedMillis, failure)

        private fun of(
            tx: KernelTransaction,
            event: Event,
            elapsedMillis: Long?,
            failure: String?,
        ): TransactionLogLine {
            val subject = tx.securityContext().subject()
            return TransactionLogLine(
                event,
                tx.getTransactionSequenceNumber(),
                tx.getDatabaseName(),
                subject.executingUser(),
                subject.authenticatedUser(),
                elapsedMillis,
                failure,
            )
        }
    }
}
