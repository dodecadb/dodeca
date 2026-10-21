package org.dozendb.backup

import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException

/** Where a restore to a point in time stops, as `neo4j-admin database restore --restore-until`. */
sealed interface RestorePoint {
    /** Whether [transaction] is restored. Transactions are restored in order until one is not. */
    fun includes(transaction: ArtifactTransaction): Boolean

    /** Before transaction [transactionId], which is not restored. */
    data class BeforeTransaction(val transactionId: Long) : RestorePoint {
        override fun includes(transaction: ArtifactTransaction) =
            transaction.transactionId < transactionId

        override fun toString() = "before transaction $transactionId"
    }

    /** Before [time]: the transactions that committed before it. */
    data class BeforeTime(val time: Instant) : RestorePoint {
        override fun includes(transaction: ArtifactTransaction) =
            transaction.batch.timeCommitted < time.toEpochMilli()

        override fun toString() = "before ${FORMAT.format(time.atOffset(UTC))} UTC"
    }

    companion object {
        private val FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

        /** Reads a transaction id, as `123`, or a time in UTC, as `2026-10-21 10:15:30`. */
        fun parse(text: String): RestorePoint {
            val transactionId = text.toLongOrNull()
            if (transactionId != null) {
                require(transactionId > 0) { "invalid transaction id $transactionId" }
                return BeforeTransaction(transactionId)
            }
            val time =
                try {
                    LocalDateTime.parse(text.trim(), FORMAT).toInstant(UTC)
                } catch (e: DateTimeParseException) {
                    throw IllegalArgumentException(
                        "'$text' is neither a transaction id nor a time in UTC as " +
                            "'yyyy-MM-dd HH:mm:ss'",
                        e,
                    )
                }
            return BeforeTime(time)
        }
    }
}
