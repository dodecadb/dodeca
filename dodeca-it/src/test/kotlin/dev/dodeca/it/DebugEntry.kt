package dev.dodeca.it

import com.fasterxml.jackson.databind.ObjectMapper
import java.time.Instant
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

/**
 * An entry of Neo4j's debug log, which Neo4j 2026 writes a JSON object a line. A line that is not
 * one fails the read, so that a check for entries of some level cannot pass on a log it misreads.
 * As `dev.dodeca.core.DebugEntry`, which these tests cannot use: they run Dodeca in containers.
 */
data class DebugEntry(
    val time: Instant,
    val level: String,
    val category: String,
    val message: String,
) {
    override fun toString() = "$time $level [$category] $message"

    companion object {
        private val JSON = ObjectMapper()
        private val TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSXX")

        /** The entries of a debug log that reads [text]. */
        fun parse(text: String): List<DebugEntry> =
            text.lines().filter { it.isNotBlank() }.map(::entry)

        private fun entry(line: String): DebugEntry {
            val entry =
                runCatching { JSON.readTree(line) }.getOrNull()?.takeIf { it.isObject }
                    ?: throw IllegalStateException("not a JSON entry of the debug log: $line")
            return DebugEntry(
                OffsetDateTime.parse(entry.path("time").asText(), TIME).toInstant(),
                entry.path("level").asText(),
                entry.path("category").asText(),
                entry.path("message").asText(),
            )
        }
    }
}
