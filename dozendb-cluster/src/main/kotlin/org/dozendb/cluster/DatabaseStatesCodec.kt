package org.dozendb.cluster

import java.util.UUID
import org.dozendb.core.edition.DatabaseState

/**
 * Writes the databases of a member as text for [MemberMetadata.DATABASES]: a line per database with
 * its name, id, status and, if it failed, why, separated by tabs.
 */
object DatabaseStatesCodec {
    fun encode(states: List<DatabaseState>): String =
        states.joinToString("\n") { state ->
            listOfNotNull(
                    state.name,
                    state.id.toString(),
                    state.status.name,
                    state.failure?.replace(Regex("[\t\r\n]+"), " "),
                )
                .joinToString("\t")
        }

    fun decode(text: String): List<DatabaseState> =
        text
            .lineSequence()
            .filter { it.isNotEmpty() }
            .map { line ->
                val fields = line.split('\t')
                require(fields.size in 3..4) { "Not a database state: $line" }
                DatabaseState(
                    fields[0],
                    UUID.fromString(fields[1]),
                    DatabaseState.Status.valueOf(fields[2]),
                    fields.getOrNull(3),
                )
            }
            .toList()
}
