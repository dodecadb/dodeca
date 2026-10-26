package org.dozendb.cluster

import java.util.UUID
import org.dozendb.core.edition.DatabaseState

/**
 * Writes the databases of a member as text for [MemberMetadata.DATABASES]: a line per database with
 * its name, id, status and, if it failed, why, separated by tabs. A status that a later version
 * added is read as if that member did not report the database, so that versions can add statuses.
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
            .mapNotNull { line ->
                val fields = line.split('\t')
                require(fields.size in 3..4) { "Not a database state: $line" }
                val status =
                    DatabaseState.Status.entries.firstOrNull { it.name == fields[2] }
                        ?: return@mapNotNull null
                DatabaseState(fields[0], UUID.fromString(fields[1]), status, fields.getOrNull(3))
            }
            .toList()
}
