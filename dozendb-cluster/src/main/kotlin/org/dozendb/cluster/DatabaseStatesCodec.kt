package org.dozendb.cluster

import java.util.UUID
import org.dozendb.core.edition.DatabaseState
import org.dozendb.core.servers.CopyMode

/**
 * Writes the databases of a member as text for [MemberMetadata.DATABASES]: a line per database with
 * its name, id, status, whether the member knows its leader, if it failed, why, or nothing, the
 * mode the member hosts it in, and whether its pulls are paused, separated by tabs. A status or
 * mode that a later version added is read as if that member did not report the database, and fields
 * that it added after these are left out, so that versions can add both. A database reported
 * without a mode, by a version before secondaries, is a primary, and one reported without the pause
 * is not paused.
 */
object DatabaseStatesCodec {
    fun encode(states: List<DatabaseState>): String =
        states.joinToString("\n") { state ->
            listOf(
                    state.name,
                    state.id.toString(),
                    state.status.name,
                    state.leaderKnown.toString(),
                    state.failure?.replace(Regex("[\t\r\n]+"), " ").orEmpty(),
                    state.mode.label,
                    state.paused.toString(),
                )
                .joinToString("\t")
        }

    fun decode(text: String): List<DatabaseState> =
        text
            .lineSequence()
            .filter { it.isNotEmpty() }
            .mapNotNull { line ->
                val fields = line.split('\t')
                require(fields.size >= 5) { "Not a database state: $line" }
                val status =
                    DatabaseState.Status.entries.firstOrNull { it.name == fields[2] }
                        ?: return@mapNotNull null
                val mode =
                    fields.getOrNull(5)?.let { label ->
                        CopyMode.entries.firstOrNull { it.label == label } ?: return@mapNotNull null
                    } ?: CopyMode.PRIMARY
                DatabaseState(
                    fields[0],
                    UUID.fromString(fields[1]),
                    status,
                    fields[4].ifEmpty { null },
                    fields[3].toBooleanStrict(),
                    mode,
                    fields.getOrNull(6)?.toBooleanStrict() ?: false,
                )
            }
            .toList()
}
