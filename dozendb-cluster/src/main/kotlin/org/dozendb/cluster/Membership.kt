package org.dozendb.cluster

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.util.UUID
import org.dozendb.consensus.Member
import org.dozendb.consensus.MemberId

/** This member's place in the cluster. */
enum class MemberState {
    /** Started without Raft state and found the cluster formed without it: waits to be added. */
    JOINING,

    /** One of the voting members of the `system` group. */
    MEMBER,

    /** Was a member and was removed. It stays out until its data is deleted. */
    REMOVED,
}

/**
 * What this member knows about its own membership, kept across restarts.
 *
 * @property formation the members this member formed the cluster with, if it is one of them: the
 *   initial members of the groups of `system` and the default database.
 * @property formatLevel the cluster's format level, as this member last saw it: it refuses to start
 *   with a DozenDB version that does not support it.
 * @property storesToDetach the databases whose stores this member brought to the cluster it formed
 *   and has not yet detached from the Raft groups they may have been replicated in before.
 * @property memberId the member this record, and the Raft state beside it, belongs to; null in a
 *   record written before DozenDB kept it.
 * @property systemGroup the id of the Raft group of `system`, once a recovery re-formed it under an
 *   id of its own; null for the id of the database, as before (see [DatabaseGroups]).
 * @property systemUnbound whether `neo4j-admin dbms unbind-system-db` removed this member's Raft
 *   state of `system`, which it re-forms or rejoins when it starts.
 */
data class MembershipRecord(
    val state: MemberState,
    val formation: List<Member> = emptyList(),
    val formatLevel: Int = FormatLevels.INITIAL,
    val storesToDetach: Set<String> = emptySet(),
    val memberId: MemberId? = null,
    val systemGroup: UUID? = null,
    val systemUnbound: Boolean = false,
)

/** Keeps this member's [MembershipRecord]. */
interface MembershipStore {
    /** The record, or null if none was written. */
    fun read(): MembershipRecord?

    /** Replaces the record; once this returns, the new record survives a crash. */
    fun write(record: MembershipRecord)
}

/** A [MembershipStore] in a text file, replaced atomically. */
class MembershipFile(private val path: Path) : MembershipStore {
    override fun read(): MembershipRecord? {
        if (!Files.exists(path)) return null
        val fields =
            Files.readAllLines(path)
                .filter { it.isNotBlank() }
                .associate { line ->
                    val separator = line.indexOf('=')
                    if (separator < 0) throw IOException("$path: malformed line '$line'")
                    line.substring(0, separator) to line.substring(separator + 1)
                }
        if (fields[VERSION] != CURRENT_VERSION) {
            throw IOException("$path: unsupported version ${fields[VERSION]}")
        }
        val state =
            fields[STATE]?.let { name -> MemberState.entries.firstOrNull { it.name == name } }
                ?: throw IOException("$path: invalid state ${fields[STATE]}")
        val formation =
            try {
                fields[FORMATION].orEmpty().split(',').filter { it.isNotEmpty() }.map(Member::parse)
            } catch (e: IllegalArgumentException) {
                throw IOException("$path: invalid formation: ${e.message}", e)
            }
        val formatLevel =
            fields[FORMAT_LEVEL]?.let {
                it.toIntOrNull() ?: throw IOException("$path: invalid format level $it")
            } ?: FormatLevels.INITIAL
        val storesToDetach =
            fields[STORES_TO_DETACH].orEmpty().split(',').filter { it.isNotEmpty() }.toSet()
        val memberId =
            fields[MEMBER_ID]?.let {
                try {
                    MemberId.parse(it)
                } catch (e: IllegalArgumentException) {
                    throw IOException("$path: invalid member id $it", e)
                }
            }
        val systemGroup =
            fields[SYSTEM_GROUP]?.let {
                try {
                    UUID.fromString(it)
                } catch (e: IllegalArgumentException) {
                    throw IOException("$path: invalid system group $it", e)
                }
            }
        val systemUnbound =
            fields[SYSTEM_UNBOUND]?.let {
                it.toBooleanStrictOrNull() ?: throw IOException("$path: invalid systemUnbound $it")
            } ?: false
        return MembershipRecord(
            state,
            formation,
            formatLevel,
            storesToDetach,
            memberId,
            systemGroup,
            systemUnbound,
        )
    }

    override fun write(record: MembershipRecord) {
        val text = buildString {
            appendLine("$VERSION=$CURRENT_VERSION")
            appendLine("$STATE=${record.state.name}")
            appendLine("$FORMATION=${record.formation.joinToString(",") { it.toText() }}")
            appendLine("$FORMAT_LEVEL=${record.formatLevel}")
            appendLine("$STORES_TO_DETACH=${record.storesToDetach.sorted().joinToString(",")}")
            record.memberId?.let { appendLine("$MEMBER_ID=$it") }
            record.systemGroup?.let { appendLine("$SYSTEM_GROUP=$it") }
            if (record.systemUnbound) appendLine("$SYSTEM_UNBOUND=true")
        }
        val temporary = path.resolveSibling("${path.fileName}.tmp")
        Files.createDirectories(path.parent)
        FileChannel.open(
                temporary,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING,
            )
            .use { channel ->
                val bytes = ByteBuffer.wrap(text.encodeToByteArray())
                while (bytes.hasRemaining()) channel.write(bytes)
                channel.force(true)
            }
        Files.move(
            temporary,
            path,
            StandardCopyOption.ATOMIC_MOVE,
            StandardCopyOption.REPLACE_EXISTING,
        )
        FileChannel.open(path.parent, StandardOpenOption.READ).use { it.force(true) }
    }

    companion object {
        /** The name of a member's record, in its Raft directory. */
        const val FILE_NAME = "membership"

        private const val VERSION = "version"
        private const val CURRENT_VERSION = "1"
        private const val STATE = "state"
        private const val FORMATION = "formation"
        private const val FORMAT_LEVEL = "formatLevel"
        private const val STORES_TO_DETACH = "storesToDetach"
        private const val MEMBER_ID = "memberId"
        private const val SYSTEM_GROUP = "systemGroup"
        private const val SYSTEM_UNBOUND = "systemUnbound"
    }
}
