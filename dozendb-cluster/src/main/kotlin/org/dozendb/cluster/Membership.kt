package org.dozendb.cluster

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import org.dozendb.consensus.Member

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
 */
data class MembershipRecord(val state: MemberState, val formation: List<Member> = emptyList())

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
        return MembershipRecord(state, formation)
    }

    override fun write(record: MembershipRecord) {
        val text = buildString {
            appendLine("$VERSION=$CURRENT_VERSION")
            appendLine("$STATE=${record.state.name}")
            appendLine("$FORMATION=${record.formation.joinToString(",") { it.toText() }}")
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

    private companion object {
        const val VERSION = "version"
        const val CURRENT_VERSION = "1"
        const val STATE = "state"
        const val FORMATION = "formation"
    }
}
