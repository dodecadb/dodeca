package dev.dodeca.cluster.internal

import dev.dodeca.consensus.BulkTransfer
import dev.dodeca.consensus.ConsensusUnavailableException
import dev.dodeca.consensus.Member
import dev.dodeca.core.edition.DatabaseQuarantine
import dev.dodeca.core.edition.UnquarantineOperation
import dev.dodeca.core.edition.Unquarantines
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException
import java.io.OutputStream
import java.time.Duration
import java.util.UUID

/**
 * Lifts the quarantines of copies on other members, for `dbms.unquarantineDatabase` called on this
 * one, by asking them with [QuarantineProtocol], and lifts those on this member when another asks,
 * as the bulk source of that name.
 *
 * @property members the members of the cluster, by id, where they are reached.
 * @property local lifts quarantines on this member.
 * @property timeout how long a member may take to answer: replacing a copy's cluster state waits
 *   for its group to remove it.
 */
internal class QuarantineLifts(
    private val transfer: BulkTransfer,
    private val members: () -> Collection<Member>,
    private val local: () -> DatabaseQuarantine,
    private val timeout: Duration,
) : Unquarantines.Remote, dev.dodeca.consensus.BulkSource {
    override fun lift(
        server: UUID,
        database: String,
        operation: UnquarantineOperation,
        user: String,
    ) {
        val member =
            checkNotNull(members().firstOrNull { it.id.uuid == server }) {
                "Server '$server' is not a member of the cluster."
            }
        val failure =
            try {
                transfer.fetchBulk(
                    member.raftAddress,
                    QuarantineProtocol.NAME,
                    QuarantineProtocol.encode(database, operation, user),
                    timeout,
                    QuarantineProtocol::readAnswer,
                )
            } catch (e: ConsensusUnavailableException) {
                throw IllegalStateException("Server '$server' did not answer: ${e.message}", e)
            }
        if (failure != null) throw IllegalStateException(failure)
    }

    override fun serve(request: ByteArray, out: OutputStream) {
        val asked = QuarantineProtocol.decode(request)
        val failure =
            try {
                local().lift(asked.database, asked.operation, asked.user)
                null
            } catch (e: RuntimeException) {
                e.message ?: e.toString()
            }
        QuarantineProtocol.writeAnswer(out, failure)
    }
}

/**
 * Asks a member to lift the quarantine of its copy of a database: the database's name, the
 * operation's name and the user who asks; it answers whether it did, or why not.
 */
internal object QuarantineProtocol {
    const val NAME = "dodeca.unquarantine"
    const val VERSION = 1
    private const val LIFTED = 0
    private const val FAILED = 1

    /** A message's most characters, so that its modified UTF-8 fits `writeUTF`'s 64 KB. */
    private const val MESSAGE_LIMIT = 8_000

    data class Request(
        val database: String,
        val operation: UnquarantineOperation,
        val user: String,
    )

    fun encode(database: String, operation: UnquarantineOperation, user: String): ByteArray {
        val bytes = ByteArrayOutputStream()
        DataOutputStream(bytes).use {
            it.writeByte(VERSION)
            it.writeUTF(database)
            it.writeUTF(operation.label)
            it.writeUTF(user)
        }
        return bytes.toByteArray()
    }

    fun decode(request: ByteArray): Request {
        val input = DataInputStream(request.inputStream())
        val version = input.readUnsignedByte()
        check(version == VERSION) { "Unknown version $version of $NAME" }
        return Request(
            input.readUTF(),
            UnquarantineOperation.of(input.readUTF()),
            input.readUTF(),
        )
    }

    /** Writes that the quarantine was lifted, or [failure], why not. */
    fun writeAnswer(out: OutputStream, failure: String?) {
        val data = DataOutputStream(out)
        if (failure == null) {
            data.writeByte(LIFTED)
        } else {
            data.writeByte(FAILED)
            data.writeUTF(failure.take(MESSAGE_LIMIT))
        }
        data.flush()
    }

    /** Null if the quarantine was lifted, else why not. */
    fun readAnswer(input: java.io.InputStream): String? {
        val data = DataInputStream(input)
        return when (val answer = data.readUnsignedByte()) {
            LIFTED -> null
            FAILED -> data.readUTF()
            else -> throw IOException("Unknown answer $answer of $NAME")
        }
    }
}
