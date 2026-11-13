package dev.dodeca.cluster.internal.secondaries

import dev.dodeca.consensus.BulkSource
import dev.dodeca.consensus.DatabaseRef
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.util.UUID
import org.neo4j.logging.InternalLog

/**
 * What a secondary asks a primary before a read that must see every write acknowledged before it, a
 * linearizable read or a backup, as one bulk transfer under [NAME]: the id of the primary's last
 * transaction once it applied everything the database's Raft group had committed when asked (a Raft
 * ReadIndex). The secondary then pulls from that primary until it applied that transaction too.
 *
 * A request is `byte version, UTF name, long, long` (the database's id). A response is `byte
 * version, byte status`, then `long transactionId` if the status is 0, else a UTF message.
 */
internal object ReadBarrierProtocol {
    const val NAME = "dodeca.readBarrier"
    const val VERSION = 1
    private const val REACHED = 0
    private const val REFUSED = 1

    /** What a primary answered. */
    sealed interface Answer {
        /**
         * The primary applied every write acknowledged before it was asked, up to [transactionId].
         */
        data class Reached(val transactionId: Long) : Answer

        /** The primary could not tell, and why. */
        data class Refused(val message: String) : Answer
    }

    fun encode(database: DatabaseRef): ByteArray =
        ByteArrayOutputStream().use { bytes ->
            DataOutputStream(bytes).use {
                it.writeByte(VERSION)
                it.writeUTF(database.name)
                it.writeLong(database.id.mostSignificantBits)
                it.writeLong(database.id.leastSignificantBits)
            }
            bytes.toByteArray()
        }

    fun decode(bytes: ByteArray): DatabaseRef =
        DataInputStream(bytes.inputStream()).use {
            checkVersion(it.readUnsignedByte())
            DatabaseRef(it.readUTF(), UUID(it.readLong(), it.readLong()))
        }

    fun write(out: OutputStream, answer: Answer) {
        val data = DataOutputStream(out)
        data.writeByte(VERSION)
        when (answer) {
            is Answer.Reached -> {
                data.writeByte(REACHED)
                data.writeLong(answer.transactionId)
            }
            is Answer.Refused -> {
                data.writeByte(REFUSED)
                data.writeUTF(answer.message)
            }
        }
        data.flush()
    }

    fun read(input: InputStream): Answer {
        val data = DataInputStream(input)
        checkVersion(data.readUnsignedByte())
        return when (val status = data.readUnsignedByte()) {
            REACHED -> Answer.Reached(data.readLong())
            REFUSED -> Answer.Refused(data.readUTF())
            else -> throw IOException("unknown read barrier status $status")
        }
    }

    private fun checkVersion(version: Int) {
        if (version != VERSION) {
            throw IOException("unsupported read barrier protocol version $version, not $VERSION")
        }
    }
}

/**
 * Answers the read barriers of secondaries ([ReadBarrierProtocol]) for the databases this member
 * hosts as a primary: [reach] returns the id of the last transaction applied here once this member
 * applied what the database's Raft group had committed, or throws why it cannot tell.
 */
internal class ReadBarrierSource(
    private val reach: (DatabaseRef) -> Long,
    private val log: InternalLog,
) : BulkSource {
    override fun serve(request: ByteArray, out: OutputStream) {
        val database = ReadBarrierProtocol.decode(request)
        val answer =
            try {
                ReadBarrierProtocol.Answer.Reached(reach(database))
            } catch (e: Exception) {
                log.debug("Refused a read barrier of %s: %s", database.name, e.toString())
                ReadBarrierProtocol.Answer.Refused(e.message ?: e.toString())
            }
        ReadBarrierProtocol.write(out, answer)
    }
}
