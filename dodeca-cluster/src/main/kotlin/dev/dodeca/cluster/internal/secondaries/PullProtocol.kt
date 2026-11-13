package dev.dodeca.cluster.internal.secondaries

import dev.dodeca.consensus.DatabaseRef
import dev.dodeca.core.store.CommittedTransaction
import dev.dodeca.core.store.TransactionRecords
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.EOFException
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.util.UUID
import java.util.zip.CRC32C
import org.neo4j.storageengine.api.CommandReaderFactory

/**
 * What a secondary and its upstream exchange, as one bulk transfer under [NAME] per pull.
 *
 * A request is `byte version, UTF name, long, long` (the database's id), `long after, int checksum,
 * int maximumBytes`: the transactions of the database after the one at append index `after`, whose
 * checksum is `checksum`. A response is `byte version, byte status`, then, for [Status.OK], `long
 * last`, the append index of the upstream's last transaction when it began to read, and each
 * transaction after `after` up to it as `byte 1, int length, record, int crc32c(record)`, with the
 * record as [TransactionRecords] writes it, then `byte 0`; for [Status.BEHIND], `long last, int
 * checksum` of the upstream's last transaction; else a UTF message. The records stop after the one
 * that reaches `maximumBytes`, so that a secondary that is further behind asks again.
 */
internal object PullProtocol {
    const val NAME = "dodeca.pull"
    const val VERSION = 1
    private const val TAG_END = 0
    private const val TAG_TRANSACTION = 1
    private const val BUFFER_SIZE = 1 shl 16

    /** What an upstream answers to a pull. */
    enum class Status {
        OK,
        /** The upstream's logs no longer hold the transaction after the one given. */
        PRUNED,
        /** The upstream's history does not continue with the transaction given. */
        DIVERGED,
        /** The upstream does not run the database, or none with that id. */
        UNAVAILABLE,
        /**
         * The upstream's last transaction comes before the one given, as when the secondary pulled
         * from another further ahead: it has nothing newer, and the secondary judges the history.
         */
        BEHIND,
    }

    /**
     * The transactions of [database] after the one at append index [after], whose checksum is
     * [checksum], up to about [maximumBytes] of records.
     */
    data class Request(
        val database: DatabaseRef,
        val after: Long,
        val checksum: Int,
        val maximumBytes: Int,
    ) {
        init {
            require(after > 0) { "invalid append index $after" }
            require(maximumBytes > 0) { "invalid maximum of $maximumBytes bytes" }
        }
    }

    /** What a pull returned. */
    sealed interface Answer {
        /**
         * The upstream continues the secondary's history: [transactions] follow its last one, up to
         * the upstream's transaction at append index [last] or fewer, read as they are taken.
         */
        class Pulled(val last: Long, val transactions: Sequence<CommittedTransaction>) : Answer

        /** The upstream refused, with [status] and why. */
        data class Refused(val status: Status, val message: String) : Answer

        /**
         * The upstream's last transaction, at append index [last] with [checksum], comes before the
         * secondary's last.
         */
        data class Behind(val last: Long, val checksum: Int) : Answer
    }

    fun encode(request: Request): ByteArray =
        ByteArrayOutputStream().use { bytes ->
            DataOutputStream(bytes).use {
                it.writeByte(VERSION)
                it.writeUTF(request.database.name)
                it.writeLong(request.database.id.mostSignificantBits)
                it.writeLong(request.database.id.leastSignificantBits)
                it.writeLong(request.after)
                it.writeInt(request.checksum)
                it.writeInt(request.maximumBytes)
            }
            bytes.toByteArray()
        }

    fun decode(bytes: ByteArray): Request =
        DataInputStream(bytes.inputStream()).use {
            val version = it.readUnsignedByte()
            if (version != VERSION) {
                throw IOException("unsupported pull protocol version $version, not $VERSION")
            }
            val name = it.readUTF()
            Request(
                DatabaseRef(name, UUID(it.readLong(), it.readLong())),
                it.readLong(),
                it.readInt(),
                it.readInt(),
            )
        }

    /**
     * Answers that the upstream's last transaction, at append index [last] with [checksum], comes
     * before the one given.
     */
    fun writeBehind(out: OutputStream, last: Long, checksum: Int) {
        val data = DataOutputStream(out)
        data.writeByte(VERSION)
        data.writeByte(Status.BEHIND.ordinal)
        data.writeLong(last)
        data.writeInt(checksum)
        data.flush()
    }

    /** Answers that the upstream refuses the pull, with [status] and [message]. */
    fun writeRefusal(out: OutputStream, status: Status, message: String) {
        require(status != Status.OK && status != Status.BEHIND) {
            "a refusal needs a status other than OK and BEHIND"
        }
        val data = DataOutputStream(out)
        data.writeByte(VERSION)
        data.writeByte(status.ordinal)
        data.writeUTF(message)
        data.flush()
    }

    /**
     * Answers with [transactions], which end at the transaction at append index [last] or before,
     * as far as [maximumBytes] allows; returns how many it wrote.
     */
    fun writeTransactions(
        out: OutputStream,
        last: Long,
        transactions: Sequence<CommittedTransaction>,
        maximumBytes: Int,
    ): Int {
        val data = DataOutputStream(BufferedOutputStream(out, BUFFER_SIZE))
        data.writeByte(VERSION)
        data.writeByte(Status.OK.ordinal)
        data.writeLong(last)
        var count = 0
        var bytes = 0L
        for (transaction in transactions) {
            val record = TransactionRecords.encode(transaction)
            data.writeByte(TAG_TRANSACTION)
            data.writeInt(record.size)
            data.write(record)
            data.writeInt(crc(record))
            count++
            bytes += record.size
            if (bytes >= maximumBytes) break
        }
        data.writeByte(TAG_END)
        data.flush()
        return count
    }

    /**
     * Passes [block] what the upstream answered on [input]; the transactions are read as [block]
     * takes them, with [readers] for their commands, and must be taken before it returns.
     */
    fun <T> read(input: InputStream, readers: CommandReaderFactory, block: (Answer) -> T): T {
        val data = DataInputStream(BufferedInputStream(input, BUFFER_SIZE))
        val version = data.readUnsignedByte()
        if (version != VERSION) {
            throw IOException("unsupported pull protocol version $version, not $VERSION")
        }
        val status =
            Status.entries.getOrNull(data.readUnsignedByte())
                ?: throw IOException("unknown pull status")
        when (status) {
            Status.OK -> {}
            Status.BEHIND -> return block(Answer.Behind(data.readLong(), data.readInt()))
            else -> return block(Answer.Refused(status, data.readUTF()))
        }
        val last = data.readLong()
        val transactions = sequence {
            while (true) {
                when (val tag = data.read()) {
                    TAG_END -> break
                    TAG_TRANSACTION -> {
                        val length = data.readInt()
                        if (length < 0) throw IOException("invalid record length $length")
                        val record = data.readNBytes(length)
                        if (record.size < length) throw EOFException()
                        if (data.readInt() != crc(record)) throw IOException("a record is corrupt")
                        yield(TransactionRecords.decode(record, readers))
                    }
                    -1 -> throw EOFException("the answer ends before its last transaction")
                    else -> throw IOException("unknown tag $tag")
                }
            }
        }
        return block(Answer.Pulled(last, transactions))
    }

    private fun crc(record: ByteArray): Int = CRC32C().apply { update(record) }.value.toInt()
}
