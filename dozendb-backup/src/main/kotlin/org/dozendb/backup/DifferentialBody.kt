package org.dozendb.backup

import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.EOFException
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.nio.ByteBuffer
import java.util.zip.CRC32C
import org.neo4j.common.Subject
import org.neo4j.io.fs.ByteBufferReadableChannel
import org.neo4j.io.fs.OutputStreamWritableChannel
import org.neo4j.kernel.KernelVersion
import org.neo4j.kernel.impl.api.LeaseService
import org.neo4j.kernel.impl.transaction.log.CompleteCommandBatch
import org.neo4j.storageengine.api.CommandReaderFactory
import org.neo4j.storageengine.api.StorageCommand

/**
 * The body of a differential backup. In a compressed artifact it starts as Neo4j's dumper starts
 * one, with `int 1, long files, long bytes`, which Community's `neo4j-admin database load --info`
 * prints: here the number of transactions and the bytes of what follows. Then each transaction as
 * `byte 1, int length, record, int crc32c(record)`, then `byte 0` and the end of the stream.
 *
 * A record holds the fields of an [ArtifactTransaction] and its commands in Neo4j's serialization
 * for their kernel version: `long transactionId, long appendIndex, int previousChecksum, int
 * checksum, byte kernelVersion, long timeStarted, long latestCommittedTxWhenStarted, long
 * timeCommitted, long consensusIndex, int commandCount, commands`.
 *
 * Writing and reading both check that the transactions are the ones the header tells, each
 * following the one before.
 */
internal object DifferentialBody {
    private const val SIZE_VERSION = 1
    private const val TAG_END = 0
    private const val TAG_TRANSACTION = 1
    private const val BUFFER_SIZE = 1 shl 16

    /** How many transactions records hold, and their bytes with the end tag. */
    class Totals(val transactions: Long, val bytes: Long)

    /** Writes the records of [transactions] and the end tag to [out], without closing it. */
    fun writeRecords(
        out: OutputStream,
        header: ArtifactHeader,
        transactions: Sequence<ArtifactTransaction>,
    ): Totals {
        val chain = Chain(header)
        val data = DataOutputStream(BufferedOutputStream(out, BUFFER_SIZE))
        var count = 0L
        var bytes = 0L
        for (transaction in transactions) {
            chain.next(transaction)
            val record = encode(transaction)
            data.writeByte(TAG_TRANSACTION)
            data.writeInt(record.size)
            data.write(record)
            data.writeInt(crc(record))
            count++
            bytes += framed(record.size)
        }
        chain.end()
        data.writeByte(TAG_END)
        data.flush()
        return Totals(count, bytes + 1)
    }

    /** Writes the body to [out]: [records], which [writeRecords] wrote as [totals]. */
    fun write(out: OutputStream, header: ArtifactHeader, totals: Totals, records: InputStream) {
        if (header.compressed) {
            val data = DataOutputStream(out)
            data.writeInt(SIZE_VERSION)
            data.writeLong(totals.transactions)
            data.writeLong(totals.bytes)
            data.flush()
        }
        val copied = records.transferTo(out)
        if (copied != totals.bytes) {
            throw IOException("the records changed: ${totals.bytes} bytes written, $copied read")
        }
    }

    fun read(
        body: InputStream,
        header: ArtifactHeader,
        readers: CommandReaderFactory,
    ): Sequence<ArtifactTransaction> = sequence {
        val chain = Chain(header)
        val data = DataInputStream(BufferedInputStream(body, BUFFER_SIZE))
        try {
            val totals =
                if (header.compressed) {
                    val version = data.readInt()
                    if (version != SIZE_VERSION)
                        throw IOException("unsupported size header $version")
                    Totals(data.readLong(), data.readLong())
                } else null
            var count = 0L
            var bytes = 1L // the end tag
            while (true) {
                when (val tag = data.read()) {
                    TAG_END -> break
                    TAG_TRANSACTION -> {
                        val length = data.readInt()
                        if (length < 0) throw IOException("invalid record length $length")
                        val record = data.readNBytes(length)
                        if (record.size < length) throw EOFException()
                        if (data.readInt() != crc(record)) {
                            throw IOException(
                                "the record after ${chain.last ?: "the header"} is corrupt"
                            )
                        }
                        val transaction = decode(record, readers)
                        chain.next(transaction)
                        count++
                        bytes += framed(length)
                        yield(transaction)
                    }
                    -1 -> throw EOFException()
                    else ->
                        throw IOException("unknown tag $tag after ${chain.last ?: "the header"}")
                }
            }
            chain.end()
            if (data.read() != -1) throw IOException("unexpected bytes after the last transaction")
            if (totals != null && (totals.transactions != count || totals.bytes != bytes)) {
                throw IOException(
                    "the size header tells ${totals.transactions} transactions in ${totals.bytes} " +
                        "bytes, but the backup holds $count in $bytes"
                )
            }
        } catch (e: EOFException) {
            throw EOFException("the backup ends before its last transaction").initCause(e)
        }
    }

    /** The bytes of a record of [length] with its tag, length and checksum. */
    private fun framed(length: Int): Long = 1L + Int.SIZE_BYTES + length + Int.SIZE_BYTES

    private fun crc(record: ByteArray): Int = CRC32C().apply { update(record) }.value.toInt()

    private fun encode(transaction: ArtifactTransaction): ByteArray {
        val batch = transaction.batch
        require(batch.isFirst && batch.isLast && !batch.isRollback) {
            "$transaction is not a complete batch"
        }
        val buffer = ByteArrayOutputStream()
        val channel = OutputStreamWritableChannel(buffer)
        channel.putLong(transaction.transactionId)
        channel.putLong(transaction.appendIndex)
        channel.putInt(transaction.previousChecksum)
        channel.putInt(transaction.checksum)
        channel.put(batch.kernelVersion().version())
        channel.putLong(batch.timeStarted)
        channel.putLong(batch.latestCommittedTxWhenStarted)
        channel.putLong(batch.timeCommitted)
        channel.putLong(batch.consensusIndex())
        channel.putInt(batch.commandCount())
        batch.accept { command ->
            command.serialize(channel)
            false
        }
        channel.close()
        return buffer.toByteArray()
    }

    private fun decode(record: ByteArray, readers: CommandReaderFactory): ArtifactTransaction {
        val buffer = ByteBuffer.wrap(record)
        val channel = ByteBufferReadableChannel(buffer)
        val transactionId = channel.long
        val appendIndex = channel.long
        val previousChecksum = channel.int
        val checksum = channel.int
        val version = channel.get()
        val kernelVersion =
            try {
                KernelVersion.getForVersion(version)
            } catch (e: IllegalArgumentException) {
                throw IOException(
                    "transaction $transactionId has unknown kernel version $version",
                    e,
                )
            }
        val timeStarted = channel.long
        val latestCommittedTxWhenStarted = channel.long
        val timeCommitted = channel.long
        val consensusIndex = channel.long
        val count = channel.int
        val reader = readers.get(kernelVersion)
        val commands =
            List<StorageCommand>(count) {
                reader.read(channel)
                    ?: throw IOException(
                        "command ${it + 1} of $count of transaction $transactionId is missing"
                    )
            }
        if (buffer.hasRemaining()) {
            throw IOException(
                "${buffer.remaining()} unexpected bytes after the commands of transaction " +
                    transactionId
            )
        }
        val batch =
            CompleteCommandBatch(
                commands,
                consensusIndex,
                timeStarted,
                latestCommittedTxWhenStarted,
                timeCommitted,
                LeaseService.NO_LEASE,
                kernelVersion,
                Subject.ANONYMOUS,
            )
        return ArtifactTransaction(transactionId, appendIndex, previousChecksum, checksum, batch)
    }

    /** Checks that transactions are the ones [header] tells, in order. */
    private class Chain(private val header: ArtifactHeader) {
        var last: ArtifactTransaction? = null
            private set

        fun next(transaction: ArtifactTransaction) {
            val previous = last
            if (previous == null) {
                if (header.empty) throw IOException("an empty backup holds $transaction")
                if (
                    transaction.appendIndex != header.lowestAppendIndex ||
                        transaction.previousChecksum != header.previousChecksum
                ) {
                    throw IOException(
                        "the backup starts with $transaction after checksum " +
                            "${transaction.previousChecksum}, but its header tells append index " +
                            "${header.lowestAppendIndex} after checksum ${header.previousChecksum}"
                    )
                }
            } else {
                if (
                    transaction.appendIndex != previous.appendIndex + 1 ||
                        transaction.transactionId != previous.transactionId + 1
                ) {
                    throw IOException("$transaction does not follow $previous")
                }
                if (transaction.previousChecksum != previous.checksum) {
                    throw IOException(
                        "$transaction follows checksum ${transaction.previousChecksum}, but " +
                            "$previous has checksum ${previous.checksum}"
                    )
                }
            }
            if (transaction.appendIndex > header.highestAppendIndex) {
                throw IOException(
                    "$transaction is after the last one its header tells, append index " +
                        header.highestAppendIndex
                )
            }
            last = transaction
        }

        fun end() {
            val last = last
            when {
                header.empty -> return
                last == null ->
                    throw IOException(
                        "the backup holds no transactions, but its header tells append indexes " +
                            "${header.lowestAppendIndex} to ${header.highestAppendIndex}"
                    )
                last.appendIndex != header.highestAppendIndex ->
                    throw IOException(
                        "the backup ends with $last, but its header tells append index " +
                            header.highestAppendIndex
                    )
                last.checksum != header.lastChecksum ->
                    throw IOException(
                        "$last has checksum ${last.checksum}, but its header tells " +
                            "${header.lastChecksum}"
                    )
            }
        }
    }
}
