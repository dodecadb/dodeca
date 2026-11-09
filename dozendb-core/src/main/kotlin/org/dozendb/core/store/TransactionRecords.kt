package org.dozendb.core.store

import java.io.ByteArrayOutputStream
import java.io.IOException
import java.nio.ByteBuffer
import org.neo4j.common.Subject
import org.neo4j.io.fs.ByteBufferReadableChannel
import org.neo4j.io.fs.OutputStreamWritableChannel
import org.neo4j.kernel.KernelVersion
import org.neo4j.kernel.impl.api.LeaseService
import org.neo4j.kernel.impl.transaction.log.CompleteCommandBatch
import org.neo4j.storageengine.api.CommandReaderFactory
import org.neo4j.storageengine.api.StorageCommand

/**
 * A [CommittedTransaction] as bytes, as differential backups and secondaries' pulls carry it: its
 * fields and its commands in Neo4j's serialization for their kernel version, `long transactionId,
 * long appendIndex, int previousChecksum, int checksum, byte kernelVersion, long timeStarted, long
 * latestCommittedTxWhenStarted, long timeCommitted, long consensusIndex, int commandCount,
 * commands`.
 */
object TransactionRecords {
    fun encode(transaction: CommittedTransaction): ByteArray {
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

    fun decode(record: ByteArray, readers: CommandReaderFactory): CommittedTransaction {
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
        return CommittedTransaction(transactionId, appendIndex, previousChecksum, checksum, batch)
    }
}
