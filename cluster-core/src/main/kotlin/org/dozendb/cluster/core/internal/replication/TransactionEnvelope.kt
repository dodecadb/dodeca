package org.dozendb.cluster.core.internal.replication

import java.io.ByteArrayOutputStream
import java.io.IOException
import java.nio.ByteBuffer
import org.neo4j.common.Subject
import org.neo4j.io.fs.ByteBufferReadableChannel
import org.neo4j.io.fs.OutputStreamWritableChannel
import org.neo4j.kernel.KernelVersion
import org.neo4j.kernel.impl.transaction.log.CompleteCommandBatch
import org.neo4j.storageengine.api.CommandBatch
import org.neo4j.storageengine.api.CommandReaderFactory
import org.neo4j.storageengine.api.StorageCommand

/**
 * The payload of a replicated transaction: the kernel's commands in Neo4j's own log serialization
 * for their kernel version, plus the transaction metadata every member needs to apply them.
 *
 * Format version 1: `byte format, byte kind, byte kernelVersion, long timeStarted, long
 * latestCommittedTxWhenStarted, long timeCommitted, int leaseId, subject, int commandCount,
 * commands`. A subject is a kind byte, followed by the UTF-8 user name for [SUBJECT_USER].
 */
internal object TransactionEnvelope {
    private const val FORMAT: Byte = 1
    private const val KIND_TRANSACTION: Byte = 1

    private const val SUBJECT_SYSTEM: Byte = 0
    private const val SUBJECT_AUTH_DISABLED: Byte = 1
    private const val SUBJECT_ANONYMOUS: Byte = 2
    private const val SUBJECT_USER: Byte = 3

    fun encode(batch: CommandBatch): ByteArray {
        val buffer = ByteArrayOutputStream()
        val channel = OutputStreamWritableChannel(buffer)
        channel.put(FORMAT)
        channel.put(KIND_TRANSACTION)
        channel.put(batch.kernelVersion().version())
        channel.putLong(batch.timeStarted)
        channel.putLong(batch.latestCommittedTxWhenStarted)
        channel.putLong(batch.timeCommitted)
        channel.putInt(batch.leaseId)
        writeSubject(channel, batch.subject())
        channel.putInt(batch.commandCount())
        batch.accept { command ->
            command.serialize(channel)
            false
        }
        channel.close()
        return buffer.toByteArray()
    }

    /** Decodes a transaction, which takes [consensusIndex] as the index of its log entry. */
    fun decode(
        payload: ByteArray,
        consensusIndex: Long,
        readers: CommandReaderFactory,
    ): CompleteCommandBatch {
        val buffer = ByteBuffer.wrap(payload)
        val channel = ByteBufferReadableChannel(buffer)
        val format = channel.get()
        if (format != FORMAT) throw IOException("unsupported transaction envelope format $format")
        val kind = channel.get()
        if (kind != KIND_TRANSACTION) throw IOException("unsupported entry kind $kind")
        val kernelVersion = KernelVersion.getForVersion(channel.get())
        val timeStarted = channel.long
        val latestCommittedTxWhenStarted = channel.long
        val timeCommitted = channel.long
        val leaseId = channel.int
        val subject = readSubject(channel)
        val count = channel.int
        val reader = readers.get(kernelVersion)
        val commands =
            List<StorageCommand>(count) {
                reader.read(channel) ?: throw IOException("command ${it + 1} of $count is missing")
            }
        if (buffer.hasRemaining()) {
            throw IOException("${buffer.remaining()} unexpected bytes after $count commands")
        }
        return CompleteCommandBatch(
            commands,
            consensusIndex,
            timeStarted,
            latestCommittedTxWhenStarted,
            timeCommitted,
            leaseId,
            kernelVersion,
            subject,
        )
    }

    private fun writeSubject(channel: OutputStreamWritableChannel, subject: Subject) {
        when {
            subject === Subject.SYSTEM -> channel.put(SUBJECT_SYSTEM)
            subject === Subject.AUTH_DISABLED -> channel.put(SUBJECT_AUTH_DISABLED)
            subject === Subject.ANONYMOUS -> channel.put(SUBJECT_ANONYMOUS)
            else -> {
                val name = subject.username.encodeToByteArray()
                channel.put(SUBJECT_USER)
                channel.putInt(name.size)
                channel.put(name, name.size)
            }
        }
    }

    private fun readSubject(channel: ByteBufferReadableChannel): Subject =
        when (val kind = channel.get()) {
            SUBJECT_SYSTEM -> Subject.SYSTEM
            SUBJECT_AUTH_DISABLED -> Subject.AUTH_DISABLED
            SUBJECT_ANONYMOUS -> Subject.ANONYMOUS
            SUBJECT_USER -> {
                val name = ByteArray(channel.int)
                channel.get(name, name.size)
                Subject(name.decodeToString())
            }
            else -> throw IOException("unknown subject kind $kind")
        }
}

/** What applying a replicated transaction produced, sent back to the member that submitted it. */
internal sealed interface ApplyOutcome {
    /** Applied as the local transaction [transactionId], appended at [appendIndex]. */
    data class Applied(val appendIndex: Long, val transactionId: Long) : ApplyOutcome

    /** Not applied: the transaction ran under lease [leaseId], but the entry is of [term]. */
    data class LeaseExpired(val leaseId: Int, val term: Long) : ApplyOutcome

    companion object {
        private const val APPLIED: Byte = 0
        private const val LEASE_EXPIRED: Byte = 1

        fun encode(outcome: ApplyOutcome): ByteArray =
            when (outcome) {
                is Applied ->
                    ByteBuffer.allocate(1 + 2 * Long.SIZE_BYTES)
                        .put(APPLIED)
                        .putLong(outcome.appendIndex)
                        .putLong(outcome.transactionId)
                        .array()
                is LeaseExpired ->
                    ByteBuffer.allocate(1 + Int.SIZE_BYTES + Long.SIZE_BYTES)
                        .put(LEASE_EXPIRED)
                        .putInt(outcome.leaseId)
                        .putLong(outcome.term)
                        .array()
            }

        /** Null for an empty response, which Ratis sends for an entry applied before a restart. */
        fun decode(response: ByteArray): ApplyOutcome? {
            if (response.isEmpty()) return null
            val buffer = ByteBuffer.wrap(response)
            return when (val kind = buffer.get()) {
                APPLIED -> Applied(buffer.long, buffer.long)
                LEASE_EXPIRED -> LeaseExpired(buffer.int, buffer.long)
                else -> throw IOException("unknown apply outcome $kind")
            }
        }
    }
}
