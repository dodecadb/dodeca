package dev.dodeca.core.importer

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.APPEND
import java.nio.file.StandardOpenOption.CREATE
import java.nio.file.StandardOpenOption.READ
import java.nio.file.StandardOpenOption.WRITE
import org.neo4j.internal.batchimport.cache.LongArray
import org.neo4j.internal.batchimport.cache.NumberArrayFactories
import org.neo4j.memory.MemoryTracker

/**
 * What the merge of an increment did, so that a merge cut short continues after the batches the
 * database committed. Before a batch commits, the journal records the batch and the transaction id
 * it will commit as, and, for nodes, the ids of its new nodes; after, the checksum it committed
 * with. Only the merge writes to the database meanwhile, so the database's last transaction tells
 * which of the recorded batches it committed.
 */
internal class MergeJournal(
    private val journal: Path,
    private val nodeIds: Path,
    private val memoryTracker: MemoryTracker,
) : AutoCloseable {
    enum class Phase {
        NODES,
        RELATIONSHIPS,
    }

    /** A batch of [count] entities of [phase] from the [from]th on, committed as [transaction]. */
    data class Batch(
        val phase: Phase,
        val from: Long,
        val count: Long,
        val transaction: Long,
        val checksum: Int?,
    )

    /**
     * Where the merge stands: [tokens] tells whether the tokens exist, [nodes] and [relationships]
     * how many entities of each the database holds, [newNodeIds] the ids of those new nodes, -1 for
     * duplicates; the caller closes it.
     */
    class Position(
        val tokens: Boolean,
        val nodes: Long,
        val relationships: Long,
        val newNodeIds: LongArray,
    )

    private var channel: FileChannel? = null
    private var ids: FileChannel? = null

    /**
     * Reads the journal against the database's last transaction, [lastTransaction] with
     * [lastChecksum], and the store's position when `prepare` read it; drops the batch that did not
     * commit. Fails if the database moved otherwise.
     */
    fun recover(lastTransaction: Long, lastChecksum: Int, prepared: IncrementState): Position {
        var tokens: Pair<Long, Int>? = null
        val batches = mutableListOf<Batch>()
        if (Files.exists(journal)) {
            val buffer = ByteBuffer.wrap(Files.readAllBytes(journal))
            while (buffer.remaining() > 0) {
                when (val kind = buffer.get()) {
                    TOKENS -> tokens = buffer.getLong() to buffer.getInt()
                    INTENT ->
                        batches +=
                            Batch(
                                Phase.entries[buffer.get().toInt()],
                                buffer.getLong(),
                                buffer.getLong(),
                                buffer.getLong(),
                                null,
                            )
                    COMMITTED ->
                        batches[batches.size - 1] = batches.last().copy(checksum = buffer.getInt())
                    else -> throw IOException("$journal: unknown record $kind")
                }
            }
        }
        fun moved(): Nothing =
            throw IncrementalImportException(
                "Database '${prepared.database}' changed since its merge began: its last " +
                    "transaction is $lastTransaction. Restore it from the backup taken before the " +
                    "import."
            )
        fun at(transaction: Long, checksum: Int?) =
            lastTransaction == transaction && (checksum == null || checksum == lastChecksum)
        val committed: List<Batch> =
            when {
                tokens == null ->
                    if (at(prepared.lastTransaction, prepared.lastChecksum)) {
                        emptyList()
                    } else {
                        throw IncrementalImportException(
                            "Database '${prepared.database}' changed since the prepare stage " +
                                "read it: its last transaction is $lastTransaction, not " +
                                "${prepared.lastTransaction}. Run the prepare and build stages " +
                                "again."
                        )
                    }
                batches.isEmpty() -> if (at(tokens.first, tokens.second)) emptyList() else moved()
                at(batches.last().transaction, batches.last().checksum) -> batches
                batches.size == 1 && at(tokens.first, tokens.second) -> emptyList()
                batches.size > 1 &&
                    at(batches[batches.size - 2].transaction, batches[batches.size - 2].checksum) ->
                    batches.dropLast(1)
                else -> moved()
            }
        val nodes = committed.filter { it.phase == Phase.NODES }.sumOf { it.count }
        val relationships = committed.filter { it.phase == Phase.RELATIONSHIPS }.sumOf { it.count }
        rewrite(tokens, committed)
        val newNodeIds = readNodeIds(nodes)
        return Position(tokens != null, nodes, relationships, newNodeIds)
    }

    /** Records that the tokens exist, the database's last transaction then being [transaction]. */
    fun tokensCreated(transaction: Long, checksum: Int) {
        write(ByteBuffer.allocate(1 + 8 + 4).put(TOKENS).putLong(transaction).putInt(checksum))
    }

    /**
     * Records the batch that is about to commit as [transaction], and the ids of its new nodes,
     * [newNodeIds], for a batch of nodes.
     */
    fun intend(
        phase: Phase,
        from: Long,
        count: Long,
        transaction: Long,
        newNodeIds: kotlin.LongArray?,
    ) {
        if (newNodeIds != null) {
            val ids = checkNotNull(ids)
            val buffer = ByteBuffer.allocate(newNodeIds.size * Long.SIZE_BYTES)
            newNodeIds.forEach(buffer::putLong)
            buffer.flip()
            while (buffer.hasRemaining()) ids.write(buffer)
            ids.force(false)
        }
        write(
            ByteBuffer.allocate(1 + 1 + 8 + 8 + 8)
                .put(INTENT)
                .put(phase.ordinal.toByte())
                .putLong(from)
                .putLong(count)
                .putLong(transaction)
        )
    }

    /** Records that the batch last intended committed with [checksum]. */
    fun committed(checksum: Int) {
        write(ByteBuffer.allocate(1 + 4).put(COMMITTED).putInt(checksum))
    }

    private fun write(record: ByteBuffer) {
        val channel = checkNotNull(channel) { "the journal was not recovered" }
        record.flip()
        while (record.hasRemaining()) channel.write(record)
        channel.force(false)
    }

    /** Keeps only the records of [committed] batches, so that the next ones follow them. */
    private fun rewrite(tokens: Pair<Long, Int>?, committed: List<Batch>) {
        val temporary = journal.resolveSibling("${journal.fileName}.tmp")
        FileChannel.open(
                temporary,
                CREATE,
                WRITE,
                java.nio.file.StandardOpenOption.TRUNCATE_EXISTING,
            )
            .use { out ->
                val buffer = ByteBuffer.allocate(14 + committed.size * (27 + 5))
                tokens?.let { buffer.put(TOKENS).putLong(it.first).putInt(it.second) }
                for (batch in committed) {
                    buffer
                        .put(INTENT)
                        .put(batch.phase.ordinal.toByte())
                        .putLong(batch.from)
                        .putLong(batch.count)
                        .putLong(batch.transaction)
                    batch.checksum?.let { buffer.put(COMMITTED).putInt(it) }
                }
                buffer.flip()
                while (buffer.hasRemaining()) out.write(buffer)
                out.force(false)
            }
        Files.move(
            temporary,
            journal,
            java.nio.file.StandardCopyOption.REPLACE_EXISTING,
            java.nio.file.StandardCopyOption.ATOMIC_MOVE,
        )
        channel = FileChannel.open(journal, WRITE, APPEND)
    }

    /** The first [count] ids of new nodes, the file cut after them. */
    private fun readNodeIds(count: Long): LongArray {
        val array =
            NumberArrayFactories.AUTO_WITHOUT_SWAP.newDynamicLongArray(
                1 shl 20,
                -1,
                memoryTracker,
            )
        val ids = FileChannel.open(nodeIds, CREATE, READ, WRITE)
        this.ids = ids
        val length = count * Long.SIZE_BYTES
        if (ids.size() < length) {
            throw IOException("$nodeIds holds ${ids.size() / 8} node ids, fewer than $count")
        }
        ids.truncate(length)
        ids.position(length)
        val buffer = ByteBuffer.allocate(1 shl 16)
        var position = 0L
        var i = 0L
        while (position < length) {
            buffer.clear()
            buffer.limit(minOf(buffer.capacity().toLong(), length - position).toInt())
            val read = ids.read(buffer, position)
            if (read < 0) throw IOException("$nodeIds ends at $position")
            position += read
            buffer.flip()
            while (buffer.remaining() >= Long.SIZE_BYTES) array.set(i++, buffer.getLong())
            check(buffer.remaining() == 0) { "partial read of $nodeIds" }
        }
        return array
    }

    override fun close() {
        channel?.close()
        ids?.close()
    }

    private companion object {
        const val TOKENS: Byte = 1
        const val INTENT: Byte = 2
        const val COMMITTED: Byte = 3
    }
}
