package org.dozendb.core.importer

import java.io.BufferedInputStream
import java.io.DataInputStream
import java.io.EOFException
import java.io.PrintStream
import java.nio.file.Files
import org.dozendb.core.importer.MergeJournal.Phase
import org.neo4j.internal.batchimport.cache.LongArray
import org.neo4j.internal.kernel.api.Write
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.storageengine.api.TransactionIdStore
import org.neo4j.values.storable.Values

/**
 * The merge stage of an incremental import: writes the new nodes and relationships of [increment]
 * into [database], opened by the import, through Neo4j's own transactions of [batchSize] entities,
 * which keep its relationship chains, counts, indexes and constraints. Each batch goes through
 * [journal], so that a merge cut short continues after the last batch the database committed.
 */
internal class IncrementMerger(
    private val increment: Increment,
    private val state: IncrementState,
    private val database: GraphDatabaseAPI,
    private val journal: MergeJournal,
    private val out: PrintStream,
    private val batchSize: Int = BATCH_SIZE,
    /** Called with false before each batch commits, and with true after; for tests. */
    private val onBatch: (committed: Boolean) -> Unit = {},
) {
    private val transactionIds =
        database.dependencyResolver.resolveDependency(TransactionIdStore::class.java)

    /** The token ids of the increment's labels, relationship types and property keys. */
    private class TokenIds(val labels: IntArray, val types: IntArray, val keys: IntArray)

    /** Merges what the increment holds beyond [position]. */
    fun merge(position: MergeJournal.Position) {
        position.newNodeIds.use { newNodeIds ->
            val tokens = createTokens(Tokens.read(increment.tokens))
            if (!position.tokens) {
                val last = transactionIds.lastCommittedTransaction
                journal.tokensCreated(last.id(), last.checksum())
            }
            mergeNodes(position.nodes, position.relationships, tokens, newNodeIds)
            mergeRelationships(position.relationships, tokens, newNodeIds)
        }
    }

    /** Creates the tokens the increment uses, which Neo4j commits in transactions of their own. */
    private fun createTokens(tokens: Tokens): TokenIds = transaction { ktx ->
        val write = ktx.tokenWrite()
        val ids =
            TokenIds(
                IntArray(tokens.labels.names.size),
                IntArray(tokens.types.names.size),
                IntArray(tokens.keys.names.size),
            )
        write.labelGetOrCreateForNames(tokens.labels.names.toTypedArray(), ids.labels)
        write.relationshipTypeGetOrCreateForNames(tokens.types.names.toTypedArray(), ids.types)
        write.propertyKeyGetOrCreateForNames(tokens.keys.names.toTypedArray(), ids.keys)
        ids
    }

    private fun mergeNodes(
        from: Long,
        relationships: Long,
        tokens: TokenIds,
        newNodeIds: LongArray,
    ) {
        val total = state.nodes
        if (from >= total) return
        val progress = Progress("new nodes", total, from)
        Duplicates().use { duplicates ->
            EntityReader(increment.nodes, from, relationships = false).use { reader ->
                var done = from
                while (done < total) {
                    val count = minOf(batchSize.toLong(), total - done).toInt()
                    val ids = kotlin.LongArray(count)
                    commit(Phase.NODES, done, count, ids, done, relationships) { write ->
                        for (i in 0 until count) {
                            val node = checkNotNull(reader.node()) { "the increment ends early" }
                            if (duplicates.contains(done + i)) {
                                ids[i] = -1
                                continue
                            }
                            val labels = node.labels.map { tokens.labels[it] }.distinct().sorted()
                            val id = write.nodeCreateWithLabels(labels.toIntArray())
                            for (property in node.properties) {
                                write.nodeSetProperty(
                                    id,
                                    tokens.keys[property.key],
                                    Values.of(property.value),
                                )
                            }
                            ids[i] = id
                        }
                        ids.any { it >= 0 }
                    }
                    ids.forEachIndexed { i, id -> newNodeIds.set(done + i, id) }
                    done += count
                    progress.reached(done)
                }
            }
        }
    }

    private fun mergeRelationships(from: Long, tokens: TokenIds, newNodeIds: LongArray) {
        val total = state.relationships
        if (from >= total) return
        val progress = Progress("new relationships", total, from)
        fun node(ref: Long): Long {
            if (!NewRelationship.isNew(ref)) return ref
            val id = newNodeIds.get(NewRelationship.of(ref))
            check(id >= 0) {
                "a relationship ends at new node ${NewRelationship.of(ref)}, a duplicate"
            }
            return id
        }
        EntityReader(increment.relationships, from, relationships = true).use { reader ->
            var done = from
            while (done < total) {
                val count = minOf(batchSize.toLong(), total - done).toInt()
                commit(Phase.RELATIONSHIPS, done, count, null, state.nodes, done) { write ->
                    repeat(count) {
                        val relationship =
                            checkNotNull(reader.relationship()) { "the increment ends early" }
                        val id =
                            write.relationshipCreate(
                                node(relationship.start),
                                tokens.types[relationship.type],
                                node(relationship.end),
                            )
                        for (property in relationship.properties) {
                            write.relationshipSetProperty(
                                id,
                                tokens.keys[property.key],
                                Values.of(property.value),
                            )
                        }
                    }
                    true
                }
                done += count
                progress.reached(done)
            }
        }
    }

    /**
     * Runs [batch] of [count] entities of [phase] from [from] in a transaction, recorded in the
     * journal before it commits, with [newNodeIds] for nodes. [batch] tells whether it wrote
     * anything: a batch of duplicates alone commits no transaction. [nodes] and [relationships] are
     * what the database holds before the batch, for a failure's message.
     */
    private fun commit(
        phase: Phase,
        from: Long,
        count: Int,
        newNodeIds: kotlin.LongArray?,
        nodes: Long,
        relationships: Long,
        batch: (Write) -> Boolean,
    ) {
        val before = transactionIds.lastCommittedTransaction
        try {
            transaction { ktx ->
                val wrote = batch(ktx.dataWrite())
                val transaction = if (wrote) before.id() + 1 else before.id()
                journal.intend(phase, from, count.toLong(), transaction, newNodeIds)
                onBatch(false)
                wrote
            }
        } catch (e: Exception) {
            val what = if (phase == Phase.NODES) "new nodes" else "new relationships"
            throw IncrementalImportException(
                "Merging $what ${from + 1} to ${from + count} into database '${state.database}' " +
                    "failed: ${e.message}. The database keeps the $nodes new nodes and " +
                    "$relationships new relationships merged before them; the merge stage " +
                    "continues from there when run again.",
                cause = e,
            )
        }
        onBatch(true)
        val after = transactionIds.lastCommittedTransaction
        check(after.id() == before.id() || after.id() == before.id() + 1) {
            "Database '${state.database}' committed transactions ${before.id() + 1} to " +
                "${after.id()} during a batch of the merge, which writes alone"
        }
        journal.committed(after.checksum())
    }

    /** Runs [work] in a transaction, committed if it returns true or other than a Boolean. */
    private fun <T> transaction(work: (KernelTransaction) -> T): T =
        database
            .beginTransaction(KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED)
            .use { tx ->
                val result = work(tx.kernelTransaction())
                if (result != false) tx.commit()
                result
            }

    /** The places of the new nodes that are duplicates, asked in increasing order. */
    private inner class Duplicates : AutoCloseable {
        private val input =
            DataInputStream(BufferedInputStream(Files.newInputStream(increment.duplicates)))
        private var next = read()

        fun contains(place: Long): Boolean {
            while (next != null && next!! < place) next = read()
            return next == place
        }

        private fun read(): Long? =
            try {
                input.readLong()
            } catch (_: EOFException) {
                null
            }

        override fun close() = input.close()
    }

    /** Reports how far a phase got, at every tenth of it. */
    private inner class Progress(private val what: String, private val total: Long, from: Long) {
        private val start = System.nanoTime()
        private val first = from
        private var reported = from * 10 / total

        fun reached(done: Long) {
            val tenth = done * 10 / total
            if (tenth == reported && done < total) return
            reported = tenth
            val seconds = (System.nanoTime() - start) / 1e9
            val rate = if (seconds > 0) ((done - first) / seconds).toLong() else 0
            out.println("Merged $done of $total $what ($rate/s)")
        }
    }

    companion object {
        /** Entities per transaction: large, as Neo4j commits them at a fixed cost each. */
        const val BATCH_SIZE = 100_000
    }
}
