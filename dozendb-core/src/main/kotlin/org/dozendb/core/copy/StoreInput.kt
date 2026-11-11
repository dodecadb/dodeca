package org.dozendb.core.copy

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicLongArray
import org.neo4j.batchimport.api.InputIterable
import org.neo4j.batchimport.api.InputIterator
import org.neo4j.batchimport.api.input.Collector
import org.neo4j.batchimport.api.input.Group
import org.neo4j.batchimport.api.input.IdType
import org.neo4j.batchimport.api.input.Input
import org.neo4j.batchimport.api.input.InputChunk
import org.neo4j.batchimport.api.input.InputEntityVisitor
import org.neo4j.batchimport.api.input.PropertySizeCalculator
import org.neo4j.batchimport.api.input.ReadableGroups
import org.neo4j.internal.batchimport.input.Groups
import org.neo4j.io.pagecache.context.CursorContext
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.storageengine.api.PropertySelection
import org.neo4j.storageengine.api.StorageEntityCursor
import org.neo4j.storageengine.api.StoragePropertyCursor
import org.neo4j.token.api.TokenHolder

/**
 * The nodes and relationships of [source] as input of Neo4j's batch importer, with [filters]
 * applied. Nodes keep their ids, as `--id-type=actual` does, unless [compact], when they are given
 * by their old ids for the importer to number anew; relationships get new ids either way.
 *
 * An entity whose records cannot be read, as in a store with inconsistencies, is left out and
 * recorded in [report]; so are the relationships of a node left out.
 */
internal class StoreInput(
    private val source: SourceStore,
    private val filters: CopyFilters,
    private val compact: Boolean,
    private val report: CopyReport,
) : Input {
    private val groups = Groups()
    private val global: Group? = if (compact) groups.getOrCreate(null) else null

    /** The nodes copied, by id, one bit each; complete once the importer has read every node. */
    private val copied = AtomicLongArray(((source.nodeHighId + 63) / 64).toInt().coerceAtLeast(1))

    val nodesCopied = AtomicLong()
    val nodesLeftOut = AtomicLong()
    val relationshipsCopied = AtomicLong()
    val relationshipsLeftOut = AtomicLong()

    private val labelTokens: TokenHolder = source.tokens.labelTokens()
    private val typeTokens: TokenHolder = source.tokens.relationshipTypeTokens()
    private val keyTokens: TokenHolder = source.tokens.propertyKeyTokens()

    override fun nodes(badCollector: Collector): InputIterable = InputIterable {
        Scan(source.nodeHighId) { NodeChunk() }
    }

    override fun relationships(badCollector: Collector): InputIterable = InputIterable {
        Scan(source.relationshipHighId) { RelationshipChunk() }
    }

    override fun idType(): IdType = if (compact) IdType.INTEGER else IdType.ACTUAL

    override fun groups(): ReadableGroups = groups

    override fun validateAndEstimate(valueSizeCalculator: PropertySizeCalculator): Input.Estimates {
        val properties = source.neoStores.propertyStore.idGenerator.highId
        val propertyBytes =
            properties * source.neoStores.propertyStore.recordSize +
                source.neoStores.propertyStore.stringStore.idGenerator.highId *
                    source.neoStores.propertyStore.stringStore.recordSize
        return Input.knownEstimates(
            source.nodeHighId,
            source.relationshipHighId,
            properties / 2,
            properties / 2,
            propertyBytes / 2,
            propertyBytes / 2,
            0,
        )
    }

    private fun markCopied(nodeId: Long) {
        val word = (nodeId ushr 6).toInt()
        val bit = 1L shl (nodeId and 63).toInt()
        while (true) {
            val current = copied.get(word)
            if (current and bit != 0L || copied.compareAndSet(word, current, current or bit)) return
        }
    }

    private fun isCopied(nodeId: Long): Boolean {
        if (nodeId < 0 || nodeId >= source.nodeHighId) return false
        return copied.get((nodeId ushr 6).toInt()) and (1L shl (nodeId and 63).toInt()) != 0L
    }

    /** Hands out the ids below [highId] to chunks, a range at a time. */
    private class Scan(private val highId: Long, private val chunk: () -> IdChunk) : InputIterator {
        private var next = 0L

        override fun newChunk(): InputChunk = chunk()

        @Synchronized
        override fun next(chunk: InputChunk): Boolean {
            if (next >= highId) return false
            val from = next
            next = minOf(highId, from + RANGE)
            (chunk as IdChunk).range(from, next)
            return true
        }

        override fun close() {}

        companion object {
            const val RANGE = 10_000L
        }
    }

    /** Reads the entities of a range of ids, for one thread. */
    private abstract inner class IdChunk : InputChunk {
        protected val context: CursorContext = source.contextFactory.create("dozendb-copy")
        protected val cursors = CachedStoreCursors(source.neoStores, context)
        protected val reader = source.reader()
        protected val properties: StoragePropertyCursor =
            reader.allocatePropertyCursor(context, cursors, EmptyMemoryTracker.INSTANCE)
        private var id = 0L
        private var end = 0L

        fun range(from: Long, to: Long) {
            id = from
            end = to
        }

        override fun next(visitor: InputEntityVisitor): Boolean {
            while (id < end) {
                if (visit(id++, visitor)) {
                    visitor.endOfEntity()
                    return true
                }
            }
            return false
        }

        /** Visits the entity [id], if in use and copied; returns whether it did. */
        abstract fun visit(id: Long, visitor: InputEntityVisitor): Boolean

        /** The properties of [entity] that [keep] keeps, by name. */
        protected fun read(
            entity: StorageEntityCursor,
            keep: (String) -> Boolean,
        ): List<Pair<String, Any>> {
            val read = mutableListOf<Pair<String, Any>>()
            entity.properties(properties, PropertySelection.ALL_PROPERTIES)
            while (properties.next()) {
                val key = keyTokens.getTokenById(properties.propertyKey()).name()
                if (keep(key)) read += key to properties.propertyValue().asObject()
            }
            return read
        }

        override fun close() {
            properties.close()
            cursors.close()
            context.close()
        }
    }

    private inner class NodeChunk : IdChunk() {
        private val nodes = reader.allocateNodeCursor(context, cursors, EmptyMemoryTracker.INSTANCE)

        override fun visit(id: Long, visitor: InputEntityVisitor): Boolean {
            nodes.single(id)
            if (!nodes.next()) return false
            // Read whole before the importer sees any of it, so that a record that cannot be
            // read leaves nothing behind.
            val labels: List<String>
            val kept: List<Pair<String, Any>>
            try {
                labels = nodes.labels().map { labelTokens.getTokenById(it).name() }
                if (!filters.copiesNode(labels)) {
                    nodesLeftOut.incrementAndGet()
                    return false
                }
                kept = read(nodes) { filters.keepsNodeProperty(labels, it) }
            } catch (e: Exception) {
                report.unreadable("node", id, e)
                return false
            }
            if (global != null) visitor.id(id, global) else visitor.id(id)
            visitor.labels(filters.keptLabels(labels).toTypedArray())
            for ((key, value) in kept) visitor.property(key, value)
            markCopied(id)
            nodesCopied.incrementAndGet()
            return true
        }

        override fun close() {
            nodes.close()
            super.close()
        }
    }

    private inner class RelationshipChunk : IdChunk() {
        private val relationships =
            reader.allocateRelationshipScanCursor(context, cursors, EmptyMemoryTracker.INSTANCE)

        override fun visit(id: Long, visitor: InputEntityVisitor): Boolean {
            relationships.single(id)
            if (!relationships.next()) return false
            val type: String
            val start: Long
            val end: Long
            val kept: List<Pair<String, Any>>
            try {
                type = typeTokens.getTokenById(relationships.type()).name()
                start = relationships.sourceNodeReference()
                end = relationships.targetNodeReference()
                if (!filters.copiesRelationship(type) || !isCopied(start) || !isCopied(end)) {
                    relationshipsLeftOut.incrementAndGet()
                    return false
                }
                kept = read(relationships) { filters.keepsRelationshipProperty(type, it) }
            } catch (e: Exception) {
                report.unreadable("relationship", id, e)
                return false
            }
            if (global != null) {
                visitor.startId(start, global)
                visitor.endId(end, global)
            } else {
                visitor.startId(start)
                visitor.endId(end)
            }
            visitor.type(type)
            for ((key, value) in kept) visitor.property(key, value)
            relationshipsCopied.incrementAndGet()
            return true
        }

        override fun close() {
            relationships.close()
            super.close()
        }
    }
}
