package org.dozendb.core.importer

import java.io.BufferedOutputStream
import java.io.DataOutputStream
import java.io.PrintStream
import java.nio.file.Files
import org.dozendb.core.importer.IncrementalImporter.Companion.describe
import org.eclipse.collections.api.factory.primitive.LongSets
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList
import org.neo4j.batchimport.api.Configuration
import org.neo4j.batchimport.api.InputIterable
import org.neo4j.batchimport.api.input.Collector
import org.neo4j.batchimport.api.input.Group
import org.neo4j.batchimport.api.input.Input
import org.neo4j.cli.ExitCode
import org.neo4j.internal.batchimport.cache.NumberArrayFactories
import org.neo4j.internal.batchimport.cache.idmapping.IdMapper
import org.neo4j.internal.batchimport.cache.idmapping.IdMappers
import org.neo4j.internal.batchimport.input.InputEntity
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory
import org.neo4j.memory.MemoryTracker

/**
 * The build stage of an incremental import: reads [input] against the keys of the existing nodes
 * that `prepare` kept in [increment], and writes the new nodes and relationships to it.
 *
 * Neo4j's own ID mapper, which keeps its data off the heap, maps every key to a place: the existing
 * nodes come first, so that a new node whose key exists, in the database or earlier in the input,
 * is the duplicate, which [collector] takes as the import's `--skip-duplicate-nodes` says. A new
 * node of a keyed ID space gets the label of its key if its labels lack it, so that the key's
 * constraint covers it. Relationship ends resolve to existing node ids or to the places of new
 * nodes; those that resolve to neither are bad relationships, which [collector] takes as
 * `--skip-bad-relationships` says.
 */
internal class IncrementBuilder(
    private val increment: Increment,
    private val state: IncrementState,
    private val input: Input,
    private val collector: Collector,
    private val importConfig: Configuration,
    private val memoryTracker: MemoryTracker,
    private val out: PrintStream,
) {
    private val factory = NumberArrayFactories.AUTO_WITHOUT_SWAP
    private val existing = state.groups.sumOf { it.existing }
    private val keyedGroups = state.groups.associateBy { it.name }
    private val tokens = Tokens()

    /** Builds the increment and returns the state that records it. */
    fun build(): IncrementState {
        val estimate = maxOf(existing * 2, 1_000_000)
        return KeyStore(increment.ids, factory, memoryTracker).use { keys ->
            val mapper =
                when (state.keyType) {
                    KeyType.TEXT ->
                        IdMappers.strings(
                            factory,
                            input.groups(),
                            importConfig.strictNodeCheck(),
                            memoryTracker,
                            estimate,
                            keys,
                        )
                    KeyType.INTEGER ->
                        IdMappers.longs(factory, input.groups(), memoryTracker, estimate)
                }
            mapper.use { build(mapper, keys) }
        }
    }

    private fun build(mapper: IdMapper, keys: KeyStore): IncrementState {
        val setter = mapper.newSetter(IdMapper.WORKER_ID_AGNOSTIC)
        val newNodes = readNodes(setter, keys)
        val nodeIds = factory.newLongArray(maxOf(existing, 1), -1, memoryTracker)
        try {
            var place = 0L
            state.groups.forEachIndexed { i, keyed ->
                val group =
                    input.groups().get(keyed.name)
                        ?: throw IncrementalImportException(
                            "The node files no longer declare ${describe(keyed.name)}, which " +
                                "prepare read: run prepare again with the files of the build.",
                            ExitCode.DATAERR,
                        )
                readKeys(increment.keys(i)) { key, nodeId ->
                    val mapped = mapperKey(key)
                    setter.put(mapped, place, group)
                    keys.add(place, mapped)
                    nodeIds.set(place, nodeId)
                    place++
                }
            }
            setter.close()
            keys.complete()
            mapper.prepare(keys, collector, ProgressMonitorFactory.NONE, LongSets.immutable.empty())
            val skipped = writeDuplicates(mapper)
            val relationships =
                readRelationships(mapper) { place ->
                    if (place < existing) NewRelationship.existing(nodeIds.get(place))
                    else NewRelationship.new(place - existing)
                }
            tokens.write(increment.tokens)
            out.println(
                "Built an increment of ${newNodes - skipped} new nodes and $relationships new " +
                    "relationships" +
                    (if (skipped > 0) ", skipping $skipped nodes whose keys exist" else "")
            )
            return state.copy(
                stage = IncrementState.Stage.BUILT,
                nodes = newNodes,
                skippedNodes = skipped,
                relationships = relationships,
            )
        } finally {
            nodeIds.close()
        }
    }

    /** Writes the new nodes, giving the keyed ones to [setter]; returns how many there are. */
    private fun readNodes(setter: IdMapper.Setter, keys: KeyStore): Long {
        var ordinal = 0L
        EntityWriter(increment.nodes).use { writer ->
            read(input.nodes(collector)) { node ->
                val labels = node.labels.toMutableList()
                node.objectId?.let { id ->
                    val group = node.idGroup
                    val keyed = keyed(group)
                    val key = key(id, keyed.keyType, group)
                    checkSingleId(node, id, key, keyed, group)
                    if (keyed.label !in labels) labels += keyed.label
                    val mapped = mapperKey(key)
                    setter.put(mapped, existing + ordinal, group)
                    keys.add(existing + ordinal, mapped)
                }
                writer.write(
                    NewNode(
                        labels.distinct().map(tokens.labels::of).toIntArray(),
                        properties(node),
                    )
                )
                ordinal++
            }
        }
        return ordinal
    }

    /**
     * Fails unless [node]'s ID, whose [key] it is, is its key property, as with one ID column:
     * Neo4j joins several into an ID that is no property, and finding nodes by the first would
     * match others than the input means.
     */
    private fun checkSingleId(
        node: InputEntity,
        id: Any,
        key: Any,
        keyed: KeyedGroup,
        group: Group,
    ) {
        val property = node.properties.firstOrNull { it.keyName() == keyed.key }?.value()
        if (property != null && key(property, keyed.keyType, group) == key) return
        throw IncrementalImportException(
            "${describe(group.name())} has a node whose ID, ${shown(id)}, is not its " +
                "${keyed.key} property: $ONE_ID_COLUMN",
            ExitCode.DATAERR,
        )
    }

    /** Writes the places of the new nodes that are duplicates; returns how many there are. */
    private fun writeDuplicates(mapper: IdMapper): Long {
        val duplicates = LongArrayList()
        val iterator = mapper.leftOverDuplicateNodesIds()
        while (iterator.hasNext()) {
            val place = iterator.next()
            if (place >= existing) duplicates.add(place - existing)
        }
        duplicates.sortThis()
        DataOutputStream(BufferedOutputStream(Files.newOutputStream(increment.duplicates))).use {
            out ->
            duplicates.forEach(out::writeLong)
        }
        return duplicates.size().toLong()
    }

    /** Writes the relationships whose ends resolve; returns how many there are. */
    private fun readRelationships(mapper: IdMapper, end: (place: Long) -> Long): Long {
        var count = 0L
        EntityWriter(increment.relationships).use { writer ->
            mapper.newGetter(IdMapper.WORKER_ID_AGNOSTIC).use { getter ->
                fun resolve(id: Any?, group: Group?): Long? {
                    if (id == null || group == null) return null
                    val keyed = keyedGroups[group.name()] ?: return null
                    val place = getter.get(mapperKey(key(id, keyed.keyType, group)), group)
                    return if (place == IdMapper.ID_NOT_FOUND) null else end(place)
                }
                read(input.relationships(collector)) { relationship ->
                    val type =
                        relationship.stringType
                            ?: throw IncrementalImportException(
                                "A relationship has no type: give the relationship files a " +
                                    ":TYPE column, or a type with --relationships=TYPE=<files>.",
                                ExitCode.DATAERR,
                            )
                    val start = resolve(relationship.objectStartId, relationship.startIdGroup)
                    val end = resolve(relationship.objectEndId, relationship.endIdGroup)
                    if (start == null || end == null) {
                        collector.collectBadRelationship(
                            relationship.objectStartId,
                            relationship.startIdGroup,
                            type,
                            relationship.objectEndId,
                            relationship.endIdGroup,
                            if (start == null) relationship.objectStartId
                            else relationship.objectEndId,
                            relationship.sourceDescription,
                            relationship.lineNumber,
                        )
                    } else {
                        writer.write(
                            NewRelationship(
                                tokens.types.of(type),
                                start,
                                end,
                                properties(relationship),
                            )
                        )
                        count++
                    }
                }
            }
        }
        return count
    }

    private fun keyed(group: Group): KeyedGroup =
        keyedGroups[group.name()]
            ?: throw IncrementalImportException(
                "Prepare did not read ${describe(group.name())}: run prepare again with the node " +
                    "files of the build.",
                ExitCode.DATAERR,
            )

    private fun properties(entity: InputEntity): List<Property> =
        entity.properties.map {
            Property(tokens.keys.of(it.keyName()), EntityWriter.storable(it.value()))
        }

    /** [key] as the ID mapper takes keys: as text, or as a Long for an integer mapper. */
    private fun mapperKey(key: Any): Any =
        when (state.keyType) {
            KeyType.TEXT -> key.toString()
            KeyType.INTEGER -> key as Long
        }

    /** Reads [entities] one by one, single-threaded. */
    private fun read(entities: InputIterable, visit: (InputEntity) -> Unit) {
        val entity = InputEntity()
        entities.iterator().use { iterator ->
            iterator.newChunk().use { chunk ->
                while (iterator.next(chunk)) {
                    while (chunk.next(entity)) visit(entity)
                }
            }
        }
    }

    companion object {
        /**
         * The key that the input ID [id] of [group] stands for, as [keyType] compares keys: a
         * String, or a Long.
         */
        internal fun key(id: Any, keyType: KeyType, group: Group): Any =
            when {
                keyType == KeyType.TEXT && id is String -> id
                keyType == KeyType.TEXT && id is Char -> id.toString()
                keyType == KeyType.INTEGER &&
                    (id is Long || id is Int || id is Short || id is Byte) ->
                    (id as Number).toLong()
                else ->
                    throw IncrementalImportException(
                        "${describe(group.name())} has an ID, ${shown(id)}, that is not a single " +
                            (if (keyType == KeyType.TEXT) "text" else "integer") +
                            " value: $ONE_ID_COLUMN",
                        ExitCode.DATAERR,
                    )
            }

        /** Why IDs of several columns are refused, as Enterprise imports them in full only. */
        private const val ONE_ID_COLUMN =
            "incremental import takes one ID column per node file, which names the label and " +
                "property key of the nodes' keys, such as uuid:ID{label:Person}. Several ID " +
                "columns work in full imports only."

        /** [id] as the input has it: Neo4j joins the values of several ID columns with BEL. */
        private fun shown(id: Any): String = id.toString().replace('\u0007', ',')
    }
}
