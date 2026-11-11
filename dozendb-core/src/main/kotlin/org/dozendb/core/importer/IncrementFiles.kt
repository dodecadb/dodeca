package org.dozendb.core.importer

import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import org.neo4j.batchimport.api.PropertyValueLookup
import org.neo4j.internal.batchimport.cache.LongArray
import org.neo4j.internal.batchimport.cache.NumberArrayFactory
import org.neo4j.internal.batchimport.input.ValueType
import org.neo4j.io.fs.InputStreamReadableChannel
import org.neo4j.io.fs.OutputStreamWritableChannel
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.storable.Value

/**
 * The names of the tokens an increment uses, by index: labels, relationship types, property keys.
 */
internal class Tokens(
    val labels: Names = Names(),
    val types: Names = Names(),
    val keys: Names = Names(),
) {
    class Names(val names: MutableList<String> = mutableListOf()) {
        private val index = names.withIndex().associate { it.value to it.index }.toMutableMap()

        fun of(name: String): Int =
            index.getOrPut(name) {
                names += name
                names.size - 1
            }

        operator fun get(i: Int): String = names[i]
    }

    fun write(path: Path) {
        DataOutputStream(BufferedOutputStream(Files.newOutputStream(path))).use { out ->
            for (names in listOf(labels, types, keys)) {
                out.writeInt(names.names.size)
                names.names.forEach { writeString(out, it) }
            }
        }
    }

    companion object {
        fun read(path: Path): Tokens =
            DataInputStream(BufferedInputStream(Files.newInputStream(path))).use { input ->
                fun names() = Names(MutableList(input.readInt()) { readString(input) })
                Tokens(names(), names(), names())
            }
    }
}

/** A node or relationship property of an increment: its key's index and its value. */
internal class Property(val key: Int, val value: Any)

/** A new node of an increment. */
internal class NewNode(val labels: IntArray, val properties: List<Property>)

/**
 * A new relationship of an increment, whose ends are existing nodes, by their ids, or new nodes, by
 * their place in the increment, as [ref] gives them.
 */
internal class NewRelationship(
    val type: Int,
    val start: Long,
    val end: Long,
    val properties: List<Property>,
) {
    companion object {
        /** An end that is the existing node [nodeId]. */
        fun existing(nodeId: Long): Long = nodeId

        /** An end that is the new node at [ordinal] in the increment. */
        fun new(ordinal: Long): Long = -ordinal - 1

        fun isNew(ref: Long) = ref < 0

        /** The node id of an end of an existing node, or the ordinal of an end of a new one. */
        fun of(ref: Long): Long = if (ref < 0) -ref - 1 else ref
    }
}

/** Writes the new nodes or relationships of an increment, in order. */
internal class EntityWriter(path: Path) : AutoCloseable {
    private val stream = BufferedOutputStream(Files.newOutputStream(path), 1 shl 16)
    private val channel = OutputStreamWritableChannel(stream)

    fun write(node: NewNode) {
        channel.put(RECORD)
        channel.putInt(node.labels.size)
        node.labels.forEach { channel.putInt(it) }
        writeProperties(node.properties)
    }

    fun write(relationship: NewRelationship) {
        channel.put(RECORD)
        channel.putInt(relationship.type)
        channel.putLong(relationship.start)
        channel.putLong(relationship.end)
        writeProperties(relationship.properties)
    }

    private fun writeProperties(properties: List<Property>) {
        channel.putInt(properties.size)
        for (property in properties) {
            channel.putInt(property.key)
            val type = ValueType.typeOf(property.value)
            channel.put(type.id())
            type.write(property.value, channel)
        }
    }

    override fun close() {
        channel.put(END)
        channel.prepareForFlush()
        stream.close()
    }

    companion object {
        const val RECORD: Byte = 1
        const val END: Byte = 0

        /**
         * [value] as the import's value types keep it: Neo4j values, as the CSV reader gives
         * temporal and spatial values, as Java objects.
         */
        fun storable(value: Any): Any =
            when {
                value is Value -> value.asObjectCopy()
                value is Array<*> && value.isNotEmpty() && value[0] is Value -> {
                    val objects = value.map { (it as Value).asObjectCopy() }
                    val array =
                        java.lang.reflect.Array.newInstance(objects[0].javaClass, objects.size)
                    objects.forEachIndexed { i, o -> java.lang.reflect.Array.set(array, i, o) }
                    array
                }
                else -> value
            }
    }
}

/** Reads what an [EntityWriter] wrote, from the entity at [skip] on. */
internal class EntityReader(path: Path, skip: Long = 0, relationships: Boolean) : AutoCloseable {
    private val stream = BufferedInputStream(Files.newInputStream(path), 1 shl 16)
    private val channel = InputStreamReadableChannel(stream)

    init {
        for (i in 0 until skip) {
            checkNotNull(if (relationships) relationship() else node()) {
                "$path holds $i entities, fewer than $skip"
            }
        }
    }

    /** The next node, null at the end. */
    fun node(): NewNode? {
        if (!more()) return null
        val labels = IntArray(channel.int) { channel.int }
        return NewNode(labels, properties())
    }

    /** The next relationship, null at the end. */
    fun relationship(): NewRelationship? {
        if (!more()) return null
        val type = channel.int
        val start = channel.long
        val end = channel.long
        return NewRelationship(type, start, end, properties())
    }

    private fun more(): Boolean =
        when (val marker = channel.get()) {
            EntityWriter.RECORD -> true
            EntityWriter.END -> false
            else -> throw IOException("unexpected record marker $marker")
        }

    private fun properties(): List<Property> =
        List(channel.int) {
            val key = channel.int
            Property(key, ValueType.typeOf(channel.get()).read(channel))
        }

    override fun close() = stream.close()
}

/**
 * The keys of the nodes an increment's ID mapper holds, by their place in it, for Neo4j's ID mapper
 * to compare keys whose encodings collide.
 */
internal class KeyStore(
    private val path: Path,
    factory: NumberArrayFactory,
    memoryTracker: MemoryTracker,
) : PropertyValueLookup, AutoCloseable {
    private val offsets: LongArray = factory.newDynamicLongArray(1L shl 20, -1, memoryTracker)
    private val out = DataOutputStream(BufferedOutputStream(Files.newOutputStream(path), 1 shl 16))
    private var position = 0L
    private var channel: FileChannel? = null

    /** Keeps [key], a String or a Long, of the node at [place]. */
    fun add(place: Long, key: Any) {
        offsets.set(place, position)
        when (key) {
            is String -> {
                val bytes = key.toByteArray(Charsets.UTF_8)
                out.writeByte(KeyWriter.TEXT)
                out.writeInt(bytes.size)
                out.write(bytes)
                position += 1 + Int.SIZE_BYTES + bytes.size
            }
            is Long -> {
                out.writeByte(KeyWriter.INTEGER)
                out.writeLong(key)
                position += 1 + Long.SIZE_BYTES
            }
            else -> throw IllegalArgumentException("a key is text or an integer, not $key")
        }
    }

    /** Ends the adding: the keys can be looked up from now on. */
    fun complete() {
        out.close()
        channel = FileChannel.open(path, StandardOpenOption.READ)
    }

    override fun newLookup(): PropertyValueLookup.Lookup =
        object : PropertyValueLookup.Lookup {
            override fun lookupProperty(nodeId: Long, memoryTracker: MemoryTracker): Any? {
                val offset = offsets.get(nodeId)
                return if (offset < 0) null else read(offset)
            }

            override fun close() {}
        }

    private fun read(offset: Long): Any {
        val channel = checkNotNull(channel) { "the keys are still being added" }
        return when (val tag = read(channel, offset, 1).get().toInt()) {
            KeyWriter.INTEGER -> read(channel, offset + 1, Long.SIZE_BYTES).getLong()
            KeyWriter.TEXT -> {
                val length = read(channel, offset + 1, Int.SIZE_BYTES).getInt()
                String(read(channel, offset + 1 + Int.SIZE_BYTES, length).array(), Charsets.UTF_8)
            }
            else -> throw IOException("$path: unknown key tag $tag at $offset")
        }
    }

    /** The [length] bytes of [channel] at [from]. */
    private fun read(channel: FileChannel, from: Long, length: Int): ByteBuffer {
        val buffer = ByteBuffer.allocate(length)
        var position = from
        while (buffer.hasRemaining()) {
            val read = channel.read(buffer, position)
            if (read < 0) throw IOException("$path ends at $position")
            position += read
        }
        return buffer.flip()
    }

    override fun close() {
        if (channel == null) out.close() else channel?.close()
        offsets.close()
    }
}

private fun writeString(out: DataOutputStream, value: String) {
    val bytes = value.toByteArray(Charsets.UTF_8)
    out.writeInt(bytes.size)
    out.write(bytes)
}

private fun readString(input: DataInputStream): String {
    val bytes = ByteArray(input.readInt())
    input.readFully(bytes)
    return String(bytes, Charsets.UTF_8)
}
