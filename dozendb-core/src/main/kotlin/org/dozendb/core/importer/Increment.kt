package org.dozendb.core.importer

import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.EOFException
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.util.Properties
import org.neo4j.io.layout.DatabaseLayout

/**
 * The increment of an incremental import into the database of [layout], in a directory next to the
 * database's, `.<database>.increment`: what `prepare` found in the store, what `build` made of the
 * input, and how far `merge` got. Dot-prefixed, so that DozenDB does not take it for a database.
 */
internal class Increment(val layout: DatabaseLayout) {
    val directory: Path =
        layout.neo4jLayout.databasesDirectory().resolve(".${layout.databaseName}.increment")

    /** The home of the DBMS that opens the database, while it does. */
    val home: Path = directory.resolve("home")

    /** The logs of that DBMS. */
    val logs: Path = directory.resolve("logs")

    private val stateFile = directory.resolve("increment.properties")

    /** The keys of the existing nodes of the ID space [group], as `prepare` found them. */
    fun keys(group: Int): Path = directory.resolve("keys-$group")

    /** The keys the build's ID mapper holds, by their place in it. */
    val ids: Path = directory.resolve("ids")

    /** The new nodes, in order. */
    val nodes: Path = directory.resolve("nodes")

    /** The places of the new nodes that are duplicates, in order. */
    val duplicates: Path = directory.resolve("duplicates")

    /** The new relationships, in order. */
    val relationships: Path = directory.resolve("relationships")

    /** The names of the tokens of the new nodes and relationships. */
    val tokens: Path = directory.resolve("tokens")

    /** The state recorded so far, null if there is no increment. */
    fun state(): IncrementState? {
        if (!Files.exists(stateFile)) return null
        val properties = Properties()
        Files.newInputStream(stateFile).use(properties::load)
        return IncrementState.from(properties, stateFile)
    }

    /** Records [state], replacing the earlier one in one step. */
    fun record(state: IncrementState) {
        val temporary = directory.resolve("increment.properties.tmp")
        Files.newOutputStream(temporary).use { out ->
            state.toProperties().store(out, "DozenDB incremental import of ${layout.databaseName}")
            out.flush()
        }
        Files.move(
            temporary,
            stateFile,
            StandardCopyOption.REPLACE_EXISTING,
            StandardCopyOption.ATOMIC_MOVE,
        )
    }

    /** Starts a new increment, deleting any earlier one. */
    fun reset() {
        delete()
        Files.createDirectories(directory)
    }

    /** Deletes the increment, without following the links of a DBMS home left by a crash. */
    fun delete() {
        ScratchDbms.deleteHome(directory)
    }
}

/** What an increment's stages recorded. */
internal data class IncrementState(
    val database: String,
    val stage: Stage,
    /** The store's last transaction when `prepare` read it. */
    val lastTransaction: Long,
    val lastChecksum: Int,
    val consensusIndex: Long,
    val kernelVersion: Byte,
    /** How node IDs in the input are typed. */
    val keyType: KeyType,
    /** The ID spaces of the input's nodes, in the order of their key files. */
    val groups: List<KeyedGroup>,
    /** How many new nodes the build read, duplicates included. */
    val nodes: Long = 0,
    /** How many of those are duplicates. */
    val skippedNodes: Long = 0,
    /** How many new relationships the build read. */
    val relationships: Long = 0,
) {
    enum class Stage {
        /** The store's keys are read. */
        PREPARED,
        /** The input is read into the increment. */
        BUILT,
        /** The increment is being written into the database. */
        MERGING,
    }

    fun toProperties(): Properties =
        Properties().apply {
            setProperty("format", FORMAT.toString())
            setProperty("database", database)
            setProperty("stage", stage.name)
            setProperty("store.lastTransaction", lastTransaction.toString())
            setProperty("store.lastChecksum", lastChecksum.toString())
            setProperty("store.consensusIndex", consensusIndex.toString())
            setProperty("store.kernelVersion", kernelVersion.toString())
            setProperty("keyType", keyType.name)
            setProperty("groups", groups.size.toString())
            groups.forEachIndexed { i, group ->
                group.name?.let { setProperty("group.$i.name", it) }
                setProperty("group.$i.label", group.label)
                setProperty("group.$i.key", group.key)
                setProperty("group.$i.keyType", group.keyType.name)
                setProperty("group.$i.existing", group.existing.toString())
            }
            setProperty("build.nodes", nodes.toString())
            setProperty("build.skippedNodes", skippedNodes.toString())
            setProperty("build.relationships", relationships.toString())
        }

    companion object {
        private const val FORMAT = 1

        fun from(properties: Properties, file: Path): IncrementState {
            fun value(key: String): String =
                properties.getProperty(key) ?: throw IOException("$file has no $key")
            val format = value("format").toInt()
            if (format != FORMAT) {
                throw IOException(
                    "$file is of format $format, which this version of DozenDB does not read"
                )
            }
            return IncrementState(
                database = value("database"),
                stage = Stage.valueOf(value("stage")),
                lastTransaction = value("store.lastTransaction").toLong(),
                lastChecksum = value("store.lastChecksum").toInt(),
                consensusIndex = value("store.consensusIndex").toLong(),
                kernelVersion = value("store.kernelVersion").toByte(),
                keyType = KeyType.valueOf(value("keyType")),
                groups =
                    (0 until value("groups").toInt()).map { i ->
                        KeyedGroup(
                            name = properties.getProperty("group.$i.name"),
                            label = value("group.$i.label"),
                            key = value("group.$i.key"),
                            keyType = KeyType.valueOf(value("group.$i.keyType")),
                            existing = value("group.$i.existing").toLong(),
                        )
                    },
                nodes = value("build.nodes").toLong(),
                skippedNodes = value("build.skippedNodes").toLong(),
                relationships = value("build.relationships").toLong(),
            )
        }
    }
}

/**
 * An ID space of the input whose nodes are identified by [key] of their [label], [existing] of them
 * in the store; [name] is null for the global ID space.
 */
internal data class KeyedGroup(
    val name: String?,
    val label: String,
    val key: String,
    val keyType: KeyType,
    val existing: Long,
)

/**
 * How node keys compare, as a uniqueness constraint compares the values the import writes: as text,
 * or as integers, whatever their size in the input or the store.
 */
internal enum class KeyType(val idType: String) {
    TEXT("string"),
    INTEGER("integer"),
}

/** Writes the keys of an ID space with the IDs of their nodes. */
internal class KeyWriter(path: Path) : AutoCloseable {
    private val out = DataOutputStream(BufferedOutputStream(Files.newOutputStream(path), 1 shl 16))

    fun write(key: Any, nodeId: Long) {
        when (key) {
            is String -> {
                val bytes = key.toByteArray(Charsets.UTF_8)
                out.writeByte(TEXT)
                out.writeInt(bytes.size)
                out.write(bytes)
            }
            is Long -> {
                out.writeByte(INTEGER)
                out.writeLong(key)
            }
            else -> throw IllegalArgumentException("a key is text or an integer, not $key")
        }
        out.writeLong(nodeId)
    }

    override fun close() = out.close()

    companion object {
        const val TEXT = 1
        const val INTEGER = 2
    }
}

/** Reads what a [KeyWriter] wrote, calling [visit] with each key and node ID. */
internal fun readKeys(path: Path, visit: (key: Any, nodeId: Long) -> Unit) {
    DataInputStream(BufferedInputStream(Files.newInputStream(path), 1 shl 16)).use { input ->
        while (true) {
            val tag =
                try {
                    input.readByte().toInt()
                } catch (_: EOFException) {
                    return
                }
            val key: Any =
                when (tag) {
                    KeyWriter.TEXT -> {
                        val bytes = ByteArray(input.readInt())
                        input.readFully(bytes)
                        String(bytes, Charsets.UTF_8)
                    }
                    KeyWriter.INTEGER -> input.readLong()
                    else -> throw IOException("$path: unknown key tag $tag")
                }
            visit(key, input.readLong())
        }
    }
}
