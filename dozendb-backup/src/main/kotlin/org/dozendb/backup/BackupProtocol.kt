package org.dozendb.backup

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.EOFException
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.time.Instant
import org.dozendb.core.security.MetadataScript
import org.dozendb.core.store.StoreArchive

/**
 * What a server that serves backups and `neo4j-admin database backup` exchange, as one bulk
 * transfer under [NAME] per request.
 *
 * A request is `byte version, byte kind` and the kind's fields, a response `byte version, byte
 * status`, then a UTF message unless the status is [Status.OK], else the kind's body:
 * - [Request.List]: `int count`, and as many UTF database names.
 * - [Request.Full]: a [StoreArchive] whose metadata is [FullMetadata] and whose trailer is [Last]:
 *   a copy of the database that strict recovery completes up to that transaction.
 * - [Request.Differential]: an uncompressed differential backup of the transactions after the given
 *   one.
 * - [Request.Metadata]: `boolean present`, and if so `int length` and as many bytes of UTF-8: the
 *   [MetadataScript] of the database.
 */
internal object BackupProtocol {
    const val NAME = "dozendb.backup"
    private const val VERSION = 1

    /** What a server answers to a request. */
    enum class Status {
        OK,
        /** The server has no database of that name. */
        NO_DATABASE,
        /** The database is not available, such as when stopped. */
        UNAVAILABLE,
        /** The database's logs no longer hold the transactions after the one given. */
        PRUNED,
        /** The database does not continue the history of the transaction given. */
        DIVERGED,
        /** The server failed to serve the request. */
        FAILED,
    }

    sealed interface Request {
        /** The names of the databases that match [pattern], with `*` and `?` as Neo4j's globs. */
        data class List(val pattern: String) : Request

        /** A copy of [database]. */
        data class Full(val database: String) : Request

        /**
         * The transactions of [database] after the one at append index [after], whose checksum is
         * [checksum].
         */
        data class Differential(val database: String, val after: Long, val checksum: Int) : Request

        /** What [include] asks for of the users, roles and privileges of [database]. */
        data class Metadata(val database: String, val include: MetadataScript.Include) : Request
    }

    /** A full copy's metadata: when the server took it, and which server. */
    class FullMetadata(val time: Instant, val source: String)

    /** The last transaction of a full copy. */
    data class Last(
        val transactionId: Long,
        val appendIndex: Long,
        val checksum: Int,
        val commitTime: Instant,
    )

    fun encode(request: Request): ByteArray = bytes { out ->
        out.writeByte(VERSION)
        when (request) {
            is Request.List -> {
                out.writeByte(KIND_LIST)
                out.writeUTF(request.pattern)
            }
            is Request.Full -> {
                out.writeByte(KIND_FULL)
                out.writeUTF(request.database)
            }
            is Request.Differential -> {
                out.writeByte(KIND_DIFFERENTIAL)
                out.writeUTF(request.database)
                out.writeLong(request.after)
                out.writeInt(request.checksum)
            }
            is Request.Metadata -> {
                out.writeByte(KIND_METADATA)
                out.writeUTF(request.database)
                out.writeUTF(request.include.name)
            }
        }
    }

    fun decode(bytes: ByteArray): Request =
        read(bytes) { input ->
            val version = input.readUnsignedByte()
            if (version != VERSION) {
                throw IOException("unsupported backup protocol version $version, not $VERSION")
            }
            when (val kind = input.readUnsignedByte()) {
                KIND_LIST -> Request.List(input.readUTF())
                KIND_FULL -> Request.Full(input.readUTF())
                KIND_DIFFERENTIAL ->
                    Request.Differential(input.readUTF(), input.readLong(), input.readInt())
                KIND_METADATA ->
                    Request.Metadata(
                        input.readUTF(),
                        input.readUTF().let { include ->
                            MetadataScript.Include.entries.find { it.name == include }
                                ?: throw IOException("unknown metadata $include")
                        },
                    )
                else -> throw IOException("unknown backup request $kind")
            }
        }

    /** Writes the start of a response with [status] to [out], and for a refusal [message]. */
    fun writeStatus(out: OutputStream, status: Status, message: String = "") {
        val data = DataOutputStream(out)
        data.writeByte(VERSION)
        data.writeByte(status.ordinal)
        if (status != Status.OK) data.writeUTF(message)
        data.flush()
    }

    /**
     * A refusal, by its status and message. Not an [IOException], which the transport takes for its
     * own failures.
     */
    class Refused(val status: Status, message: String) : Exception(message)

    /**
     * Reads the start of a response from [input].
     *
     * @throws Refused if its status is not [Status.OK].
     */
    fun readStatus(input: InputStream) {
        val data = DataInputStream(input)
        try {
            val version = data.readUnsignedByte()
            if (version != VERSION) {
                throw IOException("unsupported backup protocol version $version, not $VERSION")
            }
            val status =
                Status.entries.getOrNull(data.readUnsignedByte())
                    ?: throw IOException("unknown backup response status")
            if (status != Status.OK) throw Refused(status, data.readUTF())
        } catch (e: EOFException) {
            throw IOException("the server ended its response early", e)
        }
    }

    fun writeNames(out: OutputStream, names: kotlin.collections.List<String>) {
        val data = DataOutputStream(out)
        data.writeInt(names.size)
        names.forEach(data::writeUTF)
        data.flush()
    }

    fun readNames(input: InputStream): kotlin.collections.List<String> {
        val data = DataInputStream(input)
        return List(data.readInt()) { data.readUTF() }
    }

    fun writeScript(out: OutputStream, script: String?) {
        val data = DataOutputStream(out)
        data.writeBoolean(script != null)
        if (script != null) {
            val bytes = script.toByteArray(Charsets.UTF_8)
            data.writeInt(bytes.size)
            data.write(bytes)
        }
        data.flush()
    }

    fun readScript(input: InputStream): String? {
        val data = DataInputStream(input)
        if (!data.readBoolean()) return null
        val length = data.readInt()
        if (length < 0) throw IOException("invalid metadata length $length")
        return String(
            data.readNBytes(length).also {
                if (it.size != length) throw IOException("the server ended its metadata early")
            },
            Charsets.UTF_8,
        )
    }

    fun encode(metadata: FullMetadata): ByteArray = bytes {
        it.writeLong(metadata.time.toEpochMilli())
        it.writeUTF(metadata.source)
    }

    fun decodeFullMetadata(bytes: ByteArray): FullMetadata =
        read(bytes) { FullMetadata(Instant.ofEpochMilli(it.readLong()), it.readUTF()) }

    fun encode(last: Last): ByteArray = bytes {
        it.writeLong(last.transactionId)
        it.writeLong(last.appendIndex)
        it.writeInt(last.checksum)
        it.writeLong(last.commitTime.toEpochMilli())
    }

    fun decodeLast(bytes: ByteArray): Last =
        read(bytes) {
            Last(it.readLong(), it.readLong(), it.readInt(), Instant.ofEpochMilli(it.readLong()))
        }

    private const val KIND_LIST = 1
    private const val KIND_FULL = 2
    private const val KIND_DIFFERENTIAL = 3
    private const val KIND_METADATA = 4

    private fun bytes(write: (DataOutputStream) -> Unit): ByteArray {
        val buffer = ByteArrayOutputStream()
        DataOutputStream(buffer).use(write)
        return buffer.toByteArray()
    }

    private fun <T> read(bytes: ByteArray, read: (DataInputStream) -> T): T =
        DataInputStream(ByteArrayInputStream(bytes)).use { input ->
            val value =
                try {
                    read(input)
                } catch (e: EOFException) {
                    throw IOException("truncated backup message", e)
                }
            if (input.available() > 0) throw IOException("unexpected bytes in backup message")
            value
        }
}
