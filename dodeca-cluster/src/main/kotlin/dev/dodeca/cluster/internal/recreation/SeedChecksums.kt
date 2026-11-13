package dev.dodeca.cluster.internal.recreation

import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.util.UUID

/**
 * `dodeca.seedChecksums`, a bulk request: the checksums of transactions in the seed a member keeps
 * of a recreated database, by append index, read from its logs. The leader of `system` asks the
 * seeder with the latest store, the creator to be, for those at the other seeders' last append
 * indexes: a seeder whose last transaction differs there does not hold a prefix of its store.
 *
 * A request is `byte version, UTF name, long, long` (the recreated id), `int count`, then `count`
 * longs. A response is `byte version, byte ok`, then for ok `int count` and per append index `long
 * index, boolean known, int checksum`, else a UTF message.
 */
internal object SeedChecksums {
    const val NAME = "dodeca.seedChecksums"
    const val VERSION: Byte = 1

    data class Request(val name: String, val recreated: UUID, val appendIndexes: List<Long>)

    fun encodeRequest(request: Request): ByteArray {
        val bytes = java.io.ByteArrayOutputStream()
        DataOutputStream(bytes).use {
            it.writeByte(VERSION.toInt())
            it.writeUTF(request.name)
            it.writeLong(request.recreated.mostSignificantBits)
            it.writeLong(request.recreated.leastSignificantBits)
            it.writeInt(request.appendIndexes.size)
            request.appendIndexes.forEach(it::writeLong)
        }
        return bytes.toByteArray()
    }

    fun decodeRequest(bytes: ByteArray): Request {
        val input = DataInputStream(bytes.inputStream())
        val version = input.readByte()
        if (version != VERSION) throw IOException("unknown seed checksums request version $version")
        val name = input.readUTF()
        val recreated = UUID(input.readLong(), input.readLong())
        val count = input.readInt()
        return Request(name, recreated, List(count) { input.readLong() })
    }

    /** Answers [request] on [out] with what [read] finds, or why it cannot. */
    fun serve(request: ByteArray, out: OutputStream, read: (Request) -> Map<Long, Int?>) {
        val data = DataOutputStream(out)
        data.writeByte(VERSION.toInt())
        val answer =
            try {
                Result.success(read(decodeRequest(request)))
            } catch (e: Exception) {
                Result.failure(e)
            }
        answer.fold(
            { checksums ->
                data.writeBoolean(true)
                data.writeInt(checksums.size)
                for ((index, checksum) in checksums) {
                    data.writeLong(index)
                    data.writeBoolean(checksum != null)
                    data.writeInt(checksum ?: 0)
                }
            },
            { e ->
                data.writeBoolean(false)
                data.writeUTF(e.message ?: e.toString())
            },
        )
        data.flush()
    }

    /** The checksums an answer holds, by append index; throws with the member's reason if none. */
    fun decodeResponse(input: InputStream): Map<Long, Int?> {
        val data = DataInputStream(input)
        val version = data.readByte()
        if (version != VERSION) throw IOException("unknown seed checksums answer version $version")
        if (!data.readBoolean()) throw IOException(data.readUTF())
        return List(data.readInt()) {
                val index = data.readLong()
                val known = data.readBoolean()
                val checksum = data.readInt()
                index to checksum.takeIf { known }
            }
            .toMap()
    }
}
