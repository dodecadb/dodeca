package dev.dodeca.consensus.ratis

import dev.dodeca.consensus.HostPort
import dev.dodeca.consensus.Member
import dev.dodeca.consensus.MemberId
import dev.dodeca.consensus.PeerInfo
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException
import java.util.UUID

/**
 * Binary encoding of probe messages. Every message starts with a format version byte so that
 * members running different plugin versions can recognise formats they do not understand.
 */
internal object ProbeCodec {
    /** 2: texts as UTF-8 after their length in 4 bytes, no longer limited to 64 KB each. */
    const val FORMAT_VERSION: Byte = 2

    class UnsupportedFormatException(version: Int) :
        IOException("unsupported probe format version $version, supported: $FORMAT_VERSION")

    /** A probe request names the prober, for the target's logs. */
    fun encodeRequest(sender: Member): ByteArray = write { writeMember(sender) }

    fun decodeRequest(bytes: ByteArray): Member = read(bytes) { readMember() }

    fun encodeResponse(info: PeerInfo): ByteArray = write {
        writeMember(info.member)
        writeInt(info.metadata.size)
        info.metadata.toSortedMap().forEach { (key, value) ->
            writeText(key)
            writeText(value)
        }
        writeInt(info.groups.size)
        info.groups.toSortedMap().forEach { (groupId, members) ->
            writeUuid(groupId)
            writeInt(members.size)
            members.forEach { writeMember(it) }
        }
    }

    fun decodeResponse(bytes: ByteArray): PeerInfo =
        read(bytes) {
            val member = readMember()
            val metadata = buildMap { repeat(readCount()) { put(readText(), readText()) } }
            val groups = buildMap {
                repeat(readCount()) {
                    val groupId = readUuid()
                    put(groupId, List(readCount()) { readMember() })
                }
            }
            PeerInfo(member, metadata, groups)
        }

    /** An announcement carries what the sender answers when probed. */
    fun encodeAnnouncement(info: PeerInfo): ByteArray = encodeResponse(info)

    fun decodeAnnouncement(bytes: ByteArray): PeerInfo = decodeResponse(bytes)

    private fun write(body: DataOutputStream.() -> Unit): ByteArray {
        val buffer = ByteArrayOutputStream()
        DataOutputStream(buffer).use {
            it.writeByte(FORMAT_VERSION.toInt())
            it.body()
        }
        return buffer.toByteArray()
    }

    private fun <T> read(bytes: ByteArray, body: DataInputStream.() -> T): T =
        DataInputStream(ByteArrayInputStream(bytes)).use {
            val version = it.readByte().toInt()
            if (version != FORMAT_VERSION.toInt()) throw UnsupportedFormatException(version)
            val result = it.body()
            if (it.available() != 0) throw IOException("${it.available()} trailing bytes in probe")
            result
        }

    private fun DataOutputStream.writeUuid(uuid: UUID) {
        writeLong(uuid.mostSignificantBits)
        writeLong(uuid.leastSignificantBits)
    }

    private fun DataInputStream.readUuid(): UUID = UUID(readLong(), readLong())

    private fun DataOutputStream.writeMember(member: Member) {
        writeUuid(member.id.uuid)
        writeText(member.raftAddress.toString())
    }

    private fun DataInputStream.readMember(): Member =
        Member(MemberId(readUuid()), HostPort.parse(readText()))

    /**
     * Writes [text] as its UTF-8 bytes after their count: what members report, such as the states
     * of their databases, grows with the number of databases past the 64 KB of `writeUTF`.
     */
    private fun DataOutputStream.writeText(text: String) {
        val bytes = text.encodeToByteArray()
        writeInt(bytes.size)
        write(bytes)
    }

    private fun DataInputStream.readText(): String {
        val size = readInt()
        if (size < 0 || size > available()) throw IOException("invalid text length $size")
        return readNBytes(size).decodeToString(throwOnInvalidSequence = true)
    }

    private fun DataInputStream.readCount(): Int {
        val count = readInt()
        if (count < 0 || count > MAX_COUNT) throw IOException("invalid element count $count")
        return count
    }

    private const val MAX_COUNT = 1 shl 16
}
