package org.dozendb.cluster.ratis

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException
import java.util.UUID
import org.dozendb.cluster.api.HostPort
import org.dozendb.cluster.api.Member
import org.dozendb.cluster.api.MemberId
import org.dozendb.cluster.api.PeerInfo

/**
 * Binary encoding of probe messages. Every message starts with a format version byte so that
 * members running different plugin versions can recognise formats they do not understand.
 */
internal object ProbeCodec {
    const val FORMAT_VERSION: Byte = 1

    class UnsupportedFormatException(version: Int) :
        IOException("unsupported probe format version $version, supported: $FORMAT_VERSION")

    /** A probe request names the prober, for the target's logs. */
    fun encodeRequest(sender: Member): ByteArray = write { writeMember(sender) }

    fun decodeRequest(bytes: ByteArray): Member = read(bytes) { readMember() }

    fun encodeResponse(info: PeerInfo): ByteArray = write {
        writeMember(info.member)
        writeInt(info.metadata.size)
        info.metadata.toSortedMap().forEach { (key, value) ->
            writeUTF(key)
            writeUTF(value)
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
            val metadata = buildMap { repeat(readCount()) { put(readUTF(), readUTF()) } }
            val groups = buildMap {
                repeat(readCount()) {
                    val groupId = readUuid()
                    put(groupId, List(readCount()) { readMember() })
                }
            }
            PeerInfo(member, metadata, groups)
        }

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
        writeUTF(member.raftAddress.toString())
    }

    private fun DataInputStream.readMember(): Member =
        Member(MemberId(readUuid()), HostPort.parse(readUTF()))

    private fun DataInputStream.readCount(): Int {
        val count = readInt()
        if (count < 0 || count > MAX_COUNT) throw IOException("invalid element count $count")
        return count
    }

    private const val MAX_COUNT = 1 shl 16
}
