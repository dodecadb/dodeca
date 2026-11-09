package org.dozendb.cluster.internal.replication

import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.util.UUID
import org.dozendb.consensus.BulkSource
import org.dozendb.consensus.ConsensusLog
import org.dozendb.consensus.DatabaseRef

/** What the leader of a database's group tells of the term of one of its log entries. */
internal sealed interface EntryTerm {
    data class Known(val term: Long) : EntryTerm

    /** Its log no longer holds the entry. */
    data object Pruned : EntryTerm

    /** No leader told, as while there is none: to be asked again. */
    data object Unknown : EntryTerm
}

/**
 * What a secondary that turns primary asks the leader of its database's group, as one bulk transfer
 * under [NAME]: the term of the log entry that its store's last transaction came from, so that its
 * own store seeds the group it joins (see [ConsensusLog.seed]). Neo4j's transaction log keeps the
 * entry's index, not its term.
 *
 * A request is `byte version, UTF name, long, long` (the database's id), `long index`. A response
 * is `byte version, byte status`, then `long term` if the status is 0; 1 if the member's log no
 * longer holds the entry.
 */
internal object EntryTermProtocol {
    const val NAME = "dozendb.entryTerm"
    private const val VERSION = 1
    private const val KNOWN = 0
    private const val PRUNED = 1

    fun encode(database: DatabaseRef, index: Long): ByteArray =
        ByteArrayOutputStream().use { bytes ->
            DataOutputStream(bytes).use {
                it.writeByte(VERSION)
                it.writeUTF(database.name)
                it.writeLong(database.id.mostSignificantBits)
                it.writeLong(database.id.leastSignificantBits)
                it.writeLong(index)
            }
            bytes.toByteArray()
        }

    fun decode(bytes: ByteArray): Pair<DatabaseRef, Long> =
        DataInputStream(bytes.inputStream()).use {
            checkVersion(it.readUnsignedByte())
            DatabaseRef(it.readUTF(), UUID(it.readLong(), it.readLong())) to it.readLong()
        }

    /** Answers with [term], or that the log no longer holds the entry if null. */
    fun write(out: OutputStream, term: Long?) {
        val data = DataOutputStream(out)
        data.writeByte(VERSION)
        if (term == null) {
            data.writeByte(PRUNED)
        } else {
            data.writeByte(KNOWN)
            data.writeLong(term)
        }
        data.flush()
    }

    fun read(input: InputStream): EntryTerm {
        val data = DataInputStream(input)
        checkVersion(data.readUnsignedByte())
        return when (val status = data.readUnsignedByte()) {
            KNOWN -> EntryTerm.Known(data.readLong())
            PRUNED -> EntryTerm.Pruned
            else -> throw IOException("unknown entry term status $status")
        }
    }

    private fun checkVersion(version: Int) {
        if (version != VERSION) {
            throw IOException("unsupported entry term protocol version $version, not $VERSION")
        }
    }
}

/** Answers [EntryTermProtocol] requests from this member's logs. */
internal class EntryTermSource(private val consensus: ConsensusLog) : BulkSource {
    override fun serve(request: ByteArray, out: OutputStream) {
        val (database, index) = EntryTermProtocol.decode(request)
        EntryTermProtocol.write(out, consensus.termOf(database, index))
    }
}
