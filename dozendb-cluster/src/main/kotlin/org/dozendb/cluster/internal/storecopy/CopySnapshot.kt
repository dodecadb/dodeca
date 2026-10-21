package org.dozendb.cluster.internal.storecopy

import java.io.IOException
import java.nio.ByteBuffer
import org.dozendb.consensus.LogPosition
import org.dozendb.consensus.Snapshot

/**
 * The Raft snapshot a member's copy of a database holds, kept in the copy's metadata: nothing for a
 * database that is not replicated yet, else `long index, long term, long appliedIndex`.
 */
internal object CopySnapshot {
    private const val SIZE = 3 * Long.SIZE_BYTES

    fun encode(snapshot: Snapshot?): ByteArray {
        if (snapshot == null) return ByteArray(0)
        return ByteBuffer.allocate(SIZE)
            .putLong(snapshot.position.index)
            .putLong(snapshot.position.term)
            .putLong(snapshot.appliedIndex)
            .array()
    }

    fun decode(metadata: ByteArray): Snapshot? {
        if (metadata.isEmpty()) return null
        if (metadata.size != SIZE) throw IOException("invalid snapshot of ${metadata.size} bytes")
        val buffer = ByteBuffer.wrap(metadata)
        val position = LogPosition(index = buffer.long, term = buffer.long)
        val appliedIndex = buffer.long
        if (appliedIndex > position.index) {
            throw IOException("invalid snapshot $position with applied index $appliedIndex")
        }
        return Snapshot(position, appliedIndex)
    }
}
