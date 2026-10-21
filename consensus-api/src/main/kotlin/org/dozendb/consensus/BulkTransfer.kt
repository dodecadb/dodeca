package org.dozendb.consensus

import java.io.IOException
import java.io.InputStream
import java.io.OutputStream

/**
 * Serves one kind of bulk data, such as a store copy, to other members.
 *
 * [serve] runs on a dedicated thread and may block. Writes to `out` block while the receiver is not
 * ready for more data, and fail with an [IOException] once the receiver has gone away. An
 * [IllegalStateException] refuses the request: the receiver's failure gives its message.
 */
public fun interface BulkSource {
    public fun serve(request: ByteArray, out: OutputStream)
}

/**
 * Streams bulk data between members over the consensus transport, so that it shares its address,
 * port and, later, its encryption.
 */
public interface BulkTransfer {
    /** Serves [source] under [name]. Registering a name twice replaces the earlier source. */
    public fun registerBulkSource(name: String, source: BulkSource)

    /**
     * Asks the member at [address] for [name] with [request] and passes the response to [consume]
     * on the calling thread. Blocks; bulk data is consumed by code that must not run in coroutines.
     *
     * @throws ConsensusUnavailableException if the member cannot be reached, does not serve [name],
     *   or fails while streaming.
     */
    public fun <T> fetchBulk(
        address: HostPort,
        name: String,
        request: ByteArray,
        consume: (InputStream) -> T,
    ): T
}
