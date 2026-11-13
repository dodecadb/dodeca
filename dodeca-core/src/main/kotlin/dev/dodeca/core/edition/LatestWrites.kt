package dev.dodeca.core.edition

import java.time.Duration
import org.neo4j.kernel.database.NamedDatabaseId

/**
 * Waits until this server holds the writes acknowledged before, as a backup taken from it must.
 * Dodeca's editions register one as a global dependency.
 */
fun interface LatestWrites {
    /**
     * Waits up to [timeout] until this server holds every write to [database] that any server
     * acknowledged before the call.
     *
     * @throws java.util.concurrent.TimeoutException if it does not within [timeout].
     */
    fun await(database: NamedDatabaseId, timeout: Duration)

    companion object {
        /** Without clustering: this server acknowledges a write once it holds it. */
        val LOCAL = LatestWrites { _, _ -> }
    }
}
