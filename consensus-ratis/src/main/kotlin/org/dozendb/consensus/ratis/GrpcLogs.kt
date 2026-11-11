package org.dozendb.consensus.ratis

import java.util.logging.Filter
import java.util.logging.Logger

/**
 * gRPC's log lines that members leave out. gRPC logs to java.util.logging, which Neo4j sends to its
 * user log. A channel warns each time it fails to resolve its host name: the channels to a member
 * that is down do so every second or two where its name goes with it, as a stopped container's or
 * Kubernetes pod's does. DozenDB's member directory says once that a member does not answer, and
 * why, instead.
 */
internal object GrpcLogs {
    /** The logger of gRPC's channels, named after their class, which is not public. */
    const val CHANNEL_LOGGER = "org.apache.ratis.thirdparty.io.grpc.internal.ManagedChannelImpl"

    /** What a channel says when it failed to resolve its host name. */
    const val UNRESOLVED = "Failed to resolve name"

    // Held, as java.util.logging keeps loggers weakly, and with them their filters.
    private val channels: Logger =
        Logger.getLogger(CHANNEL_LOGGER).apply {
            filter = Filter { record -> record.message?.contains(UNRESOLVED) != true }
        }

    /** Leaves those lines out from now on, in this JVM. */
    fun install() {
        checkNotNull(channels.filter)
    }
}
