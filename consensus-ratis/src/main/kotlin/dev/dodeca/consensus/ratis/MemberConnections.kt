package dev.dodeca.consensus.ratis

import dev.dodeca.consensus.HostPort
import dev.dodeca.consensus.MemberConnection
import java.net.InetSocketAddress
import java.net.SocketAddress
import java.util.Collections
import java.util.IdentityHashMap
import java.util.concurrent.ConcurrentHashMap
import org.apache.ratis.thirdparty.io.grpc.Attributes
import org.apache.ratis.thirdparty.io.grpc.ForwardingServerCallListener
import org.apache.ratis.thirdparty.io.grpc.Grpc
import org.apache.ratis.thirdparty.io.grpc.Metadata
import org.apache.ratis.thirdparty.io.grpc.ServerCall
import org.apache.ratis.thirdparty.io.grpc.ServerCallHandler
import org.apache.ratis.thirdparty.io.grpc.ServerInterceptor
import org.apache.ratis.thirdparty.io.grpc.ServerTransportFilter

/**
 * The connections between this member and others that are open now, for `dbms.cluster.protocols`.
 * Inbound: those opened to this member's consensus port, seen by [filter], with the protocols that
 * [interceptor] saw called on each so far, after the calls that are refused, as those of servers
 * that are no members, were refused. Outbound: those this member opens for a call of its own, a
 * probe or a bulk transfer, while the call lasts ([open]). Ratis's own connections to other members
 * are not seen here.
 *
 * Protocols are named [RAFT], [PROBE], [BULK] and, for a bulk transfer, its name.
 */
internal class MemberConnections(private val tls: Boolean) {
    private class Inbound(val remote: SocketAddress, val tls: Boolean) {
        val protocols: MutableSet<String> = ConcurrentHashMap.newKeySet()
    }

    private val inbound = ConcurrentHashMap<SocketAddress, Inbound>()

    private val outbound: MutableSet<MemberConnection> =
        Collections.synchronizedSet(Collections.newSetFromMap(IdentityHashMap()))

    /** Tracks the connections opened to the consensus port. */
    val filter: ServerTransportFilter =
        object : ServerTransportFilter() {
            override fun transportReady(attributes: Attributes): Attributes {
                attributes.get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR)?.let { remote ->
                    inbound[remote] =
                        Inbound(remote, attributes.get(Grpc.TRANSPORT_ATTR_SSL_SESSION) != null)
                }
                return attributes
            }

            // Null for a connection that ended before it was ready, as when its TLS handshake
            // failed. A filter that throws here keeps the server from ever terminating.
            override fun transportTerminated(attributes: Attributes?) {
                attributes?.get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR)?.let { inbound.remove(it) }
            }
        }

    /** Records the protocols called on each connection opened to the consensus port. */
    val interceptor: ServerInterceptor =
        object : ServerInterceptor {
            override fun <Q, A> interceptCall(
                call: ServerCall<Q, A>,
                headers: Metadata,
                next: ServerCallHandler<Q, A>,
            ): ServerCall.Listener<Q> {
                val listener = next.startCall(call, headers)
                val connection =
                    call.attributes.get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR)?.let { inbound[it] }
                        ?: return listener
                val service = call.methodDescriptor.serviceName.orEmpty()
                when {
                    service == BulkService.SERVICE_NAME -> {
                        connection.protocols += BULK
                        return object :
                            ForwardingServerCallListener.SimpleForwardingServerCallListener<Q>(
                                listener
                            ) {
                            override fun onMessage(message: Q) {
                                (message as? ByteArray)?.let(BulkService::transferName)?.let {
                                    connection.protocols += it
                                }
                                super.onMessage(message)
                            }
                        }
                    }
                    service == ProbeService.SERVICE_NAME -> connection.protocols += PROBE
                    service.startsWith(RATIS_SERVICES) -> connection.protocols += RAFT
                }
                return listener
            }
        }

    /** Records a connection to [remote] carrying [protocols] until the result is closed. */
    fun open(remote: HostPort, protocols: Set<String>): AutoCloseable {
        val connection = MemberConnection(false, remote, protocols, tls)
        outbound += connection
        return AutoCloseable { outbound -= connection }
    }

    /** The connections open now that carried a protocol, inbound first. */
    fun connections(): List<MemberConnection> =
        inbound.values
            .filter { it.protocols.isNotEmpty() }
            .map { MemberConnection(true, it.remote.hostPort(), it.protocols.toSet(), it.tls) } +
            synchronized(outbound) { outbound.toList() }

    private fun SocketAddress.hostPort(): HostPort =
        if (this is InetSocketAddress) HostPort(hostString, port) else HostPort(toString(), 0)

    companion object {
        /** Ratis's own calls: replication, elections and configuration changes. */
        const val RAFT = "raft"
        const val PROBE = "probe"
        const val BULK = "bulk"

        /** The prefix of the names of Ratis's gRPC services. */
        private const val RATIS_SERVICES = "ratis."
    }
}
