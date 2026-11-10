package org.dozendb.cluster.internal

import java.util.UUID
import org.dozendb.cluster.ClusterService
import org.dozendb.cluster.MemberView
import org.dozendb.cluster.internal.forwarding.RemoteTransactions
import org.dozendb.consensus.HostPort
import org.dozendb.core.servers.ServerRecord
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.internal.kernel.api.security.AuthSubject

/**
 * The checks of `dbms.cluster.checkConnectivity`: whether this member reaches each other member at
 * the ports Neo4j Enterprise names. DozenDB carries Enterprise's cluster traffic, database copies
 * and pulls, and Raft on one port, its Raft address, which counts as CLUSTER and RAFT; INTRA_BOLT
 * is the routing connector, of members that take writes this member forwards.
 */
class ConnectivityChecks
internal constructor(
    private val cluster: ClusterService,
    private val records: () -> Map<UUID, ServerRecord>,
    private val remote: () -> RemoteTransactions?,
) {
    /** A port of another member, as Enterprise names it. */
    enum class Port {
        CLUSTER,
        INTRA_BOLT,
        RAFT,
    }

    /** A checked port of a member, and whether it answered: [result] is OK or why not. */
    data class Check(
        val member: MemberView,
        val record: ServerRecord?,
        val port: Port,
        val address: HostPort,
        val result: String,
    )

    /**
     * Checks the ports of the other members, only [port] if given, of only the member [server] if
     * given, logging in to routing connectors as the user of [subject]. All at once, as a member
     * that does not answer keeps its checks waiting until they time out.
     */
    fun check(port: Port?, server: UUID?, subject: AuthSubject): List<Check> {
        val records = records()
        val sending = remote()
        val others =
            cluster.members().filter {
                !it.isSelf && (server == null || it.member.id.uuid == server)
            }
        val ports = Port.entries.filter { port == null || it == port }
        val routing =
            if (sending == null || Port.INTRA_BOLT !in ports) emptyMap()
            else
                others
                    .mapNotNull { view ->
                        view.routingAddress?.let { address ->
                            view.member.id to
                                (address to
                                    sending.connectionFailure(
                                        SocketAddress(address.host, address.port),
                                        subject,
                                    ))
                        }
                    }
                    .toMap()
        // One probe for both names of the Raft port.
        val raft =
            if (ports.none { it != Port.INTRA_BOLT }) emptyMap()
            else cluster.probeFailures(others.map { it.member })
        return others.flatMap { view ->
            val record = records[view.member.id.uuid]
            ports.mapNotNull { checked ->
                when (checked) {
                    Port.CLUSTER,
                    Port.RAFT ->
                        Check(
                            view,
                            record,
                            checked,
                            view.member.raftAddress,
                            raft[view.member.id] ?: OK,
                        )
                    Port.INTRA_BOLT ->
                        routing[view.member.id]?.let { (address, failure) ->
                            Check(
                                view,
                                record,
                                checked,
                                address,
                                failure.toCompletableFuture().join() ?: OK,
                            )
                        }
                }
            }
        }
    }

    companion object {
        const val OK = "OK"
    }
}
