package dev.dodeca.cluster.internal.routing

import dev.dodeca.cluster.MemberMetadata
import dev.dodeca.consensus.Member
import dev.dodeca.consensus.MemberId
import java.util.concurrent.ConcurrentHashMap
import org.neo4j.logging.Log

/**
 * Warns in [log] when another server reports other routing policies than this server's [digest]
 * ([RoutingPolicies.digest]): Neo4j Enterprise asks for the same policies on every server and says
 * only that otherwise the result is unpredictable, as a driver's routing table then depends on the
 * server it asks. Once per server and change of what it reports; when the server agrees again, it
 * says so. A server that reports no digest, of a version before routing policies, is left alone.
 */
internal class PolicyAgreement(private val digest: String, private val log: Log) {
    /** The digests of the servers that disagree, as they reported them last. */
    private val disagreeing = ConcurrentHashMap<MemberId, String>()

    /** Takes what [member] reported, its [report]. */
    fun reported(member: Member, report: Map<String, String>) {
        val theirs = report[MemberMetadata.ROUTING_POLICIES] ?: return
        if (theirs == digest) {
            if (disagreeing.remove(member.id) != null) {
                log.info(
                    "Server %s at %s has the same routing policies as this server again",
                    member.id,
                    member.raftAddress,
                )
            }
        } else if (disagreeing.put(member.id, theirs) != theirs) {
            log.warn(
                "Server %s at %s has other routing policies than this server, so that the " +
                    "routing tables of drivers that name a policy depend on the server they ask: " +
                    "set the same dbms.routing.load_balancing.config.server_policies.* on every " +
                    "server",
                member.id,
                member.raftAddress,
            )
        }
    }
}
