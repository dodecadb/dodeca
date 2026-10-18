package org.dozendb.cluster.core

import java.time.Duration
import java.util.UUID
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.dozendb.cluster.api.ConsensusLog
import org.dozendb.cluster.api.HostPort
import org.dozendb.cluster.api.Member
import org.dozendb.cluster.api.PeerInfo
import org.neo4j.logging.Log

/** Formation cannot succeed without an operator fixing the configuration or the members. */
class ClusterFormationException(message: String) : RuntimeException(message)

/**
 * Decides the initial voting members of a consensus group that has no local state yet.
 *
 * Every initial member probes every address in `dbms.dozendb.cluster.initial_members`, including
 * its own. When all of them answer, each member computes the same list, ordered by member id. If
 * one of them already hosts the group, its member list is adopted instead, which covers members
 * that start after the others already formed the group. Members that run a different Neo4j version
 * are refused.
 */
class ClusterFormation(
    private val consensus: ConsensusLog,
    private val initialMembers: List<HostPort>,
    private val neo4jVersion: String,
    private val userLog: Log,
    private val retryInterval: Duration = Duration.ofSeconds(1),
    private val progressInterval: Duration = Duration.ofSeconds(10),
    private val sleep: (Duration) -> Unit = { Thread.sleep(it.toMillis()) },
) {
    init {
        require(initialMembers.isNotEmpty()) {
            "${DozenDbMetadata.INITIAL_MEMBERS_SETTING} must list the Raft address of every initial member"
        }
    }

    /** Blocks until the initial members of [group] are known and returns them. */
    fun awaitInitialMembers(group: UUID): List<Member> {
        val self = consensus.self
        var waited = Duration.ZERO
        var nextProgress = progressInterval
        while (true) {
            val outcomes = probeAll()
            val reached = outcomes.mapNotNull { (_, outcome) -> outcome.getOrNull() }

            reached
                .firstNotNullOfOrNull { it.groups[group] }
                ?.let { formed ->
                    if (formed.none { it.id == self.id }) {
                        throw ClusterFormationException(
                            "The cluster was already formed by $formed without this member ($self). " +
                                "A member whose data was removed rejoins as a new member, which must " +
                                "be added to the cluster explicitly."
                        )
                    }
                    userLog.info("Joining the already formed cluster of %s", formed)
                    return formed
                }

            reached.forEach(::checkCompatible)

            if (reached.size == initialMembers.size) {
                return decide(self, reached)
            }

            if (waited >= nextProgress) {
                val missing =
                    outcomes
                        .filter { it.second.isFailure }
                        .map { (address, outcome) ->
                            "$address (${outcome.exceptionOrNull()?.message})"
                        }
                userLog.info(
                    "Waiting for initial cluster members, %d of %d reachable. Unreachable: %s",
                    reached.size,
                    initialMembers.size,
                    missing.joinToString(),
                )
                nextProgress = waited + progressInterval
            }
            sleep(retryInterval)
            waited += retryInterval
        }
    }

    private fun probeAll(): List<Pair<HostPort, Result<PeerInfo>>> = runBlocking {
        initialMembers
            .map { address -> async { address to runCatching { consensus.probe(address) } } }
            .awaitAll()
    }

    private fun checkCompatible(peer: PeerInfo) {
        val version = peer.metadata[DozenDbMetadata.NEO4J_VERSION]
        if (version != neo4jVersion) {
            throw ClusterFormationException(
                "Member ${peer.member} runs Neo4j $version but this member runs Neo4j " +
                    "$neo4jVersion. All members must run the same Neo4j version."
            )
        }
    }

    private fun decide(self: Member, reached: List<PeerInfo>): List<Member> {
        val members = reached.map { it.member }
        val duplicates = members.groupBy { it.id }.filterValues { it.size > 1 }
        if (duplicates.isNotEmpty()) {
            throw ClusterFormationException(
                "${DozenDbMetadata.INITIAL_MEMBERS_SETTING} reaches the same member through " +
                    "several addresses: ${duplicates.keys}. List each member once."
            )
        }
        if (members.none { it.id == self.id }) {
            throw ClusterFormationException(
                "This member ($self) is not one of ${DozenDbMetadata.INITIAL_MEMBERS_SETTING} " +
                    "$initialMembers. Add its advertised Raft address to the list."
            )
        }
        val ordered = members.sortedBy { it.id }
        userLog.info("All %d initial cluster members are reachable: %s", ordered.size, ordered)
        return ordered
    }
}
