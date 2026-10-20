package org.dozendb.cluster.core

import java.time.Duration
import java.util.UUID
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.dozendb.consensus.ConsensusLog
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.Member
import org.dozendb.consensus.PeerInfo
import org.neo4j.logging.Log

/** Formation cannot succeed without an operator fixing the configuration or the members. */
class ClusterFormationException(message: String) : RuntimeException(message)

/**
 * The outcome of forming or joining a group.
 *
 * @property members the initial voting members, ordered by member id; for a group formed before,
 *   its members as a member that runs it knows them, which leave this member out if it is not one
 *   of them yet.
 * @property storeSource for a newly formed cluster, the one member whose databases every other
 *   member copies; null when joining a cluster that was formed before.
 */
data class Formation(val members: List<Member>, val storeSource: Member?)

/**
 * Decides the initial voting members of a consensus group that has no local state yet, and which
 * member's databases the cluster starts from.
 *
 * Every initial member probes every address in `dbms.dozendb.cluster.initial_members`, including
 * its own. When all of them answer, each member computes the same list, ordered by member id. If
 * one of them already hosts the group, its member list is adopted instead, which covers members
 * that start after the others already formed the group, and members that join the cluster later:
 * they are not on the list until a member adds them. Members that run a different Neo4j version are
 * refused.
 *
 * Members must start from identical databases. The store source of a new cluster is the only member
 * that had databases before startup, or the member with the lowest id if none had. More than one
 * member with databases is refused, since their contents cannot be merged.
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

    /** Blocks until the initial members of [group] are known. */
    fun awaitFormation(group: UUID): Formation {
        val self = consensus.self
        var waited = Duration.ZERO
        var nextProgress = progressInterval
        while (true) {
            val outcomes = probeAll(initialMembers)
            val reached = outcomes.mapNotNull { (_, outcome) -> outcome.getOrNull() }

            reached
                .firstNotNullOfOrNull { peer -> peer.groups[group]?.let { peer to it } }
                ?.let { (peer, formed) ->
                    checkCompatible(peer)
                    if (formed.none { it.id == self.id }) {
                        userLog.info(
                            "The cluster of %s was formed without this member, which joins it",
                            formed,
                        )
                    } else {
                        userLog.info("Joining the already formed cluster of %s", formed)
                    }
                    return Formation(formed, storeSource = null)
                }

            reached.forEach(::checkCompatible)

            if (reached.size == initialMembers.size) {
                return decide(self, reached)
            }

            if (waited >= nextProgress) {
                userLog.info(
                    "Waiting for initial cluster members, %d of %d reachable. Unreachable: %s",
                    reached.size,
                    initialMembers.size,
                    describeFailures(outcomes),
                )
                nextProgress = waited + progressInterval
            }
            sleep(retryInterval)
            waited += retryInterval
        }
    }

    /**
     * Blocks until another member whose databases can be copied answers, and returns its address.
     * Only [preferred] qualifies if given, otherwise any initial member other than this one.
     */
    fun awaitStoreSource(preferred: Member?): HostPort {
        val candidates = preferred?.let { listOf(it.raftAddress) } ?: initialMembers
        var waited = Duration.ZERO
        var nextProgress = progressInterval
        while (true) {
            val outcomes = probeAll(candidates)
            val ready = outcomes.firstOrNull { (_, outcome) ->
                val peer = outcome.getOrNull()
                peer != null &&
                    peer.member.id != consensus.self.id &&
                    peer.metadata[DozenDbMetadata.STORES_READY] == "true"
            }
            if (ready != null) {
                checkCompatible(ready.second.getOrThrow())
                return ready.first
            }
            if (waited >= nextProgress) {
                userLog.info(
                    "Waiting for %s to serve its databases. Unreachable: %s",
                    preferred ?: "another member",
                    describeFailures(outcomes).ifEmpty { "none" },
                )
                nextProgress = waited + progressInterval
            }
            sleep(retryInterval)
            waited += retryInterval
        }
    }

    /** Waits [retryInterval] before the caller tries again. */
    fun pause() = sleep(retryInterval)

    private fun probeAll(addresses: List<HostPort>): List<Pair<HostPort, Result<PeerInfo>>> =
        runBlocking {
            addresses
                .map { address -> async { address to runCatching { consensus.probe(address) } } }
                .awaitAll()
        }

    private fun describeFailures(outcomes: List<Pair<HostPort, Result<PeerInfo>>>): String =
        outcomes
            .filter { it.second.isFailure }
            .joinToString { (address, outcome) ->
                "$address (${outcome.exceptionOrNull()?.message})"
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

    private fun decide(self: Member, reached: List<PeerInfo>): Formation {
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
        val withStores =
            reached.filter { it.metadata[DozenDbMetadata.HAS_STORES] == "true" }.map { it.member }
        if (withStores.size > 1) {
            throw ClusterFormationException(
                "Members ${withStores.sortedBy { it.id }} all have existing databases. A new cluster " +
                    "starts from the databases of at most one member; remove the databases of the others."
            )
        }
        val ordered = members.sortedBy { it.id }
        val source = withStores.singleOrNull() ?: ordered.first()
        userLog.info(
            "All %d initial cluster members are reachable: %s. Databases come from %s%s.",
            ordered.size,
            ordered,
            source,
            if (withStores.isEmpty()) ", which creates them" else ", which has existing databases",
        )
        return Formation(ordered, source)
    }
}
