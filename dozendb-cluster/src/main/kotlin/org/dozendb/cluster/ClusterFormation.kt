package org.dozendb.cluster

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
 * @property group the id of the group of `system`: of a new cluster, the database's id; of a formed
 *   one, the id its members report; for a group this member re-forms, a new one.
 * @property reforms whether this member re-forms `system` from its own store after `neo4j-admin
 *   dbms unbind-system-db`, as the member with the latest one.
 */
data class Formation(
    val members: List<Member>,
    val storeSource: Member?,
    val group: UUID = DatabaseGroups.SYSTEM_DATABASE,
    val reforms: Boolean = false,
)

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
 *
 * Members that `neo4j-admin dbms unbind-system-db` unbound re-form `system` instead, under a new
 * group id ([DatabaseGroups]): every initial member must be unbound or new, with no databases. The
 * unbound member with the latest store of `system` re-forms it from that store, and the others join
 * its group once it runs it. A member that still runs a group they were unbound from holds them
 * back until it is unbound too, or taken off the list.
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
            "${MemberMetadata.INITIAL_MEMBERS_SETTING} must list the Raft address of every initial member"
        }
    }

    /**
     * Refuses this member, which hosts `system` as a secondary, if the initial members list it:
     * secondaries never form a cluster.
     */
    fun refuseInitialSecondary(setting: String) {
        val self = consensus.self.raftAddress
        if (self in initialMembers) {
            throw ClusterFormationException(
                "This server hosts the system database as a secondary ($setting=SECONDARY), but " +
                    "its address $self is one of ${MemberMetadata.INITIAL_MEMBERS_SETTING} " +
                    "$initialMembers: secondaries never form a cluster. Remove it from the list, " +
                    "or make the server a primary."
            )
        }
    }

    /**
     * Blocks until the initial members of [group], this member's group of `system`, are known: it
     * joins the members that run it, or forms a new cluster under it. With [joinAny], as a member
     * without a record, it joins any group the members report for `system`. Unless [form], this
     * member forms no group, as a system secondary does not: it waits until a member it reaches
     * runs the group.
     *
     * An [unbound] member was unbound from [group]: it joins no member that runs it, and re-forms
     * `system` with the others, or joins the group that re-formed it if it holds a store of the
     * same `system` ([storeId]) or none.
     */
    fun awaitFormation(
        group: UUID,
        form: Boolean = true,
        joinAny: Boolean = false,
        unbound: Boolean = false,
        storeId: String? = null,
    ): Formation {
        val self = consensus.self
        var waited = Duration.ZERO
        var nextProgress = progressInterval
        while (true) {
            val outcomes = probeAll(initialMembers)
            val reached = outcomes.mapNotNull { (_, outcome) -> outcome.getOrNull() }
            val running = reached.filter { systemGroup(it) in it.groups }
            // Unbinding deleted the state of the old group: an unbound member that runs the group
            // it reports has just re-formed `system` in it.
            val oldGroups =
                (reached.filter(::unbound) - running.toSet()).map(::systemGroup).toSet() +
                    listOfNotNull(group.takeIf { unbound })

            running
                .firstOrNull { peer ->
                    val runs = systemGroup(peer)
                    runs !in oldGroups && (unbound || joinAny || runs == group)
                }
                ?.let { peer ->
                    checkCompatible(peer)
                    if (unbound) checkSameSystem(peer, storeId)
                    val formed = checkNotNull(peer.groups[systemGroup(peer)])
                    if (formed.none { it.id == self.id }) {
                        userLog.info(
                            "The cluster of %s was formed without this member, which joins it",
                            formed,
                        )
                    } else {
                        userLog.info("Joining the already formed cluster of %s", formed)
                    }
                    return Formation(formed, storeSource = null, group = systemGroup(peer))
                }

            reached.forEach(::checkCompatible)
            val stale = running.filter { systemGroup(it) in oldGroups }.map { it.member }
            val reforming = unbound || reached.any(::unbound)

            var waitingFor: String? = null
            if (stale.isEmpty() && form && reached.size == initialMembers.size) {
                if (!reforming) return decide(self, reached, group)
                val source = reformationSource(self, reached)
                if (source.member.id == self.id) return reformation(self, reached, source)
                waitingFor =
                    "Waiting for ${source.member}, which holds the latest store of the system " +
                        "database (last transaction ${lastTransaction(source)}), to re-form it"
            }

            if (waited >= nextProgress || (waited == Duration.ZERO && stale.isNotEmpty())) {
                when {
                    stale.isNotEmpty() ->
                        userLog.warn(
                            "Servers %s still run the Raft group of the system database that " +
                                "neo4j-admin dbms unbind-system-db unbound the others from. Run " +
                                "it on them too, or take them off %s.",
                            stale,
                            MemberMetadata.INITIAL_MEMBERS_SETTING,
                        )
                    waitingFor != null -> userLog.info(waitingFor)
                    else ->
                        userLog.info(
                            if (form)
                                "Waiting for initial cluster members, %d of %d reachable. " +
                                    "Unreachable: %s"
                            else
                                "Waiting for the initial cluster members to form the cluster, " +
                                    "%d of %d reachable. Unreachable: %s",
                            reached.size,
                            initialMembers.size,
                            describeFailures(outcomes).ifEmpty { "none" },
                        )
                }
                nextProgress = waited + progressInterval
            }
            sleep(retryInterval)
            waited += retryInterval
        }
    }

    /**
     * Blocks until another member whose databases can be copied answers, and returns its address.
     * Only [preferred] qualifies if given, otherwise any initial member other than this one; and
     * only a member whose group of `system` is [group], if given.
     */
    fun awaitStoreSource(preferred: Member?, group: UUID? = null): HostPort {
        val candidates = preferred?.let { listOf(it.raftAddress) } ?: initialMembers
        var waited = Duration.ZERO
        var nextProgress = progressInterval
        while (true) {
            val outcomes = probeAll(candidates)
            val ready = outcomes.firstOrNull { (_, outcome) ->
                val peer = outcome.getOrNull()
                peer != null &&
                    peer.member.id != consensus.self.id &&
                    peer.metadata[MemberMetadata.STORES_READY] == "true" &&
                    (group == null || systemGroup(peer) == group)
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
        val version = peer.metadata[MemberMetadata.NEO4J_VERSION]
        if (version != neo4jVersion) {
            throw ClusterFormationException(
                "Member ${peer.member} runs Neo4j $version but this member runs Neo4j " +
                    "$neo4jVersion. All members must run the same Neo4j version."
            )
        }
    }

    private fun decide(self: Member, reached: List<PeerInfo>, group: UUID): Formation {
        val members = checkMembers(self, reached)
        val withStores =
            reached.filter { it.metadata[MemberMetadata.HAS_STORES] == "true" }.map { it.member }
        if (withStores.size > 1) {
            throw ClusterFormationException(
                "Members ${withStores.sortedBy { it.id }} all have existing databases. A new cluster " +
                    "starts from the databases of at most one member; remove the databases of the others."
            )
        }
        val source = withStores.singleOrNull() ?: members.first()
        userLog.info(
            "All %d initial cluster members are reachable: %s. Databases come from %s%s.",
            members.size,
            members,
            source,
            if (withStores.isEmpty()) ", which creates them" else ", which has existing databases",
        )
        return Formation(members, source, group)
    }

    /**
     * The member that re-forms `system`, once every initial member answered and some are unbound:
     * of those with a store of it, the one with the latest, then the lowest id. Every member must
     * be unbound or new, and their stores copies of one cluster's `system`.
     */
    private fun reformationSource(self: Member, reached: List<PeerInfo>): PeerInfo {
        checkMembers(self, reached)
        val withDatabases =
            reached
                .filter { !unbound(it) && it.metadata[MemberMetadata.HAS_STORES] == "true" }
                .map { it.member }
        if (withDatabases.isNotEmpty()) {
            throw ClusterFormationException(
                "Servers ${withDatabases.sortedBy { it.id }} have databases but were not unbound " +
                    "with neo4j-admin dbms unbind-system-db, while " +
                    "${reached.filter(::unbound).map { it.member }.sortedBy { it.id }} were. " +
                    "The system database is re-formed from unbound servers and new ones without " +
                    "databases: run neo4j-admin dbms unbind-system-db on them too, or take them " +
                    "off ${MemberMetadata.INITIAL_MEMBERS_SETTING}."
            )
        }
        val withStore = reached.filter {
            unbound(it) &&
                it.metadata[MemberMetadata.SYSTEM_STORE] != null &&
                lastTransaction(it) != null
        }
        if (withStore.isEmpty()) {
            throw ClusterFormationException(
                "None of the servers ${reached.map { it.member }.sortedBy { it.id }} holds a " +
                    "store of the system database to re-form it from. List a server that was " +
                    "unbound with its store in ${MemberMetadata.INITIAL_MEMBERS_SETTING}, or load a " +
                    "dump of the system database on one of them."
            )
        }
        val clusters = withStore.groupBy { it.metadata[MemberMetadata.SYSTEM_STORE] }
        if (clusters.size > 1) {
            throw ClusterFormationException(
                "The servers hold stores of the system databases of different clusters: " +
                    clusters.values.joinToString("; ") { peers ->
                        peers.map { it.member }.sortedBy { it.id }.joinToString()
                    } +
                    ". Re-form the system database from the servers of one cluster."
            )
        }
        return withStore
            .sortedWith(
                compareByDescending<PeerInfo> { lastTransaction(it) }.thenBy { it.member.id }
            )
            .first()
    }

    /**
     * This member, [source], re-forms `system` with every initial member. The id of the new group
     * follows from the group it was unbound from, this member and its store, so that it re-forms
     * the same group if it restarts before the others joined: copies of an older `system` that kept
     * that group's state cannot hold this one.
     */
    private fun reformation(self: Member, reached: List<PeerInfo>, source: PeerInfo): Formation {
        val members = reached.map { it.member }.sortedBy { it.id }
        val last = lastTransaction(source)
        val group =
            UUID.nameUUIDFromBytes(
                "${systemGroup(source)}/${self.id}/$last/${source.metadata[MemberMetadata.SYSTEM_STORE]}"
                    .toByteArray()
            )
        userLog.info(
            "All %d initial cluster members are reachable and were unbound from the system " +
                "database or are new: %s. This member re-forms it from its store, the latest (last " +
                "transaction %d), under the new Raft group %s; the others copy it.",
            members.size,
            members,
            last,
            group,
        )
        return Formation(members, storeSource = self, group = group, reforms = true)
    }

    /**
     * The initial members, ordered by id, after refusing a list that lacks this member or names one
     * twice.
     */
    private fun checkMembers(self: Member, reached: List<PeerInfo>): List<Member> {
        val members = reached.map { it.member }
        val duplicates = members.groupBy { it.id }.filterValues { it.size > 1 }
        if (duplicates.isNotEmpty()) {
            throw ClusterFormationException(
                "${MemberMetadata.INITIAL_MEMBERS_SETTING} reaches the same member through " +
                    "several addresses: ${duplicates.keys}. List each member once."
            )
        }
        if (members.none { it.id == self.id }) {
            throw ClusterFormationException(
                "This member ($self) is not one of ${MemberMetadata.INITIAL_MEMBERS_SETTING} " +
                    "$initialMembers. Add its advertised Raft address to the list."
            )
        }
        return members.sortedBy { it.id }
    }

    /**
     * Refuses to join [peer]'s group of `system` with a store of another cluster's `system`
     * ([storeId]), as an unbound member whose initial members name another cluster.
     */
    private fun checkSameSystem(peer: PeerInfo, storeId: String?) {
        val theirs = peer.metadata[MemberMetadata.SYSTEM_STORE] ?: return
        if (storeId == null || theirs == storeId) return
        throw ClusterFormationException(
            "Member ${peer.member} runs the system database of another cluster: its store is not " +
                "a copy of this server's. Check ${MemberMetadata.INITIAL_MEMBERS_SETTING}."
        )
    }

    private fun unbound(peer: PeerInfo) = peer.metadata[MemberMetadata.SYSTEM_UNBOUND] == "true"

    private fun systemGroup(peer: PeerInfo) = MemberMetadata.systemGroup(peer.metadata)

    private fun lastTransaction(peer: PeerInfo) =
        peer.metadata[MemberMetadata.SYSTEM_LAST_TRANSACTION]?.toLongOrNull()
}
