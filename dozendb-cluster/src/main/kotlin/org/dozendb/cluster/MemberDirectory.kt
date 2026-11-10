package org.dozendb.cluster

import java.time.Duration
import java.util.UUID
import java.util.concurrent.CancellationException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.dozendb.consensus.ConsensusLog
import org.dozendb.consensus.ConsensusUnavailableException
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.Member
import org.dozendb.consensus.MemberId
import org.dozendb.consensus.PeerInfo
import org.dozendb.core.edition.DatabaseState
import org.neo4j.logging.Log

/**
 * The Bolt and routing addresses of the cluster's members, and what else they report about
 * themselves. Every member reports its advertised addresses and its databases when probed; the
 * directory probes the cluster's [servers] periodically, and on demand: soon after someone asks for
 * a member it does not know yet, and when someone waits for it ([awaitRefresh]). A probe on demand
 * waits for each member for at most [demandDeadline], on a thread of its own, so that a member that
 * answers nothing at all holds up only the periodic probe, which finds out that it no longer
 * answers. A member that does not answer keeps its last known address and report, and a report that
 * arrives after a later one of the same member is left out ([MemberMetadata.REPORT_NUMBER]). This
 * member's own come from [selfBoltAddress], [selfRoutingAddress], [selfDatabases], [selfReadOnly],
 * [selfLastCommitted] and [selfReport].
 *
 * While no server is known, as on a system secondary that just started, it probes the [bootstrap]
 * addresses, the initial members, instead: what those report tells the servers.
 */
class MemberDirectory(
    private val consensus: ConsensusLog,
    private val databaseGroups: DatabaseGroups,
    private val selfBoltAddress: () -> HostPort?,
    private val selfRoutingAddress: () -> HostPort?,
    private val selfDatabases: () -> List<DatabaseState>,
    private val log: Log,
    private val refreshInterval: Duration = Duration.ofSeconds(10),
    private val selfReadOnly: () -> Set<UUID> = { emptySet() },
    private val selfLastCommitted: () -> Map<UUID, Long> = { emptyMap() },
    private val selfReport: () -> Map<String, String> = { emptyMap() },
    private val demandDeadline: Duration = Duration.ofSeconds(1),
    private val servers: () -> List<Member> = { consensus.members(databaseGroups.system) },
    private val bootstrap: () -> List<HostPort> = { emptyList() },
) {
    /** The group of `system` (see [DatabaseGroups]). */
    private val systemDatabase: DatabaseRef
        get() = databaseGroups.system

    private val addresses = ConcurrentHashMap<MemberId, HostPort>()
    private val reports = ConcurrentHashMap<MemberId, Map<String, String>>()
    private val groups = ConcurrentHashMap<MemberId, Map<UUID, List<Member>>>()
    private val answered: MutableSet<MemberId> = ConcurrentHashMap.newKeySet()

    /**
     * Numbers probes in the order they start, so that what an earlier one found out is left out.
     */
    private val probes = AtomicLong()

    /** The latest probe of each member whose outcome [answered] holds. */
    private val outcomes = ConcurrentHashMap<MemberId, Long>()

    /** Held while what members answered is recorded, as probes may overlap. */
    private val recording = Any()

    /** Whether every member was probed at least once since this directory started. */
    @Volatile
    var refreshed: Boolean = false
        private set

    private val refreshPending = AtomicBoolean()

    /** Completed when the probe on demand that starts next ends, for [awaitRefresh]. */
    private val nextRefresh = AtomicReference(CompletableFuture<Unit>())

    @Volatile private var scheduler: ScheduledExecutorService? = null

    /** Runs the probes on demand. */
    @Volatile private var demand: ExecutorService? = null

    private var announcements: AutoCloseable? = null

    /** The advertised Bolt address of [member], or null while it is not known. */
    fun boltAddress(member: MemberId): HostPort? {
        if (member == consensus.self.id) return selfBoltAddress()
        return addresses[member]
            ?: run {
                requestRefresh()
                null
            }
    }

    /**
     * The routing address of [member], to which writes of the databases it leads can be forwarded,
     * or null while it is not known or if the member takes no forwarded writes.
     */
    fun routingAddress(member: MemberId): HostPort? {
        if (member == consensus.self.id) return selfRoutingAddress()
        val reported =
            reports[member]
                ?: run {
                    requestRefresh()
                    return null
                }
        return reported[MemberMetadata.ROUTING_ADDRESS]?.let { parsed(it) }
    }

    /**
     * What [member] reported when it last answered, such as its versions, or what this member
     * reports now; see [MemberMetadata].
     */
    fun reported(member: MemberId): Map<String, String>? =
        if (member == consensus.self.id) selfReport() else reports[member]

    /**
     * The members whose copy of the database [id] failed, such as by a panic, or is in quarantine:
     * this member's as it is now, the others' as they last reported it. A member that has not
     * reported is not one.
     */
    fun failedCopies(id: UUID): Set<MemberId> =
        (reports.keys + consensus.self.id)
            .filter { member ->
                databaseStates(member).orEmpty().any { it.id == id && it.status in FAILED }
            }
            .toSet()

    /**
     * The databases on [member] and their states: this member's as they are now, another's as it
     * last reported them, or null if it has not.
     */
    fun databaseStates(member: MemberId): List<DatabaseState>? {
        if (member == consensus.self.id) return selfDatabases()
        val databases = reports[member]?.get(MemberMetadata.DATABASES) ?: return null
        return runCatching { DatabaseStatesCodec.decode(databases) }.getOrNull()
    }

    /**
     * The ids of the databases that are read-only on [member]: this member's as they are now,
     * another's as it last reported them; none if it has not.
     */
    fun readOnlyDatabases(member: MemberId): Set<UUID> {
        if (member == consensus.self.id) return selfReadOnly()
        val ids = reports[member]?.get(MemberMetadata.READ_ONLY_DATABASES) ?: return emptySet()
        return ids.split(',')
            .filter { it.isNotBlank() }
            .mapNotNull { runCatching { UUID.fromString(it.trim()) }.getOrNull() }
            .toSet()
    }

    /**
     * The id of the last transaction committed to each database on [member]: this member's as they
     * are now, another's as it last reported them; none if it has not.
     */
    fun lastCommitted(member: MemberId): Map<UUID, Long> {
        if (member == consensus.self.id) return selfLastCommitted()
        val entries =
            reports[member]?.get(MemberMetadata.LAST_COMMITTED_TRANSACTIONS) ?: return emptyMap()
        return MemberMetadata.numbersById(entries)
    }

    /**
     * The groups that [member] leads, with their terms: this member's as they are now, another's as
     * it last reported them; none if it has not.
     */
    fun leading(member: MemberId): Map<UUID, Long> =
        reported(member)?.get(MemberMetadata.LEADING)?.let(MemberMetadata::numbersById).orEmpty()

    /**
     * The groups that the other members run, with the voting members each knows, as they last
     * reported them; members that have not reported are left out.
     */
    fun groups(): Map<MemberId, Map<UUID, List<Member>>> = HashMap(groups)

    /**
     * Probes the members soon, as when what they reported may be out of date, waiting for each for
     * at most [demandDeadline].
     */
    fun refreshSoon() = requestRefresh()

    /**
     * Probes the members soon, as [refreshSoon], and waits for that probe to end, for at most
     * [timeout]: for an answer that what they reported last cannot give, as which member leads a
     * database just created. Those who wait at the same time share one probe, which starts after
     * they asked. Returns whether it ended in time; while this directory is stopped, false at once.
     */
    fun awaitRefresh(timeout: Duration): Boolean {
        if (demand == null) return false
        val refreshed = nextRefresh.get()
        requestRefresh()
        return try {
            refreshed.get(timeout.toMillis(), TimeUnit.MILLISECONDS)
            true
        } catch (_: TimeoutException) {
            false
        } catch (_: CancellationException) {
            false
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
            false
        }
    }

    /**
     * Records what a member announces about itself, as one that starts or stops leading a group
     * does, as if it had answered a probe: unless it is not one of the cluster's servers, or a
     * later report of it is recorded already. Whether it answers probes stays as it was.
     */
    fun announced(peer: PeerInfo) {
        val member = peer.member.id
        if (member == consensus.self.id) return
        val known =
            try {
                servers().any { it.id == member }
            } catch (_: ConsensusUnavailableException) {
                false
            }
        if (!known) {
            log.debug("Ignored what %s, not a member, announced", peer.member)
            return
        }
        synchronized(recording) { record(peer) }
    }

    /** Whether [member] answered the latest probe. This member always does. */
    fun answering(member: MemberId): Boolean = member == consensus.self.id || member in answered

    /**
     * Probes every member once and records what they report. With [deadline], waits that long at
     * most: a member that has not answered by then keeps what it reported last and whether it
     * answered. Returns the members that answered.
     */
    fun refresh(deadline: Duration? = null): Set<MemberId> {
        val probe = probes.incrementAndGet()
        val others = others(deadline)
        val answers = runBlocking {
            others
                .map { member ->
                    async {
                        member to
                            runCatching {
                                if (deadline == null) consensus.probe(member.raftAddress)
                                else
                                    withTimeoutOrNull(deadline.toMillis()) {
                                        consensus.probe(member.raftAddress)
                                    }
                            }
                    }
                }
                .awaitAll()
        }
        val answering = mutableSetOf<MemberId>()
        var late = false
        synchronized(recording) {
            val ids = others.map { it.id }.toSet()
            answered.retainAll(ids)
            groups.keys.retainAll(ids)
            outcomes.keys.retainAll(ids)
            for ((member, answer) in answers) {
                if (answer.isSuccess && answer.getOrNull() == null) {
                    late = true
                    continue
                }
                // Probes overlap: what an earlier one found out is out of date.
                val latest = outcomes.getOrDefault(member.id, 0) < probe
                if (latest) outcomes[member.id] = probe
                val peer = answer.getOrNull()?.takeIf { it.member.id == member.id }
                if (peer == null) {
                    if (latest) answered -= member.id
                    continue
                }
                if (latest) answered += member.id
                answering += member.id
                record(peer)
            }
        }
        if (!late) refreshed = true
        return answering
    }

    /**
     * The other servers, to probe; while none is known, the members that answer at the [bootstrap]
     * addresses, waiting for each for at most [deadline].
     */
    private fun others(deadline: Duration?): List<Member> {
        val known =
            try {
                servers()
            } catch (e: ConsensusUnavailableException) {
                val addresses = bootstrap()
                if (addresses.isEmpty()) throw e
                return runBlocking {
                    addresses
                        .map { address ->
                            async {
                                runCatching {
                                    if (deadline == null) consensus.probe(address)
                                    else
                                        withTimeoutOrNull(deadline.toMillis()) {
                                            consensus.probe(address)
                                        }
                                }
                                    .getOrNull()
                                    ?.member
                            }
                        }
                        .awaitAll()
                }
                    .filterNotNull()
                    .filter { it.id != consensus.self.id }
                    .distinctBy { it.id }
            }
        return known.filter { it.id != consensus.self.id }
    }

    /** Records what [peer] reports, unless a later report of it is recorded already. */
    private fun record(peer: PeerInfo) {
        val member = peer.member
        if (!MemberMetadata.laterReport(peer.metadata, reports[member.id])) return
        groups[member.id] = peer.groups
        val routing = peer.metadata[MemberMetadata.ROUTING_ADDRESS]
        if (routing != reports.put(member.id, peer.metadata)?.get(MemberMetadata.ROUTING_ADDRESS)) {
            when {
                routing == null -> log.info("Member %s takes no forwarded writes", member.id)
                parsed(routing) == null ->
                    log.warn("Member %s reports an invalid routing address: %s", member, routing)
                else -> log.info("Member %s takes forwarded writes at %s", member.id, routing)
            }
        }
        val reported = peer.metadata[MemberMetadata.BOLT_ADDRESS] ?: return
        val address = runCatching { HostPort.parse(reported) }.getOrNull()
        if (address == null) {
            log.warn("Member %s reports an invalid Bolt address: %s", member, reported)
            return
        }
        if (addresses.put(member.id, address) != address) {
            log.info("Member %s serves Bolt at %s", member.id, address)
        }
    }

    fun start() {
        announcements = consensus.addAnnouncementListener(::announced)
        demand = Executors.newSingleThreadExecutor { runnable ->
            Thread(runnable, "dozendb-member-directory-demand").apply { isDaemon = true }
        }
        val executor = Executors.newSingleThreadScheduledExecutor { runnable ->
            Thread(runnable, "dozendb-member-directory").apply { isDaemon = true }
        }
        scheduler = executor
        executor.scheduleWithFixedDelay(
            ::refreshQuietly,
            0,
            refreshInterval.toMillis(),
            TimeUnit.MILLISECONDS,
        )
    }

    fun stop() {
        announcements?.close()
        announcements = null
        scheduler?.shutdownNow()
        scheduler = null
        val executor = demand
        // First, so that a probe on demand under way, interrupted, ends no one's wait as if done.
        demand = null
        executor?.shutdownNow()
        nextRefresh.get().cancel(false)
    }

    private fun parsed(address: String): HostPort? = runCatching {
        HostPort.parse(address)
    }
        .getOrNull()

    private fun requestRefresh() {
        val executor = demand ?: return
        if (refreshPending.compareAndSet(false, true)) {
            try {
                executor.execute {
                    refreshPending.set(false)
                    refreshOnDemand()
                }
            } catch (_: RejectedExecutionException) {
                // Stopped.
                refreshPending.set(false)
            }
        }
    }

    private fun refreshOnDemand() {
        // Ends the wait of those who asked before this probe started.
        val waiting = nextRefresh.getAndSet(CompletableFuture())
        try {
            refreshQuietly(demandDeadline)
        } finally {
            if (demand != null) waiting.complete(Unit) else waiting.cancel(false)
        }
    }

    private fun refreshQuietly(deadline: Duration? = null) {
        try {
            refresh(deadline)
        } catch (e: Exception) {
            log.warn("Refreshing the Bolt addresses of members failed: %s", e.toString())
        }
    }

    private companion object {
        /** The states of copies that serve nothing, which routing tables leave out. */
        val FAILED = setOf(DatabaseState.Status.FAILED, DatabaseState.Status.QUARANTINED)
    }
}
