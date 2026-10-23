package org.dozendb.cluster

import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.dozendb.consensus.ConsensusLog
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.MemberId
import org.dozendb.core.edition.DatabaseState
import org.neo4j.logging.Log

/**
 * The Bolt and routing addresses of the cluster's members, and what else they report about
 * themselves. Every member reports its advertised addresses and its databases when probed; the
 * directory probes the members of the `system` group periodically, and soon after someone asks for
 * a member it does not know yet. A member that does not answer keeps its last known address and
 * report. This member's own come from [selfBoltAddress], [selfRoutingAddress] and [selfDatabases].
 */
class MemberDirectory(
    private val consensus: ConsensusLog,
    private val systemDatabase: DatabaseRef,
    private val selfBoltAddress: () -> HostPort?,
    private val selfRoutingAddress: () -> HostPort?,
    private val selfDatabases: () -> List<DatabaseState>,
    private val log: Log,
    private val refreshInterval: Duration = Duration.ofSeconds(10),
) {
    private val addresses = ConcurrentHashMap<MemberId, HostPort>()
    private val reports = ConcurrentHashMap<MemberId, Map<String, String>>()
    private val refreshPending = AtomicBoolean()
    private var scheduler: ScheduledExecutorService? = null

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

    /** What [member] reported when it last answered, such as its versions; see [MemberMetadata]. */
    fun reported(member: MemberId): Map<String, String>? = reports[member]

    /**
     * The members whose copy of the database [id] failed, such as by a panic: this member's as it
     * is now, the others' as they last reported it. A member that has not reported is not one.
     */
    fun failedCopies(id: UUID): Set<MemberId> {
        val failed = mutableSetOf<MemberId>()
        if (selfDatabases().any { it.id == id && it.status == DatabaseState.Status.FAILED }) {
            failed += consensus.self.id
        }
        for ((member, report) in reports) {
            val databases = report[MemberMetadata.DATABASES] ?: continue
            val states =
                runCatching { DatabaseStatesCodec.decode(databases) }.getOrNull() ?: continue
            if (states.any { it.id == id && it.status == DatabaseState.Status.FAILED }) {
                failed += member
            }
        }
        return failed
    }

    /** Probes every member once and records the addresses they report. */
    fun refresh() {
        val others = consensus.members(systemDatabase).filter { it.id != consensus.self.id }
        val answers = runBlocking {
            others
                .map { member ->
                    async { member to runCatching { consensus.probe(member.raftAddress) } }
                }
                .awaitAll()
        }
        for ((member, answer) in answers) {
            val peer = answer.getOrNull() ?: continue
            if (peer.member.id != member.id) continue
            val routing = peer.metadata[MemberMetadata.ROUTING_ADDRESS]
            if (
                routing !=
                    reports.put(member.id, peer.metadata)?.get(MemberMetadata.ROUTING_ADDRESS)
            ) {
                when {
                    routing == null -> log.info("Member %s takes no forwarded writes", member.id)
                    parsed(routing) == null ->
                        log.warn(
                            "Member %s reports an invalid routing address: %s",
                            member,
                            routing,
                        )
                    else -> log.info("Member %s takes forwarded writes at %s", member.id, routing)
                }
            }
            val reported = peer.metadata[MemberMetadata.BOLT_ADDRESS] ?: continue
            val address = runCatching { HostPort.parse(reported) }.getOrNull()
            if (address == null) {
                log.warn("Member %s reports an invalid Bolt address: %s", member, reported)
                continue
            }
            if (addresses.put(member.id, address) != address) {
                log.info("Member %s serves Bolt at %s", member.id, address)
            }
        }
    }

    fun start() {
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
        scheduler?.shutdownNow()
        scheduler = null
    }

    private fun parsed(address: String): HostPort? = runCatching {
        HostPort.parse(address)
    }
        .getOrNull()

    private fun requestRefresh() {
        val executor = scheduler ?: return
        if (refreshPending.compareAndSet(false, true)) {
            executor.execute {
                refreshPending.set(false)
                refreshQuietly()
            }
        }
    }

    private fun refreshQuietly() {
        try {
            refresh()
        } catch (e: Exception) {
            log.warn("Refreshing the Bolt addresses of members failed: %s", e.toString())
        }
    }
}
