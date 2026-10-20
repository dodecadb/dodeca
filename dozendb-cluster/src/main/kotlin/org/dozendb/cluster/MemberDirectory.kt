package org.dozendb.cluster

import java.time.Duration
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
import org.neo4j.logging.Log

/**
 * The Bolt addresses of the cluster's members, for routing, and what else they report about
 * themselves. Every member reports its advertised Bolt address when probed; the directory probes
 * the members of the `system` group periodically, and soon after someone asks for a member it does
 * not know yet. A member that does not answer keeps its last known address and report.
 */
class MemberDirectory(
    private val consensus: ConsensusLog,
    private val systemDatabase: DatabaseRef,
    private val selfBoltAddress: () -> HostPort?,
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

    /** What [member] reported when it last answered, such as its versions; see [MemberMetadata]. */
    fun reported(member: MemberId): Map<String, String>? = reports[member]

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
            reports[member.id] = peer.metadata
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
