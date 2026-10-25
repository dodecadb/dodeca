package org.dozendb.cluster.internal

import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import org.dozendb.cluster.MemberMetadata
import org.dozendb.consensus.ConsensusLog
import org.dozendb.consensus.ConsensusUnavailableException
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.MemberId
import org.dozendb.core.servers.ServerOptions
import org.dozendb.core.servers.ServerRecord
import org.dozendb.core.servers.ServerRecords
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.logging.InternalLog

/**
 * Keeps the server records of the system graph (see [ServerRecords]) in line with the cluster's
 * members, on the `system` leader, in replicated transactions:
 * - Until the initial servers are recorded, every member of `system` counts as enabled. The leader
 *   records each with the options it reports, from its `initial.server.*` settings, once it has its
 *   report, and marks the initial servers recorded once every member has a record. This covers a
 *   new cluster and one formed before DozenDB recorded servers alike.
 * - Afterwards, a member that joins is Free until ENABLE SERVER records it, or at once while
 *   `auto_enable_free_servers` is set.
 * - The record of a server that left `system` is deleted.
 *
 * It looks when this member becomes the leader, after each applied `system` transaction, and while
 * work remains, again after [retryDelay], twice as long each time up to [maximumRetryDelay].
 *
 * @property report what a member reports about itself, this member included; null while unknown.
 * @property autoEnable the first value of `auto_enable_free_servers`, from
 *   `initial.dbms.automatically_enable_free_servers`.
 * @property current whether this member's system graph holds every committed transaction, as the
 *   leader confirms: a new leader may not have applied the last ones of the previous leader yet.
 */
internal class ServerReconciler(
    private val consensus: ConsensusLog,
    private val systemDatabase: DatabaseRef,
    private val system: () -> GraphDatabaseService,
    private val reading: (() -> Unit) -> Unit,
    private val leading: () -> Boolean,
    private val current: () -> Boolean,
    private val report: (MemberId) -> Map<String, String>?,
    private val autoEnable: Boolean,
    private val log: InternalLog,
    private val retryDelay: Duration = Duration.ofSeconds(1),
    private val maximumRetryDelay: Duration = Duration.ofSeconds(30),
) : AutoCloseable {
    private val scheduler: ScheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor { runnable ->
            Thread(runnable, "dozendb-servers").apply { isDaemon = true }
        }
    private val requested = AtomicBoolean(false)
    private var statusListener: AutoCloseable? = null

    // Used on the scheduler thread only.
    private var nextRetryDelay = retryDelay
    private var retry: ScheduledFuture<*>? = null

    fun start() {
        statusListener = consensus.addStatusListener(systemDatabase) { request() }
        request()
    }

    /** Asks for a look soon; requests made while one waits are merged. */
    fun request() {
        if (!requested.compareAndSet(false, true)) return
        try {
            scheduler.execute(::run)
        } catch (_: RejectedExecutionException) {
            // Closing.
        }
    }

    private fun run() {
        requested.set(false)
        retry?.cancel(false)
        retry = null
        val again =
            try {
                reconcile()
            } catch (e: Exception) {
                log.info("Recording the cluster's servers failed: %s", e.toString())
                true
            }
        if (!again) {
            nextRetryDelay = retryDelay
            return
        }
        try {
            retry = scheduler.schedule(::run, nextRetryDelay.toMillis(), TimeUnit.MILLISECONDS)
        } catch (_: RejectedExecutionException) {
            // Closing.
        }
        nextRetryDelay = minOf(nextRetryDelay.multipliedBy(2), maximumRetryDelay)
    }

    /** Looks once; returns whether work remains. */
    private fun reconcile(): Boolean {
        if (!leading()) return false
        val membership =
            try {
                consensus.membership(systemDatabase)
            } catch (_: ConsensusUnavailableException) {
                return true
            }
        if (!membership.settled) return true
        if (!current()) return true
        val members = membership.members.map { it.id }
        var again = false
        reading {
            system().beginTx().use { tx ->
                ServerRecords.lock(tx)
                val records = ServerRecords.records(tx)
                val initial = ServerRecords.initialServersEnabled(tx)
                var changed = ServerRecords.removeDuplicates(tx)
                for (id in records.keys - members.map { it.uuid }.toSet()) {
                    log.info("Deleting the record of server %s, which left the cluster", id)
                    ServerRecords.delete(tx, id)
                    changed = true
                }
                val enabling = !initial || ServerRecords.autoEnable(tx)
                val free = members.filter { it.uuid !in records }
                if (enabling) {
                    for (member in free) {
                        val options = initialOptions(member)
                        if (options == null) {
                            again = true
                            continue
                        }
                        log.info(
                            "Enabling server %s with its initial options %s%s",
                            member,
                            options,
                            if (initial) ", as servers that join are enabled at once" else "",
                        )
                        ServerRecords.write(tx, ServerRecord(member.uuid, options = options))
                        changed = true
                    }
                }
                if (!initial && !again) {
                    log.info("Recorded the servers the cluster started with, %s", members)
                    ServerRecords.markInitialServersEnabled(tx, autoEnable)
                    changed = true
                }
                if (changed) tx.commit()
            }
        }
        return again
    }

    /** The options [member] reports from its `initial.server.*` settings, or null if unknown. */
    private fun initialOptions(member: MemberId): ServerOptions? {
        val reported = report(member) ?: return null
        val tags = reported[MemberMetadata.INITIAL_SERVER_TAGS] ?: return null
        return ServerOptions(tags = MemberMetadata.splitList(tags))
    }

    override fun close() {
        statusListener?.close()
        scheduler.shutdownNow()
    }
}
