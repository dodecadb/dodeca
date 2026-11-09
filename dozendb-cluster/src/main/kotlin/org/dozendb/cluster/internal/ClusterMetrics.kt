package org.dozendb.cluster.internal

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.LongAdder
import org.dozendb.consensus.ConsensusLog
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.GroupProgress
import org.dozendb.core.monitoring.MetricRegistry
import org.dozendb.core.monitoring.MetricsContributor
import org.dozendb.core.monitoring.SummaryHistogram
import org.neo4j.kernel.database.AbstractDatabase

/**
 * The cluster's metrics. Per database, Neo4j Enterprise's Raft metrics under `cluster.raft.` with
 * its names, and DozenDB's under `dozendb.cluster.`; for the DBMS, the member count.
 *
 * Replication and store copy counts are kept per database name from when this member started: a
 * copy restarts its database, which would otherwise reset them.
 */
internal class ClusterMetrics(
    private val consensus: ConsensusLog,
    private val systemDatabase: DatabaseRef,
) : MetricsContributor {
    /** What this member counts of one database, from when it started. */
    class Counters {
        val replicationNew = LongAdder()
        val replicationSuccess = LongAdder()
        val replicationFail = LongAdder()
        val replicationMaybe = LongAdder()
        val replicationMillis = SummaryHistogram()
        val copiesReceived = LongAdder()
        val copiesRepeated = LongAdder()
        val copyMillis = SummaryHistogram()
        val copiesServed = LongAdder()
        val pullsServed = LongAdder()
    }

    private val counters = ConcurrentHashMap<String, Counters>()

    /** The counts of the database [name]. */
    fun of(name: String): Counters = counters.computeIfAbsent(name) { Counters() }

    /** Forgets the counts of the database [name], which was dropped. */
    fun forget(name: String) {
        counters.remove(name)
    }

    override fun dbms(metrics: MetricRegistry.MetricGroup) {
        metrics.gauge("dozendb.cluster.members", "Voting members of the cluster, as known here.") {
            runCatching { consensus.members(systemDatabase).size }.getOrNull()
        }
    }

    override fun database(database: AbstractDatabase, metrics: MetricRegistry.MetricGroup) {
        val id = database.namedDatabaseId
        val ref = DatabaseRef(id.name(), id.databaseId().uuid())
        val progress = RecentProgress(consensus, ref)
        fun gauge(name: String, description: String, read: (GroupProgress) -> Number?) =
            metrics.gauge("cluster.raft.$name", description) { progress.get()?.let(read) }
        gauge("append_index", "The index of the last entry in this member's Raft log.") {
            it.appendIndex
        }
        gauge("commit_index", "The index of the last Raft entry known committed.") {
            it.commitIndex
        }
        gauge("applied_index", "The index of the last Raft entry applied to the database.") {
            it.appliedIndex
        }
        gauge("prune_index", "The index of the first entry still in this member's Raft log.") {
            it.pruneIndex
        }
        gauge("term", "The Raft term.") { it.term }
        gauge("is_leader", "1 on the member that leads the database and takes writes, else 0.") {
            if (it.leader) 1 else 0
        }
        gauge("last_leader_message", "Milliseconds since this member last heard from the leader.") {
            it.sinceLeaderContactMillis
        }
        val counts = of(id.name())
        fun counter(name: String, description: String, read: () -> Long) =
            metrics.counter(name, description, read)
        counter(
            "cluster.raft.replication_new",
            "Transactions this member asked Raft to replicate.",
        ) {
            counts.replicationNew.sum()
        }
        // DozenDB submits each transaction once.
        counter("cluster.raft.replication_attempt", "Attempts to replicate a transaction.") {
            counts.replicationNew.sum()
        }
        counter("cluster.raft.replication_success", "Transactions replicated and applied.") {
            counts.replicationSuccess.sum()
        }
        counter("cluster.raft.replication_fail", "Transactions refused before replicating.") {
            counts.replicationFail.sum()
        }
        counter("cluster.raft.replication_maybe", "Transactions whose outcome is unknown.") {
            counts.replicationMaybe.sum()
        }
        metrics.histogram(
            "dozendb.cluster.replication.latency.millis",
            "Milliseconds from asking Raft to replicate a transaction until it was applied.",
            counts.replicationMillis,
        )
        counter("dozendb.cluster.raft.snapshots", "Raft snapshots taken.") {
            progress.get()?.snapshotsTaken ?: 0
        }
        counter("dozendb.cluster.raft.leader_changes", "Leaders this member learned of.") {
            progress.get()?.leaderChanges ?: 0
        }
        counter(
            "dozendb.cluster.raft.leadership_transfers",
            "Leaderships this member transferred to another member.",
        ) {
            progress.get()?.leadershipTransfers ?: 0
        }
        metrics.gauge(
            "dozendb.cluster.raft.followers",
            "On the leader, the followers it sends to.",
        ) {
            progress.get()?.followerMatchIndexes?.size
        }
        metrics.gauge(
            "dozendb.cluster.raft.follower_lag_max",
            "On the leader, the most committed entries a follower does not have yet.",
        ) {
            progress.get()?.let { p ->
                p.followerMatchIndexes?.values?.maxOfOrNull {
                    (p.commitIndex - it).coerceAtLeast(0)
                }
            }
        }
        counter(
            "dozendb.cluster.store_copy.received",
            "Copies of the database this member installed, to join the cluster or catch up.",
        ) {
            counts.copiesReceived.sum()
        }
        counter(
            "dozendb.cluster.store_copy.repeated",
            "Copies needed within an hour of the previous one: this member does not keep up.",
        ) {
            counts.copiesRepeated.sum()
        }
        metrics.histogram(
            "dozendb.cluster.store_copy.received.millis",
            "Milliseconds a member behind took to catch up with a copy: from needing it until " +
                "the database ran again.",
            counts.copyMillis,
        )
        counter(
            "dozendb.cluster.store_copy.served",
            "Copies of the database sent to other members.",
        ) {
            counts.copiesServed.sum()
        }
        counter(
            "cluster.catchup.tx_pull_requests_received",
            "Pulls of transactions that secondaries sent this member.",
        ) {
            counts.pullsServed.sum()
        }
    }

    /** The progress of one group, read at most once a second: every Raft gauge reads it. */
    private class RecentProgress(
        private val consensus: ConsensusLog,
        private val ref: DatabaseRef,
    ) {
        private var progress: GroupProgress? = null
        private var readAt = 0L

        @Synchronized
        fun get(): GroupProgress? {
            val now = System.nanoTime()
            if (readAt == 0L || now - readAt >= MAX_AGE_NANOS) {
                progress = runCatching { consensus.progress(ref) }.getOrNull()
                readAt = now
            }
            return progress
        }

        companion object {
            private val MAX_AGE_NANOS = TimeUnit.SECONDS.toNanos(1)
        }
    }
}
