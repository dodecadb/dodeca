package org.dozendb.cluster.internal

import java.time.Duration
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.dozendb.consensus.ConsensusLog
import org.dozendb.consensus.ConsensusUnavailableException
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.MemberId
import org.dozendb.core.servers.AllocationException
import org.dozendb.core.servers.DroppedServer
import org.dozendb.core.servers.Placements
import org.dozendb.core.servers.ServerOptions
import org.dozendb.core.servers.ServerRecord
import org.dozendb.core.servers.ServerRecords
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Transaction
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
 * - The record of a server that left `system` is deleted, with the databases placed on it.
 * - A server that DROP SERVER dropped is removed from `system` (see [remove]), and is never enabled
 *   again. Once it left, it is asked whether it still answers where it was, as SHOW SERVERS shows
 *   it Dropped while it does; its address is forgotten once it no longer does.
 * - Once the initial servers are recorded, a database not placed yet is placed (see [Placements]),
 *   best effort, on the default number of servers, those that hold it first: the default database
 *   of a new cluster, and the databases of a member that formed the cluster with databases of its
 *   own, which it holds alone until then. Such a database is placed only once a member holds it,
 *   unless it has no creator yet, as the default database of a new cluster, which the creator named
 *   among the servers it is placed on then creates.
 *
 * It looks when this member becomes the leader, after each applied `system` transaction, and while
 * work remains, again after [retryDelay], twice as long each time up to [maximumRetryDelay].
 *
 * @property report what a member reports about itself, this member included; null while unknown.
 * @property autoEnable the first value of `auto_enable_free_servers`, from
 *   `initial.dbms.automatically_enable_free_servers`.
 * @property current whether this member's system graph holds every committed transaction, as the
 *   leader confirms: a new leader may not have applied the last ones of the previous leader yet.
 * @property holders the members that vote in a database's group, as far as known.
 * @property available whether a member answers this one, so that copies go to those that do first.
 * @property defaults the default numbers of copies the configuration sets, recorded with the
 *   initial servers.
 * @property remove removes a member from the cluster, returning once the change is committed.
 */
class ServerReconciler(
    private val consensus: ConsensusLog,
    private val systemDatabase: DatabaseRef,
    private val system: () -> GraphDatabaseService,
    private val reading: (() -> Unit) -> Unit,
    private val leading: () -> Boolean,
    private val current: () -> Boolean,
    private val report: (MemberId) -> Map<String, String>?,
    private val autoEnable: Boolean,
    private val log: InternalLog,
    private val holders: (DatabaseRef) -> List<MemberId> = { emptyList() },
    private val available: (MemberId) -> Boolean = { true },
    private val defaults: Placements.Defaults = Placements.Defaults(1, 0),
    private val remove: (MemberId) -> Unit = {},
    private val retryDelay: Duration = Duration.ofSeconds(1),
    private val maximumRetryDelay: Duration = Duration.ofSeconds(30),
) : AutoCloseable {
    private val scheduler: ScheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor { runnable ->
            Thread(runnable, "dozendb-servers").apply { isDaemon = true }
        }
    private val requested = AtomicBoolean(false)
    private var statusListener: AutoCloseable? = null

    @Volatile private var closed = false

    /** Databases that no server could be placed on, logged once; scheduler only. */
    private val unplaceable = mutableSetOf<String>()

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
                // Closing interrupts the look in progress.
                if (closed) return
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

    /**
     * Sets whether servers that join are enabled at once, true enabling those Free now, as
     * `dbms.cluster.setAutomaticallyEnableFreeServers` does. Runs on the `system` leader.
     *
     * @throws IllegalStateException if this member does not lead `system`.
     */
    fun setAutoEnable(autoEnable: Boolean) {
        check(leading()) {
            "This member does not lead the system database; run it on the one that does."
        }
        reading {
            system().beginTx().use { tx ->
                ServerRecords.lock(tx)
                ServerRecords.setAutoEnable(tx, autoEnable)
                tx.commit()
            }
        }
        log.info("Servers that join are enabled at once: %s", autoEnable)
        request()
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
        var dropped = emptyMap<UUID, DroppedServer>()
        reading { system().beginTx().use { tx -> dropped = ServerRecords.dropped(tx) } }
        // Asked before the records' lock is taken, as one that does not answer takes a while.
        val left = dropped.values.filter { it.address != null && MemberId(it.id) !in members }
        val gone = left.filterNot(::answers)
        // One that still answers is asked again later.
        var again = gone.size < left.size
        reading {
            system().beginTx().use { tx ->
                ServerRecords.lock(tx)
                val records = ServerRecords.records(tx)
                var changed = ServerRecords.removeDuplicates(tx)
                for (id in records.keys - members.map { it.uuid }.toSet()) {
                    log.info("Deleting the record of server %s, which left the cluster", id)
                    ServerRecords.delete(tx, id)
                    changed = true
                }
                for (server in gone) {
                    log.info(
                        "Dropped server %s no longer answers at %s",
                        server.id,
                        server.address,
                    )
                    ServerRecords.forgetAddress(tx, server.id)
                    changed = true
                }
                val initial = ServerRecords.initialServersEnabled(tx)
                if (!initial || ServerRecords.autoEnable(tx, autoEnable)) {
                    val (enabled, all) = enable(tx, members, joined = initial)
                    changed = enabled || changed
                    again = !all || again
                }
                if (!initial && !again) {
                    log.info("Recorded the servers the cluster started with, %s", members)
                    markInitial(tx)
                    changed = true
                }
                if (ServerRecords.initialServersEnabled(tx)) {
                    val (placed, waiting) = placeUnplaced(tx)
                    changed = placed || changed
                    again = waiting || again
                }
                if (changed) tx.commit()
            }
        }
        for (member in members.filter { it.uuid in dropped }) {
            log.info("Removing dropped server %s from the cluster", member)
            remove(member)
            again = true
        }
        return again
    }

    /** Whether the dropped [server] answers at its address, as itself. */
    private fun answers(server: DroppedServer): Boolean {
        val address = server.address?.let { runCatching { HostPort.parse(it) }.getOrNull() }
        return try {
            address != null &&
                runBlocking {
                    withTimeoutOrNull(PROBE_DEADLINE.toMillis()) { consensus.probe(address) }
                }
                    ?.member
                    ?.id
                    ?.uuid == server.id
        } catch (_: ConsensusUnavailableException) {
            false
        }
    }

    /**
     * Records, in [tx] on the `system` leader, the servers the cluster started with, if they are
     * not yet, as this reconciler does once it has their reports, so that a command can place
     * databases on them at once.
     *
     * @throws AllocationException if a member's options are not known yet, or the cluster's members
     *   change.
     */
    fun recordInitial(tx: Transaction) {
        if (ServerRecords.initialServersEnabled(tx)) return
        val membership =
            try {
                consensus.membership(systemDatabase)
            } catch (e: ConsensusUnavailableException) {
                throw AllocationException("the cluster's members are not known: ${e.message}")
            }
        if (!membership.settled) {
            throw AllocationException("the cluster's members are changing: try again in a moment")
        }
        val members = membership.members.map { it.id }
        val (_, all) = enable(tx, members, joined = false)
        if (!all) {
            throw AllocationException(
                "not every server the cluster started with has reported its initial options " +
                    "yet: try again in a moment"
            )
        }
        log.info("Recorded the servers the cluster started with, %s", members)
        markInitial(tx)
    }

    /**
     * Enables the [members] without a record, with the options they report; [joined] if they join a
     * cluster whose initial servers are recorded. Returns whether it recorded any, and whether
     * every member has a record now.
     */
    private fun enable(
        tx: Transaction,
        members: List<MemberId>,
        joined: Boolean,
    ): Pair<Boolean, Boolean> {
        val records = ServerRecords.records(tx)
        val dropped = ServerRecords.dropped(tx)
        var changed = false
        var all = true
        for (member in members.filter { it.uuid !in records && it.uuid !in dropped }) {
            val options = initialOptions(member)
            if (options == null) {
                all = false
                continue
            }
            log.info(
                "Enabling server %s with its initial options %s%s",
                member,
                options,
                if (joined) ", as servers that join are enabled at once" else "",
            )
            ServerRecords.write(tx, ServerRecord(member.uuid, options = options))
            changed = true
        }
        return changed to all
    }

    private fun markInitial(tx: Transaction) {
        ServerRecords.markInitialServersEnabled(tx, autoEnable)
        if (Placements.defaults(tx, NONE) == NONE) Placements.setDefaults(tx, defaults)
    }

    /**
     * Places the databases not placed yet, best effort, on as many servers as their topology asks
     * for, or the default number if larger, as a server that formed a cluster with databases of its
     * own recorded one primary for each; returns whether it placed any, and whether one waits for a
     * member to hold it.
     */
    private fun placeUnplaced(tx: Transaction): Pair<Boolean, Boolean> {
        var placed = false
        var waiting = false
        val defaultPrimaries = Placements.defaults(tx, defaults).primaries
        for (database in Placements.placed(tx).filter { it.hosts.isEmpty() }) {
            val node = Placements.database(tx, database.name) ?: continue
            val ref =
                DatabaseRef(
                    database.name,
                    UUID.fromString(
                        node.getProperty(TopologyGraphDbmsModel.DATABASE_UUID_PROPERTY) as String
                    ),
                )
            val creator = Neo4jSystemGraph.creator(node)
            val holding = holders(ref)
            if (creator != null && holding.isEmpty()) {
                // Its creator holds its store, and has not told it runs its group yet.
                waiting = true
                continue
            }
            val prefer = (listOfNotNull(creator) + holding).distinct().map { it.uuid }
            val hosts =
                try {
                    Placements.allocator(tx) { available(MemberId(it)) }
                        .place(
                            database.name,
                            maxOf(database.primaries, defaultPrimaries),
                            prefer = prefer,
                            bestEffort = true,
                        )
                } catch (e: AllocationException) {
                    if (unplaceable.add(database.name)) {
                        log.warn(
                            "Database %s cannot be placed on any server: %s. It stays where it " +
                                "is until a server can host it",
                            database.name,
                            e.message,
                        )
                    }
                    continue
                }
            unplaceable -= database.name
            log.info("Placing database %s on servers %s", database.name, hosts)
            Placements.place(tx, node, hosts)
            placed = true
        }
        return placed to waiting
    }

    /** The options [member] reports from its `initial.server.*` settings, or null if unknown. */
    private fun initialOptions(member: MemberId): ServerOptions? {
        return report(member)?.let(InitialServerOptions::reported)
    }

    /**
     * Stops looking. Waits for a look in progress, which reads the system graph, so that the system
     * database can stop once this returns.
     */
    override fun close() {
        closed = true
        statusListener?.close()
        scheduler.shutdownNow()
        scheduler.awaitTermination(30, TimeUnit.SECONDS)
    }

    private companion object {
        /** No default numbers of copies recorded, as [Placements.defaults] tells. */
        val NONE = Placements.Defaults(0, 0)

        /** How long a dropped server may take to answer. */
        val PROBE_DEADLINE: Duration = Duration.ofSeconds(2)
    }
}
