package org.dozendb.cluster.internal.replication

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import org.dozendb.cluster.internal.ClusterMetrics
import org.dozendb.consensus.ConsensusLog
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.Member
import org.dozendb.consensus.Snapshot
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.logging.InternalLog

/** Stops and starts databases: the database manager. */
internal interface DatabaseControl {
    fun stop(id: NamedDatabaseId)

    /** Starts the database; returns whether it is running. */
    fun start(id: NamedDatabaseId): Boolean
}

/** A copy of a replicated database next to it, holding [snapshot], ready to replace it. */
internal interface StagedCopy {
    val snapshot: Snapshot

    /** Replaces the database, which must be stopped, with this copy. */
    fun moveIntoPlace()
}

/** Copies a replicated database next to it while it keeps running. */
internal fun interface DatabaseCopier {
    /** A copy of [database] from [source] holding every Raft entry up to [minimumIndex]. */
    fun stage(source: HostPort, database: DatabaseRef, minimumIndex: Long): StagedCopy
}

/**
 * Brings a replicated database up to date when its group's log no longer holds entries it needs. A
 * copy holding every entry up to the requested index is first fetched next to the database from the
 * leader or another member, retried until one succeeds or the thread is interrupted. The database
 * keeps serving reads meanwhile. Only then is it stopped, which detaches its group, replaced with
 * the copy, whose snapshot is recorded, and started again.
 *
 * A database that needs another copy within [REPEAT_WARNING] of the previous one gets a warning:
 * its member does not keep up with the others. [metrics], when given, counts repeated copies and
 * how long catching up took.
 */
internal class CatchUp(
    private val consensus: ConsensusLog,
    private val systemDatabase: DatabaseRef,
    private val control: () -> DatabaseControl,
    private val copier: DatabaseCopier,
    private val log: InternalLog,
    private val retryDelay: Duration = Duration.ofSeconds(2),
    private val clock: Clock = Clock.systemUTC(),
    private val metrics: ClusterMetrics? = null,
) {
    /** When each database, by group id, last started again after it was replaced. */
    private val replaced = ConcurrentHashMap<UUID, Instant>()

    /**
     * Replaces [replica] with a copy holding every entry up to [minimumIndex]. [exclusively] runs
     * the part from stopping the database to starting it again, for example under a lock that
     * readers of the database hold.
     */
    fun run(
        replica: ReplicatedDatabase,
        leader: Member?,
        minimumIndex: Long,
        exclusively: (() -> Unit) -> Unit = { it() },
    ) {
        val ref = replica.ref
        val started = System.nanoTime()
        log.warn(
            "Database %s needs Raft entries up to %d that the log no longer holds; replacing it " +
                "with a copy from %s",
            ref.name,
            minimumIndex,
            leader?.raftAddress ?: "another member",
        )
        warnIfRepeated(ref)
        val copy =
            try {
                stage(ref, leader, minimumIndex)
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                log.warn("Copying database %s was interrupted; it keeps running", ref.name)
                return
            }
        exclusively { replace(replica, copy, started) }
    }

    private fun warnIfRepeated(ref: DatabaseRef) {
        val previous = replaced[ref.id] ?: return
        val since = Duration.between(previous, clock.instant())
        if (since < REPEAT_WARNING) {
            metrics?.of(ref.name)?.copiesRepeated?.increment()
            log.warn(
                "Database %s needs another copy %d s after the previous one. This member does " +
                    "not keep up with the others: it keeps losing its connection to them, or its " +
                    "links to them are slower than the cluster writes.",
                ref.name,
                since.seconds,
            )
        }
    }

    /** Stops the database, moves [copy] into place and starts the database again. */
    private fun replace(replica: ReplicatedDatabase, copy: StagedCopy, started: Long) {
        val ref = replica.ref
        control().stop(replica.namedDatabaseId)
        // The shutdown event detached the group; detach it also if the database was not running.
        replica.stop()
        try {
            install(ref, copy)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            log.warn("Replacing database %s was interrupted; it stays stopped", ref.name)
            return
        } catch (e: Exception) {
            log.error("Replacing database ${ref.name} failed; starting it as it is", e)
        }
        if (control().start(replica.namedDatabaseId)) {
            log.info("Database %s runs again", ref.name)
            metrics
                ?.of(ref.name)
                ?.copyMillis
                ?.update(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started))
        } else {
            log.error("Database %s did not start after it was replaced", ref.name)
        }
        replaced[ref.id] = clock.instant()
    }

    private fun stage(ref: DatabaseRef, leader: Member?, minimumIndex: Long): StagedCopy {
        while (true) {
            // Never install behind the group's own snapshot.
            val required = maxOf(minimumIndex, consensus.latestSnapshot(ref)?.position?.index ?: -1)
            val copy = copyFromAnyMember(ref, leader, required)
            if (copy != null) {
                log.info("Copied database %s holding Raft snapshot %s", ref.name, copy.snapshot)
                return copy
            }
            Thread.sleep(retryDelay.toMillis())
        }
    }

    private fun install(ref: DatabaseRef, copy: StagedCopy) {
        copy.moveIntoPlace()
        val skipped = consensus.installSnapshot(ref, copy.snapshot)
        log.info(
            "Database %s now holds Raft snapshot %s; %s",
            ref.name,
            copy.snapshot,
            if (skipped) "the group continues after it"
            else "the group continues from its local log",
        )
    }

    /** Copies from the leader first, then from the other members; null if all of them failed. */
    private fun copyFromAnyMember(
        database: DatabaseRef,
        leader: Member?,
        minimumIndex: Long,
    ): StagedCopy? {
        val candidates =
            (listOfNotNull(leader) + consensus.members(systemDatabase))
                .filter { it.id != consensus.self.id }
                .distinctBy { it.id }
        for (member in candidates) {
            try {
                return copier.stage(member.raftAddress, database, minimumIndex)
            } catch (e: InterruptedException) {
                throw e
            } catch (e: Exception) {
                log.warn(
                    "Copying database %s from %s failed: %s",
                    database.name,
                    member.raftAddress,
                    e.message,
                )
            }
        }
        return null
    }

    companion object {
        /**
         * A member that keeps up needs a copy only after an absence of about two checkpoints, which
         * Neo4j takes every 15 minutes by default.
         */
        val REPEAT_WARNING: Duration = Duration.ofHours(1)
    }
}
