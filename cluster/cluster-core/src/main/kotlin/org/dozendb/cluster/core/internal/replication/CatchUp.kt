package org.dozendb.cluster.core.internal.replication

import java.time.Duration
import org.dozendb.cluster.api.ConsensusLog
import org.dozendb.cluster.api.DatabaseRef
import org.dozendb.cluster.api.HostPort
import org.dozendb.cluster.api.Member
import org.dozendb.cluster.api.Snapshot
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.logging.InternalLog

/** Stops and starts databases: DozerDB's database manager. */
internal interface DatabaseControl {
    fun stop(id: NamedDatabaseId)

    /** Starts the database; returns whether it is running. */
    fun start(id: NamedDatabaseId): Boolean
}

/** Replaces a stopped replicated database with a copy and returns the Raft snapshot it holds. */
internal fun interface DatabaseCopier {
    fun copy(source: HostPort, name: String, minimumIndex: Long): Snapshot
}

/**
 * Brings a replicated database up to date when its group's log no longer holds entries it needs:
 * stops the database, which detaches its group, replaces it with a copy holding every entry up to
 * the requested index from the leader or another member, records the copy's snapshot, and starts
 * the database again. Copies are retried until one succeeds or the thread is interrupted.
 */
internal class CatchUp(
    private val consensus: ConsensusLog,
    private val systemDatabase: DatabaseRef,
    private val control: () -> DatabaseControl,
    private val copier: DatabaseCopier,
    private val log: InternalLog,
    private val retryDelay: Duration = Duration.ofSeconds(2),
) {
    fun run(replica: ReplicatedDatabase, leader: Member?, minimumIndex: Long) {
        val ref = replica.ref
        log.warn(
            "Database %s needs Raft entries up to %d that the log no longer holds; replacing it " +
                "with a copy from %s",
            ref.name,
            minimumIndex,
            leader?.raftAddress ?: "another member",
        )
        control().stop(replica.namedDatabaseId)
        // The shutdown event detached the group; detach it also if the database was not running.
        replica.stop()
        try {
            replace(ref, leader, minimumIndex)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            log.warn("Replacing database %s was interrupted; it stays stopped", ref.name)
            return
        } catch (e: Exception) {
            log.error("Replacing database ${ref.name} failed; starting it as it is", e)
        }
        if (control().start(replica.namedDatabaseId)) {
            log.info("Database %s runs again", ref.name)
        } else {
            log.error("Database %s did not start after it was replaced", ref.name)
        }
    }

    private fun replace(ref: DatabaseRef, leader: Member?, minimumIndex: Long) {
        while (true) {
            // Never install behind the group's own snapshot.
            val required = maxOf(minimumIndex, consensus.latestSnapshot(ref)?.position?.index ?: -1)
            val snapshot = copyFromAnyMember(ref.name, leader, required)
            if (snapshot != null) {
                val skipped = consensus.installSnapshot(ref, snapshot)
                log.info(
                    "Database %s now holds Raft snapshot %s; %s",
                    ref.name,
                    snapshot,
                    if (skipped) "the group continues after it"
                    else "the group continues from its local log",
                )
                return
            }
            Thread.sleep(retryDelay.toMillis())
        }
    }

    /** Copies from the leader first, then from the other members; null if all of them failed. */
    private fun copyFromAnyMember(name: String, leader: Member?, minimumIndex: Long): Snapshot? {
        val candidates =
            (listOfNotNull(leader) + consensus.members(systemDatabase))
                .filter { it.id != consensus.self.id }
                .distinctBy { it.id }
        for (member in candidates) {
            try {
                return copier.copy(member.raftAddress, name, minimumIndex)
            } catch (e: InterruptedException) {
                throw e
            } catch (e: Exception) {
                log.warn(
                    "Copying database %s from %s failed: %s",
                    name,
                    member.raftAddress,
                    e.message,
                )
            }
        }
        return null
    }
}
