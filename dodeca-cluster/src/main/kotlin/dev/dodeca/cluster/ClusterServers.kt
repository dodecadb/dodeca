package dev.dodeca.cluster

import dev.dodeca.consensus.ConsensusLog
import dev.dodeca.consensus.ConsensusUnavailableException
import dev.dodeca.consensus.DatabaseRef
import dev.dodeca.consensus.GroupMembership
import dev.dodeca.consensus.Member
import dev.dodeca.consensus.MemberId
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

/**
 * The servers of the cluster: the voting members of the `system` group, and the system secondaries
 * that the system graph records, which pull `system` instead of voting in it.
 *
 * A member that runs the `system` group knows its voters from its own view of the group; a system
 * secondary, which does not, from what the voters report ([reportedVoters]). The recorded
 * secondaries are read from this member's system graph ([recorded]), again only once
 * [systemChanged] tells that it changed: a caller gets them as read after the last change it could
 * see, never as read before it, also while another caller reads them; while they cannot be read, as
 * while `system` stops, [secondaries] are as last read.
 *
 * @property pullsSystem whether this member hosts `system` as a secondary, or joins as one.
 * @property reportedVoters the voters of `system` as they report them, or null while none is known.
 * @property reportedLeader the leader of `system` as the voters report it, or null while unknown.
 */
class ClusterServers(
    private val consensus: ConsensusLog,
    private val groups: DatabaseGroups,
    private val recorded: () -> List<Member> = { emptyList() },
    private val pullsSystem: () -> Boolean = { false },
    private val reportedVoters: () -> GroupMembership? = { null },
    private val reportedLeader: () -> MemberId? = { null },
) {
    /** The group of `system` (see [DatabaseGroups]). */
    private val systemDatabase: DatabaseRef
        get() = groups.system

    /** The recorded secondaries as read once `system` had changed [changes] times. */
    private class Read(val changes: Long, val secondaries: List<Member>)

    /** How many times `system` changed, as [systemChanged] counts. */
    private val changes = AtomicLong()
    private val last = AtomicReference(Read(-1, emptyList()))

    /** Has the recorded secondaries read again when next asked, as `system` changed. */
    fun systemChanged() {
        changes.incrementAndGet()
    }

    /**
     * The voting members of `system`, as this member knows them.
     *
     * @throws ConsensusUnavailableException while they are not known, as while `system` does not
     *   run here.
     */
    fun voters(): GroupMembership =
        if (pullsSystem()) {
            reportedVoters()
                ?: throw ConsensusUnavailableException("no voting member of system is known yet")
        } else {
            consensus.membership(systemDatabase)
        }

    /** The leader of `system`, as this member knows it; null while unknown. */
    fun leader(): MemberId? =
        if (pullsSystem()) {
            reportedLeader()
        } else {
            try {
                consensus.status(systemDatabase).leader
            } catch (_: ConsensusUnavailableException) {
                null
            }
        }

    /**
     * The system secondaries that the system graph records, as read since `system` last changed;
     * while they cannot be read, as last read.
     */
    fun secondaries(): List<Member> =
        try {
            currentSecondaries()
        } catch (e: Exception) {
            if (e is InterruptedException) Thread.currentThread().interrupt()
            last.get().secondaries
        }

    /**
     * The system secondaries that the system graph records, as read since `system` last changed:
     * read again if it changed since they were last read. A read that began before a later change
     * does not replace one after it.
     *
     * @throws Exception as [recorded] does, while `system` cannot be read here.
     */
    fun currentSecondaries(): List<Member> {
        val changed = changes.get()
        val known = last.get()
        if (known.changes >= changed) return known.secondaries
        val read = Read(changed, recorded())
        last.accumulateAndGet(read) { kept, new -> if (new.changes > kept.changes) new else kept }
        return read.secondaries
    }

    /**
     * Every server: the voting members of `system`, then the system secondaries.
     *
     * @throws ConsensusUnavailableException while the voting members are not known.
     */
    fun all(): List<Member> = (voters().members + secondaries()).distinctBy { it.id }

    /** Every server, or null while a change of the voting members of `system` is under way. */
    fun settled(): List<Member>? =
        voters()
            .takeIf { it.settled }
            ?.let { (it.members + secondaries()).distinctBy { m -> m.id } }
}
