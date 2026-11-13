package dev.dodeca.cluster

import dev.dodeca.consensus.ConsensusLog
import dev.dodeca.consensus.ConsensusUnavailableException
import dev.dodeca.consensus.DatabaseRef
import dev.dodeca.consensus.GroupMembership
import dev.dodeca.consensus.Member
import dev.dodeca.consensus.MemberId
import java.util.concurrent.atomic.AtomicBoolean

/**
 * The servers of the cluster: the voting members of the `system` group, and the system secondaries
 * that the system graph records, which pull `system` instead of voting in it.
 *
 * A member that runs the `system` group knows its voters from its own view of the group; a system
 * secondary, which does not, from what the voters report ([reportedVoters]). The recorded
 * secondaries are read from this member's system graph ([recorded]), again only once
 * [systemChanged] tells that it changed; while it cannot be read, as while `system` stops, they are
 * as last read.
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

    @Volatile private var secondaries: List<Member> = emptyList()
    private val stale = AtomicBoolean(true)

    /** Has the recorded secondaries read again when next asked, as `system` changed. */
    fun systemChanged() {
        stale.set(true)
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

    /** The system secondaries that the system graph records, as this member last read them. */
    fun secondaries(): List<Member> {
        if (stale.compareAndSet(true, false)) {
            try {
                secondaries = recorded()
            } catch (e: Exception) {
                stale.set(true)
                if (e is InterruptedException) Thread.currentThread().interrupt()
            }
        }
        return secondaries
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
