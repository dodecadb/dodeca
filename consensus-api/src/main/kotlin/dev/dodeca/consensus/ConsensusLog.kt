package dev.dodeca.consensus

import java.time.Duration
import java.util.UUID

/**
 * What a member reports about itself when probed.
 *
 * @property metadata facts used to decide compatibility and formation, such as the Neo4j version,
 *   as they are at the time of the probe.
 * @property groups the consensus groups this member already hosts, with their current members.
 */
public data class PeerInfo(
    val member: Member,
    val metadata: Map<String, String>,
    val groups: Map<UUID, List<Member>>,
)

/**
 * The voting members of a consensus group as this member knows them.
 *
 * @property members the voting members; while a change is in progress, the members it changes to. A
 *   follower that installs a snapshot may briefly know an older configuration.
 * @property settled whether no change is in progress and this member knows that [members] are
 *   committed.
 * @property removed whether this member was removed from the group, which then stopped running
 *   here.
 */
public data class GroupMembership(
    val members: List<Member>,
    val settled: Boolean,
    val removed: Boolean,
)

/**
 * A connection between this member and another that is open now.
 *
 * @property inbound whether the other member opened it, to this member's consensus port; else this
 *   member did.
 * @property remoteAddress the other end: an inbound connection's port is one the other member
 *   chose, an outbound one's the other member's consensus port.
 * @property protocols the protocols it carried so far: `raft`, the consensus library's own,
 *   `probe`, `bulk` and the names of the bulk transfers ([BulkTransfer]).
 * @property tls whether it is encrypted.
 */
public data class MemberConnection(
    val inbound: Boolean,
    val remoteAddress: HostPort,
    val protocols: Set<String>,
    val tls: Boolean,
)

/** The consensus layer could not complete an operation, for example because there is no leader. */
public class ConsensusUnavailableException(message: String, cause: Throwable? = null) :
    Exception(message, cause)

/**
 * Replicated, majority-committed logs, one consensus group per database.
 *
 * Implementations hide the consensus library completely; no library type appears in this API.
 * Methods that wait on the network are `suspend` functions. Callers on Neo4j kernel threads bridge
 * with `runBlocking`, and coroutines never call into Neo4j.
 */
public interface ConsensusLog : BulkTransfer, AutoCloseable {
    /** This member. */
    public val self: Member

    /** Starts serving consensus traffic. Groups found in local storage are recovered. */
    public fun start()

    /**
     * Asks the member at [address] who it is and which groups it hosts.
     *
     * @throws ConsensusUnavailableException if the member cannot be reached.
     */
    public suspend fun probe(address: HostPort): PeerInfo

    /**
     * Tells the member at [address], unasked, what this member answers when probed ([probe]): for a
     * change the others should learn before they next probe it.
     *
     * @throws ConsensusUnavailableException if the member cannot be reached or takes no
     *   announcements, as a version without them.
     */
    public suspend fun announce(address: HostPort)

    /**
     * Registers [listener] for what other members announce about themselves ([announce]). It is
     * called on an internal thread and must not block. Closing the returned handle unregisters it.
     */
    public fun addAnnouncementListener(listener: (PeerInfo) -> Unit): AutoCloseable

    /** Ids of the groups that have local consensus state. */
    public fun localGroups(): Set<UUID>

    /**
     * Starts the group of [database] if it is not running here, and attaches [stateMachine] to it,
     * which then receives every entry it has not applied yet. A group that stopped here because an
     * entry failed to apply starts again, from its local state.
     *
     * If the group has local state it is recovered and [bootstrapMembers] is ignored. Otherwise the
     * group is created with [bootstrapMembers] as its voting members. If they include this member,
     * they must be the same list on every member they include: the group's initial members. A
     * member that is not among them takes part only once it is added with [addMember].
     */
    public fun startGroup(
        database: DatabaseRef,
        stateMachine: ReplicatedStateMachine,
        bootstrapMembers: List<Member>,
    )

    /**
     * Starts the group of [database] if it is not running here, without a state machine: this
     * member votes and stores entries, which are applied once [startGroup] attaches one. Bootstrap
     * members work as in [startGroup].
     */
    public fun joinGroup(database: DatabaseRef, bootstrapMembers: List<Member>)

    /**
     * Stops the group of [database] on this member and deletes its local consensus state, after its
     * database was dropped. The other members keep their own state until they remove the group too.
     * A group that is not running here only has its local state deleted.
     */
    public fun removeGroup(database: DatabaseRef)

    /**
     * Detaches the state machine of [database], for example while its database is stopped. The
     * group keeps running on this member, which keeps voting and storing entries; they are applied
     * once a state machine is attached again with [startGroup]. Returns once no entry is being
     * applied.
     *
     * With [handOverLeadership], this member hands leadership to another member whenever it leads
     * meanwhile, so that the group keeps committing. Without it, it keeps leading, as when the
     * database stops on every member.
     */
    public fun detachGroup(database: DatabaseRef, handOverLeadership: Boolean = true)

    /**
     * Chooses whom this member hands leadership over to after [detachGroup]. Configuration order
     * until set.
     */
    public var leaderPlacement: LeaderPlacement

    /**
     * Transfers the leadership of the group of [database], which this member leads, to the voting
     * member [to]. [to] takes over once its log holds every entry of this member's, which it may
     * have to catch up on first, within [timeout]. Meanwhile this member refuses new entries:
     * [submit] returns [CommitResult.NotLeader]. The transfer is logged with [reason], which
     * completes "Transferred leadership of <database> to <member>: ".
     *
     * @return whether [to] leads the group afterwards; false, after logging why, if this member did
     *   not lead it, [to] did not take over within [timeout], or a membership change is in
     *   progress.
     */
    public suspend fun transferLeadership(
        database: DatabaseRef,
        to: MemberId,
        reason: String,
        timeout: Duration,
    ): Boolean

    /**
     * The state of the local state machine of [database] as a snapshot: the last entry applied and
     * the state machine's applied index, read together. Null if the group is not running here. A
     * copy of the state machine taken after this call holds every entry up to the snapshot.
     */
    public fun currentSnapshot(database: DatabaseRef): Snapshot?

    /**
     * The latest snapshot of the group of [database] on this member: the local log may lack entries
     * up to it. Null if there is none. Also available while the group is stopped.
     */
    public fun latestSnapshot(database: DatabaseRef): Snapshot?

    /**
     * Records that the detached state machine of [database] was replaced by a copy holding
     * [snapshot], as [ReplicatedStateMachine.onSnapshotNeeded] asks. If this member fell behind the
     * start of the leader's log and [snapshot] reaches it, the group continues after the snapshot;
     * otherwise the local log holds the entries after the copy's and nothing changes.
     *
     * @return whether the group continues after [snapshot].
     * @throws IllegalArgumentException if [snapshot] is behind [latestSnapshot].
     * @throws IllegalStateException if the group is not running here or a state machine is
     *   attached.
     */
    public fun installSnapshot(database: DatabaseRef, snapshot: Snapshot): Boolean

    /**
     * Tells that this member holds a copy of the state of [database] that holds every entry up to
     * [snapshot], made before the group ran here, as a member that is being added copies the
     * databases it needs first. When the group's leader asks this member for a copy that [snapshot]
     * covers, and the state machine still holds it, [snapshot] is installed instead of asking the
     * state machine for another copy (see [ReplicatedStateMachine.onSnapshotNeeded]).
     */
    public fun seed(database: DatabaseRef, snapshot: Snapshot)

    /**
     * The term of the entry at [index] in this member's log of [database], or of its latest
     * snapshot if that ends at [index]; null if this member holds neither, as when its log no
     * longer holds the entry, or the group does not run here.
     */
    public fun termOf(database: DatabaseRef, index: Long): Long?

    /** The largest payload, in bytes, that [submit] accepts. The same on every member. */
    public val maxEntrySize: Int

    /**
     * Appends [payload] to the log of [database] and suspends until it is committed by a majority
     * and applied by the local state machine. Only the leader accepts entries. The caller must not
     * modify [payload] afterwards.
     *
     * @throws IllegalArgumentException if [payload] is larger than [maxEntrySize].
     */
    public suspend fun submit(database: DatabaseRef, payload: ByteArray): CommitResult

    /**
     * Returns a log index such that every entry committed before this call is at or below it, after
     * waiting until the local state machine has applied up to that index. Used for linearizable
     * reads, on the leader or a follower.
     *
     * @throws ConsensusUnavailableException if no leader confirms its commit index in time.
     */
    public suspend fun readIndex(database: DatabaseRef): Long

    /**
     * Suspends until the local member applied the entry at [index] of the log of [database], or
     * installed a snapshot past it. An entry that the state machine skips counts as applied. Waits
     * while no state machine applies the group's entries, as while its database is stopped.
     *
     * @throws ConsensusUnavailableException if the group does not run here.
     */
    public suspend fun awaitApplied(database: DatabaseRef, index: Long)

    /** The local view of the group of [database]. */
    public fun status(database: DatabaseRef): GroupStatus

    /** The local member's progress in the group of [database]; null when it does not run here. */
    public fun progress(database: DatabaseRef): GroupProgress?

    /** The local member's role in the group of [database]. */
    public fun role(database: DatabaseRef): Role = status(database).role

    /** The leader of the group of [database] as known locally, if any. */
    public fun leader(database: DatabaseRef): MemberId? = status(database).leader

    /**
     * The voting members of the group of [database] as this member knows them.
     *
     * @throws ConsensusUnavailableException if the group is not running here.
     */
    public fun membership(database: DatabaseRef): GroupMembership

    /** The voting members of the group of [database]; see [membership]. */
    public fun members(database: DatabaseRef): List<Member> = membership(database).members

    /**
     * Registers [listener] for changes of the local role or the known leader of [database]. It is
     * called on an internal thread and must not block. Closing the returned handle unregisters it.
     */
    public fun addStatusListener(
        database: DatabaseRef,
        listener: (GroupStatus) -> Unit,
    ): AutoCloseable

    /**
     * Adds [member] as a voting member of the group of [database], through the group's leader,
     * which first brings it up to date with the log; this may ask [member]'s state machine for a
     * copy (see [ReplicatedStateMachine.onSnapshotNeeded]). [member] must already run the group,
     * with [startGroup] or [joinGroup] and bootstrap members without itself. Returns once the
     * change is committed, at once if [member] is a member already. Only one change at a time is
     * possible in a group.
     *
     * A member that does not run the group changes it through [members], the group's voting members
     * as the members that run it report them (see [PeerInfo.groups]); where the group runs, its own
     * view is used instead.
     *
     * @throws ConsensusUnavailableException if the change was not committed: no leader answered,
     *   which is looked for as long as one request may take, not up to [timeout]; another change is
     *   in progress; or the leader did not commit it within [timeout], for example because [member]
     *   does not run the group or stopped responding. Also if the group does not run here and
     *   [members] is empty.
     */
    public suspend fun addMember(
        database: DatabaseRef,
        member: Member,
        timeout: Duration,
        members: List<Member> = emptyList(),
    )

    /**
     * Removes the member [id] from the group of [database], through the group's leader. Returns
     * once the change is committed, at once if [id] is not a member as far as this member knows.
     * The removed member stops running the group (see [GroupMembership.removed]).
     *
     * A member that does not run the group changes it through [members], as for [addMember]; the
     * change then applies only if they are still the group's members.
     *
     * @throws ConsensusUnavailableException if the change was not committed: no leader answered,
     *   which is looked for as long as one request may take, not up to [timeout]; another change is
     *   in progress; this member's view of the members is outdated; or the leader did not commit it
     *   within [timeout]. Also if the group does not run here and [members] is empty.
     */
    public suspend fun removeMember(
        database: DatabaseRef,
        id: MemberId,
        timeout: Duration,
        members: List<Member> = emptyList(),
    )

    /**
     * The connections between this member and others that are open now: those opened to it, those
     * it opened for its own calls while the calls last, and those of the groups it leads to the
     * members it replicates to, while they answer.
     */
    public fun connections(): List<MemberConnection> = emptyList()
}
