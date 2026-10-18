package org.dozendb.cluster.api

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

    /** Ids of the groups that have local consensus state. */
    public fun localGroups(): Set<UUID>

    /**
     * Starts the group of [database] if it is not running here, and attaches [stateMachine] to it,
     * which then receives every entry it has not applied yet.
     *
     * If the group has local state it is recovered and [bootstrapMembers] is ignored. Otherwise the
     * group is created with exactly [bootstrapMembers] as its voting members, which must be the
     * same list on every one of them.
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

    /** The local view of the group of [database]. */
    public fun status(database: DatabaseRef): GroupStatus

    /** The local member's role in the group of [database]. */
    public fun role(database: DatabaseRef): Role = status(database).role

    /** The leader of the group of [database] as known locally, if any. */
    public fun leader(database: DatabaseRef): MemberId? = status(database).leader

    /** The current voting members of the group of [database]. */
    public fun members(database: DatabaseRef): List<Member>

    /**
     * Registers [listener] for changes of the local role or the known leader of [database]. It is
     * called on an internal thread and must not block. Closing the returned handle unregisters it.
     */
    public fun addStatusListener(
        database: DatabaseRef,
        listener: (GroupStatus) -> Unit,
    ): AutoCloseable

    /**
     * Adds [member] as a voting member of the group of [database].
     *
     * @throws ConsensusUnavailableException if the change could not be committed.
     */
    public suspend fun addMember(database: DatabaseRef, member: Member)

    /**
     * Removes the member [id] from the group of [database].
     *
     * @throws ConsensusUnavailableException if the change could not be committed.
     */
    public suspend fun removeMember(database: DatabaseRef, id: MemberId)
}
