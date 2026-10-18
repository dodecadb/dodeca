package org.dozendb.cluster.api

import java.util.UUID

/**
 * What a member reports about itself when probed.
 *
 * @property metadata facts used to decide compatibility, such as the Neo4j version.
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
public interface ConsensusLog : AutoCloseable {
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
     * Starts the group of [database] and attaches [stateMachine] to it.
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

    /** Stops taking part in the group of [database]. Its local state is kept. */
    public fun stopGroup(database: DatabaseRef)

    /**
     * Appends [payload] to the log of [database] and suspends until it is committed by a majority
     * and applied by the local state machine. Only the leader accepts entries.
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
