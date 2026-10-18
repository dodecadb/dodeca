package org.dozendb.cluster.ratis

import java.io.File
import java.io.IOException
import java.net.InetSocketAddress
import java.util.EnumSet
import java.util.UUID
import java.util.concurrent.CompletionException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.future.await
import kotlinx.coroutines.withContext
import org.apache.ratis.client.RaftClient
import org.apache.ratis.conf.Parameters
import org.apache.ratis.conf.RaftProperties
import org.apache.ratis.grpc.GrpcConfigKeys
import org.apache.ratis.grpc.server.GrpcServices
import org.apache.ratis.proto.RaftProtos.RaftPeerRole
import org.apache.ratis.protocol.ClientId
import org.apache.ratis.protocol.GroupManagementRequest
import org.apache.ratis.protocol.Message
import org.apache.ratis.protocol.RaftClientReply
import org.apache.ratis.protocol.RaftClientRequest
import org.apache.ratis.protocol.RaftGroup
import org.apache.ratis.protocol.RaftGroupId
import org.apache.ratis.protocol.RaftPeer
import org.apache.ratis.protocol.RaftPeerId
import org.apache.ratis.protocol.SetConfigurationRequest
import org.apache.ratis.protocol.exceptions.AlreadyClosedException
import org.apache.ratis.protocol.exceptions.GroupMismatchException
import org.apache.ratis.protocol.exceptions.LeaderNotReadyException
import org.apache.ratis.protocol.exceptions.NotLeaderException
import org.apache.ratis.protocol.exceptions.ServerNotReadyException
import org.apache.ratis.retry.RetryPolicies
import org.apache.ratis.server.RaftServer
import org.apache.ratis.server.RaftServerConfigKeys
import org.apache.ratis.server.storage.RaftStorage
import org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations
import org.apache.ratis.thirdparty.io.grpc.netty.NettyServerBuilder
import org.apache.ratis.util.TimeDuration
import org.dozendb.cluster.api.CommitResult
import org.dozendb.cluster.api.ConsensusLog
import org.dozendb.cluster.api.ConsensusUnavailableException
import org.dozendb.cluster.api.DatabaseRef
import org.dozendb.cluster.api.GroupStatus
import org.dozendb.cluster.api.HostPort
import org.dozendb.cluster.api.Member
import org.dozendb.cluster.api.MemberId
import org.dozendb.cluster.api.PeerInfo
import org.dozendb.cluster.api.ReplicatedStateMachine
import org.dozendb.cluster.api.Role
import org.slf4j.LoggerFactory

/**
 * [ConsensusLog] on Apache Ratis: one Raft server per member, one Raft group per database, all on
 * one gRPC port that also serves the probe used for cluster formation.
 *
 * Writes are submitted to the local server without a network hop, so only the leader accepts them.
 * A write that Ratis rejects with "not leader" is reported as [CommitResult.NotLeader] only if it
 * never reached the log; one that was appended before leadership was lost may still commit and is
 * reported as [CommitResult.OutcomeUnknown].
 */
class RatisConsensusLog(private val config: RatisConsensusConfig) : ConsensusLog {
    private val log = LoggerFactory.getLogger(RatisConsensusLog::class.java)
    private val peerId = RaftPeerId.valueOf(config.memberId.toString())
    private val clientId = ClientId.randomId()
    private val callIds = AtomicLong()
    private val appending: MutableSet<Long> = ConcurrentHashMap.newKeySet()
    private val stateMachines = ConcurrentHashMap<UUID, GroupStateMachine>()
    private val listeners = ConcurrentHashMap<UUID, CopyOnWriteArrayList<(GroupStatus) -> Unit>>()
    private val events: ExecutorService = Executors.newSingleThreadExecutor { runnable ->
        Thread(runnable, "dozendb-consensus-events").apply { isDaemon = true }
    }
    private val properties = raftProperties()

    @Volatile private var server: RaftServer? = null
    @Volatile private var boundAddress: InetSocketAddress? = null

    private val groupListener =
        object : GroupStateMachine.Listener {
            override fun onGroupChanged(groupId: UUID) = publishStatus(groupId)

            override fun onAppending(callId: Long) {
                appending += callId
            }
        }

    override val self: Member
        get() {
            config.advertisedAddress?.let {
                return Member(config.memberId, it)
            }
            val bound =
                checkNotNull(boundAddress) { "the advertised address is known once started" }
            return Member(
                config.memberId,
                HostPort(config.listenHost.ifEmpty { "localhost" }, bound.port),
            )
        }

    override fun start() {
        check(server == null) { "already started" }
        val parameters = Parameters()
        GrpcConfigKeys.Server.setServicesCustomizer(
            parameters,
            object : GrpcServices.Customizer {
                override fun customize(
                    builder: NettyServerBuilder,
                    types: EnumSet<GrpcServices.Type>,
                ): NettyServerBuilder =
                    if (GrpcServices.Type.SERVER in types) {
                        builder.addService(ProbeService.definition(::describeSelf))
                    } else {
                        builder
                    }
            },
        )
        val started =
            RaftServer.newBuilder()
                .setServerId(peerId)
                .setProperties(properties)
                .setParameters(parameters)
                .setStateMachineRegistry { groupId -> groupStateMachine(groupId.uuid) }
                .setOption(RaftStorage.StartupOption.RECOVER)
                .build()
        started.start()
        server = started
        boundAddress = started.serverRpc.inetSocketAddress
        log.info(
            "Raft member {} listening on {}, advertised as {}, recovered groups {}",
            config.memberId,
            boundAddress,
            self.raftAddress,
            started.groupIds.map { it.uuid },
        )
    }

    override suspend fun probe(address: HostPort): PeerInfo =
        ProbeService.probe(address, self, config.requestTimeout)

    override fun localGroups(): Set<UUID> {
        val running = server?.groupIds?.map { it.uuid }.orEmpty()
        val stored =
            config.storageDirectory.toFile().listFiles(File::isDirectory).orEmpty().mapNotNull {
                runCatching { UUID.fromString(it.name) }.getOrNull()
            }
        return (running + stored).toSet()
    }

    override fun startGroup(
        database: DatabaseRef,
        stateMachine: ReplicatedStateMachine,
        bootstrapMembers: List<Member>,
    ) {
        val raftServer = requireServer()
        val groupId = RaftGroupId.valueOf(database.id)
        groupStateMachine(database.id).bind(database, stateMachine)
        if (groupId in raftServer.groupIds) {
            log.info("Raft group {} recovered from local storage", database)
            publishStatus(database.id)
            return
        }
        val hasLocalState =
            config.storageDirectory.resolve(database.id.toString()).toFile().isDirectory
        val group =
            if (hasLocalState) {
                RaftGroup.valueOf(groupId)
            } else {
                require(bootstrapMembers.isNotEmpty()) {
                    "group $database has no local state and no bootstrap members"
                }
                RaftGroup.valueOf(groupId, bootstrapMembers.map(::toPeer))
            }
        val reply =
            raftServer.groupManagement(
                GroupManagementRequest.newAdd(
                    clientId,
                    peerId,
                    callIds.incrementAndGet(),
                    group,
                    !hasLocalState,
                )
            )
        if (!reply.isSuccess) {
            throw IllegalStateException("could not start Raft group $database", reply.exception)
        }
        log.info(
            "Raft group {} {}",
            database,
            if (hasLocalState) "restarted from local storage" else "created with $bootstrapMembers",
        )
        publishStatus(database.id)
    }

    override fun stopGroup(database: DatabaseRef) {
        val raftServer = server ?: return
        val groupId = RaftGroupId.valueOf(database.id)
        if (groupId in raftServer.groupIds) {
            val reply =
                raftServer.groupManagement(
                    GroupManagementRequest.newRemove(
                        clientId,
                        peerId,
                        callIds.incrementAndGet(),
                        groupId,
                        false,
                        false,
                    )
                )
            if (!reply.isSuccess) {
                throw IllegalStateException("could not stop Raft group $database", reply.exception)
            }
        }
        stateMachines.remove(database.id)
        publishStatus(database.id)
    }

    override suspend fun submit(database: DatabaseRef, payload: ByteArray): CommitResult {
        val raftServer = server ?: return CommitResult.NotLeader(null)
        val callId = callIds.incrementAndGet()
        val request =
            RaftClientRequest.newBuilder()
                .setClientId(clientId)
                .setServerId(peerId)
                .setGroupId(RaftGroupId.valueOf(database.id))
                .setCallId(callId)
                .setMessage(Message.valueOf(UnsafeByteOperations.unsafeWrap(payload)))
                .setType(RaftClientRequest.writeRequestType())
                .build()
        try {
            val reply =
                try {
                    raftServer.submitClientRequestAsync(request).await()
                } catch (e: Exception) {
                    return classifyFailure(unwrap(e), appended = callId in appending)
                }
            if (reply.isSuccess) {
                return CommitResult.Committed(
                    reply.logIndex,
                    GroupStateMachine.decodeLong(reply.message),
                )
            }
            return classifyFailure(reply.exception, appended = callId in appending)
        } finally {
            appending -= callId
        }
    }

    private fun classifyFailure(cause: Throwable?, appended: Boolean): CommitResult {
        val failure = cause ?: IllegalStateException("write failed without an exception")
        if (appended) return CommitResult.OutcomeUnknown(failure)
        return when (failure) {
            is NotLeaderException ->
                CommitResult.NotLeader(failure.suggestedLeader?.let(::memberId))
            is LeaderNotReadyException -> CommitResult.NotLeader(config.memberId)
            is GroupMismatchException,
            is ServerNotReadyException,
            is AlreadyClosedException -> CommitResult.NotLeader(null)
            else -> CommitResult.OutcomeUnknown(failure)
        }
    }

    override suspend fun readIndex(database: DatabaseRef): Long {
        val raftServer =
            server ?: throw ConsensusUnavailableException("consensus for $database is not started")
        val request =
            RaftClientRequest.newBuilder()
                .setClientId(clientId)
                .setServerId(peerId)
                .setGroupId(RaftGroupId.valueOf(database.id))
                .setCallId(callIds.incrementAndGet())
                .setMessage(Message.EMPTY)
                .setType(RaftClientRequest.readRequestType())
                .build()
        val reply =
            try {
                raftServer.submitClientRequestAsync(request).await()
            } catch (e: Exception) {
                throw ConsensusUnavailableException("read index of $database failed", unwrap(e))
            }
        if (!reply.isSuccess) {
            throw ConsensusUnavailableException("read index of $database failed", reply.exception)
        }
        return GroupStateMachine.decodeLong(reply.message)
    }

    override fun status(database: DatabaseRef): GroupStatus {
        val division = server?.let {
            runCatching { it.getDivision(RaftGroupId.valueOf(database.id)) }.getOrNull()
        }
        val info = division?.info
        if (division == null || info == null || !info.isAlive) {
            return GroupStatus(database, Role.Inactive, null, 0, -1, -1)
        }
        val role =
            when (info.currentRole) {
                // A leader accepts writes only once an entry of its term is committed. Until then
                // it is still establishing leadership.
                RaftPeerRole.LEADER -> if (info.isLeaderReady) Role.Leader else Role.Candidate
                RaftPeerRole.FOLLOWER -> Role.Follower
                RaftPeerRole.CANDIDATE -> Role.Candidate
                // Non-voting members are not supported yet.
                RaftPeerRole.LISTENER,
                RaftPeerRole.UNRECOGNIZED,
                null -> Role.Inactive
            }
        val leader = if (role == Role.Candidate) null else info.leaderId?.let(::memberId)
        return GroupStatus(
            database,
            role,
            leader,
            info.currentTerm,
            division.raftLog.lastCommittedIndex,
            info.lastAppliedIndex,
        )
    }

    override fun members(database: DatabaseRef): List<Member> {
        val division =
            try {
                requireServer().getDivision(RaftGroupId.valueOf(database.id))
            } catch (e: IOException) {
                throw ConsensusUnavailableException("group $database is not running", e)
            }
        return division.group.peers.map { Member(memberId(it), HostPort.parse(it.address)) }
    }

    override fun addStatusListener(
        database: DatabaseRef,
        listener: (GroupStatus) -> Unit,
    ): AutoCloseable {
        listeners.computeIfAbsent(database.id) { CopyOnWriteArrayList() } += listener
        return AutoCloseable { listeners[database.id]?.remove(listener) }
    }

    override suspend fun addMember(database: DatabaseRef, member: Member) {
        changeConfiguration(
            database,
            SetConfigurationRequest.Arguments.newBuilder()
                .setServersInNewConf(listOf(toPeer(member)))
                .setMode(SetConfigurationRequest.Mode.ADD)
                .build(),
        )
    }

    override suspend fun removeMember(database: DatabaseRef, id: MemberId) {
        val current = members(database)
        require(current.any { it.id == id }) { "$id is not a member of $database" }
        changeConfiguration(
            database,
            SetConfigurationRequest.Arguments.newBuilder()
                .setServersInCurrentConf(current.map(::toPeer))
                .setServersInNewConf(current.filterNot { it.id == id }.map(::toPeer))
                .setMode(SetConfigurationRequest.Mode.COMPARE_AND_SET)
                .build(),
        )
    }

    /** Sends a configuration change through a client, which finds the leader and retries. */
    private suspend fun changeConfiguration(
        database: DatabaseRef,
        arguments: SetConfigurationRequest.Arguments,
    ) {
        val group =
            RaftGroup.valueOf(RaftGroupId.valueOf(database.id), members(database).map(::toPeer))
        val attempts = maxOf(1, (config.requestTimeout.toMillis() / RETRY_SLEEP_MS).toInt())
        val reply: RaftClientReply =
            withContext(Dispatchers.IO) {
                try {
                    RaftClient.newBuilder()
                        .setProperties(properties)
                        .setRaftGroup(group)
                        .setClientId(ClientId.randomId())
                        .setRetryPolicy(
                            RetryPolicies.retryUpToMaximumCountWithFixedSleep(
                                attempts,
                                TimeDuration.valueOf(RETRY_SLEEP_MS, TimeUnit.MILLISECONDS),
                            )
                        )
                        .build()
                        .use { it.admin().setConfiguration(arguments) }
                } catch (e: IOException) {
                    throw ConsensusUnavailableException(
                        "configuration change of $database failed",
                        e,
                    )
                }
            }
        if (!reply.isSuccess) {
            throw ConsensusUnavailableException(
                "configuration change of $database failed",
                reply.exception,
            )
        }
    }

    override fun close() {
        val raftServer = server
        server = null
        try {
            raftServer?.close()
        } finally {
            events.shutdown()
            if (!events.awaitTermination(EVENTS_SHUTDOWN_SECONDS, TimeUnit.SECONDS)) {
                log.warn(
                    "Consensus event listeners did not finish within {}s",
                    EVENTS_SHUTDOWN_SECONDS,
                )
            }
        }
    }

    private fun describeSelf(): PeerInfo {
        val raftServer = requireServer()
        val groups =
            raftServer.groupIds.associate { groupId ->
                groupId.uuid to
                    raftServer.getDivision(groupId).group.peers.map {
                        Member(memberId(it), HostPort.parse(it.address))
                    }
            }
        return PeerInfo(self, config.metadata, groups)
    }

    private fun publishStatus(groupId: UUID) {
        try {
            events.execute {
                val database = stateMachines[groupId]?.database ?: DatabaseRef("unbound", groupId)
                val status = status(database)
                listeners[groupId]?.forEach { listener ->
                    try {
                        listener(status)
                    } catch (e: Exception) {
                        log.warn("Status listener for {} failed", database, e)
                    }
                }
            }
        } catch (_: RejectedExecutionException) {
            // Closing: no one is listening any more.
        }
    }

    private fun groupStateMachine(groupId: UUID): GroupStateMachine =
        stateMachines.computeIfAbsent(groupId) { GroupStateMachine(it, clientId, groupListener) }

    private fun requireServer(): RaftServer = checkNotNull(server) { "consensus is not started" }

    private fun raftProperties(): RaftProperties {
        val properties = RaftProperties()
        RaftServerConfigKeys.setStorageDir(properties, listOf(config.storageDirectory.toFile()))
        GrpcConfigKeys.Server.setHost(properties, config.listenHost.ifEmpty { null })
        GrpcConfigKeys.Server.setPort(properties, config.listenPort)
        RaftServerConfigKeys.Rpc.setTimeoutMin(properties, timeDuration(config.electionTimeoutMin))
        RaftServerConfigKeys.Rpc.setTimeoutMax(properties, timeDuration(config.electionTimeoutMax))
        // Linearizable reads use ReadIndex, on the leader and on followers.
        RaftServerConfigKeys.Read.setOption(
            properties,
            RaftServerConfigKeys.Read.Option.LINEARIZABLE,
        )
        // Durability: an entry is acknowledged only after it is flushed on a majority.
        RaftServerConfigKeys.Log.setUnsafeFlushEnabled(properties, false)
        RaftServerConfigKeys.Log.setAsyncFlushEnabled(properties, false)
        // Snapshots are Neo4j checkpoints, taken by the database integration.
        RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(properties, false)
        return properties
    }

    private companion object {
        const val RETRY_SLEEP_MS = 200L
        const val EVENTS_SHUTDOWN_SECONDS = 5L

        fun timeDuration(duration: java.time.Duration): TimeDuration =
            TimeDuration.valueOf(duration.toMillis(), TimeUnit.MILLISECONDS)

        fun toPeer(member: Member): RaftPeer =
            RaftPeer.newBuilder()
                .setId(RaftPeerId.valueOf(member.id.toString()))
                .setAddress(member.raftAddress.toString())
                .build()

        fun memberId(peer: RaftPeer): MemberId = memberId(peer.id)

        fun memberId(peerId: RaftPeerId): MemberId = MemberId.parse(peerId.toString())

        fun unwrap(e: Throwable): Throwable =
            if (e is CompletionException && e.cause != null) e.cause!! else e
    }
}
