package org.dozendb.consensus.ratis

import java.io.File
import java.io.IOException
import java.io.InputStream
import java.net.InetSocketAddress
import java.time.Duration
import java.util.EnumSet
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.future.await
import kotlinx.coroutines.withContext
import org.apache.ratis.RaftConfigKeys
import org.apache.ratis.client.RaftClient
import org.apache.ratis.client.RaftClientConfigKeys
import org.apache.ratis.conf.Parameters
import org.apache.ratis.conf.RaftProperties
import org.apache.ratis.grpc.GrpcConfigKeys
import org.apache.ratis.grpc.server.GrpcServices
import org.apache.ratis.proto.RaftProtos.RaftPeerRole
import org.apache.ratis.proto.RaftProtos.RoleInfoProto
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
import org.apache.ratis.protocol.SnapshotManagementRequest
import org.apache.ratis.protocol.TransferLeadershipRequest
import org.apache.ratis.protocol.exceptions.AlreadyClosedException
import org.apache.ratis.protocol.exceptions.GroupMismatchException
import org.apache.ratis.protocol.exceptions.LeaderNotReadyException
import org.apache.ratis.protocol.exceptions.LeaderSteppingDownException
import org.apache.ratis.protocol.exceptions.NotLeaderException
import org.apache.ratis.protocol.exceptions.ReconfigurationInProgressException
import org.apache.ratis.protocol.exceptions.ReconfigurationTimeoutException
import org.apache.ratis.protocol.exceptions.ServerNotReadyException
import org.apache.ratis.protocol.exceptions.TransferLeadershipException
import org.apache.ratis.retry.ExceptionDependentRetry
import org.apache.ratis.retry.RetryPolicies
import org.apache.ratis.server.DivisionInfo
import org.apache.ratis.server.RaftConfiguration
import org.apache.ratis.server.RaftServer
import org.apache.ratis.server.RaftServerConfigKeys
import org.apache.ratis.server.raftlog.RaftLog
import org.apache.ratis.server.storage.RaftStorage
import org.apache.ratis.server.storage.RaftStorageDirectory
import org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations
import org.apache.ratis.thirdparty.io.grpc.netty.NettyServerBuilder
import org.apache.ratis.util.SizeInBytes
import org.apache.ratis.util.TimeDuration
import org.dozendb.consensus.BulkSource
import org.dozendb.consensus.CommitResult
import org.dozendb.consensus.ConsensusLog
import org.dozendb.consensus.ConsensusUnavailableException
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.GroupMembership
import org.dozendb.consensus.GroupProgress
import org.dozendb.consensus.GroupStatus
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.LeaderPlacement
import org.dozendb.consensus.Member
import org.dozendb.consensus.MemberConnection
import org.dozendb.consensus.MemberId
import org.dozendb.consensus.PeerInfo
import org.dozendb.consensus.ReplicatedStateMachine
import org.dozendb.consensus.Role
import org.dozendb.consensus.Snapshot
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
    init {
        RatisJvmSafety.ensure()
    }

    private val log = LoggerFactory.getLogger(RatisConsensusLog::class.java)
    private val peerId = RaftPeerId.valueOf(config.memberId.toString())
    private val clientId = ClientId.randomId()
    private val callIds = AtomicLong()
    private val appending: MutableSet<Long> = ConcurrentHashMap.newKeySet()
    private val stateMachines = ConcurrentHashMap<UUID, GroupStateMachine>()

    /**
     * Groups being added to the running server. Ratis serves requests for a group before it has
     * started it; a leader's snapshot notification that arrives before its log is open leaves Ratis
     * reporting the installation as in progress for good. Until started, a group is not offered,
     * and its leader's requests are refused ([RefuseUnavailableGroups]).
     */
    private val startingGroups: MutableSet<UUID> = ConcurrentHashMap.newKeySet()
    private val listeners = ConcurrentHashMap<UUID, CopyOnWriteArrayList<(GroupStatus) -> Unit>>()
    private val announcementListeners = CopyOnWriteArrayList<(PeerInfo) -> Unit>()
    private val events: ExecutorService = Executors.newSingleThreadExecutor { runnable ->
        Thread(runnable, "dozendb-consensus-events").apply { isDaemon = true }
    }
    private val properties = raftProperties()
    private val tls = config.tls?.let(::TlsContexts)
    private val connections = MemberConnections(config.tls != null)
    private val bulkSources = ConcurrentHashMap<String, BulkSource>()
    private val bulkExecutor: ExecutorService = Executors.newCachedThreadPool { runnable ->
        Thread(runnable, "dozendb-bulk-source").apply { isDaemon = true }
    }
    /** Takes snapshots and hands leadership over; both may wait for Ratis. */
    private val maintenance: ScheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor { runnable ->
            Thread(runnable, "dozendb-consensus-maintenance").apply { isDaemon = true }
        }

    @Volatile private var server: RaftServer? = null

    /** Whether this member is stopping, and takes no more requests (see [RefuseWhileStopping]). */
    @Volatile private var stopping = false
    @Volatile private var boundAddress: InetSocketAddress? = null
    @Volatile override var leaderPlacement: LeaderPlacement = LeaderPlacement.CONFIGURATION_ORDER

    private val groupListener =
        object : GroupStateMachine.Listener {
            override fun onGroupChanged(groupId: UUID) = publishStatus(groupId)

            override fun onAppending(callId: Long) {
                appending += callId
            }

            override fun onLeadingDetached(groupId: UUID) = handOverLeadership(groupId)
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
        stopping = false
        GrpcLogs.install()
        val parameters = tlsParameters()
        GrpcConfigKeys.Server.setServicesCustomizer(
            parameters,
            object : GrpcServices.Customizer {
                override fun customize(
                    builder: NettyServerBuilder,
                    types: EnumSet<GrpcServices.Type>,
                ): NettyServerBuilder =
                    if (GrpcServices.Type.SERVER in types) {
                        builder
                            .addTransportFilter(connections.filter)
                            // Called last, with the calls the others let through.
                            .intercept(connections.interceptor)
                            .intercept(RefuseWhileStopping { stopping })
                            .intercept(RefuseOtherMembers(peerId))
                            .intercept(
                                RefuseUnavailableGroups {
                                    when {
                                        it in startingGroups -> "starting"
                                        stateMachines[it]?.closed == true -> "closed"
                                        else -> null
                                    }
                                }
                            )
                            .addService(
                                ProbeService.definition(::describeSelf, ::publishAnnouncement)
                            )
                            .addService(BulkService.definition(bulkSources::get, bulkExecutor))
                            // Bulk clients send keep-alive pings to detect dead peers.
                            .permitKeepAliveTime(BulkService.KEEP_ALIVE_SECONDS, TimeUnit.SECONDS)
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
        val interval = config.snapshotInterval.toMillis()
        maintenance.scheduleWithFixedDelay(
            ::takeDueSnapshots,
            interval,
            interval,
            TimeUnit.MILLISECONDS,
        )
        log.info(
            "Raft member {} listening on {} ({}), advertised as {}, recovered groups {}",
            config.memberId,
            // Ratis reports the wildcard address even when it binds one interface.
            HostPort(config.listenHost.ifEmpty { "0.0.0.0" }, checkNotNull(boundAddress).port),
            transport(),
            self.raftAddress,
            started.groupIds.map { it.uuid },
        )
    }

    override suspend fun probe(address: HostPort): PeerInfo =
        connections.open(address, PROBE).use {
            ProbeService.probe(address, self, config.requestTimeout, tls?.client)
        }

    override suspend fun announce(address: HostPort) =
        connections.open(address, PROBE).use {
            ProbeService.announce(address, describeSelf(), config.requestTimeout, tls?.client)
        }

    override fun addAnnouncementListener(listener: (PeerInfo) -> Unit): AutoCloseable {
        announcementListeners += listener
        return AutoCloseable { announcementListeners -= listener }
    }

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
        removeFailedGroup(database)
        groupStateMachine(database.id).attach(database, stateMachine)
        if (RaftGroupId.valueOf(database.id) in requireServer().groupIds) {
            log.info("Raft group {} attached to its state machine", database)
            publishStatus(database.id)
            return
        }
        addGroup(database, bootstrapMembers)
    }

    /**
     * Ratis closes a group on this member when an entry fails to apply, and does not run it again.
     * Such a group is removed from the server with its state machine, its storage kept, so that it
     * is added again from there and catches up. A group closed for another reason stays: one this
     * member left may still be closing, which waits for this state machine to apply the entries it
     * stored, so removing it would wait forever.
     */
    private fun removeFailedGroup(database: DatabaseRef) {
        val raftServer = requireServer()
        val groupId = RaftGroupId.valueOf(database.id)
        if (groupId !in raftServer.groupIds) return
        if (stateMachines[database.id]?.applyFailed != true) return
        val division = runCatching { raftServer.getDivision(groupId) }.getOrNull() ?: return
        if (division.info.isAlive) return
        // Returns once Ratis finished closing the group, which frees its storage.
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
            throw IllegalStateException(
                "could not remove the failed Raft group $database",
                reply.exception,
            )
        }
        stateMachines.remove(database.id)
        log.info(
            "Raft group {} failed on this member; starting it again from its local state",
            database,
        )
    }

    override fun joinGroup(database: DatabaseRef, bootstrapMembers: List<Member>) {
        groupStateMachine(database.id).bind(database)
        if (RaftGroupId.valueOf(database.id) in requireServer().groupIds) return
        addGroup(database, bootstrapMembers)
    }

    private fun addGroup(database: DatabaseRef, bootstrapMembers: List<Member>) {
        val raftServer = requireServer()
        val groupId = RaftGroupId.valueOf(database.id)
        val hasLocalState = groupDirectory(database.id).toFile().isDirectory
        val group =
            if (hasLocalState) {
                RaftGroup.valueOf(groupId)
            } else {
                require(bootstrapMembers.isNotEmpty()) {
                    "group $database has no local state and no bootstrap members"
                }
                RaftGroup.valueOf(groupId, bootstrapMembers.map(::toPeer))
            }
        // Ratis answers once the group has started.
        startingGroups += database.id
        val reply =
            try {
                raftServer.groupManagement(
                    GroupManagementRequest.newAdd(
                        clientId,
                        peerId,
                        callIds.incrementAndGet(),
                        group,
                        !hasLocalState,
                    )
                )
            } finally {
                startingGroups -= database.id
            }
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

    override fun removeGroup(database: DatabaseRef) {
        val raftServer = requireServer()
        val groupId = RaftGroupId.valueOf(database.id)
        // Closing the division waits for every committed entry to be applied.
        stateMachines[database.id]?.let {
            it.detach(handOverLeadership = false)
            it.release()
        }
        if (groupId in raftServer.groupIds) {
            val reply =
                raftServer.groupManagement(
                    GroupManagementRequest.newRemove(
                        clientId,
                        peerId,
                        callIds.incrementAndGet(),
                        groupId,
                        true,
                        false,
                    )
                )
            if (!reply.isSuccess) {
                throw IllegalStateException(
                    "could not remove Raft group $database",
                    reply.exception,
                )
            }
        }
        val directory = groupDirectory(database.id)
        if (directory.toFile().exists()) directory.toFile().deleteRecursively()
        stateMachines.remove(database.id)
        log.info("Raft group {} removed with its local state", database)
        publishStatus(database.id)
    }

    private fun groupDirectory(groupId: UUID) = config.storageDirectory.resolve(groupId.toString())

    /** Whether this member waits for a copy of [database], which the leader asked it to install. */
    internal fun awaitingCopy(database: DatabaseRef): Boolean =
        stateMachines[database.id]?.awaitingCopy == true

    override fun detachGroup(database: DatabaseRef, handOverLeadership: Boolean) {
        val stateMachine = stateMachines[database.id] ?: return
        stateMachine.detach(handOverLeadership)
        log.info("Raft group {} detached from its state machine", database)
        val leading =
            server?.let {
                runCatching { it.getDivision(RaftGroupId.valueOf(database.id)).info.leads() }
                    .getOrDefault(false)
            } == true
        if (leading && handOverLeadership) handOverLeadership(database.id)
        publishStatus(database.id)
    }

    /** Ratis keeps the role a group had when it closed: a closed group leads nothing. */
    private fun DivisionInfo.leads(): Boolean = isAlive && isLeader

    /**
     * Transfers leadership of [groupId] to another member: a detached member cannot apply what the
     * group commits. Tries again every [HANDOVER_RETRY] while this member leads the group detached,
     * for example while no other member is reachable yet.
     */
    private fun handOverLeadership(
        groupId: UUID,
        delay: java.time.Duration = java.time.Duration.ZERO,
        attempt: Int = 1,
    ) {
        try {
            maintenance.schedule(
                { tryHandOver(groupId, attempt) },
                delay.toMillis(),
                TimeUnit.MILLISECONDS,
            )
        } catch (_: RejectedExecutionException) {
            // Closing: leadership ends with the server.
        }
    }

    private fun tryHandOver(groupId: UUID, attempt: Int) {
        val raftServer = server ?: return
        val division =
            runCatching { raftServer.getDivision(RaftGroupId.valueOf(groupId)) }.getOrNull()
                ?: return
        val stateMachine = stateMachines[groupId] ?: return
        if (!division.info.leads() || !stateMachine.handsOverLeadership) return
        val database = stateMachine.database
        val others = division.raftConf.currentPeers.map(::memberId).filter { it != config.memberId }
        val candidates =
            (leaderPlacement.rank(database, others).filter { it in others } + others).distinct()
        for (candidate in candidates) {
            val failure = runCatching {
                requestTransfer(database, candidate, config.requestTimeout).get()
            }
                .getOrElse { it }
            if (failure == null) {
                transferred(database, candidate, HANDOVER_REASON)
                return
            }
        }
        if (attempt == 1) {
            log.warn(
                "Could not hand leadership of {} over to another member; trying again every {}",
                database,
                HANDOVER_RETRY,
            )
        }
        handOverLeadership(groupId, HANDOVER_RETRY, attempt + 1)
    }

    override suspend fun transferLeadership(
        database: DatabaseRef,
        to: MemberId,
        reason: String,
        timeout: java.time.Duration,
    ): Boolean {
        val failure =
            try {
                requestTransfer(database, to, timeout).await()
            } catch (e: Exception) {
                unwrap(e)
            }
        if (failure != null) {
            log.info(
                "Could not transfer leadership of {} to {} ({}): {}",
                database,
                to,
                reason,
                failure.message,
            )
            return false
        }
        transferred(database, to, reason)
        return true
    }

    /**
     * Asks Ratis to transfer the leadership of [database] to [to]. Completes with null once [to]
     * leads, or with why not.
     */
    private fun requestTransfer(
        database: DatabaseRef,
        to: MemberId,
        timeout: java.time.Duration,
    ): CompletableFuture<Throwable?> {
        val raftServer =
            server
                ?: return CompletableFuture.completedFuture(
                    IllegalStateException("consensus is not started")
                )
        val request =
            TransferLeadershipRequest(
                clientId,
                peerId,
                RaftGroupId.valueOf(database.id),
                callIds.incrementAndGet(),
                RaftPeerId.valueOf(to.toString()),
                timeout.toMillis(),
            )
        return try {
            raftServer.transferLeadershipAsync(request).handle { reply, failure ->
                when {
                    failure != null -> unwrap(failure)
                    reply.isSuccess -> null
                    else -> reply.exception ?: IllegalStateException("the transfer failed")
                }
            }
        } catch (e: IOException) {
            CompletableFuture.completedFuture(e)
        }
    }

    private fun transferred(database: DatabaseRef, to: MemberId, reason: String) {
        stateMachines[database.id]?.countTransfer()
        log.info("Transferred leadership of {} to {}: {}", database, to, reason)
    }

    override fun currentSnapshot(database: DatabaseRef): Snapshot? {
        val raftServer = server ?: return null
        if (RaftGroupId.valueOf(database.id) !in raftServer.groupIds) return null
        return stateMachines[database.id]?.currentSnapshot()
    }

    override fun latestSnapshot(database: DatabaseRef): Snapshot? {
        val running = server?.let { RaftGroupId.valueOf(database.id) in it.groupIds } == true
        val stateMachine = stateMachines[database.id]
        return if (running && stateMachine != null) {
            stateMachine.latestSnapshot()
        } else {
            SnapshotRecord.read(
                    config.storageDirectory
                        .resolve(database.id.toString())
                        .resolve(RaftStorageDirectory.STATE_MACHINE_DIR_NAME)
                )
                ?.snapshot
        }
    }

    override fun termOf(database: DatabaseRef, index: Long): Long? {
        val division =
            server?.let {
                runCatching { it.getDivision(RaftGroupId.valueOf(database.id)) }.getOrNull()
            } ?: return null
        raftLog(division, database).getTermIndex(index)?.let {
            return it.term
        }
        return stateMachines[database.id]
            ?.latestSnapshot()
            ?.position
            ?.takeIf { it.index == index }
            ?.term
    }

    override fun seed(database: DatabaseRef, snapshot: Snapshot) {
        groupStateMachine(database.id).seed(snapshot)
    }

    override fun installSnapshot(database: DatabaseRef, snapshot: Snapshot): Boolean {
        val running = server?.let { RaftGroupId.valueOf(database.id) in it.groupIds } == true
        val stateMachine = stateMachines[database.id]
        check(running && stateMachine != null) { "group $database is not running" }
        return stateMachine.install(snapshot)
    }

    /** Asks Ratis for a snapshot of every group whose state machine made new state durable. */
    private fun takeDueSnapshots() {
        val raftServer = server ?: return
        for ((id, stateMachine) in stateMachines) {
            try {
                val groupId = RaftGroupId.valueOf(id)
                if (groupId !in raftServer.groupIds || !stateMachine.snapshotDue()) continue
                val reply =
                    raftServer.snapshotManagement(
                        SnapshotManagementRequest.newCreate(
                            clientId,
                            peerId,
                            groupId,
                            callIds.incrementAndGet(),
                            config.requestTimeout.toMillis(),
                            1,
                        )
                    )
                if (!reply.isSuccess && server != null) {
                    log.warn("Snapshot of {} failed", stateMachine.database, reply.exception)
                }
            } catch (e: Exception) {
                // Stopping interrupts a snapshot under way; the member takes it when it starts.
                if (server == null) return
                log.warn("Snapshot of {} failed", stateMachine.database, e)
            }
        }
    }

    override val maxEntrySize: Int
        get() = config.maxEntrySize

    override suspend fun submit(database: DatabaseRef, payload: ByteArray): CommitResult {
        require(payload.size <= maxEntrySize) {
            "entry of ${payload.size} bytes exceeds the maximum of $maxEntrySize"
        }
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
                val (term, response) = GroupStateMachine.decodeReply(reply.message)
                return CommitResult.Committed(reply.logIndex, term, response)
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
            // Refused before appending, while leadership moves to another member.
            is LeaderSteppingDownException,
            is TransferLeadershipException -> CommitResult.NotLeader(null)
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

    override suspend fun awaitApplied(database: DatabaseRef, index: Long) {
        val stateMachine =
            stateMachines[database.id]
                ?: throw ConsensusUnavailableException("the group of $database does not run here")
        stateMachine.awaitApplied(index)
    }

    override fun progress(database: DatabaseRef): GroupProgress? {
        val status = status(database)
        if (status.role == Role.Inactive) return null
        val division =
            server?.let {
                runCatching { it.getDivision(RaftGroupId.valueOf(database.id)) }.getOrNull()
            } ?: return null
        val raftLog = raftLog(division, database)
        val roleInfo = division.info.roleInfoProto
        val leading = status.role == Role.Leader
        val stateMachine = stateMachines[database.id]
        val matchIndexes = if (leading) followerMatchIndexes(roleInfo, division.info) else null
        return GroupProgress(
            appendIndex = raftLog.lastEntryTermIndex?.index ?: RaftLog.INVALID_LOG_INDEX,
            commitIndex = status.commitIndex,
            appliedIndex = status.appliedIndex,
            pruneIndex = raftLog.startIndex,
            term = status.term,
            leader = leading,
            sinceLeaderContactMillis =
                when {
                    leading -> 0
                    status.leader != null && roleInfo.hasFollowerInfo() ->
                        roleInfo.followerInfo.leaderInfo.lastRpcElapsedTimeMs
                    else -> null
                },
            followerMatchIndexes = matchIndexes,
            followerContactMillis =
                matchIndexes?.let {
                    roleInfo.leaderInfo.followerInfoList.associate {
                        memberId(RaftPeerId.valueOf(it.id.id)) to it.lastRpcElapsedTimeMs
                    }
                },
            snapshotsTaken = stateMachine?.snapshotsTaken ?: 0,
            leaderChanges = stateMachine?.leaderChanges ?: 0,
            leadershipTransfers = stateMachine?.leadershipTransfers ?: 0,
        )
    }

    /**
     * The followers' match indexes on the leader. Ratis lists the followers and their match indexes
     * in two passes over its log appenders: if the followers changed in between, the lists do not
     * line up, and none are reported this time; nor when the member stopped leading meanwhile.
     */
    private fun followerMatchIndexes(
        roleInfo: RoleInfoProto,
        info: DivisionInfo,
    ): Map<MemberId, Long>? {
        if (!roleInfo.hasLeaderInfo()) return null
        val followers = roleInfo.leaderInfo.followerInfoList.map { RaftPeerId.valueOf(it.id.id) }
        val indexes = info.followerMatchIndices ?: return null
        if (followers.size != indexes.size) return null
        return followers.zip(indexes.asList()).associate { (peer, index) ->
            memberId(peer) to index
        }
    }

    override fun status(database: DatabaseRef): GroupStatus {
        val division = server?.let {
            runCatching { it.getDivision(RaftGroupId.valueOf(database.id)) }.getOrNull()
        }
        val info = division?.info
        // Ratis initialises the log of a starting group after it counts as alive.
        val committed = division?.let {
            runCatching { raftLog(it, database).lastCommittedIndex }.getOrNull()
        }
        val applied = division?.let(::appliedIndex)
        if (
            division == null ||
                info == null ||
                committed == null ||
                applied == null ||
                !info.isAlive ||
                !isVoter(division.raftConf)
        ) {
            return GroupStatus(database, Role.Inactive, null, 0, -1, -1)
        }
        val role =
            when (info.currentRole) {
                // A leader accepts writes once an entry of its term is committed and applied here,
                // so that everything committed before its term is applied first.
                RaftPeerRole.LEADER ->
                    if (info.isLeaderReady && appliedTerm(database) == info.currentTerm) {
                        Role.Leader
                    } else {
                        Role.Candidate
                    }
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
            committed,
            applied,
        )
    }

    override fun membership(database: DatabaseRef): GroupMembership {
        // Also asked while the member stops, as by routing requests that Bolt still serves.
        val raftServer = server ?: throw ConsensusUnavailableException("consensus is not running")
        val division =
            try {
                raftServer.getDivision(RaftGroupId.valueOf(database.id))
            } catch (e: IOException) {
                throw ConsensusUnavailableException("group $database is not running", e)
            }
        val conf = division.raftConf
        val settled =
            conf.previousPeers.isEmpty() &&
                conf.logEntryIndex <= raftLog(division, database).lastCommittedIndex
        // Ratis closes the group of a removed member: the leader once the change is committed,
        // a follower when it finds itself left out of the configuration or the leader says so.
        val removed =
            !division.info.isAlive &&
                (stateMachines[database.id]?.shutDownAsNonMember == true || !isVoter(conf))
        return GroupMembership(conf.currentPeers.map(::toMember), settled, removed)
    }

    /** The log of [division], which Ratis initialises only as the group starts. */
    private fun raftLog(division: RaftServer.Division, database: DatabaseRef): RaftLog =
        try {
            division.raftLog
        } catch (e: IllegalStateException) {
            throw ConsensusUnavailableException("group $database is starting", e)
        }

    /** Whether this member votes in [conf], or in the configuration it changes from. */
    private fun isVoter(conf: RaftConfiguration): Boolean =
        conf.getPeer(peerId, RaftPeerRole.FOLLOWER) != null ||
            conf.previousPeers.any { it.id == peerId }

    override fun addStatusListener(
        database: DatabaseRef,
        listener: (GroupStatus) -> Unit,
    ): AutoCloseable {
        listeners.computeIfAbsent(database.id) { CopyOnWriteArrayList() } += listener
        return AutoCloseable { listeners[database.id]?.remove(listener) }
    }

    override suspend fun addMember(
        database: DatabaseRef,
        member: Member,
        timeout: java.time.Duration,
        members: List<Member>,
    ) {
        changeConfiguration(
            database,
            knownMembers(database, members),
            SetConfigurationRequest.Arguments.newBuilder()
                .setServersInNewConf(listOf(toPeer(member)))
                .setMode(SetConfigurationRequest.Mode.ADD)
                .build(),
            timeout,
        )
    }

    override suspend fun removeMember(
        database: DatabaseRef,
        id: MemberId,
        timeout: java.time.Duration,
        members: List<Member>,
    ) {
        val current = knownMembers(database, members)
        if (current.none { it.id == id }) return
        changeConfiguration(
            database,
            current,
            SetConfigurationRequest.Arguments.newBuilder()
                .setServersInCurrentConf(current.map(::toPeer))
                .setServersInNewConf(current.filterNot { it.id == id }.map(::toPeer))
                .setMode(SetConfigurationRequest.Mode.COMPARE_AND_SET)
                .build(),
            timeout,
        )
    }

    /**
     * The voting members of the group of [database] as this member knows them where the group runs
     * and counts it in, else [given]: those that run it report them. A member waiting to be added,
     * or one the group removed, may know none or old ones.
     */
    private fun knownMembers(database: DatabaseRef, given: List<Member>): List<Member> {
        val local = runCatching { membership(database) }.getOrNull()
        if (local != null && !local.removed && local.members.any { it.id == self.id }) {
            return local.members
        }
        if (given.isNotEmpty()) return given
        return local?.members?.takeIf { it.isNotEmpty() }
            ?: throw ConsensusUnavailableException(
                "group $database does not run on this member, and no members were given to " +
                    "reach it through"
            )
    }

    /**
     * Sends a configuration change to the group of [database] through a client, which finds the
     * leader among [members]: this member only where the group runs here, as one that does not run
     * it fails the change instead of passing it on. The leader answers once the change is
     * committed, which may take as long as bringing a new member up to date, so the client waits up
     * to [timeout] for the answer.
     */
    private suspend fun changeConfiguration(
        database: DatabaseRef,
        members: List<Member>,
        arguments: SetConfigurationRequest.Arguments,
        timeout: java.time.Duration,
    ) {
        val groupId = RaftGroupId.valueOf(database.id)
        val runsHere = server?.let { groupId in it.groupIds } == true
        val through = if (runsHere) members else members.filter { it.id != self.id }
        val group = RaftGroup.valueOf(groupId, through.ifEmpty { members }.map(::toPeer))
        val clientProperties =
            RaftProperties(properties).also {
                RaftClientConfigKeys.Rpc.setRequestTimeout(it, timeDuration(timeout))
            }
        val attempts = maxOf(1, (config.requestTimeout.toMillis() / RETRY_SLEEP_MS).toInt())
        val reply: RaftClientReply =
            withContext(Dispatchers.IO) {
                try {
                    RaftClient.newBuilder()
                        .setProperties(clientProperties)
                        .setParameters(tlsParameters())
                        .setRaftGroup(group)
                        .setClientId(ClientId.randomId())
                        .setRetryPolicy(
                            // Retried while the leader is unknown, but not once a change failed
                            // or another one is in progress.
                            ExceptionDependentRetry.newBuilder()
                                .setDefaultPolicy(
                                    RetryPolicies.retryUpToMaximumCountWithFixedSleep(
                                        attempts,
                                        TimeDuration.valueOf(RETRY_SLEEP_MS, TimeUnit.MILLISECONDS),
                                    )
                                )
                                .setExceptionToPolicy(
                                    ReconfigurationTimeoutException::class.java,
                                    RetryPolicies.noRetry(),
                                )
                                .setExceptionToPolicy(
                                    ReconfigurationInProgressException::class.java,
                                    RetryPolicies.noRetry(),
                                )
                                .build()
                        )
                        .build()
                        .use { it.admin().setConfiguration(arguments) }
                } catch (e: IOException) {
                    throw ConsensusUnavailableException(
                        "configuration change of $database failed: ${e.message}",
                        e,
                    )
                }
            }
        if (!reply.isSuccess) {
            throw ConsensusUnavailableException(
                "configuration change of $database failed: ${reply.exception?.message}",
                reply.exception,
            )
        }
    }

    override fun registerBulkSource(name: String, source: BulkSource) {
        bulkSources[name] = source
    }

    override fun <T> fetchBulk(
        address: HostPort,
        name: String,
        request: ByteArray,
        timeout: Duration?,
        consume: (InputStream) -> T,
    ): T =
        connections.open(address, setOf(MemberConnections.BULK, name)).use {
            BulkService.fetch(address, name, request, tls?.client, consume, timeout)
        }

    /**
     * The connections open now (see [MemberConnections]), and Ratis's own to the members that the
     * groups this member leads replicate to, while they answered within twice the longest election
     * timeout: Ratis keeps one connection to each member for every group.
     */
    override fun connections(): List<MemberConnection> {
        val raftServer = server ?: return connections.connections()
        val recent = config.electionTimeoutMax.toMillis() * 2
        val replicatedTo =
            raftServer.groupIds
                .mapNotNull { runCatching { raftServer.getDivision(it) }.getOrNull() }
                .map { it.info.roleInfoProto }
                .filter { it.hasLeaderInfo() }
                .flatMap { role ->
                    role.leaderInfo.followerInfoList
                        .filter { it.lastRpcElapsedTimeMs <= recent }
                        .map { it.id.address }
                }
                .distinct()
                .mapNotNull { runCatching { HostPort.parse(it) }.getOrNull() }
                .map { MemberConnection(false, it, RAFT, tls != null) }
        return connections.connections() + replicatedTo
    }

    private fun appliedTerm(database: DatabaseRef): Long =
        stateMachines[database.id]?.appliedTerm() ?: -1

    override fun close() {
        val raftServer = server
        server = null
        maintenance.shutdownNow()
        stateMachines.values.forEach(GroupStateMachine::release)
        stopping = true
        try {
            raftServer?.close()
        } finally {
            bulkExecutor.shutdownNow()
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
        // A group Ratis closed here, as for a removed member, is no longer hosted; one still
        // starting is not hosted yet.
        val groups =
            raftServer.groupIds
                .filter { it.uuid !in startingGroups }
                .map(raftServer::getDivision)
                .filter { it.info.isAlive }
                .associate { it.memberId.groupId.uuid to it.raftConf.currentPeers.map(::toMember) }
        return PeerInfo(self, config.metadata(), groups)
    }

    private fun publishAnnouncement(info: PeerInfo) {
        try {
            events.execute {
                announcementListeners.forEach { listener ->
                    try {
                        listener(info)
                    } catch (e: Exception) {
                        log.warn("Announcement listener failed for {}", info.member, e)
                    }
                }
            }
        } catch (_: RejectedExecutionException) {
            // Closing: no one is listening any more.
        }
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

    /** How this member talks to the others, for the log. */
    private fun transport(): String =
        config.tls?.start?.let {
            listOf(
                    "TLS",
                    if (it.requireClientCertificates) "client certificates required"
                    else "client certificates not required",
                    if (it.verifyHostname) "hostnames verified" else "hostnames not verified",
                )
                .joinToString(", ")
        } ?: "plaintext"

    /** Ratis parameters with the TLS of the server, its clients and admin clients, if any. */
    private fun tlsParameters(): Parameters =
        Parameters().also { parameters ->
            tls?.let { GrpcConfigKeys.TLS.setConf(parameters, it.ratis) }
        }

    private fun raftProperties(): RaftProperties {
        val properties = RaftProperties()
        // Ratis' gRPC transport, with a leader that ends the streams it gives up on.
        RaftConfigKeys.Rpc.setType(properties, PatchedGrpcRpcType())
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
        // A follower composing appends keys those in flight by their first index: a longer request
        // with the same first index, as a leader resends after a reset, is acknowledged without
        // appending the entries beyond the earlier one. The leader then counts them as replicated
        // and never sends them again (Ratis 3.3.1).
        RaftServerConfigKeys.Log.setAppendEntriesComposeEnabled(properties, false)
        // After each commit the leader appends an entry recording the commit index. It doubles the
        // entries of sequential commits, but it is how followers learn of a commit at once: without
        // it they apply only with the next heartbeat, half the minimum election timeout later.
        RaftServerConfigKeys.Log.setLogMetadataEnabled(properties, true)
        // Snapshots follow the state machines' durable indexes; see takeDueSnapshots.
        RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(properties, false)
        // A member that is down must not keep the others from discarding their logs. Snapshots
        // are rare (one per database checkpoint), so each discards what it covers.
        RaftServerConfigKeys.Log.setPurgeUptoSnapshotIndex(properties, true)
        RaftServerConfigKeys.Log.setPurgeGap(properties, 1)
        // A member behind the leader's log installs a copy of the database: the leader only
        // notifies it.
        RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(properties, false)
        // After 30 failed attempts in a row to reach a follower, Ratis waits 5 s before each next
        // one, and counts the INCONSISTENCY replies of a follower that came back as failures too.
        // A member back from a stop of half a minute, as in a rolling restart, would hear from the
        // leader every 5 s while it catches up: no heartbeats within its election timeout, and a
        // leader that needs it for a majority, because another member is down, steps down and
        // leaves its commits under way with an unknown outcome. Attempts stay well within the
        // minimum election timeout instead; Ratis waits 0.5 to 1.5 times the time given.
        val retryWait = maxOf(1, config.electionTimeoutMin.toMillis() / 4)
        RaftServerConfigKeys.Log.Appender.setRetryPolicy(
            properties,
            "1ms,10, ${retryWait}ms,${Int.MAX_VALUE}",
        )
        // A log entry must fit the appender buffer, which must fit the write buffer. Both are per
        // group; the write buffer is direct memory.
        val entryLimit = config.maxEntrySize.toLong() + ENTRY_OVERHEAD
        RaftServerConfigKeys.Log.Appender.setBufferByteLimit(
            properties,
            SizeInBytes.valueOf(entryLimit),
        )
        RaftServerConfigKeys.Log.setWriteBufferSize(
            properties,
            SizeInBytes.valueOf(entryLimit + ENTRY_OVERHEAD),
        )
        RaftServerConfigKeys.Log.setSegmentSizeMax(
            properties,
            SizeInBytes.valueOf(maxOf(2 * entryLimit, config.logSegmentSize)),
        )
        GrpcConfigKeys.setMessageSizeMax(
            properties,
            SizeInBytes.valueOf(maxOf(2 * entryLimit, DEFAULT_MESSAGE_SIZE)),
        )
        // Pending writes and the log worker queue are bounded in bytes; one entry must fit.
        val queueLimit = SizeInBytes.valueOf(maxOf(4 * entryLimit, DEFAULT_QUEUE_BYTES))
        RaftServerConfigKeys.Write.setByteLimit(properties, queueLimit)
        RaftServerConfigKeys.Log.setQueueByteLimit(properties, queueLimit)
        return properties
    }

    private companion object {
        val PROBE = setOf(MemberConnections.PROBE)
        val RAFT = setOf(MemberConnections.RAFT)
        const val RETRY_SLEEP_MS = 200L
        val HANDOVER_RETRY: java.time.Duration = java.time.Duration.ofSeconds(1)
        const val HANDOVER_REASON = "this member stopped applying its entries"
        /** Room for the Ratis entry header around a payload, generously rounded. */
        const val ENTRY_OVERHEAD = 64L shl 10
        const val DEFAULT_MESSAGE_SIZE = 64L shl 20
        const val DEFAULT_QUEUE_BYTES = 64L shl 20
        const val EVENTS_SHUTDOWN_SECONDS = 5L

        fun timeDuration(duration: java.time.Duration): TimeDuration =
            TimeDuration.valueOf(duration.toMillis(), TimeUnit.MILLISECONDS)

        fun toPeer(member: Member): RaftPeer =
            RaftPeer.newBuilder()
                .setId(RaftPeerId.valueOf(member.id.toString()))
                .setAddress(member.raftAddress.toString())
                .build()

        fun memberId(peer: RaftPeer): MemberId = memberId(peer.id)

        fun toMember(peer: RaftPeer): Member = Member(memberId(peer), HostPort.parse(peer.address))

        fun memberId(peerId: RaftPeerId): MemberId = MemberId.parse(peerId.toString())

        fun unwrap(e: Throwable): Throwable =
            if (e is CompletionException && e.cause != null) e.cause!! else e
    }
}

/**
 * The index of the last entry that [division] applied, or null while Ratis starts its group: it
 * counts a starting group as running, with its log loaded, a moment before it creates the updater
 * that applies the log, and until then fails to tell.
 */
internal fun appliedIndex(division: RaftServer.Division): Long? =
    try {
        division.info.lastAppliedIndex
    } catch (_: IllegalStateException) {
        null
    }
