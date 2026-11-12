package org.dozendb.cluster.internal

import java.io.IOException
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeoutException
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.dozendb.cluster.ClusterFormation
import org.dozendb.cluster.ClusterServers
import org.dozendb.cluster.ClusterService
import org.dozendb.cluster.DatabaseGroups
import org.dozendb.cluster.DatabaseStatesCodec
import org.dozendb.cluster.FormatLevelStore
import org.dozendb.cluster.FormatLevels
import org.dozendb.cluster.MemberDirectory
import org.dozendb.cluster.MemberMetadata
import org.dozendb.cluster.MemberState
import org.dozendb.cluster.MembershipChangeException
import org.dozendb.cluster.MembershipFile
import org.dozendb.cluster.MembershipReconciler
import org.dozendb.cluster.SystemHosting
import org.dozendb.cluster.internal.durability.FsyncPolicy
import org.dozendb.cluster.internal.durability.SingleFsyncFileSystem
import org.dozendb.cluster.internal.forwarding.ForwardedWrites
import org.dozendb.cluster.internal.forwarding.ForwardingLocations
import org.dozendb.cluster.internal.forwarding.ForwardingWatch
import org.dozendb.cluster.internal.forwarding.MemberSsl
import org.dozendb.cluster.internal.forwarding.RemoteTransactions
import org.dozendb.cluster.internal.forwarding.RoutingBoltDatabases
import org.dozendb.cluster.internal.http.ClusterNeoWebServer
import org.dozendb.cluster.internal.http.ClusterStatuses
import org.dozendb.cluster.internal.http.DatabaseClusterStatus
import org.dozendb.cluster.internal.importer.ImportedStores
import org.dozendb.cluster.internal.leadership.LeaderAnnouncer
import org.dozendb.cluster.internal.leadership.LeaderBalancer
import org.dozendb.cluster.internal.reads.ConsistentBoltDatabases
import org.dozendb.cluster.internal.reads.ReadConsistencyException
import org.dozendb.cluster.internal.recreation.DatabaseRecreation
import org.dozendb.cluster.internal.recreation.RecreationSeeds
import org.dozendb.cluster.internal.recreation.Seed
import org.dozendb.cluster.internal.recreation.SeedChecksums
import org.dozendb.cluster.internal.recreation.SeedReports
import org.dozendb.cluster.internal.replication.CatchUp
import org.dozendb.cluster.internal.replication.DatabaseControl
import org.dozendb.cluster.internal.replication.DatabaseReconciler
import org.dozendb.cluster.internal.replication.DesiredDatabase
import org.dozendb.cluster.internal.replication.EntryTerm
import org.dozendb.cluster.internal.replication.EntryTermProtocol
import org.dozendb.cluster.internal.replication.EntryTermSource
import org.dozendb.cluster.internal.replication.ReplicationService
import org.dozendb.cluster.internal.routing.ClusterQueryRouterBootstrap
import org.dozendb.cluster.internal.routing.ClusterRoutingTableProvider
import org.dozendb.cluster.internal.routing.ClusterRoutingValidator
import org.dozendb.cluster.internal.routing.ConfirmedReferences
import org.dozendb.cluster.internal.routing.PolicyAgreement
import org.dozendb.cluster.internal.routing.RoutingPolicies
import org.dozendb.cluster.internal.secondaries.ClusterHosting
import org.dozendb.cluster.internal.secondaries.PullProtocol
import org.dozendb.cluster.internal.secondaries.PullSource
import org.dozendb.cluster.internal.secondaries.ReadBarrierProtocol
import org.dozendb.cluster.internal.secondaries.ReadBarrierSource
import org.dozendb.cluster.internal.secondaries.Secondaries
import org.dozendb.cluster.internal.secondaries.SecondaryCopies
import org.dozendb.cluster.internal.secondaries.SecondaryMarks
import org.dozendb.cluster.internal.secondaries.Upstreams
import org.dozendb.cluster.internal.storecopy.Neo4jLocalStores
import org.dozendb.cluster.internal.storecopy.StoreCopySource
import org.dozendb.cluster.internal.topology.ClusterTopologyInfoService
import org.dozendb.cluster.internal.topology.DatabaseLocations
import org.dozendb.consensus.ConsensusLog
import org.dozendb.consensus.ConsensusUnavailableException
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.GroupMembership
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.LogPosition
import org.dozendb.consensus.Member
import org.dozendb.consensus.MemberId
import org.dozendb.consensus.Role
import org.dozendb.consensus.Snapshot
import org.dozendb.consensus.ratis.RatisConsensusConfig
import org.dozendb.consensus.ratis.RatisConsensusLog
import org.dozendb.consensus.ratis.ReloadableTls
import org.dozendb.core.DozenDb
import org.dozendb.core.edition.BackupSources
import org.dozendb.core.edition.DatabaseHosting
import org.dozendb.core.edition.DatabaseQuarantine
import org.dozendb.core.edition.DatabaseState
import org.dozendb.core.edition.DozenDbEditionModule
import org.dozendb.core.edition.DozenDbQueryRouterBootstrap
import org.dozendb.core.edition.HostingServer
import org.dozendb.core.edition.LatestWrites
import org.dozendb.core.edition.SeedServers
import org.dozendb.core.edition.Unquarantines
import org.dozendb.core.edition.WantedDatabase
import org.dozendb.core.routing.DozenDbRoutingService
import org.dozendb.core.routing.LocalRoutingTables
import org.dozendb.core.routing.SingleAddressRoutingTable
import org.dozendb.core.security.DozenDbDefaultDatabaseResolver
import org.dozendb.core.security.MemberLogins
import org.dozendb.core.servers.CopyMode
import org.dozendb.core.servers.DatabasePlacement
import org.dozendb.core.servers.Placements
import org.dozendb.core.servers.ServerOptions
import org.dozendb.core.servers.ServerRecord
import org.dozendb.core.servers.ServerRecords
import org.dozendb.core.servers.ServerRules
import org.dozendb.core.servers.Servers
import org.dozendb.core.servers.UpstreamSelection
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI
import org.neo4j.bolt.tx.TransactionManager
import org.neo4j.collection.Dependencies
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.ConnectorType
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.dbms.database.DefaultSystemGraphInitializer
import org.neo4j.dbms.database.SystemGraphInitializer
import org.neo4j.dbms.database.TopologyInfoService
import org.neo4j.dbms.database.readonly.SystemGraphReadOnlyDatabaseLookupFactory
import org.neo4j.dbms.database.readonly.SystemGraphReadOnlyListener
import org.neo4j.dbms.routing.ClientRoutingDomainChecker
import org.neo4j.dbms.routing.RoutingService
import org.neo4j.dbms.systemgraph.SystemDatabaseProvider
import org.neo4j.fabric.bookmark.LocalGraphTransactionIdTracker
import org.neo4j.fabric.executor.Location
import org.neo4j.graphdb.Transaction
import org.neo4j.graphdb.event.DatabaseEventContext
import org.neo4j.graphdb.event.DatabaseEventListenerAdapter
import org.neo4j.graphdb.factory.module.GlobalModule
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog
import org.neo4j.kernel.api.procedure.GlobalProcedures
import org.neo4j.kernel.database.DatabaseIdFactory
import org.neo4j.kernel.database.DatabaseIdRepository
import org.neo4j.kernel.database.DatabaseTracers
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.kernel.impl.factory.DbmsInfo
import org.neo4j.kernel.internal.Version
import org.neo4j.kernel.lifecycle.Lifecycle
import org.neo4j.kernel.lifecycle.LifecycleAdapter
import org.neo4j.kernel.recovery.LogTailExtractor
import org.neo4j.logging.InternalLogProvider
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.router.transaction.DatabaseTransactionFactory
import org.neo4j.ssl.config.SslPolicyProvider
import org.neo4j.storageengine.api.StorageEngineFactory
import org.neo4j.storageengine.api.TransactionId
import org.neo4j.storageengine.api.TransactionIdStore

/**
 * DozenDB's edition module with clustering added. Only used when clustering is enabled.
 *
 * The cluster service is registered in the global lifecycle from the constructor, before the base
 * module registers its database starters, so consensus starts (and the databases are copied from
 * another member if needed) before any database starts, and stops after every database.
 *
 * Databases other than `system` and the default database are created, started, stopped and dropped
 * by the [DatabaseReconciler], as `StandaloneDatabases` does without clustering.
 */
class ClusterEditionModule(globalModule: GlobalModule) : DozenDbEditionModule(globalModule) {
    private val cluster: ClusterService
    private val replication: ReplicationService
    private val secondaries: Secondaries
    private val clusterHosting: ClusterHosting

    /** Whether this member hosts `system` as a secondary; see [SystemHosting.pulls]. */
    private val pullsSystem: Boolean
    private val directory: MemberDirectory
    private val locations: DatabaseLocations
    private val consensus: ConsensusLog
    private val clusterServers: ClusterServers

    /** `dbms.cluster.statusCheck`, which also answers other members' parts in it. */
    private val statusChecks: StatusChecks

    /** Lifts quarantines on other members, and on this one when another asks. */
    private val quarantineLifts: QuarantineLifts
    private val membership: MembershipReconciler
    private val balancer: LeaderBalancer
    private val announcer: LeaderAnnouncer
    private val memberId = MemberId(identityModule.serverId().uuid())
    /**
     * The Raft groups of the databases on this member, `system`'s as its membership record keeps
     * it; a record that cannot be read fails the cluster service's start instead. Tests may give
     * their own.
     */
    private val groups: DatabaseGroups =
        AbstractEditionModule.tryResolveOrCreate(
            DatabaseGroups::class.java,
            globalModule.externalDependencyResolver,
        ) {
            DatabaseGroups(
                runCatching {
                    MembershipFile(
                            globalModule.globalConfig
                                .get(ClusterSettings.raftDirectory)
                                .resolve(MembershipFile.FILE_NAME)
                        )
                        .read()
                        ?.systemGroup
                }
                    .getOrNull() ?: DatabaseGroups.SYSTEM_DATABASE
            )
        }

    /** The group of `system`. */
    private val systemDatabase: DatabaseRef
        get() = groups.system

    private val localStores: Neo4jLocalStores

    /** The TLS between members, or null for plaintext. */
    private val memberTls: ReloadableTls?

    /** Why other members may not forward writes to this member, or null if they may. */
    private val forwardingRefusal: String?

    /** Why this member forwards no writes to other members, or null if it does. */
    private val sendingRefusal: String?

    override val hosting: DatabaseHosting
        get() = clusterHosting

    /** The routing policies drivers name, from the configuration. */
    private val routingPolicies: RoutingPolicies

    private lateinit var systemGraph: Neo4jSystemGraph
    private lateinit var reconciler: DatabaseReconciler

    /** Replaces the cluster state of copies in quarantine, as dbms.unquarantineDatabase asks. */
    private lateinit var copyReplacement: CopyReplacement
    private lateinit var servers: ServerReconciler
    private lateinit var administration: ServerAdministration
    private lateinit var recreation: DatabaseRecreation

    /** The id of the default database as the system graph has it, read once `system` runs. */
    @Volatile private var clusterId: UUID? = null

    /** The system graph's [Neo4jSystemGraph.systemId], read once `system` runs. */
    @Volatile private var systemId: Long? = null
    private var roles: RolesInitializer? = null

    /** The transactions this member forwards to others, if it does. */
    @Volatile private var remoteTransactions: RemoteTransactions? = null

    /** What the cluster status endpoints of the HTTP server report. */
    private val statuses = ClusterStatuses(::clusterStatuses)

    init {
        val config = globalModule.globalConfig
        val logService = globalModule.logService
        // Before anything creates a Ratis or DozenDB SLF4J logger.
        Slf4jLogRouting.useDebugLogProvider(logService.internalLogProvider)
        val userLog = logService.getUserLog(ClusterService::class.java)
        routingPolicies = RoutingPolicies.of(config)
        val neo4jVersion = Version.getNeo4jVersion()
        lateinit var stores: Neo4jLocalStores
        lateinit var replicationService: ReplicationService
        val formatLevels =
            FormatLevels(
                FormatLevels.SUPPORTED.oldest,
                config.get(ClusterSettings.newestFormatLevel),
            )
        // What this member reports when probed; read once consensus runs.
        val reportNumbers = MemberMetadata.ReportNumbers()
        val metadata = {
            mapOf(
                MemberMetadata.REPORT_NUMBER to reportNumbers.next(),
                MemberMetadata.NEO4J_VERSION to neo4jVersion,
                MemberMetadata.DOZENDB_VERSION to DozenDb.version,
                MemberMetadata.HAS_STORES to stores.existed.toString(),
                MemberMetadata.STORES_READY to systemRunsHere().toString(),
                MemberMetadata.MEMBER_STATE to cluster.state.name,
                MemberMetadata.DATABASES to DatabaseStatesCodec.encode(reportedStates()),
                MemberMetadata.READ_ONLY_DATABASES to readOnlyHere().joinToString(","),
                MemberMetadata.LAST_COMMITTED_TRANSACTIONS to
                    MemberMetadata.numbersText(lastCommittedHere()),
                MemberMetadata.LEADING to MemberMetadata.numbersText(leadingHere()),
                MemberMetadata.FORMAT_LEVELS to formatLevels.toString(),
                MemberMetadata.FORMAT_LEVEL to cluster.formatLevel.toString(),
                MemberMetadata.SYSTEM_DATABASE_MODE to
                    config.get(ClusterSettings.systemDatabaseMode).name,
                MemberMetadata.SYSTEM_GROUP to groups.systemGroup.toString(),
                MemberMetadata.ROUTING_POLICIES to routingPolicies.digest,
            ) +
                cluster.systemReport() +
                seedReport() +
                InitialServerOptions.report(ServerOptions.initial(config)) +
                listOfNotNull(
                    boltHostPort()?.let { MemberMetadata.BOLT_ADDRESS to it.toString() },
                    httpAddress()?.let {
                        MemberMetadata.HTTP_ADDRESS to it.toHostPort().toString()
                    },
                    httpsAddress()?.let {
                        MemberMetadata.HTTPS_ADDRESS to it.toHostPort().toString()
                    },
                    routingHostPort()?.let { MemberMetadata.ROUTING_ADDRESS to it.toString() },
                    backupAddress()?.let {
                        MemberMetadata.BACKUP_ADDRESS to it.toHostPort().toString()
                    },
                    (replicationService.defaultDatabaseId ?: clusterId)?.let {
                        MemberMetadata.CLUSTER_ID to it.toString()
                    },
                    systemId?.let { MemberMetadata.SYSTEM_ID to it.toString() },
                )
        }
        val tuning =
            AbstractEditionModule.tryResolveOrCreate(
                ConsensusTuning::class.java,
                globalModule.externalDependencyResolver,
            ) {
                ConsensusTuning { it }
            }
        val policies =
            globalModule.globalDependencies.resolveDependency(SslPolicyProvider::class.java)
        val tlsLog = logService.getUserLog(ClusterTls::class.java)
        val tls = ClusterTls.of(config, policies, tlsLog)?.let(::ReloadableTls)
        tls?.let { ClusterTls.follow(config, policies, it, tlsLog) }
        memberTls = tls
        forwardingRefusal = ForwardedWrites.refusal(config, tls?.start)
        sendingRefusal = ForwardedWrites.sendingRefusal(config, tls?.start)
        forwardingRefusal?.let {
            userLog.info(
                "Other members do not forward writes to this member when it leads a database: " +
                    "%s. Writes that reach them over neo4j:// or HTTP fail there with NotALeader.",
                it,
            )
        }
        consensus =
            RatisConsensusLog(
                tuning.adjust(
                    RatisConsensusConfig(
                        memberId = memberId,
                        listenHost = config.get(ClusterSettings.listenAddress).hostname,
                        listenPort = config.get(ClusterSettings.listenAddress).port,
                        advertisedAddress =
                            config.get(ClusterSettings.advertisedAddress).toHostPort(),
                        storageDirectory = config.get(ClusterSettings.raftDirectory),
                        electionTimeoutMin = config.get(ClusterSettings.electionTimeoutMin),
                        electionTimeoutMax = config.get(ClusterSettings.electionTimeoutMax),
                        maxEntrySize = config.get(ClusterSettings.raftMaxEntrySize).toInt(),
                        logSegmentSize = config.get(ClusterSettings.raftLogSegmentSize),
                        metadata = metadata,
                        tls = tls,
                    )
                )
            )
        clusterServers =
            ClusterServers(
                consensus,
                groups,
                // Not while `system` starts or is replaced by a copy, which takes a while: the
                // servers are then as last read.
                recorded = {
                    checkNotNull(
                        if (systemRunsHere()) {
                            replicationService.tryReadingSystem(systemGraph::systemSecondaries)
                        } else {
                            null
                        }
                    ) {
                        "the system database cannot be read now"
                    }
                },
                pullsSystem = { clusterHosting.mode(systemDatabase.name) == CopyMode.SECONDARY },
                reportedVoters = {
                    locations.of(systemDatabase)?.let {
                        GroupMembership(it.voters, it.settled, removed = false)
                    }
                },
                reportedLeader = { locations.of(systemDatabase)?.leader },
            )
        val metrics = ClusterMetrics(consensus, clusterServers::all, groups)
        globalModule.globalDependencies.satisfyDependency(metrics)
        stores =
            Neo4jLocalStores(
                globalModule.fileSystem,
                globalModule.neo4jLayout,
                globalModule.pageCache,
                config,
                listOf(systemDatabase.name, defaultDatabase),
                consensus,
                logService.getInternalLog(Neo4jLocalStores::class.java),
                received = { name -> metrics.of(name).copiesReceived.increment() },
                seed = consensus::seed,
                groups = groups,
            )
        localStores = stores
        val catchUp =
            CatchUp(
                consensus,
                { databaseControl() },
                stores::stageReplicated,
                logService.getInternalLog(CatchUp::class.java),
                metrics = metrics,
            )
        replicationService =
            ReplicationService(
                consensus,
                groups,
                defaultDatabase,
                catchUp,
                logService.internalLogProvider,
                ::bootstrapMembers,
                mustDetach = { cluster.mustDetach(it) },
                detached = { cluster.detached(it) },
                notLeader = ::notLeaderMessage,
                adoptDatabases = {
                    val reformed = cluster.reformedWith
                    if (reformed == null) adoptStores() else recordLost(reformed)
                },
                metrics = metrics,
            )
        replication = replicationService
        globalModule.globalDependencies.satisfyDependency(replicationService)
        val upstreams =
            Upstreams(
                memberId,
                UpstreamSelection(
                    config.get(ClusterSettings.upstreamStrategy),
                    toTags = { config.get(ClusterSettings.connectRandomlyToServerTags) },
                    rules =
                        ServerRules.parse(
                            config.get(ClusterSettings.userDefinedUpstreamStrategy).orEmpty()
                        ),
                ),
                group = { database ->
                    locations.of(database)?.let { location ->
                        location.voters.mapTo(HashSet()) { it.id } to location.leader
                    } ?: (emptySet<MemberId>() to null)
                },
                secondaries = { database -> systemGraph.secondaries(database.name) },
                online = { member, database ->
                    directory.databaseStates(member).orEmpty().any {
                        it.id == database.id && it.status == DatabaseState.Status.ONLINE
                    }
                },
                answering = { directory.answering(it) },
                addresses = {
                    try {
                        clusterServers.all().associate { it.id to it.raftAddress }
                    } catch (_: ConsensusUnavailableException) {
                        emptyMap()
                    }
                },
                tags = { systemGraph.serverTags() },
            )
        secondaries =
            Secondaries(
                consensus,
                upstreams = upstreams::of,
                primaries = { database ->
                    locations
                        .of(database)
                        ?.let { location ->
                            location.voters
                                .sortedBy { it.id != location.leader }
                                .map { it.raftAddress }
                        }
                        .orEmpty()
                },
                copies =
                    object : SecondaryCopies {
                        override fun copy(source: HostPort, database: DatabaseRef) =
                            stores.copyNew(source, database, seeded = false)

                        override fun stage(source: HostPort, database: DatabaseRef) =
                            stores.stageSecondary(source, database)
                    },
                marks =
                    SecondaryMarks(
                        config.get(ClusterSettings.raftDirectory).resolve(SECONDARIES_FILE)
                    ),
                control = { databaseControl() },
                pullInterval = config.get(ClusterSettings.pullInterval),
                refusal = { database ->
                    notLeaderMessage(database.name, locations.of(database)?.leader)
                },
                metrics = metrics,
                logProvider = logService.internalLogProvider,
                afterApply = { id -> if (id.isSystemDatabase) systemApplied() },
                replacing = { id, replace ->
                    if (id.isSystemDatabase) replicationService.replacingSystem(replace)
                    else replace()
                },
                groups = groups,
            )
        clusterHosting =
            ClusterHosting(
                replicationService,
                secondaries,
                ImportedStores(
                    globalModule.neo4jLayout,
                    memberId.toString(),
                    { name -> systemGraph.hosts(name).size - systemGraph.secondaries(name).size },
                    { id -> groups.groupOf(id.databaseId().uuid()) in consensus.localGroups() },
                    logService.getInternalLog(ImportedStores::class.java),
                ),
            )
        // Before Neo4j creates `system`, which it does before the cluster service starts.
        pullsSystem =
            systemDatabase.id !in consensus.localGroups() &&
                (config.get(ClusterSettings.systemDatabaseMode) == CopyMode.SECONDARY ||
                    DatabaseGroups.SYSTEM_DATABASE in secondaries.marked())
        if (pullsSystem) {
            clusterHosting.host(NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID, CopyMode.SECONDARY)
        }
        val fileSystem = globalModule.fileSystem
        if (fileSystem is SingleFsyncFileSystem) {
            fileSystem.policy = FsyncPolicy(replicationService::singleFsync)
            userLog.info("Commits to replicated databases force only the Raft log (single fsync)")
        } else {
            userLog.info("Commits to replicated databases force the Raft log and Neo4j's log")
        }
        consensus.registerBulkSource(
            StoreCopySource.NAME,
            StoreCopySource(
                {
                    globalModule.globalDependencies.resolveDependency(
                        DatabaseContextProvider::class.java
                    )
                },
                globalModule.fileSystem,
                replicating = { id ->
                    replicationService.running().any { it.namedDatabaseId == id } ||
                        secondaries.running(id)
                },
                raft = { id -> replicationService.running().any { it.namedDatabaseId == id } },
                snapshots = { name ->
                    replicationService
                        .running()
                        .firstOrNull { it.ref.name == name }
                        ?.let { consensus.currentSnapshot(it.ref) }
                },
                log = logService.getInternalLog(StoreCopySource::class.java),
                served = { name -> metrics.of(name).copiesServed.increment() },
                groups = groups,
            ),
        )
        consensus.registerBulkSource(EntryTermProtocol.NAME, EntryTermSource(consensus))
        consensus.registerBulkSource(SeedChecksums.NAME) { request, out ->
            SeedChecksums.serve(request, out, ::seedChecksums)
        }
        consensus.registerBulkSource(
            ReadBarrierProtocol.NAME,
            ReadBarrierSource(
                ::readBarrier,
                logService.getInternalLog(ReadBarrierSource::class.java),
            ),
        )
        consensus.registerBulkSource(
            PullProtocol.NAME,
            PullSource(
                {
                    globalModule.globalDependencies.resolveDependency(
                        DatabaseContextProvider::class.java
                    )
                },
                logService.getInternalLog(PullSource::class.java),
                served = { name -> metrics.of(name).pullsServed.increment() },
                groups = groups,
            ),
        )
        globalModule.databaseEventListeners.registerDatabaseEventListener(replication)
        globalModule.databaseEventListeners.registerDatabaseEventListener(secondaries)
        val formation = ClusterFormation(consensus, initialMembers(config), neo4jVersion, userLog)
        directory =
            MemberDirectory(
                consensus,
                groups,
                ::boltHostPort,
                ::routingHostPort,
                ::reportedStates,
                logService.getInternalLog(MemberDirectory::class.java),
                refreshInterval = config.get(ClusterSettings.memberRefreshInterval),
                selfReadOnly = ::readOnlyHere,
                selfLastCommitted = ::lastCommittedHere,
                selfReport = metadata,
                servers = clusterServers::all,
                bootstrap = {
                    if (clusterHosting.mode(systemDatabase.name) == CopyMode.SECONDARY) {
                        initialMembers(config)
                    } else {
                        emptyList()
                    }
                },
                reported = PolicyAgreement(routingPolicies.digest, userLog)::reported,
                userLog = logService.getUserLog(MemberDirectory::class.java),
            )
        locations =
            DatabaseLocations(consensus, directory) { database ->
                systemGraph.secondaries(database.name)
            }
        val electionTimeout = config.get(ClusterSettings.electionTimeoutMax)
        balancer =
            LeaderBalancer(
                consensus,
                directory,
                locations,
                replicationService::created,
                {
                    if (systemRunsHere()) {
                        mapOf(systemDatabase to emptySet<MemberId>()) +
                            systemGraph.databases().associate { groups.of(it.id) to it.hosts }
                    } else {
                        replicationService.created().associateWith { emptySet() }
                    }
                },
                { database, to, reason, timeout ->
                    replicationService.replica(database)?.transferLeadership(to, reason, timeout)
                        ?: false
                },
                strategy = { config.get(ClusterSettings.balancingStrategy) },
                priorityTag = DatabasePriorityTag.lookup(config),
                tags = { systemGraph.serverTags() },
                stabilityDelay = config.get(ClusterSettings.balancingDelay),
                transferTimeout = electionTimeout.multipliedBy(2),
                contactLimit = electionTimeout,
                log = logService.getInternalLog(LeaderBalancer::class.java),
            )
        consensus.leaderPlacement = balancer
        announcer =
            LeaderAnnouncer(
                consensus,
                clusterServers::all,
                ::leadingHere,
                ANNOUNCEMENT_DEADLINE,
                logService.getInternalLog(LeaderAnnouncer::class.java),
            )
        cluster =
            ClusterService(
                consensus,
                groups,
                formation,
                stores,
                MembershipFile(
                    config.get(ClusterSettings.raftDirectory).resolve(MembershipFile.FILE_NAME)
                ),
                userLog,
                metadata = metadata,
                replicatedDatabases = { replication.running().map { it.ref } },
                directory = directory,
                formatLevels = formatLevels,
                // The system graph, once the system database runs.
                formatLevelStore =
                    object : FormatLevelStore {
                        override fun formatLevel(): Int = systemGraph.formatLevel()

                        override fun recordFormatLevel(level: Int) =
                            systemGraph.recordFormatLevel(level)
                    },
                servers = clusterServers,
                systemMode = config.get(ClusterSettings.systemDatabaseMode),
                systemHosting = systemHosting(),
            )
        globalModule.globalDependencies.satisfyDependency(cluster)
        globalModule.globalDependencies.satisfyDependency(statuses)
        statusChecks =
            StatusChecks(
                cluster,
                consensus,
                { database ->
                    replication
                        .running()
                        .firstOrNull { it.ref.id == database.id }
                        ?.replicateStatusCheck()
                        ?: throw ConsensusUnavailableException(
                            "Database '${database.name}' does not run on server ${consensus.self.id}."
                        )
                },
                ::serverRecords,
            )
        consensus.registerBulkSource(StatusCheckProtocol.NAME, statusChecks)
        quarantineLifts =
            QuarantineLifts(
                consensus,
                { cluster.members().map { it.member } },
                { quarantine },
                QUARANTINE_LIFT_TIMEOUT,
            )
        consensus.registerBulkSource(QuarantineProtocol.NAME, quarantineLifts)
        globalModule.databaseEventListeners.registerDatabaseEventListener(
            object : DatabaseEventListenerAdapter() {
                override fun databaseStart(eventContext: DatabaseEventContext) {
                    if (eventContext.databaseName == systemDatabase.name) {
                        // A removed system secondary pulls nothing, as a removed primary's group
                        // does not run.
                        if (cluster.state == MemberState.REMOVED) {
                            secondaries.stopPulling(NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID)
                        }
                        clusterServers.systemChanged()
                        cluster.systemChanged()
                        cluster.requestFormatLevelRefresh()
                        // A Free server reports it before it hosts the default database.
                        clusterId =
                            runCatching { systemGraph.databaseId(defaultDatabase) }.getOrNull()
                        systemId = runCatching { systemGraph.systemId() }.getOrNull()
                    }
                }
            }
        )
        membership =
            MembershipReconciler(
                consensus,
                groups,
                { systemGraph.databases().associate { groups.of(it.id) to it.hosts } },
                logService.getInternalLog(MembershipReconciler::class.java),
                systemRunning = ::systemRunsHere,
                systemSecondaries = clusterServers::secondaries,
                lostServers = { systemGraph.lostServers() },
            )
        globalModule.globalLife.add(
            object : LifecycleAdapter() {
                override fun start() {
                    cluster.start()
                    membership.start()
                    balancer.start()
                    announcer.start()
                }

                override fun stop() {
                    try {
                        announcer.stop()
                        balancer.stop()
                        roles?.close()
                        membership.close()
                        cluster.stop()
                    } finally {
                        secondaries.close()
                        replication.close()
                    }
                }
            }
        )
    }

    /** Whether `system` runs here: its group, or its secondary, which pulls. */
    private fun systemRunsHere(): Boolean =
        replication.replica(systemDatabase)?.isRunning() == true ||
            secondaries.running(NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID)

    /**
     * How this member hosts `system` as a secondary: its store is marked a secondary's, as other
     * secondaries' are, and the leader of `system` admits a server by recording it
     * ([SystemSecondaryRecords]).
     */
    private fun systemHosting(): SystemHosting =
        object : SystemHosting {
            override val pulls: Boolean = pullsSystem

            override fun pulled(): Boolean = DatabaseGroups.SYSTEM_DATABASE in secondaries.marked()

            override fun copyPulled(source: HostPort) =
                secondaries.copy(source, NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID)

            override fun dropped(): Boolean =
                try {
                    systemGraph.dropped(memberId)
                } catch (e: Exception) {
                    if (e is InterruptedException) Thread.currentThread().interrupt()
                    false
                }

            override fun admit(member: Member) {
                if (!replication.systemWritable) {
                    val leader = clusterServers.leader()
                    throw MembershipChangeException(
                        "Server ${member.id} hosts the system database as a secondary, which the " +
                            "leader of the system database admits: run dozendb.cluster.addMember " +
                            "there${leader?.let { ", on member $it" }.orEmpty()}."
                    )
                }
                try {
                    systemGraph.admitSecondary(member)
                } catch (e: Exception) {
                    throw MembershipChangeException(
                        "Admitting server ${member.id} failed: ${e.message}",
                        e,
                    )
                }
            }

            override fun promote(member: MemberId) {
                if (!replication.systemWritable) {
                    throw MembershipChangeException(
                        "This member does not lead the system database, which asks system " +
                            "secondaries to turn primary."
                    )
                }
                try {
                    systemGraph.promoteSecondary(member)
                } catch (e: Exception) {
                    throw MembershipChangeException(
                        "Asking system secondary $member to turn primary failed: ${e.message}",
                        e,
                    )
                }
            }

            override fun promoted(): Boolean =
                try {
                    systemGraph.promoted(memberId)
                } catch (e: Exception) {
                    if (e is InterruptedException) Thread.currentThread().interrupt()
                    false
                }

            override fun turnSecondary() = turnSystemSecondary()

            override fun turnPrimary(): Boolean = turnSystemPrimary()

            override fun forget(member: MemberId) {
                if (!replication.systemWritable) {
                    throw MembershipChangeException(
                        "This member does not lead the system database, which records system " +
                            "secondaries."
                    )
                }
                try {
                    systemGraph.forgetSecondary(member)
                } catch (e: Exception) {
                    throw MembershipChangeException(
                        "Removing system secondary $member failed: ${e.message}",
                        e,
                    )
                }
            }
        }

    /**
     * Turns `system` here, whose group no longer counts this member in, into a secondary from its
     * own store: with `system` stopped, while nothing reads it, its group goes, its store is marked
     * a secondary's, and it starts again as the same database, which then pulls.
     */
    private fun turnSystemSecondary() {
        val id = NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID
        replication.replacingSystem {
            databaseControl().stop(id)
            replication.release(id)
            secondaries.mark(id)
            clusterHosting.switchSystem(CopyMode.SECONDARY)
            check(databaseControl().start(id)) { "the system database did not start again" }
        }
        systemApplied()
    }

    /**
     * Turns `system` here from a secondary into a primary that runs its group from its own store,
     * as other databases switch ([Neo4jLocalDatabases]): the store's last transaction tells the
     * index of its Raft entry, the leader of `system` its term, which seed the group; when the
     * leader's log no longer holds that entry, `system` is replaced by a copy of the leader's with
     * its snapshot. It runs the group as a member that waits to be added. Returns false, pulling
     * on, while the leader is not known.
     */
    private fun turnSystemPrimary(): Boolean {
        val id = NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID
        val location = locations.of(systemDatabase) ?: return false
        val leader = location.voters.firstOrNull { it.id == location.leader } ?: return false
        val last = secondaries.stopPulling(id) ?: return false
        val index = last.consensusIndex()
        val told = if (index < 0) null else entryTerm(systemDatabase, index)
        if (told == EntryTerm.Unknown) {
            secondaries.startPulling(id)
            return false
        }
        val copy =
            if (told == EntryTerm.Pruned) {
                try {
                    localStores.stageReplicated(leader.raftAddress, systemDatabase, index)
                } catch (e: Exception) {
                    if (e is InterruptedException) throw e
                    globalModule.logService
                        .getInternalLog(ClusterService::class.java)
                        .warn("Copying the system database from %s failed: %s", leader, e.message)
                    secondaries.startPulling(id)
                    return false
                }
            } else {
                null
            }
        replication.replacingSystem {
            databaseControl().stop(id)
            secondaries.remove(id)
            when {
                copy != null -> {
                    copy.moveIntoPlace()
                    consensus.seed(systemDatabase, copy.snapshot)
                }
                told is EntryTerm.Known ->
                    consensus.seed(
                        systemDatabase,
                        Snapshot(LogPosition(index, told.term), index),
                    )
            }
            clusterHosting.switchSystem(CopyMode.PRIMARY)
            // Without voting until the leader adds it.
            consensus.joinGroup(systemDatabase, location.voters.filter { it.id != memberId })
            check(databaseControl().start(id)) { "the system database did not start again" }
        }
        systemApplied()
        return true
    }

    /**
     * Community's read-only checker, which also tells the balancer when the databases read-only
     * here change, so that this member hands over the leadership of those it leads.
     */
    override fun createGlobalReadOnlyChecker(
        systemDatabaseProvider: SystemDatabaseProvider,
        databaseIdRepository: DatabaseIdRepository,
        globalModule: GlobalModule,
    ) {
        val checker =
            createGlobalReadOnlyChecker(
                setOf(SystemGraphReadOnlyDatabaseLookupFactory.DEFAULT_PROVIDER),
                systemDatabaseProvider,
                { name -> databaseIdRepository.getByName(name).map { it.databaseId() } },
                { balancer.request() },
                globalModule,
            )
        globalReadOnlyChecker = checker
        globalModule.globalLife.add(
            SystemGraphReadOnlyListener(globalModule.transactionEventListeners, checker)
        )
        globalModule.globalDependencies.satisfyDependency(checker)
    }

    /**
     * This member's databases as it reports them to the others, by the ids of their groups (see
     * [DatabaseGroups]): those it copies from another member, new ones or to catch up, as being
     * copied; new ones that run before their group as starting, or as failed if their group could
     * not start; whether it knows their leaders; and the mode it hosts each in.
     */
    private fun reportedStates(): List<DatabaseState> {
        val catchingUp = replication.catchingUp() + secondaries.catchingUp()
        val copying = localStores.copying()
        val starting = replication.starting()
        val failedToStart = replication.failedToStart()
        val led = leaderKnownHere()
        val states =
            databaseStates()
                .filter { it.name !in copying }
                .map {
                    when {
                        it.name in catchingUp -> it.copy(status = DatabaseState.Status.COPYING)
                        it.status != DatabaseState.Status.ONLINE -> it
                        it.name in failedToStart ->
                            it.copy(
                                status = DatabaseState.Status.FAILED,
                                failure = failedToStart[it.name],
                            )
                        it.name in starting -> it.copy(status = DatabaseState.Status.STARTING)
                        else -> it
                    }.copy(
                        id = groups.groupOf(it.id),
                        leaderKnown = it.name in led,
                        mode = clusterHosting.mode(it.name),
                        paused = secondaries.paused(it.name),
                    )
                }
        return states +
            copying.map { (name, id) ->
                DatabaseState(
                    name,
                    id,
                    DatabaseState.Status.COPYING,
                    leaderKnown = false,
                    mode = clusterHosting.mode(name),
                )
            }
    }

    /** The names of the replicated databases whose leader this member knows. */
    private fun leaderKnownHere(): Set<String> =
        replication
            .created()
            .filter { ref ->
                runCatching { consensus.status(ref).leader != null }.getOrDefault(false)
            }
            .map { it.name }
            .toSet()

    /**
     * The ids of the replicated databases that are read-only on this member by its configuration
     * (`server.databases.read_only` and the like), not those read-only on every member.
     */
    private fun readOnlyHere(): Set<UUID> {
        val checker = globalReadOnlyChecker ?: return emptySet()
        return replication
            .created()
            .map { it.id }
            .filter { checker.isReadOnlyLocally(DatabaseIdFactory.from(groups.databaseOf(it))) }
            .toSet()
    }

    /** The groups this member leads, with their terms. */
    private fun leadingHere(): Map<UUID, Long> =
        replication
            .created()
            .mapNotNull { ref ->
                val status = runCatching { consensus.status(ref) }.getOrNull()
                status?.takeIf { it.role == Role.Leader }?.let { ref.id to it.term }
            }
            .toMap()

    /**
     * The id of the last transaction committed to each database that runs on this member, by the id
     * of its group.
     */
    private fun lastCommittedHere(): Map<UUID, Long> =
        transactionIdStoresHere().mapValues { it.value.lastCommittedTransactionId }

    /** The transaction id store of each database that runs on this member, by its group's id. */
    private fun transactionIdStoresHere(): Map<UUID, TransactionIdStore> =
        databaseRepository
            .registeredDatabases()
            .filterValues { it.database().isStarted }
            .mapNotNull { (id, context) ->
                val dependencies = context.dependencies()
                if (!dependencies.containsDependency(TransactionIdStore::class.java))
                    return@mapNotNull null
                groups.groupOf(id.databaseId().uuid()) to
                    dependencies.resolveDependency(TransactionIdStore::class.java)
            }
            .toMap()

    /** DozenDB's web server, with Neo4j Enterprise's cluster status endpoints. */
    override fun createWebServer(
        managementService: DatabaseManagementService,
        transactionManager: TransactionManager,
        globalDependencies: Dependencies,
        config: Config,
        userLogProvider: InternalLogProvider,
        dbmsInfo: DbmsInfo,
    ): Lifecycle =
        ClusterNeoWebServer(
            managementService,
            transactionManager,
            globalDependencies,
            config,
            userLogProvider,
            dbmsInfo,
            globalModule.memoryPools,
            globalModule.globalMonitors,
            globalModule.globalClock,
            statuses,
        )

    /**
     * The databases this member hosts, as the cluster status endpoints report them: a primary as
     * its Raft group has it, a secondary as it learned from the primaries.
     */
    private fun clusterStatuses(): List<DatabaseClusterStatus> {
        val readOnly = readOnlyHere()
        val pulled = lastPulledHere()
        return reportedStates().map { state ->
            val database = DatabaseRef(state.name, state.id)
            val core = state.mode == CopyMode.PRIMARY
            val group = if (core) runCatching { consensus.status(database) }.getOrNull() else null
            val location = locations.of(database)
            val running = state.status == DatabaseState.Status.ONLINE
            DatabaseClusterStatus(
                name = state.name,
                id = groups.databaseOf(state.id),
                core = core,
                running = running,
                writable = running && group?.role == Role.Leader && state.id !in readOnly,
                lastAppliedRaftIndex = group?.appliedIndex ?: pulled[state.id] ?: 0,
                participatingInRaftGroup =
                    core && location?.local == true && location.leader != null,
                votingMembers = location?.voters.orEmpty().map { it.id.toString() },
                healthy = state.status != DatabaseState.Status.FAILED,
                memberId = memberId.toString(),
                leader = (group?.leader ?: location?.leader)?.toString(),
                millisSinceLastLeaderMessage =
                    if (core) {
                        runCatching { consensus.progress(database)?.sinceLeaderContactMillis }
                            .getOrNull()
                    } else {
                        null
                    },
            )
        }
    }

    /**
     * The last transaction of the store of [id], which does not run here, read from its files; null
     * if it has none.
     */
    private fun lastTransactionOnDisk(id: NamedDatabaseId): TransactionId? {
        val fs = globalModule.fileSystem
        val layout = globalModule.neo4jLayout.databaseLayout(id.name())
        val engine =
            StorageEngineFactory.selectStorageEngine(fs, layout).orElse(null) ?: return null
        return LogTailExtractor(fs, globalModule.globalConfig, engine, DatabaseTracers.EMPTY)
            .getTailMetadata(layout, EmptyMemoryTracker.INSTANCE)
            .lastCommittedTransaction
    }

    /** The Raft index of the last transaction of each database that runs on this member. */
    private fun lastPulledHere(): Map<UUID, Long> =
        transactionIdStoresHere().mapValues {
            it.value.lastCommittedTransaction.consensusIndex().coerceAtLeast(0)
        }

    /** SHOW DATABASES with a row for each member a database runs on, as Neo4j Enterprise has. */
    override fun createTopologyInfoService(
        databaseContextProvider: DatabaseContextProvider<*>
    ): TopologyInfoService =
        ClusterTopologyInfoService(
            super.createTopologyInfoService(databaseContextProvider),
            consensus,
            groups,
            directory,
            locations,
            directory::reported,
            voters = { clusterServers.voters().members },
        )

    /** The base wiring, with the databases following the replicated system graph. */
    override fun createDatabaseContextProvider(
        globalModule: GlobalModule
    ): DatabaseContextProvider<*> {
        val provider = super.createDatabaseContextProvider(globalModule)
        val repository = databaseRepository
        systemGraph =
            Neo4jSystemGraph(
                { repository.getSystemDatabaseContext().databaseFacade() },
                replication,
                ::awaitSystemCurrent,
                ::clusterPlacement,
            )
        val localDatabases =
            Neo4jLocalDatabases(
                databaseManager,
                repository,
                localStores,
                clusterHosting,
                replication,
                secondaries,
                ::seeder,
                ::entryTerm,
                consensus::seed,
                quarantines,
                groups,
                detachOnStart = { cluster.detachOnStart(it) },
                lastTransaction = ::lastTransactionOnDisk,
            )
        copyReplacement =
            CopyReplacement(
                consensus,
                groups,
                localDatabases,
                { cluster.members().map { it.member } },
                QUARANTINE_LIFT_TIMEOUT,
                globalModule.logService.getUserLog(CopyReplacement::class.java),
            )
        reconciler =
            DatabaseReconciler(
                systemGraph,
                localDatabases,
                memberId,
                defaultDatabase,
                ::copySources,
                clusterServers::settled,
                { directory.answering(it) },
                { cluster.state == MemberState.REMOVED },
                { member, id ->
                    directory.databaseStates(member).orEmpty().any {
                        it.id == id.databaseId().uuid() && it.status == DatabaseState.Status.ONLINE
                    }
                },
                ClusterSeeds(),
                globalModule.logService.getInternalLog(DatabaseReconciler::class.java),
            )
        servers =
            ServerReconciler(
                consensus,
                groups,
                { repository.getSystemDatabaseContext().databaseFacade() },
                { block -> replication.readingSystem(block) },
                leading = { replication.systemWritable },
                current = ::awaitSystemCurrent,
                // A member that joined may not have been probed yet.
                report = { member ->
                    directory.reported(member)
                        ?: run {
                            directory.refresh(PROBE_DEADLINE)
                            directory.reported(member)
                        }
                },
                autoEnable = globalModule.globalConfig.get(ClusterSettings.autoEnableFreeServers),
                log = globalModule.logService.getInternalLog(ServerReconciler::class.java),
                remove = cluster::removeMember,
                holders = { database -> locations.of(database)?.voters?.map { it.id }.orEmpty() },
                available = directory::answering,
                defaults =
                    Placements.Defaults(
                        globalModule.globalConfig.get(ClusterSettings.defaultPrimaries),
                        globalModule.globalConfig.get(ClusterSettings.defaultSecondaries),
                    ),
            )
        administration =
            ServerAdministration(
                { repository.getSystemDatabaseContext().databaseFacade() },
                { block -> replication.readingSystem(block) },
                leading = { replication.systemWritable },
                servers = servers(),
                placement = placement(),
                defaults =
                    Placements.Defaults(
                        globalModule.globalConfig.get(ClusterSettings.defaultPrimaries),
                        globalModule.globalConfig.get(ClusterSettings.defaultSecondaries),
                    ),
                autoEnable = globalModule.globalConfig.get(ClusterSettings.autoEnableFreeServers),
            )
        recreation =
            DatabaseRecreation(
                { repository.getSystemDatabaseContext().databaseFacade() },
                { block -> replication.readingSystem(block) },
                leading = { replication.systemWritable },
                placement = clusterPlacement(),
                defaults =
                    Placements.Defaults(
                        globalModule.globalConfig.get(ClusterSettings.defaultPrimaries),
                        globalModule.globalConfig.get(ClusterSettings.defaultSecondaries),
                    ),
                seeder = ::seeder,
                quarantined = { id ->
                    clusterServers
                        .all()
                        .filter { member ->
                            directory.databaseStates(member.id).orEmpty().any {
                                it.id == id && it.status == DatabaseState.Status.QUARANTINED
                            }
                        }
                        .mapTo(HashSet()) { it.id.uuid }
                },
                servers = servers(),
            )
        cluster.onRemoved = {
            reconciler.request()
            // As a removed member's system group stops: it learns of no change any more.
            secondaries.stopPulling(NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID)
        }
        // A copy no longer placed here is deleted once this member leaves the database's group.
        replication.afterGroupChange = {
            reconciler.request()
            // Also when this member starts or stops leading a group.
            announcer.request()
        }
        replication.afterSystemApply = ::systemApplied
        return provider
    }

    /**
     * After transactions of `system` were applied here from Raft, or pulled, or `system` was
     * replaced by a copy: they fire no transaction event listeners, so this refreshes what the
     * listeners refresh after local commits, and has what follows the system graph look again.
     */
    private fun systemApplied() {
        clusterServers.systemChanged()
        databaseIdCache.invalidateAll()
        referenceCache.invalidateAll()
        refreshDefaultLanguages()
        (defaultDatabaseResolver as? DozenDbDefaultDatabaseResolver)?.clearCache()
        globalReadOnlyChecker?.refresh()
        reconciler.request()
        // A new database may need members added since its initial members were recorded.
        membership.request()
        servers.request()
        cluster.systemChanged()
        cluster.requestFormatLevelRefresh()
    }

    /**
     * Waits until `system` here holds every transaction the cluster had committed when this was
     * called: a Raft ReadIndex where this member votes in its group, a read barrier at a primary
     * where it pulls it. Returns false if that could not be confirmed in time.
     */
    private fun awaitSystemCurrent(): Boolean {
        if (secondaries.secondary(NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID) == null) {
            return replication.awaitSystemCurrent()
        }
        return try {
            secondaries.awaitLatest(NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID, SYSTEM_READ_TIMEOUT)
            true
        } catch (_: ReadConsistencyException) {
            false
        }
    }

    /**
     * The members this member starts the group of the database [id] with when it has no local state
     * for it: those that vote in it, as they report it now, so that a member that copied the
     * database joins the group as it is, also one that the group removed before, and waits to be
     * added (see `MembershipReconciler`); while no member runs the group yet, its initial members,
     * which the system leader recorded with its creator. A member that is not among them waits to
     * be added.
     */
    private fun bootstrapMembers(id: NamedDatabaseId): List<Member> {
        if (!id.isSystemDatabase) {
            try {
                directory.refresh(PROBE_DEADLINE)
            } catch (_: ConsensusUnavailableException) {
                // What the members reported last.
            }
            locations.of(groups.of(id))?.voters?.let {
                return it
            }
        }
        return checkNotNull(systemGraph.initialMembers(id)) {
            "No initial members are recorded for database $id"
        }
    }

    /**
     * Makes the databases whose stores this member holds the new cluster's, as the member that
     * forms it with databases of its own, in local transactions on `system` before it replicates
     * (see [Neo4jSystemGraph.adopt]): their groups start with this member alone.
     */
    private fun adoptStores() {
        val databases = systemGraph.adopt(consensus.self) { localStores.complete(it) }
        // A store counts only together with its group.
        for (id in databases) {
            consensus.joinGroup(groups.of(id), listOf(consensus.self))
        }
        globalModule.logService
            .getUserLog(ClusterService::class.java)
            .info(
                "This member formed the cluster with databases of its own, %s: it holds them " +
                    "until the system leader places them, and the servers they are placed on copy " +
                    "them",
                databases.joinToString { it.name() },
            )
    }

    /**
     * Marks the servers that `system` was re-formed without lost, as the member that re-formed it
     * from its store, in a local transaction before `system` replicates (see
     * [Neo4jSystemGraph.recordLost]).
     */
    private fun recordLost(members: List<Member>) {
        val lost = systemGraph.recordLost(members)
        globalModule.logService
            .getUserLog(ClusterService::class.java)
            .info(
                "This member re-formed the system database with %s. The servers it records " +
                    "besides them and its secondaries are lost: %s. They stay servers, with the " +
                    "databases placed on them, until they rejoin or DROP SERVER drops them.",
                members,
                lost.ifEmpty { "none" },
            )
    }

    /** Starts creating the roles of a cluster without them, once (see [RolesInitializer]). */
    @Synchronized
    private fun startRoles() {
        if (roles != null) return
        roles =
            RolesInitializer(
                    consensus,
                    groups,
                    { databaseRepository.getSystemDatabaseContext().databaseFacade() },
                    replication::readingSystem,
                    securityComponent,
                    globalModule.logService.getInternalLog(RolesInitializer::class.java),
                )
                .also { it.start() }
    }

    /**
     * The members to copy a new [database] from: those that vote in its group, then the servers it
     * is placed on, its creator first.
     */
    private fun copySources(database: DesiredDatabase): List<HostPort> {
        val id = database.id
        val voters = locations.of(groups.of(id))?.voters.orEmpty()
        val hosts = clusterServers.all().filter { it.id in database.hosts }
        return (voters + hosts)
            .distinctBy { it.id }
            .filter { it.id != memberId }
            .sortedBy { it.id != database.creator }
            .map { it.raftAddress }
    }

    /**
     * Neo4j's system graph initialiser, run only by the member that founds the cluster: it writes
     * the first system graph with local transactions, after which the system database is
     * replicated. Every other member copies that system graph, and changes to it come through Raft.
     * The roles of a cluster without them are looked after once this member's initialiser ran, so
     * that a founder that leads does not create them while its initialiser does.
     */
    override fun registerDatabaseInitializers(
        globalModule: GlobalModule,
        systemDatabaseProvider: SystemDatabaseProvider,
    ) {
        val initializer =
            DefaultSystemGraphInitializer(systemDatabaseProvider::database, systemGraphComponents)
        val clusterInitializer =
            object : SystemGraphInitializer() {
                override fun initializeSystemGraph() {
                    if (replication.systemBootstrapping) {
                        initializer.start()
                        // Its databases, the default one for example, live on in the new cluster.
                        if (localStores.existed) adoptStores()
                        replication.systemGraphInitialized()
                    }
                    startRoles()
                }
            }
        globalModule.globalDependencies.satisfyDependency(clusterInitializer)
        globalModule.globalLife.add(clusterInitializer)
        registerDefaultDatabaseInitializer(globalModule)
        // Once the system and default databases run; stops before the databases stop, so that a
        // member that stops makes no balancing moves, does not create roles and does not change
        // database groups meanwhile, and does not read the system graph while it stops. The
        // lifecycle above stops them again, as when this one did not run.
        globalModule.globalLife.add(
            object : LifecycleAdapter() {
                override fun start() {
                    reconciler.start()
                    servers.start()
                }

                override fun stop() {
                    balancer.stop()
                    roles?.close()
                    membership.close()
                    servers.close()
                    reconciler.close()
                }
            }
        )
    }

    /**
     * Nothing: the reconciler creates, copies and starts the default database on the servers it is
     * placed on, like every other database, once the system leader placed it.
     */
    override fun registerDefaultDatabaseInitializer(globalModule: GlobalModule) {}

    /**
     * How quarantines stop and bring back copies of databases: a copy stops on this member only, so
     * that one that leads hands its leadership over, and its group runs on detached, voting and
     * keeping entries. Once the quarantine is lifted, the reconciler brings the copy back;
     * `system`, which it does not handle, starts again and rejoins its group.
     */
    override fun quarantineCopies(): DatabaseQuarantine.Copies =
        object : DatabaseQuarantine.Copies {
            override fun stop(id: NamedDatabaseId) {
                replication.replica(id)?.handOverOnStop = true
                databaseManager.stopDatabase(id)
            }

            override fun reconcile(id: NamedDatabaseId) {
                if (id.isSystemDatabase) {
                    replication.replica(id)?.handOverOnStop = true
                    databaseManager.startDatabase(id)
                } else {
                    reconciler.request()
                }
            }

            override fun replace(id: NamedDatabaseId, keepStore: Boolean) =
                copyReplacement.replace(id, keepStore)
        }

    override fun remoteQuarantines(): Unquarantines.Remote = quarantineLifts

    /** Stops and starts databases through the database manager. */
    private fun databaseControl(): DatabaseControl =
        object : DatabaseControl {
            override fun stop(id: NamedDatabaseId) = databaseManager.stopDatabase(id)

            override fun start(id: NamedDatabaseId): Boolean {
                databaseManager.startDatabase(id)
                return databaseRepository
                    .getDatabaseContext(id)
                    .map { it.database().isStarted }
                    .orElse(false)
            }
        }

    /** Why this member refuses a write on [database], which [leader] leads, and where it can go. */
    private fun notLeaderMessage(database: String, leader: MemberId?): String =
        ForwardedWrites.notLeader(
            globalModule.globalConfig,
            database,
            leader,
            leader?.let(directory::boltAddress),
            sendingRefusal,
        )

    /**
     * Whether this member is the cluster's only server, as Neo4j's routing asks before routing. Not
     * while it stops: then the database checks that follow refuse the route as unavailable, which
     * drivers retry on another member, instead of failing it with an unknown error.
     */
    private fun alone(): Boolean =
        try {
            cluster.members().size <= 1
        } catch (_: ConsensusUnavailableException) {
            false
        }

    /**
     * Where `system` is a secondary, it pulls the transaction a member's login waits for at once.
     */
    override fun awaitingSystem(transactionId: Long) =
        secondaries.pullUpTo(
            NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID,
            transactionId,
            bookmarkTimeout(),
        )

    /** Members log in on the routing connector only if they can forward writes to this member. */
    override fun memberLogins(): MemberLogins =
        forwardingRefusal?.let { MemberLogins.Refused(it) } ?: MemberLogins.Allowed

    /** This member's advertised Bolt address, with the bound port if the configured one is 0. */
    private fun boltHostPort(): HostPort? = boltAddress()?.toHostPort()

    /**
     * This member's advertised routing address, with the bound port if the configured one is 0, or
     * null if it takes no forwarded writes.
     */
    private fun routingHostPort(): HostPort? {
        if (forwardingRefusal != null) return null
        val advertised = globalModule.globalConfig.get(ClusterSettings.routingAdvertisedAddress)
        val port =
            advertised.port.takeIf { it > 0 }
                ?: globalModule.connectorPortRegister
                    .getLocalAddress(ConnectorType.INTRA_BOLT)
                    ?.port
                ?: return null
        return HostPort(advertised.hostname, port)
    }

    /**
     * The members of the cluster that host [wanted], which WAIT waits for: those it is placed on,
     * as primaries or secondaries, or, for one dropped or not placed, the enabled servers, which
     * may hold it. Each reports its databases when probed; this member reads its own.
     */
    override fun hostingServers(wanted: WantedDatabase): List<HostingServer> {
        val placed = if (wanted.id == null) emptySet() else systemGraph.hosts(wanted.name)
        return clusterServers
            .all()
            .filter { if (placed.isEmpty()) systemGraph.enabled(it.id) else it.id in placed }
            .map { member ->
                object : HostingServer {
                    override val id: UUID = member.id.uuid
                    override val address: String =
                        (directory.boltAddress(member.id) ?: member.raftAddress).toString()

                    override fun databases(): List<DatabaseState> {
                        if (member.id == memberId) return reportedStates()
                        val peer = runBlocking { consensus.probe(member.raftAddress) }
                        check(peer.member.id == member.id) {
                            "${member.raftAddress} is ${peer.member.id} now, not ${member.id}"
                        }
                        val reported =
                            checkNotNull(peer.metadata[MemberMetadata.DATABASES]) {
                                "it does not report its databases"
                            }
                        return DatabaseStatesCodec.decode(reported)
                    }
                }
            }
    }

    /**
     * The enabled servers, which a database can be created from with existingDataSeedServer: it is
     * placed on that server first.
     */
    override fun seedServers(): SeedServers = SeedServers { id ->
        require(clusterServers.all().any { it.id.uuid == id && systemGraph.enabled(it.id) }) {
            "server $id is not an enabled server of this cluster: SHOW SERVERS lists the servers"
        }
    }

    /**
     * The servers that serve backups of a database, as Neo4j Enterprise chooses them: its
     * secondaries that pull, then its primaries that do not lead it, then the one that does, each
     * by the backup address it reported.
     */
    override fun backupSources(): BackupSources = BackupSources { name ->
        val database =
            systemGraph.databaseId(NormalizedDatabaseName(name).name())?.let {
                DatabaseRef(NormalizedDatabaseName(name).name(), it)
            } ?: return@BackupSources emptyList()
        val location = locations.of(database)
        val primaries = location?.voters.orEmpty().map { it.id }
        val leader = location?.leader
        (locations.secondaries(database) +
                primaries.filter { it != leader }.sorted() +
                listOfNotNull(leader?.takeIf { it in primaries }))
            .distinct()
            .mapNotNull { directory.backupAddress(it)?.toString() }
    }

    /** Where the copies of databases go: on the servers the allocator chooses. */
    override fun placement(): DatabasePlacement = clusterPlacement()

    private fun clusterPlacement(): ClusterPlacement =
        ClusterPlacement(
            refresh = {
                try {
                    directory.refresh(PROBE_DEADLINE)
                } catch (_: ConsensusUnavailableException) {
                    // Placed by what the members answered last.
                }
            },
            available = { directory.answering(MemberId(it)) },
            recordInitial = { tx -> servers.recordInitial(tx) },
            defaults =
                Placements.Defaults(
                    globalModule.globalConfig.get(ClusterSettings.defaultPrimaries),
                    globalModule.globalConfig.get(ClusterSettings.defaultSecondaries),
                ),
            holders = { name ->
                val id = systemGraph.databaseId(name)
                id?.let { locations.of(groups.of(name, it))?.voters?.map { m -> m.id.uuid } }
                    .orEmpty()
            },
        )

    /** The servers of the cluster, with the options they report from `initial.server.*`. */
    override fun servers(): Servers =
        object : Servers {
            override fun ids(): Set<UUID> = clusterServers.all().mapTo(HashSet()) { it.id.uuid }

            // Asked now: the directory may not have probed a member that just joined.
            override fun initialOptions(id: UUID): ServerOptions? {
                if (id == memberId.uuid) return ServerOptions.initial(globalModule.globalConfig)
                val member = clusterServers.all().firstOrNull { it.id.uuid == id } ?: return null
                val peer =
                    try {
                        runBlocking {
                            withTimeoutOrNull(PROBE_DEADLINE.toMillis()) {
                                consensus.probe(member.raftAddress)
                            }
                        }
                    } catch (_: ConsensusUnavailableException) {
                        null
                    } ?: return null
                if (peer.member.id != member.id) return null
                return InitialServerOptions.reported(peer.metadata)
            }

            override fun address(id: UUID): String? =
                clusterServers.all().firstOrNull { it.id.uuid == id }?.raftAddress?.toString()

            override fun holding(tx: Transaction, id: UUID): Set<String> {
                // As members report it now, as SHOW SERVERS judges Deallocated: a member that
                // left a group another member led may not have said so since.
                try {
                    directory.refresh(PROBE_DEADLINE)
                } catch (_: ConsensusUnavailableException) {
                    // What the members reported last.
                }
                return locations.votedIn(tx, MemberId(id))
            }
        }

    /**
     * Waits until this member has applied what its database's Raft group had committed: a Raft
     * ReadIndex, as for linearizable reads, on the leader or a follower; a secondary pulls up to a
     * read barrier, unless it is paused: its backup then holds what it held when paused, as Neo4j
     * Enterprise documents for point-in-time backups.
     */
    override fun latestWrites(): LatestWrites = LatestWrites { database, timeout ->
        if (secondaries.secondary(database) != null) {
            if (!secondaries.paused(database.name())) secondaries.awaitLatest(database, timeout)
            return@LatestWrites
        }
        val replica =
            replication.replica(database)
                ?: throw IllegalStateException(
                    "database '${database.name()}' is not replicated on this member"
                )
        val confirmed =
            try {
                runBlocking {
                    withTimeoutOrNull(timeout.toMillis()) { consensus.readIndex(replica.ref) }
                }
            } catch (e: ConsensusUnavailableException) {
                throw IOException(
                    "its Raft group did not confirm what it committed: ${(e.cause ?: e).message}",
                    e,
                )
            }
        if (confirmed == null) {
            throw TimeoutException(
                "its Raft group did not confirm what it committed within $timeout"
            )
        }
    }

    /**
     * DozenDB's routing service, with routing tables of replicated databases taken from their Raft
     * groups. The member is alone, and drivers get tables that name only this member, only in a
     * one-member cluster.
     */
    override fun createRoutingService(
        databaseContextProvider: DatabaseContextProvider<*>,
        clientRoutingDomainChecker: ClientRoutingDomainChecker,
    ): RoutingService {
        val config = globalModule.globalConfig
        val singleAddress = SingleAddressRoutingTable(globalModule.connectorPortRegister, config)
        val local = LocalRoutingTables(databaseContextProvider, singleAddress)
        val routing =
            ClusterRoutingTableProvider(
                locations,
                directory,
                ROUTING_REFRESH_WAIT,
                { config.get(ClusterSettings.readsOnWriters) },
                { config.get(GraphDatabaseSettings.routing_ttl) },
                ClusterRoutingValidator(
                    local,
                    locations,
                    directory,
                    ROUTING_REFRESH_WAIT,
                    quarantined = { quarantines.of(it) != null },
                    groups = groups,
                ),
                local,
                config.get(ClusterSettings.readsOnPrimaries),
                groups,
                routingPolicies,
                { systemGraph.serverTags() },
                config.get(ClusterSettings.loadBalancingShuffle),
            )
        return DozenDbRoutingService(
            ConfirmedReferences(referenceCache) {
                awaitLinearizable(NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID)
            },
            defaultDatabaseResolver,
            routing,
            singleAddress,
            clientRoutingDomainChecker,
            config,
            ::alone,
        )
    }

    override fun createBoltDatabaseManagementServiceProvider():
        BoltGraphDatabaseManagementServiceSPI =
        RoutingBoltDatabases(
            ConsistentBoltDatabases(
                super.createBoltDatabaseManagementServiceProvider(),
                globalModule.globalConfig.get(ClusterSettings.defaultReadConsistency),
                { transactionId ->
                    secondaries.pullUpTo(
                        NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID,
                        transactionId,
                        bookmarkTimeout(),
                    )
                },
            ) {
                // Neo4j's query router registers it, after the Bolt databases are made.
                globalModule.globalDependencies.resolveDependency(
                    LocalGraphTransactionIdTracker::class.java
                )
            },
            globalModule.globalConfig,
        )

    /**
     * Neo4j Community's query router, whose transactions here wait for linearizable reads, and
     * which runs write transactions of clients that allow routing on the leader of their databases
     * when this member does not lead them, if it can: with server-side routing on and mutual TLS
     * between members.
     */
    override fun createQueryRouterBootstrap(
        databaseProvider: DatabaseContextProvider<*>,
        securityLog: AbstractSecurityLog,
        remoteAliases: DatabaseTransactionFactory<Location.Remote>,
    ): DozenDbQueryRouterBootstrap =
        ClusterQueryRouterBootstrap(
            globalModule,
            databaseProvider,
            referenceCache,
            securityLog,
            globalModule.globalConfig.get(ClusterSettings.defaultReadConsistency),
            ::awaitLinearizable,
            { id, transactionId ->
                secondaries.pullUpTo(id, transactionId, bookmarkTimeout())
            },
            forwarding(),
            remoteAliases,
            ::hostedHere,
        )

    /** How this member forwards writes to the leaders of their databases, if it does. */
    private fun forwarding(): ClusterQueryRouterBootstrap.Forwarding? {
        val config = globalModule.globalConfig
        val tls = memberTls
        if (tls == null || sendingRefusal != null) return null
        val logs = globalModule.logService.internalLogProvider
        val remote =
            RemoteTransactions(
                    config,
                    MemberSsl.context(tls),
                    tls.start.verifyHostname,
                    ::lastSystemTransaction,
                    logs,
                )
                .also { remoteTransactions = it }
        val electionTimeout = config.get(ClusterSettings.electionTimeoutMax)
        val forwarding =
            ForwardingLocations(
                ::hostedHere,
                locations,
                consensus,
                directory::routingAddress,
                { member -> directory.reported(member) != null },
                { member, id ->
                    directory.databaseStates(member).orEmpty().any {
                        it.id == id && it.status == DatabaseState.Status.ONLINE
                    }
                },
                electionTimeout,
                logs.getLog(ForwardingLocations::class.java),
                config.get(ClusterSettings.readsOnPrimaries),
                groups,
            )
        globalModule.globalLife.add(
            ForwardingWatch(forwarding, remote, electionTimeout.multipliedBy(2))
        )
        return ClusterQueryRouterBootstrap.Forwarding(forwarding::locationService, remote)
    }

    /** Whether the database [id] is hosted here: its copy is registered on this member. */
    private fun hostedHere(id: NamedDatabaseId): Boolean =
        databaseRepository.getDatabaseContext(id).isPresent

    /**
     * Waits until [id] here holds everything the cluster had committed when this was called, for a
     * linearizable read: a Raft ReadIndex on a primary, a read barrier on a secondary, which waits
     * at most as long as for a bookmark.
     */
    private fun awaitLinearizable(id: NamedDatabaseId) {
        if (secondaries.secondary(id) != null) {
            secondaries.awaitLatest(id, bookmarkTimeout())
        } else {
            replication.awaitLinearizable(id)
        }
    }

    /** What this member reports of the seeds it keeps of recreated databases, if any. */
    private fun seedReport(): Map<String, String> {
        if (!::reconciler.isInitialized) return emptyMap()
        val reports = reconciler.seedReports
        return if (reports.isEmpty()) emptyMap()
        else mapOf(SeedReports.KEY to SeedReports.encode(reports))
    }

    /**
     * The checksums [request] asks for, from the seed this member keeps of the recreated database,
     * which does not run.
     */
    private fun seedChecksums(request: SeedChecksums.Request): Map<Long, Int?> {
        check(localStores.seeds()[request.name] == request.recreated) {
            "this member keeps no seed of database ${request.name}, recreated as ${request.recreated}"
        }
        check(databaseRepository.getDatabaseContext(request.name).isEmpty) {
            "database ${request.name} runs here"
        }
        return localStores.checksums(request.name, request.appendIndexes)
    }

    /** The seeds the seeders of recreated databases keep, as they report them. */
    private inner class ClusterSeeds : RecreationSeeds {
        override fun reported(member: MemberId): Map<UUID, Seed?>? =
            directory.reported(member)?.let { SeedReports.decode(it[SeedReports.KEY]) }

        override fun refresh() = directory.refreshSoon()

        override fun checksums(
            member: MemberId,
            database: NamedDatabaseId,
            appendIndexes: List<Long>,
        ): Map<Long, Int?> {
            val request =
                SeedChecksums.Request(database.name(), database.databaseId().uuid(), appendIndexes)
            if (member == memberId) return seedChecksums(request)
            val address =
                clusterServers.all().firstOrNull { it.id == member }?.raftAddress
                    ?: throw IllegalStateException("the address of server $member is not known")
            return consensus.fetchBulk(
                address,
                SeedChecksums.NAME,
                SeedChecksums.encodeRequest(request),
                SEED_CHECKSUMS_TIMEOUT,
                SeedChecksums::decodeResponse,
            )
        }
    }

    /** The term of the entry at [index] in the log of [database], as its leader tells it. */
    private fun entryTerm(database: DatabaseRef, index: Long): EntryTerm {
        val location = locations.of(database) ?: return EntryTerm.Unknown
        val leader =
            location.voters.firstOrNull { it.id == location.leader } ?: return EntryTerm.Unknown
        return try {
            consensus.fetchBulk(
                leader.raftAddress,
                EntryTermProtocol.NAME,
                EntryTermProtocol.encode(database, index),
                EntryTermProtocol::read,
            )
        } catch (_: ConsensusUnavailableException) {
            EntryTerm.Unknown
        }
    }

    private fun bookmarkTimeout(): Duration =
        globalModule.globalConfig.get(GraphDatabaseSettings.bookmark_ready_timeout)

    /**
     * The id of the last transaction of [database] applied here once this member applied what its
     * Raft group had committed: a read barrier for secondaries.
     */
    private fun readBarrier(database: DatabaseRef): Long {
        val replica =
            replication.replica(database)?.takeIf { it.isRunning() }
                ?: throw IllegalStateException(
                    "database ${database.name} does not run as a primary on this member"
                )
        replica.awaitLinearizable()
        return databaseRepository
            .getDatabaseContext(database.name)
            .orElseThrow { IllegalStateException("there is no database ${database.name} here") }
            .dependencies()
            .resolveDependency(TransactionIdStore::class.java)
            .highestGapFreeClosedTransactionId
    }

    /** The last transaction of `system` on this member, which its forwarded logins carry. */
    private fun lastSystemTransaction(): Long =
        databaseRepository
            .getSystemDatabaseContext()
            .dependencies()
            .resolveDependency(TransactionIdStore::class.java)
            .highestGapFreeClosedTransactionId

    override fun registerEditionSpecificProcedures(
        globalProcedures: GlobalProcedures,
        databaseContextProvider: DatabaseContextProvider<*>,
    ) {
        super.registerEditionSpecificProcedures(globalProcedures, databaseContextProvider)
        globalProcedures.registerComponent(ClusterService::class.java, { cluster }, true)
        globalProcedures.registerComponent(ServerReconciler::class.java, { servers }, true)
        globalProcedures.registerComponent(
            ServerAdministration::class.java,
            { administration },
            true,
        )
        globalProcedures.registerComponent(
            DatabaseRecreation::class.java,
            { recreation },
            true,
        )
        globalProcedures.registerComponent(
            SecondaryReplication::class.java,
            { SecondaryReplication { name, pause -> secondaries.toggle(name, pause) } },
            true,
        )
        val connectivity = ConnectivityChecks(cluster, ::serverRecords, { remoteTransactions })
        globalProcedures.registerComponent(
            ConnectivityChecks::class.java,
            { connectivity },
            true,
        )
        globalProcedures.registerComponent(StatusChecks::class.java, { statusChecks }, true)
        globalProcedures.registerProcedure(ClusterProcedures::class.java)
    }

    /** The records of the servers in the system graph, by server id. */
    private fun serverRecords(): Map<UUID, ServerRecord> =
        databaseRepository.getSystemDatabaseContext().databaseFacade().beginTx().use {
            ServerRecords.records(it)
        }

    private companion object {
        /** The databases this member holds as secondaries, next to its membership file. */
        const val SECONDARIES_FILE = "secondaries"

        /** How long a member that is asked what it reports may take to answer. */
        val PROBE_DEADLINE: Duration = Duration.ofSeconds(2)

        /**
         * How long a member asked to lift a quarantine may take, and how long replacing a copy's
         * cluster state waits for its group to remove it.
         */
        val QUARANTINE_LIFT_TIMEOUT: Duration = Duration.ofMinutes(2)

        /**
         * How long routing waits for the members' answers when what they reported last cannot
         * answer it: for a probe on demand under way, then for its own, each waiting for each
         * member for at most a second.
         */
        val ROUTING_REFRESH_WAIT: Duration = Duration.ofSeconds(3)

        /** How long a system secondary waits to confirm that its `system` is current. */
        val SYSTEM_READ_TIMEOUT: Duration = Duration.ofSeconds(10)

        /** How long a member waits for each other to take an announcement of what it leads. */
        val ANNOUNCEMENT_DEADLINE: Duration = Duration.ofSeconds(1)

        /**
         * How long the system leader waits for the seeder of a recreated database whose store is
         * the latest to read checksums from its logs.
         */
        val SEED_CHECKSUMS_TIMEOUT: Duration = Duration.ofMinutes(1)

        fun SocketAddress.toHostPort(): HostPort = HostPort(hostname, port)

        fun initialMembers(config: Config): List<HostPort> =
            config.get(ClusterSettings.initialMembers).map { it.toHostPort() }
    }
}
