package org.dozendb.cluster.internal

import java.io.IOException
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeoutException
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.dozendb.cluster.ClusterFormation
import org.dozendb.cluster.ClusterService
import org.dozendb.cluster.DatabaseStatesCodec
import org.dozendb.cluster.FormatLevelStore
import org.dozendb.cluster.FormatLevels
import org.dozendb.cluster.MemberDirectory
import org.dozendb.cluster.MemberMetadata
import org.dozendb.cluster.MemberState
import org.dozendb.cluster.MembershipFile
import org.dozendb.cluster.MembershipReconciler
import org.dozendb.cluster.internal.durability.FsyncPolicy
import org.dozendb.cluster.internal.durability.SingleFsyncFileSystem
import org.dozendb.cluster.internal.forwarding.ForwardedWrites
import org.dozendb.cluster.internal.forwarding.ForwardingLocations
import org.dozendb.cluster.internal.forwarding.ForwardingWatch
import org.dozendb.cluster.internal.forwarding.MemberSsl
import org.dozendb.cluster.internal.forwarding.RemoteTransactions
import org.dozendb.cluster.internal.forwarding.RoutingBoltDatabases
import org.dozendb.cluster.internal.leadership.LeaderBalancer
import org.dozendb.cluster.internal.reads.ConsistentBoltDatabases
import org.dozendb.cluster.internal.replication.CatchUp
import org.dozendb.cluster.internal.replication.DatabaseControl
import org.dozendb.cluster.internal.replication.DatabaseReconciler
import org.dozendb.cluster.internal.replication.DesiredDatabase
import org.dozendb.cluster.internal.replication.ReplicationService
import org.dozendb.cluster.internal.routing.ClusterQueryRouterBootstrap
import org.dozendb.cluster.internal.routing.ClusterRoutingTableProvider
import org.dozendb.cluster.internal.routing.ClusterRoutingValidator
import org.dozendb.cluster.internal.routing.ConfirmedReferences
import org.dozendb.cluster.internal.storecopy.Neo4jLocalStores
import org.dozendb.cluster.internal.storecopy.StoreCopySource
import org.dozendb.cluster.internal.topology.ClusterTopologyInfoService
import org.dozendb.cluster.internal.topology.DatabaseLocations
import org.dozendb.consensus.ConsensusLog
import org.dozendb.consensus.ConsensusUnavailableException
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.Member
import org.dozendb.consensus.MemberId
import org.dozendb.consensus.Role
import org.dozendb.consensus.ratis.RatisConsensusConfig
import org.dozendb.consensus.ratis.RatisConsensusLog
import org.dozendb.consensus.ratis.TlsConfig
import org.dozendb.core.DozenDb
import org.dozendb.core.edition.DatabaseHosting
import org.dozendb.core.edition.DatabaseState
import org.dozendb.core.edition.DozenDbEditionModule
import org.dozendb.core.edition.DozenDbQueryRouterBootstrap
import org.dozendb.core.edition.HostingServer
import org.dozendb.core.edition.LatestWrites
import org.dozendb.core.edition.SeedServers
import org.dozendb.core.edition.WantedDatabase
import org.dozendb.core.security.MemberLogins
import org.dozendb.core.servers.DatabasePlacement
import org.dozendb.core.servers.Placements
import org.dozendb.core.servers.ServerOptions
import org.dozendb.core.servers.Servers
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI
import org.neo4j.configuration.Config
import org.neo4j.configuration.connectors.ConnectorType
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.dbms.database.DefaultSystemGraphInitializer
import org.neo4j.dbms.database.SystemGraphInitializer
import org.neo4j.dbms.database.TopologyInfoService
import org.neo4j.dbms.database.readonly.SystemGraphReadOnlyDatabaseLookupFactory
import org.neo4j.dbms.database.readonly.SystemGraphReadOnlyListener
import org.neo4j.dbms.routing.ClientRoutingDomainChecker
import org.neo4j.dbms.routing.DefaultDatabaseAvailabilityChecker
import org.neo4j.dbms.routing.DefaultRoutingService
import org.neo4j.dbms.routing.RoutingOption
import org.neo4j.dbms.routing.RoutingService
import org.neo4j.dbms.routing.RoutingTableTTLProvider
import org.neo4j.dbms.routing.SingleAddressRoutingTableProvider
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
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.internal.Version
import org.neo4j.kernel.lifecycle.LifecycleAdapter
import org.neo4j.router.transaction.DatabaseTransactionFactory
import org.neo4j.server.security.systemgraph.CommunityDefaultDatabaseResolver
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
    private val directory: MemberDirectory
    private val locations: DatabaseLocations
    private val consensus: ConsensusLog
    private val membership: MembershipReconciler
    private val balancer: LeaderBalancer
    private val memberId = MemberId(identityModule.serverId().uuid())
    private val systemDatabase =
        DatabaseRef(
            NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID.name(),
            NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID.databaseId().uuid(),
        )
    private val localStores: Neo4jLocalStores

    /** The TLS between members, or null for plaintext. */
    private val memberTls: TlsConfig?

    /** Why other members may not forward writes to this member, or null if they may. */
    private val forwardingRefusal: String?

    /** Why this member forwards no writes to other members, or null if it does. */
    private val sendingRefusal: String?

    override val hosting: DatabaseHosting
        get() = replication

    private lateinit var systemGraph: Neo4jSystemGraph
    private lateinit var reconciler: DatabaseReconciler
    private lateinit var servers: ServerReconciler
    private lateinit var administration: ServerAdministration

    /** The id of the default database as the system graph has it, read once `system` runs. */
    @Volatile private var clusterId: UUID? = null
    private var roles: RolesInitializer? = null

    init {
        val config = globalModule.globalConfig
        val logService = globalModule.logService
        // Before anything creates a Ratis or DozenDB SLF4J logger.
        Slf4jLogRouting.useDebugLogProvider(logService.internalLogProvider)
        val userLog = logService.getUserLog(ClusterService::class.java)
        val neo4jVersion = Version.getNeo4jVersion()
        lateinit var stores: Neo4jLocalStores
        lateinit var replicationService: ReplicationService
        val formatLevels =
            FormatLevels(
                FormatLevels.SUPPORTED.oldest,
                config.get(ClusterSettings.newestFormatLevel),
            )
        // What this member reports when probed; read once consensus runs.
        val metadata = {
            mapOf(
                MemberMetadata.NEO4J_VERSION to neo4jVersion,
                MemberMetadata.DOZENDB_VERSION to DozenDb.version,
                MemberMetadata.HAS_STORES to stores.existed.toString(),
                MemberMetadata.STORES_READY to
                    (replicationService.replica(systemDatabase)?.isRunning() == true).toString(),
                MemberMetadata.MEMBER_STATE to cluster.state.name,
                MemberMetadata.DATABASES to DatabaseStatesCodec.encode(reportedStates()),
                MemberMetadata.READ_ONLY_DATABASES to readOnlyHere().joinToString(","),
                MemberMetadata.LAST_COMMITTED_TRANSACTIONS to
                    MemberMetadata.numbersText(lastCommittedHere()),
                MemberMetadata.LEADING to MemberMetadata.numbersText(leadingHere()),
                MemberMetadata.FORMAT_LEVELS to formatLevels.toString(),
                MemberMetadata.FORMAT_LEVEL to cluster.formatLevel.toString(),
            ) +
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
                    (replicationService.defaultDatabaseId ?: clusterId)?.let {
                        MemberMetadata.CLUSTER_ID to it.toString()
                    },
                )
        }
        val tuning =
            AbstractEditionModule.tryResolveOrCreate(
                ConsensusTuning::class.java,
                globalModule.externalDependencyResolver,
            ) {
                ConsensusTuning { it }
            }
        val tls =
            ClusterTls.of(config, sslPolicyLoader, logService.getUserLog(ClusterTls::class.java))
        memberTls = tls
        forwardingRefusal = ForwardedWrites.refusal(config, tls)
        sendingRefusal = ForwardedWrites.sendingRefusal(config, tls)
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
        val metrics = ClusterMetrics(consensus, systemDatabase)
        globalModule.globalDependencies.satisfyDependency(metrics)
        stores =
            Neo4jLocalStores(
                globalModule.fileSystem,
                globalModule.neo4jLayout,
                listOf(systemDatabase.name, defaultDatabase),
                consensus,
                logService.getInternalLog(Neo4jLocalStores::class.java),
                received = { name -> metrics.of(name).copiesReceived.increment() },
                seed = consensus::seed,
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
                systemDatabase,
                defaultDatabase,
                catchUp,
                logService.internalLogProvider,
                ::bootstrapMembers,
                mustDetach = { cluster.mustDetach(it) },
                detached = { cluster.detached(it) },
                notLeader = ::notLeaderMessage,
                adoptDatabases = ::adoptStores,
                metrics = metrics,
            )
        replication = replicationService
        globalModule.globalDependencies.satisfyDependency(replicationService)
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
                    replicationService.running().any { it.namedDatabaseId == id }
                },
                snapshots = { name ->
                    replicationService
                        .running()
                        .firstOrNull { it.ref.name == name }
                        ?.let { consensus.currentSnapshot(it.ref) }
                },
                log = logService.getInternalLog(StoreCopySource::class.java),
                served = { name -> metrics.of(name).copiesServed.increment() },
            ),
        )
        globalModule.databaseEventListeners.registerDatabaseEventListener(replication)
        val formation = ClusterFormation(consensus, initialMembers(config), neo4jVersion, userLog)
        directory =
            MemberDirectory(
                consensus,
                systemDatabase,
                ::boltHostPort,
                ::routingHostPort,
                ::reportedStates,
                logService.getInternalLog(MemberDirectory::class.java),
                selfReadOnly = ::readOnlyHere,
                selfLastCommitted = ::lastCommittedHere,
                selfReport = metadata,
            )
        locations = DatabaseLocations(consensus, directory)
        val electionTimeout = config.get(ClusterSettings.electionTimeoutMax)
        balancer =
            LeaderBalancer(
                consensus,
                directory,
                locations,
                replicationService::created,
                {
                    if (replicationService.replica(systemDatabase)?.isRunning() == true) {
                        mapOf(systemDatabase to emptySet<MemberId>()) +
                            systemGraph.databases().associate {
                                DatabaseRef(it.id.name(), it.id.databaseId().uuid()) to it.hosts
                            }
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
        cluster =
            ClusterService(
                consensus,
                systemDatabase,
                formation,
                stores,
                MembershipFile(config.get(ClusterSettings.raftDirectory).resolve(MEMBERSHIP_FILE)),
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
            )
        globalModule.globalDependencies.satisfyDependency(cluster)
        globalModule.databaseEventListeners.registerDatabaseEventListener(
            object : DatabaseEventListenerAdapter() {
                override fun databaseStart(eventContext: DatabaseEventContext) {
                    if (eventContext.databaseName == systemDatabase.name) {
                        cluster.requestFormatLevelRefresh()
                        // A Free server reports it before it hosts the default database.
                        clusterId =
                            runCatching { systemGraph.databaseId(defaultDatabase) }.getOrNull()
                    }
                }
            }
        )
        membership =
            MembershipReconciler(
                consensus,
                systemDatabase,
                {
                    systemGraph.databases().associate {
                        DatabaseRef(it.id.name(), it.id.databaseId().uuid()) to it.hosts
                    }
                },
                logService.getInternalLog(MembershipReconciler::class.java),
                systemRunning = { replicationService.replica(systemDatabase)?.isRunning() == true },
            )
        globalModule.globalLife.add(
            object : LifecycleAdapter() {
                override fun start() {
                    cluster.start()
                    membership.start()
                    balancer.start()
                }

                override fun stop() {
                    try {
                        balancer.stop()
                        roles?.close()
                        membership.close()
                        cluster.stop()
                    } finally {
                        replication.close()
                    }
                }
            }
        )
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
                databaseIdRepository,
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
     * This member's databases as it reports them to the others: those it copies from another
     * member, new ones or to catch up, as being copied; new ones that run before their group as
     * starting, or as failed if their group could not start; and whether it knows their leaders.
     */
    private fun reportedStates(): List<DatabaseState> {
        val catchingUp = replication.catchingUp()
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
                    }.copy(leaderKnown = it.name in led)
                }
        return states +
            copying.map { (name, id) ->
                DatabaseState(name, id, DatabaseState.Status.COPYING, leaderKnown = false)
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
            .filter { checker.isReadOnlyLocally(DatabaseIdFactory.from(it)) }
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

    /** The id of the last transaction committed to each database that runs on this member. */
    private fun lastCommittedHere(): Map<UUID, Long> =
        databaseRepository
            .registeredDatabases()
            .filterValues { it.database().isStarted }
            .mapNotNull { (id, context) ->
                val dependencies = context.dependencies()
                if (!dependencies.containsDependency(TransactionIdStore::class.java))
                    return@mapNotNull null
                id.databaseId().uuid() to
                    dependencies
                        .resolveDependency(TransactionIdStore::class.java)
                        .lastCommittedTransactionId
            }
            .toMap()

    /** SHOW DATABASES with a row for each member a database runs on, as Neo4j Enterprise has. */
    override fun createTopologyInfoService(
        databaseContextProvider: DatabaseContextProvider<*>
    ): TopologyInfoService =
        ClusterTopologyInfoService(
            super.createTopologyInfoService(databaseContextProvider),
            consensus,
            systemDatabase,
            directory,
            locations,
            directory::reported,
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
            )
        reconciler =
            DatabaseReconciler(
                systemGraph,
                Neo4jLocalDatabases(
                    databaseManager,
                    repository,
                    localStores,
                    replication,
                    ::seeder,
                ),
                memberId,
                defaultDatabase,
                ::copySources,
                { consensus.membership(systemDatabase).takeIf { it.settled }?.members },
                { directory.answering(it) },
                { cluster.state == MemberState.REMOVED },
                globalModule.logService.getInternalLog(DatabaseReconciler::class.java),
            )
        servers =
            ServerReconciler(
                consensus,
                systemDatabase,
                { repository.getSystemDatabaseContext().databaseFacade() },
                { block -> replication.readingSystem(block) },
                leading = { replication.systemWritable },
                current = replication::awaitSystemCurrent,
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
                        0,
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
                        0,
                    ),
                autoEnable = globalModule.globalConfig.get(ClusterSettings.autoEnableFreeServers),
            )
        cluster.onRemoved = reconciler::request
        // A copy no longer placed here is deleted once this member leaves the database's group.
        replication.afterGroupChange = reconciler::request
        // Transactions applied from Raft fire no transaction event listeners: refresh what the
        // listeners refresh after local commits.
        replication.afterSystemApply = {
            databaseIdCache.invalidateAll()
            referenceCache.invalidateAll()
            (defaultDatabaseResolver as? CommunityDefaultDatabaseResolver)?.clearCache()
            globalReadOnlyChecker?.refresh()
            reconciler.request()
            // A new database may need members added since its initial members were recorded.
            membership.request()
            servers.request()
            cluster.requestFormatLevelRefresh()
        }
        return provider
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
            locations.of(DatabaseRef(id.name(), id.databaseId().uuid()))?.voters?.let {
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
            consensus.joinGroup(
                DatabaseRef(id.name(), id.databaseId().uuid()),
                listOf(consensus.self),
            )
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

    /** Starts creating the roles of a cluster without them, once (see [RolesInitializer]). */
    @Synchronized
    private fun startRoles() {
        if (roles != null) return
        roles =
            RolesInitializer(
                    consensus,
                    systemDatabase,
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
        val voters = locations.of(DatabaseRef(id.name(), id.databaseId().uuid()))?.voters.orEmpty()
        val hosts = consensus.members(systemDatabase).filter { it.id in database.hosts }
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
     * Whether this member is the only one, as Neo4j's routing asks before routing. Not while it
     * stops: then the database checks that follow refuse the route as unavailable, which drivers
     * retry on another member, instead of failing it with an unknown error.
     */
    private fun alone(): Boolean =
        try {
            cluster.members().size <= 1
        } catch (_: ConsensusUnavailableException) {
            false
        }

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
     * or, for one dropped or not placed, the enabled servers, which may hold it. Each reports its
     * databases when probed; this member reads its own.
     */
    override fun hostingServers(wanted: WantedDatabase): List<HostingServer> {
        val placed = if (wanted.id == null) emptySet() else systemGraph.hosts(wanted.name)
        return consensus
            .members(systemDatabase)
            .filter { if (placed.isEmpty()) systemGraph.enabled(it.id) else it.id in placed }
            .map { member ->
                object : HostingServer {
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
        require(
            consensus.members(systemDatabase).any {
                it.id.uuid == id && systemGraph.enabled(it.id)
            }
        ) {
            "server $id is not an enabled server of this cluster: SHOW SERVERS lists the servers"
        }
    }

    /** Where the copies of databases go: on the servers the allocator chooses. */
    override fun placement(): DatabasePlacement =
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
                    0,
                ),
            holders = { name ->
                val id = systemGraph.databaseId(name)
                id?.let { locations.of(DatabaseRef(name, it))?.voters?.map { m -> m.id.uuid } }
                    .orEmpty()
            },
        )

    /** The members of the cluster, with the options they report from `initial.server.*`. */
    override fun servers(): Servers =
        object : Servers {
            override fun ids(): Set<UUID> =
                consensus.members(systemDatabase).mapTo(HashSet()) { it.id.uuid }

            // Asked now: the directory may not have probed a member that just joined.
            override fun initialOptions(id: UUID): ServerOptions? {
                if (id == memberId.uuid) return ServerOptions.initial(globalModule.globalConfig)
                val member =
                    consensus.members(systemDatabase).firstOrNull { it.id.uuid == id }
                        ?: return null
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
                consensus
                    .members(systemDatabase)
                    .firstOrNull { it.id.uuid == id }
                    ?.raftAddress
                    ?.toString()

            override fun holding(tx: Transaction, id: UUID): Set<String> =
                locations.votedIn(tx, MemberId(id))
        }

    /**
     * Waits until this member has applied what its database's Raft group had committed: a Raft
     * ReadIndex, as for linearizable reads, on the leader or a follower.
     */
    override fun latestWrites(): LatestWrites = LatestWrites { database, timeout ->
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
     * Neo4j Community's routing service, with routing tables of replicated databases taken from
     * their Raft groups. The member is alone, and drivers get Community's single-address tables,
     * only in a one-member cluster.
     */
    override fun createRoutingService(
        databaseContextProvider: DatabaseContextProvider<*>,
        clientRoutingDomainChecker: ClientRoutingDomainChecker,
    ): RoutingService {
        val config = globalModule.globalConfig
        val logProvider = globalModule.logService.internalLogProvider
        val ttl = RoutingTableTTLProvider.ttlFromConfig(config)
        val singleAddress =
            SingleAddressRoutingTableProvider(
                globalModule.connectorPortRegister,
                RoutingOption.ROUTE_WRITE_AND_READ,
                config,
                logProvider,
                ttl,
            )
        val routing =
            ClusterRoutingTableProvider(
                locations,
                directory,
                config.get(ClusterSettings.readsOnLeader),
                ttl,
                singleAddress,
                singleAddress,
            )
        return DefaultRoutingService(
            logProvider,
            ClusterRoutingValidator(
                DefaultDatabaseAvailabilityChecker(databaseContextProvider),
                locations,
                directory,
            ),
            routing,
            routing,
            clientRoutingDomainChecker,
            config,
            ::alone,
            defaultDatabaseResolver,
            ConfirmedReferences(databaseReferenceRepo) {
                replication.awaitLinearizable(NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID)
            },
            true,
            globalModule.globalClock,
        )
    }

    override fun createBoltDatabaseManagementServiceProvider():
        BoltGraphDatabaseManagementServiceSPI =
        RoutingBoltDatabases(
            ConsistentBoltDatabases(
                super.createBoltDatabaseManagementServiceProvider(),
                globalModule.globalConfig.get(ClusterSettings.defaultReadConsistency),
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
            databaseReferenceRepo,
            securityLog,
            globalModule.globalConfig.get(ClusterSettings.defaultReadConsistency),
            replication::awaitLinearizable,
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
                tls.verifyHostname,
                ::lastSystemTransaction,
                logs,
            )
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
            )
        globalModule.globalLife.add(
            ForwardingWatch(forwarding, remote, electionTimeout.multipliedBy(2))
        )
        return ClusterQueryRouterBootstrap.Forwarding(forwarding::locationService, remote)
    }

    /** Whether the database [id] is hosted here: its copy is registered on this member. */
    private fun hostedHere(id: NamedDatabaseId): Boolean =
        databaseRepository.getDatabaseContext(id).isPresent

    /** The last transaction of `system` on this member, which its forwarded logins carry. */
    private fun lastSystemTransaction(): Long =
        databaseRepository
            .getSystemDatabaseContext()
            .dependencies()
            .resolveDependency(TransactionIdStore::class.java)
            .lastClosedTransactionId

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
        globalProcedures.registerProcedure(ClusterProcedures::class.java)
    }

    private companion object {
        /** This member's [org.dozendb.cluster.MembershipRecord], in the Raft directory. */
        const val MEMBERSHIP_FILE = "membership"

        /** How long a member that is asked what it reports may take to answer. */
        val PROBE_DEADLINE: Duration = Duration.ofSeconds(2)

        fun SocketAddress.toHostPort(): HostPort = HostPort(hostname, port)

        fun initialMembers(config: Config): List<HostPort> =
            config.get(ClusterSettings.initialMembers).map { it.toHostPort() }
    }
}
