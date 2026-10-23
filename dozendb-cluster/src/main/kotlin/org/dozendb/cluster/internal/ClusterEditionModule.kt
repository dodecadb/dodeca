package org.dozendb.cluster.internal

import java.io.IOException
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
import org.dozendb.cluster.internal.reads.ConsistentBoltDatabases
import org.dozendb.cluster.internal.replication.CatchUp
import org.dozendb.cluster.internal.replication.DatabaseControl
import org.dozendb.cluster.internal.replication.DatabaseReconciler
import org.dozendb.cluster.internal.replication.ReplicationService
import org.dozendb.cluster.internal.routing.ClusterRoutingTableProvider
import org.dozendb.cluster.internal.storecopy.Neo4jLocalStores
import org.dozendb.cluster.internal.storecopy.StoreCopySource
import org.dozendb.consensus.ConsensusLog
import org.dozendb.consensus.ConsensusUnavailableException
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.HostPort
import org.dozendb.consensus.Member
import org.dozendb.consensus.MemberId
import org.dozendb.consensus.ratis.RatisConsensusConfig
import org.dozendb.consensus.ratis.RatisConsensusLog
import org.dozendb.consensus.ratis.TlsConfig
import org.dozendb.core.DozenDb
import org.dozendb.core.edition.DatabaseHosting
import org.dozendb.core.edition.DatabaseState
import org.dozendb.core.edition.DozenDbEditionModule
import org.dozendb.core.edition.HostingServer
import org.dozendb.core.edition.LatestWrites
import org.dozendb.core.edition.SeedServers
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI
import org.neo4j.configuration.Config
import org.neo4j.configuration.connectors.ConnectorType
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.dbms.database.DefaultSystemGraphInitializer
import org.neo4j.dbms.database.SystemGraphInitializer
import org.neo4j.dbms.routing.ClientRoutingDomainChecker
import org.neo4j.dbms.routing.DefaultDatabaseAvailabilityChecker
import org.neo4j.dbms.routing.DefaultRoutingService
import org.neo4j.dbms.routing.LocalRoutingTableServiceValidator
import org.neo4j.dbms.routing.RoutingOption
import org.neo4j.dbms.routing.RoutingService
import org.neo4j.dbms.routing.RoutingTableTTLProvider
import org.neo4j.dbms.routing.SingleAddressRoutingTableProvider
import org.neo4j.dbms.systemgraph.SystemDatabaseProvider
import org.neo4j.graphdb.event.DatabaseEventContext
import org.neo4j.graphdb.event.DatabaseEventListenerAdapter
import org.neo4j.graphdb.factory.module.GlobalModule
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule
import org.neo4j.kernel.api.procedure.GlobalProcedures
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.internal.Version
import org.neo4j.kernel.lifecycle.Lifecycle
import org.neo4j.kernel.lifecycle.LifecycleAdapter
import org.neo4j.server.security.systemgraph.CommunityDefaultDatabaseResolver

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
    private val consensus: ConsensusLog
    private val membership: MembershipReconciler
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

    override val hosting: DatabaseHosting
        get() = replication

    private lateinit var systemGraph: Neo4jSystemGraph
    private lateinit var reconciler: DatabaseReconciler
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
                MemberMetadata.STORES_READY to replicationService.defaultDatabaseStarted.toString(),
                MemberMetadata.MEMBER_STATE to cluster.state.name,
                MemberMetadata.DATABASES to DatabaseStatesCodec.encode(databaseStates()),
                MemberMetadata.FORMAT_LEVELS to formatLevels.toString(),
                MemberMetadata.FORMAT_LEVEL to cluster.formatLevel.toString(),
            ) +
                listOfNotNull(
                    boltHostPort()?.let { MemberMetadata.BOLT_ADDRESS to it.toString() },
                    routingHostPort()?.let { MemberMetadata.ROUTING_ADDRESS to it.toString() },
                    replicationService.defaultDatabaseId?.let {
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
                systemDatabase,
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
                ::initialMembers,
                mustDetach = { cluster.mustDetach(it) },
                detached = { cluster.detached(it) },
                adoptDatabases = {
                    val members =
                        checkNotNull(cluster.foundingMembers) { "this member formed no cluster" }
                    val databases = systemGraph.adopt(memberId, members)
                    // A store counts only together with its group, which the creator starts.
                    for (id in databases.filter { stores.complete(it.name()) }) {
                        consensus.joinGroup(DatabaseRef(id.name(), id.databaseId().uuid()), members)
                    }
                    userLog.info(
                        "This member formed the cluster with a system database of its own: it " +
                            "creates the databases listed there, %s, and the other members copy " +
                            "them",
                        databases.joinToString { it.name() },
                    )
                },
                metrics = metrics,
            )
        replication = replicationService
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
                ::databaseStates,
                logService.getInternalLog(MemberDirectory::class.java),
            )
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
                    }
                }
            }
        )
        membership =
            MembershipReconciler(
                consensus,
                systemDatabase,
                { replicationService.created() },
                logService.getInternalLog(MembershipReconciler::class.java),
            )
        globalModule.globalLife.add(
            object : LifecycleAdapter() {
                override fun start() {
                    cluster.start()
                    membership.start()
                }

                override fun stop() {
                    try {
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
                { cluster.state == MemberState.REMOVED },
                globalModule.logService.getInternalLog(DatabaseReconciler::class.java),
            )
        cluster.onRemoved = reconciler::request
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
            cluster.requestFormatLevelRefresh()
        }
        return provider
    }

    /**
     * The initial members of the group of the database [id], which this member creates with them if
     * it has no local state for it. A member that is not among them waits to be added.
     */
    private fun initialMembers(id: NamedDatabaseId): List<Member> =
        if (id.name() == defaultDatabase) {
            cluster.foundingMembers
                ?: consensus.members(systemDatabase).filter { it.id != memberId }
        } else {
            checkNotNull(systemGraph.initialMembers(id)) {
                "No initial members are recorded for database $id"
            }
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

    /** The members to copy a new database from, [creator] first. */
    private fun copySources(creator: MemberId): List<HostPort> =
        consensus
            .members(systemDatabase)
            .filter { it.id != memberId }
            .sortedBy { it.id != creator }
            .map { it.raftAddress }

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
                        replication.systemGraphInitialized()
                    }
                    startRoles()
                }
            }
        globalModule.globalDependencies.satisfyDependency(clusterInitializer)
        globalModule.globalLife.add(clusterInitializer)
        registerDefaultDatabaseInitializer(globalModule)
        // Once the system and default databases run; stops before the databases stop.
        globalModule.globalLife.add(
            object : LifecycleAdapter() {
                override fun start() = reconciler.start()

                override fun stop() = reconciler.close()
            }
        )
    }

    /**
     * The starter of the default database, which reads `system`: it waits while `system` is
     * replaced by a copy, as when this member restarts behind the leader's log. The reconciler
     * follows the system graph instead of `StandaloneDatabases`.
     */
    override fun registerDefaultDatabaseInitializer(globalModule: GlobalModule) {
        val starter = defaultDatabaseStarter
        globalModule.globalLife.add(
            object : Lifecycle by starter {
                override fun start() {
                    // A removed member serves no database.
                    if (cluster.state == MemberState.REMOVED) return
                    replication.readingSystem { starter.start() }
                }
            }
        )
    }

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
     * The members of the cluster, which WAIT waits for. Each reports its databases when probed;
     * this member reads its own.
     */
    override fun hostingServers(): List<HostingServer> =
        consensus.members(systemDatabase).map { member ->
            object : HostingServer {
                override val address: String =
                    (directory.boltAddress(member.id) ?: member.raftAddress).toString()

                override fun databases(): List<DatabaseState> {
                    if (member.id == memberId) return databaseStates()
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

    /** The members, which a database can be created from with existingDataSeedServer. */
    override fun seedServers(): SeedServers = SeedServers { id ->
        require(consensus.members(systemDatabase).any { it.id.uuid == id }) {
            "server $id is not a member of this cluster: CALL dozendb.cluster.members() lists " +
                "the members"
        }
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
                replication,
                consensus,
                directory,
                config.get(ClusterSettings.readsOnLeader),
                ttl,
                singleAddress,
                singleAddress,
            )
        return DefaultRoutingService(
            logProvider,
            LocalRoutingTableServiceValidator(
                DefaultDatabaseAvailabilityChecker(databaseContextProvider)
            ),
            routing,
            routing,
            clientRoutingDomainChecker,
            config,
            { cluster.members().size <= 1 },
            defaultDatabaseResolver,
            databaseReferenceRepo,
            true,
            globalModule.globalClock,
        )
    }

    override fun createBoltDatabaseManagementServiceProvider():
        BoltGraphDatabaseManagementServiceSPI =
        ConsistentBoltDatabases(
            super.createBoltDatabaseManagementServiceProvider(),
            replication,
            globalModule.globalConfig.get(ClusterSettings.defaultReadConsistency),
        )

    override fun registerEditionSpecificProcedures(
        globalProcedures: GlobalProcedures,
        databaseContextProvider: DatabaseContextProvider<*>,
    ) {
        super.registerEditionSpecificProcedures(globalProcedures, databaseContextProvider)
        globalProcedures.registerComponent(ClusterService::class.java, { cluster }, true)
        globalProcedures.registerProcedure(ClusterProcedures::class.java)
    }

    private companion object {
        /** This member's [org.dozendb.cluster.MembershipRecord], in the Raft directory. */
        const val MEMBERSHIP_FILE = "membership"

        fun SocketAddress.toHostPort(): HostPort = HostPort(hostname, port)

        fun initialMembers(config: Config): List<HostPort> =
            config.get(ClusterSettings.initialMembers).map { it.toHostPort() }
    }
}
