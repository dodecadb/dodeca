package dev.dodeca.cluster.internal

import dev.dodeca.backup.BackupSettings
import dev.dodeca.cluster.ClusterService
import dev.dodeca.cluster.DatabaseGroups
import dev.dodeca.cluster.MemberState
import dev.dodeca.cluster.internal.durability.withSingleFsync
import dev.dodeca.cluster.internal.leadership.BalancingStrategy
import dev.dodeca.consensus.GroupStatus
import dev.dodeca.consensus.Role
import dev.dodeca.consensus.testing.TestAuthority
import dev.dodeca.consensus.testing.TestPorts
import dev.dodeca.core.DebugEntry
import dev.dodeca.core.DodecaSettings
import dev.dodeca.core.monitoring.MetricsSettings
import dev.dodeca.core.monitoring.metrics
import dev.dodeca.core.monitoring.samples
import dev.dodeca.core.servers.Placements
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.concurrent.thread
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilNotNull
import org.awaitility.kotlin.withPollInterval
import org.neo4j.collection.Dependencies
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.configuration.connectors.ConnectorPortRegister
import org.neo4j.configuration.connectors.ConnectorType
import org.neo4j.configuration.connectors.HttpConnector
import org.neo4j.configuration.connectors.HttpsConnector
import org.neo4j.configuration.helpers.GlobbingPattern
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.configuration.ssl.SslPolicyConfig
import org.neo4j.configuration.ssl.SslPolicyScope
import org.neo4j.consistency.ConsistencyCheckService
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.dbms.api.DatabaseNotFoundException
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.dbms.systemgraph.InstanceModeConstraint
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.event.DatabaseEventListener
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory
import org.neo4j.graphdb.facade.GraphDatabaseDependencies
import org.neo4j.io.ByteUnit
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.io.layout.Neo4jLayout
import org.neo4j.kernel.impl.factory.DbmsInfo
import org.neo4j.kernel.internal.GraphDatabaseAPI

/**
 * Clustered DBMSes in this JVM, on loopback, for tests. The first [size] members form the cluster;
 * [newMember] adds members that join it later. [tuning] adjusts every member's consensus
 * configuration; [fileSystem] supplies the file system a member's DBMS runs on, below Dodeca's
 * single-fsync file system, each time it starts. The logging of Ratis and Dodeca's consensus, which
 * the members' debug logs lack, goes to `raft.log` in [root].
 *
 * Members talk to each other over mutual TLS, with certificates that [authority] issued for
 * loopback, or in plaintext if it is null.
 */
internal class ClusterHarness(
    private val root: Path,
    private val size: Int,
    private val fileSystem: ((Member) -> FileSystemAbstraction)? = null,
    private val settings: Map<Setting<*>, Any> = emptyMap(),
    val authority: TestAuthority? = TestAuthority(),
    /** The id of the Raft group of `system` on every member, if not the database's own. */
    private val systemGroup: UUID? = null,
    private val tuning: ConsensusTuning? = null,
) : AutoCloseable {
    inner class Member(val index: Int) {
        val home: Path = Files.createDirectories(root.resolve("member-$index"))

        /** The entries of this member's debug log. */
        fun debugEntries(): List<DebugEntry> = DebugEntry.read(home.resolve("logs/debug.log"))

        val raftPort: Int = TestPorts.free()
        // Fixed, as a real member's are, so that it reports its addresses from its first answer
        // on: with port 0 it reported none until it had bound them, and others took that answer
        // for a member that takes no forwarded writes until they probed it again.
        private val boltPort: Int = TestPorts.free()
        private val routingPort: Int = TestPorts.free()

        /** The directory of the cluster SSL policy's certificates. */
        val certificates: Path
            get() = home.resolve(CERTIFICATES)

        init {
            authority?.issue()?.writeTo(certificates)
        }

        @Volatile
        var dbms: DatabaseManagementService? = null
            private set

        val running: Boolean
            get() = dbms != null

        /** Listeners the DBMS calls before Dodeca's own, from its next start on. */
        @Volatile var databaseEventListeners: List<DatabaseEventListener> = emptyList()

        /** Settings of this member alone, over the cluster's, from its next start on. */
        @Volatile var settings: Map<Setting<*>, Any> = emptyMap()

        /** Whether this member started before, and ran the default database when it stopped. */
        private var started = false
        private var hostedDefault = false

        /**
         * Starts the DBMS; blocks until its databases run, which may wait for other members: the
         * default database, which the cluster places on every initial member by default, on their
         * first start, and on a member that ran it when it stopped, unless it was moved away or the
         * member removed meanwhile.
         */
        fun start() {
            check(dbms == null) { "member $index is running" }
            val given = Dependencies()
            tuning?.let { given.satisfyDependency(it) }
            systemGroup?.let { given.satisfyDependency(DatabaseGroups(it)) }
            fileSystem?.let { given.satisfyDependency(it(this)) }
            val config = config()
            dbms =
                DatabaseManagementServiceFactory(DbmsInfo.ENTERPRISE, ::ClusterEditionModule)
                    .build(
                        config,
                        false,
                        withSingleFsync(
                            config,
                            GraphDatabaseDependencies.newDependencies()
                                .dependencies(given)
                                .databaseEventListeners(databaseEventListeners),
                        ),
                    )
            val first = !started
            started = true
            // A server that may host only secondaries hosts no primary of the default database.
            val expected =
                if (first) {
                    index < size &&
                        config.get(ClusterSettings.defaultPrimaries) >= size &&
                        config.get(DodecaSettings.initialServerModeConstraint) !=
                            InstanceModeConstraint.SECONDARY
                } else {
                    hostedDefault
                }
            if (expected) awaitDefaultDatabase(first)
        }

        private fun awaitDefaultDatabase(first: Boolean) {
            await withPollInterval
                Duration.ofMillis(50) atMost
                START_TIMEOUT untilNotNull
                {
                    true.takeIf {
                        cluster.state == MemberState.REMOVED ||
                            runsDefault() ||
                            (!first && !placedHere())
                    }
                }
        }

        /**
         * Whether this member runs the default database, its Raft group too: Neo4j counts a
         * database available before its start ends, while it joins a group that Ratis already runs
         * from its state here, and a new database's group starts after that.
         */
        private fun runsDefault(): Boolean =
            try {
                database("system")
                    .dependencyResolver
                    .resolveDependency(DatabaseContextProvider::class.java)
                    .getDatabaseContext(DEFAULT_DATABASE)
                    .orElse(null)
                    ?.database()
                    ?.isStarted == true &&
                    status(DEFAULT_DATABASE).let { it != null && it.role != Role.Inactive }
            } catch (_: DatabaseNotFoundException) {
                false
            }

        /** Whether this member's system graph places the default database on it. */
        private fun placedHere(): Boolean =
            database("system").beginTx().use { tx ->
                Placements.database(tx, DEFAULT_DATABASE)
                    ?.let(Placements::copies)
                    ?.contains(cluster.self.id.uuid) == true
            }

        /** The first index in this member's Raft log of [database], or null if it has none. */
        fun firstLogIndex(database: UUID): Long? {
            val log = home.resolve("data/raft").resolve(database.toString()).resolve("current")
            if (!Files.isDirectory(log)) return null
            return Files.list(log).use { files ->
                files
                    .map { SEGMENT.matchEntire(it.fileName.toString())?.groupValues?.get(1) }
                    .toList()
                    .filterNotNull()
                    .minOfOrNull { it.toLong() }
            }
        }

        fun stop() {
            hostedDefault = dbms != null && runsDefault()
            try {
                dbms?.shutdown()
            } finally {
                dbms = null
            }
        }

        /** Neo4j's full consistency check of [database], on this member while it is stopped. */
        fun checkConsistency(database: String = DEFAULT_DATABASE): ConsistencyCheckService.Result {
            check(dbms == null) { "member $index is running" }
            val config = config()
            return ConsistencyCheckService(Neo4jLayout.of(config).databaseLayout(database))
                .with(config)
                .runFullConsistencyCheck()
        }

        fun database(name: String = DEFAULT_DATABASE): GraphDatabaseAPI =
            checkNotNull(dbms) { "member $index is stopped" }.database(name) as GraphDatabaseAPI

        val cluster: ClusterService
            get() =
                database("system").dependencyResolver.resolveDependency(ClusterService::class.java)

        /** The id of this member's server, as SHOW SERVERS has it. */
        val serverId: String
            get() = cluster.self.id.uuid.toString()

        fun status(name: String = DEFAULT_DATABASE): GroupStatus? =
            cluster.status().firstOrNull { it.database.name == name }

        override fun toString(): String = "member-$index"

        private fun config(): Config {
            val builder =
                Config.newBuilder()
                    .set(GraphDatabaseSettings.neo4j_home, home)
                    .set(GraphDatabaseSettings.auth_enabled, false)
                    .set(GraphDatabaseSettings.pagecache_memory, ByteUnit.mebiBytes(16))
                    // Neo4j preallocates every transaction log, 256 MB, on Linux: the tests'
                    // RAM disk would fill.
                    .set(GraphDatabaseSettings.preallocate_logical_logs, false)
                    // A database that panics would end the tests' JVM.
                    .set(DodecaSettings.shutdownOnPanic, false)
                    .set(BoltConnector.enabled, false)
                    .set(BoltConnector.listen_address, SocketAddress("127.0.0.1", boltPort))
                    .set(BoltConnector.advertised_address, SocketAddress("127.0.0.1", boltPort))
                    .set(
                        GraphDatabaseSettings.routing_listen_address,
                        SocketAddress("127.0.0.1", routingPort),
                    )
                    .set(
                        ClusterSettings.routingAdvertisedAddress,
                        SocketAddress("127.0.0.1", routingPort),
                    )
                    .set(HttpConnector.enabled, false)
                    .set(HttpsConnector.enabled, false)
                    .set(DodecaSettings.clusterEnabled, true)
                    .set(ClusterSettings.listenAddress, SocketAddress("127.0.0.1", raftPort))
                    .set(ClusterSettings.advertisedAddress, SocketAddress("127.0.0.1", raftPort))
                    .set(
                        ClusterSettings.initialMembers,
                        members.take(size).map { SocketAddress("127.0.0.1", it.raftPort) },
                    )
                    .set(ClusterSettings.electionTimeoutMin, Duration.ofMillis(300))
                    .set(ClusterSettings.electionTimeoutMax, Duration.ofMillis(600))
                    // Tests about other things keep their leaders; balancing tests turn it on.
                    .set(ClusterSettings.balancingStrategy, BalancingStrategy.NO_BALANCING)
                    // Members that join host the databases at once; tests of Free servers turn
                    // it off.
                    .set(ClusterSettings.autoEnableFreeServers, true)
                    // Databases are placed on every initial member, as before topologies.
                    .set(ClusterSettings.defaultPrimaries, size.coerceIn(1, 11))
                    .set(CLUSTER_TLS.enabled, authority != null)
                    .set(BackupSettings.listenAddress, SocketAddress("127.0.0.1", 0))
                    // Bolt's thread pools otherwise wait 5 s each for quiet before they stop.
                    .set(
                        GraphDatabaseInternalSettings.netty_server_shutdown_quiet_period,
                        Duration.ZERO,
                    )
            @Suppress("UNCHECKED_CAST")
            (this@ClusterHarness.settings + settings).forEach { (setting, value) ->
                builder.set(setting as Setting<Any>, value)
            }
            return builder.build()
        }

        /** The port this member serves backups on. */
        fun backupPort(): Int {
            val register =
                database("system")
                    .dependencyResolver
                    .resolveDependency(ConnectorPortRegister::class.java)
            return checkNotNull(register.getLocalAddress(ConnectorType.BACKUP)) {
                    "backups are off"
                }
                .port
        }

        /** The samples this member's Prometheus endpoint reports now, by name with labels. */
        fun samples(): Map<String, Double> =
            checkNotNull(database("system").metrics()) { "$this reports no metrics" }.samples()

        /** The address Bolt listens on, when enabled in the settings. */
        fun boltAddress(): String {
            val register =
                database("system")
                    .dependencyResolver
                    .resolveDependency(ConnectorPortRegister::class.java)
            val address =
                checkNotNull(register.getLocalAddress(ConnectorType.BOLT)) { "Bolt is off" }
            return "${address.host}:${address.port}"
        }
    }

    private val all = CopyOnWriteArrayList(List(size) { Member(it) })
    private val raftLog = RaftLogCapture.writeTo(root.resolve("raft.log"))

    /** The initial members, then those added with [newMember]. */
    val members: List<Member>
        get() = all.toList()

    /**
     * A member that is not one of the initial members, configured with them: once started, it waits
     * to join the cluster they formed.
     */
    fun newMember(): Member = Member(all.size).also { all += it }

    /** Starts members concurrently, as the first start of a real cluster does. */
    fun start(toStart: List<Member> = members) {
        val failures = mutableListOf<Throwable>()
        val threads = toStart.map { member ->
            thread(name = "start-$member") {
                try {
                    member.start()
                } catch (e: Throwable) {
                    synchronized(failures) { failures += e }
                }
            }
        }
        threads.forEach { it.join(START_TIMEOUT.toMillis()) }
        check(threads.none { it.isAlive }) { "members did not start within $START_TIMEOUT" }
        failures.firstOrNull()?.let { throw IllegalStateException("a member failed to start", it) }
    }

    fun running(): List<Member> = members.filter { it.running }

    /**
     * Waits until every running member of [among] agrees on a leader of [name] that can take
     * writes.
     */
    fun awaitLeader(
        name: String = DEFAULT_DATABASE,
        among: () -> List<Member> = ::running,
    ): Member =
        await.pollDelay(Duration.ZERO) withPollInterval
            Duration.ofMillis(100) atMost
            AWAIT untilNotNull
            {
                val voters = among().filter { it.running }
                val statuses = voters.associateWith { it.status(name) }
                val leaders = statuses.values.map { it?.leader }.distinct()
                val leader = leaders.singleOrNull() ?: return@untilNotNull null
                voters.firstOrNull {
                    it.status(name)?.let { s -> s.role == Role.Leader && s.leader == leader } ==
                        true
                }
            }

    override fun close() {
        members.reversed().forEach {
            try {
                it.stop()
            } catch (e: Exception) {
                System.err.println("stopping $it failed: $e")
            }
        }
        raftLog.close()
    }

    companion object {
        const val DEFAULT_DATABASE = "neo4j"
        val AWAIT: Duration = Duration.ofSeconds(60)
        val START_TIMEOUT: Duration = Duration.ofMinutes(2)

        private val SEGMENT = Regex("""log_(?:inprogress_)?(\d+)(?:-\d+)?""")

        /** Settings that report every metric over Prometheus, on a port of each member's own. */
        val METRICS: Map<Setting<*>, Any> =
            mapOf(
                MetricsSettings.prometheusEnabled to true,
                MetricsSettings.prometheusEndpoint to SocketAddress("localhost", 0),
                MetricsSettings.filter to listOf(GlobbingPattern("*")),
            )

        /** Neo4j's SSL policy of the cluster scope. */
        val CLUSTER_TLS: SslPolicyConfig = SslPolicyConfig.forScope(SslPolicyScope.CLUSTER)

        /** Where Neo4j looks for the cluster policy's certificates, under a member's home. */
        const val CERTIFICATES = "certificates/cluster"
    }
}
