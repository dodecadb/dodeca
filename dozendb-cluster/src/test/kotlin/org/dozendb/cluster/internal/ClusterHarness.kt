package org.dozendb.cluster.internal

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
import org.dozendb.cluster.ClusterService
import org.dozendb.cluster.internal.durability.withSingleFsync
import org.dozendb.consensus.GroupStatus
import org.dozendb.consensus.Role
import org.dozendb.consensus.testing.TestAuthority
import org.dozendb.consensus.testing.TestPorts
import org.neo4j.collection.Dependencies
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.configuration.connectors.ConnectorPortRegister
import org.neo4j.configuration.connectors.ConnectorType
import org.neo4j.configuration.connectors.HttpConnector
import org.neo4j.configuration.connectors.HttpsConnector
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.configuration.ssl.SslPolicyConfig
import org.neo4j.configuration.ssl.SslPolicyScope
import org.neo4j.consistency.ConsistencyCheckService
import org.neo4j.dbms.api.DatabaseManagementService
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
 * configuration; [fileSystem] supplies the file system a member's DBMS runs on, below DozenDB's
 * single-fsync file system, each time it starts. The logging of Ratis and DozenDB's consensus,
 * which the members' debug logs lack, goes to `raft.log` in [root].
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
    private val tuning: ConsensusTuning? = null,
) : AutoCloseable {
    inner class Member(val index: Int) {
        val home: Path = Files.createDirectories(root.resolve("member-$index"))
        val raftPort: Int = TestPorts.free()

        init {
            authority?.issue()?.writeTo(home.resolve(CERTIFICATES))
        }

        @Volatile
        var dbms: DatabaseManagementService? = null
            private set

        val running: Boolean
            get() = dbms != null

        /** Listeners the DBMS calls before DozenDB's own, from its next start on. */
        @Volatile var databaseEventListeners: List<DatabaseEventListener> = emptyList()

        /** Starts the DBMS; blocks until its databases run, which may wait for other members. */
        fun start() {
            check(dbms == null) { "member $index is running" }
            val given = Dependencies()
            tuning?.let { given.satisfyDependency(it) }
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

        fun status(name: String = DEFAULT_DATABASE): GroupStatus? =
            cluster.status().firstOrNull { it.database.name == name }

        override fun toString(): String = "member-$index"

        private fun config(): Config {
            val builder =
                Config.newBuilder()
                    .set(GraphDatabaseSettings.neo4j_home, home)
                    .set(GraphDatabaseSettings.auth_enabled, false)
                    .set(GraphDatabaseSettings.pagecache_memory, ByteUnit.mebiBytes(16))
                    .set(BoltConnector.enabled, false)
                    .set(BoltConnector.listen_address, SocketAddress("127.0.0.1", 0))
                    .set(
                        GraphDatabaseSettings.routing_listen_address,
                        SocketAddress("127.0.0.1", 0),
                    )
                    .set(HttpConnector.enabled, false)
                    .set(HttpsConnector.enabled, false)
                    .set(ClusterSettings.clusterEnabled, true)
                    .set(ClusterSettings.listenAddress, SocketAddress("127.0.0.1", raftPort))
                    .set(ClusterSettings.advertisedAddress, SocketAddress("127.0.0.1", raftPort))
                    .set(
                        ClusterSettings.initialMembers,
                        members.take(size).map { SocketAddress("127.0.0.1", it.raftPort) },
                    )
                    .set(ClusterSettings.electionTimeoutMin, Duration.ofMillis(300))
                    .set(ClusterSettings.electionTimeoutMax, Duration.ofMillis(600))
                    .set(CLUSTER_TLS.enabled, authority != null)
            @Suppress("UNCHECKED_CAST")
            settings.forEach { (setting, value) -> builder.set(setting as Setting<Any>, value) }
            return builder.build()
        }

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

        /** Neo4j's SSL policy of the cluster scope. */
        val CLUSTER_TLS: SslPolicyConfig = SslPolicyConfig.forScope(SslPolicyScope.CLUSTER)

        /** Where Neo4j looks for the cluster policy's certificates, under a member's home. */
        const val CERTIFICATES = "certificates/cluster"
    }
}
