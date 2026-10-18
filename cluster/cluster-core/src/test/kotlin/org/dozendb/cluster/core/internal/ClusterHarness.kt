package org.dozendb.cluster.core.internal

import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.UUID
import kotlin.concurrent.thread
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilNotNull
import org.awaitility.kotlin.withPollInterval
import org.dozendb.cluster.api.GroupStatus
import org.dozendb.cluster.api.Role
import org.dozendb.cluster.api.testing.TestPorts
import org.dozendb.cluster.core.ClusterService
import org.dozendb.cluster.core.internal.durability.withSingleFsync
import org.neo4j.collection.Dependencies
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.configuration.connectors.HttpConnector
import org.neo4j.configuration.connectors.HttpsConnector
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory
import org.neo4j.graphdb.facade.GraphDatabaseDependencies
import org.neo4j.io.ByteUnit
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.kernel.impl.factory.DbmsInfo
import org.neo4j.kernel.internal.GraphDatabaseAPI

/**
 * Clustered DBMSes in this JVM, on loopback, for tests. [tuning] adjusts every member's consensus
 * configuration; [fileSystem] supplies the file system a member's DBMS runs on, below DozenDB's
 * single-fsync file system, each time it starts.
 */
internal class ClusterHarness(
    private val root: Path,
    size: Int,
    private val fileSystem: ((Member) -> FileSystemAbstraction)? = null,
    private val tuning: ConsensusTuning? = null,
) : AutoCloseable {
    inner class Member(val index: Int) {
        val home: Path = Files.createDirectories(root.resolve("member-$index"))
        val raftPort: Int = TestPorts.free()

        @Volatile
        var dbms: DatabaseManagementService? = null
            private set

        val running: Boolean
            get() = dbms != null

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
                            GraphDatabaseDependencies.newDependencies().dependencies(given),
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
            dbms?.shutdown()
            dbms = null
        }

        fun database(name: String = DEFAULT_DATABASE): GraphDatabaseAPI =
            checkNotNull(dbms) { "member $index is stopped" }.database(name) as GraphDatabaseAPI

        val cluster: ClusterService
            get() =
                database("system").dependencyResolver.resolveDependency(ClusterService::class.java)

        fun status(name: String = DEFAULT_DATABASE): GroupStatus? =
            cluster.status().firstOrNull { it.database.name == name }

        override fun toString(): String = "member-$index"

        private fun config(): Config =
            Config.newBuilder()
                .set(GraphDatabaseSettings.neo4j_home, home)
                .set(GraphDatabaseSettings.auth_enabled, false)
                .set(GraphDatabaseSettings.pagecache_memory, ByteUnit.mebiBytes(16))
                .set(BoltConnector.enabled, false)
                .set(HttpConnector.enabled, false)
                .set(HttpsConnector.enabled, false)
                .set(DozenDbSettings.clusterEnabled, true)
                .set(DozenDbSettings.listenAddress, SocketAddress("127.0.0.1", raftPort))
                .set(DozenDbSettings.advertisedAddress, SocketAddress("127.0.0.1", raftPort))
                .set(
                    DozenDbSettings.initialMembers,
                    members.map { SocketAddress("127.0.0.1", it.raftPort) },
                )
                .set(DozenDbSettings.electionTimeoutMin, Duration.ofMillis(300))
                .set(DozenDbSettings.electionTimeoutMax, Duration.ofMillis(600))
                .build()
    }

    val members: List<Member> = List(size) { Member(it) }

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

    /** Waits until every running member agrees on a leader of [name] that can take writes. */
    fun awaitLeader(name: String = DEFAULT_DATABASE): Member =
        await withPollInterval
            Duration.ofMillis(100) atMost
            AWAIT untilNotNull
            {
                val statuses = running().associateWith { it.status(name) }
                val leaders = statuses.values.map { it?.leader }.distinct()
                val leader = leaders.singleOrNull() ?: return@untilNotNull null
                running().firstOrNull {
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
    }

    companion object {
        const val DEFAULT_DATABASE = "neo4j"
        val AWAIT: Duration = Duration.ofSeconds(60)
        val START_TIMEOUT: Duration = Duration.ofMinutes(2)

        private val SEGMENT = Regex("""log_(?:inprogress_)?(\d+)(?:-\d+)?""")
    }
}
