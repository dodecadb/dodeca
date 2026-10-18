package org.dozendb.cluster.core.internal

import java.net.ServerSocket
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import kotlin.concurrent.thread
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilNotNull
import org.awaitility.kotlin.withPollInterval
import org.dozendb.cluster.api.GroupStatus
import org.dozendb.cluster.api.Role
import org.dozendb.cluster.core.ClusterService
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
import org.neo4j.kernel.impl.factory.DbmsInfo
import org.neo4j.kernel.internal.GraphDatabaseAPI

/** Clustered DBMSes in this JVM, on loopback, for tests. */
class ClusterHarness(private val root: Path, size: Int) : AutoCloseable {
    inner class Member(val index: Int) {
        val home: Path = Files.createDirectories(root.resolve("member-$index"))
        val raftPort: Int = freePort()

        @Volatile
        var dbms: DatabaseManagementService? = null
            private set

        val running: Boolean
            get() = dbms != null

        /** Starts the DBMS; blocks until its databases run, which may wait for other members. */
        fun start() {
            check(dbms == null) { "member $index is running" }
            dbms =
                DatabaseManagementServiceFactory(DbmsInfo.ENTERPRISE, ::ClusterEditionModule)
                    .build(config(), false, GraphDatabaseDependencies.newDependencies())
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

        private fun freePort(): Int = ServerSocket(0).use { it.localPort }
    }
}
