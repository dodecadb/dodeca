package org.dozendb.cluster.ratis

import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.UUID
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilNotNull
import org.awaitility.kotlin.withPollInterval
import org.dozendb.cluster.api.DatabaseRef
import org.dozendb.cluster.api.MemberId
import org.dozendb.cluster.api.ReplicatedStateMachine
import org.dozendb.cluster.api.Role
import org.dozendb.cluster.api.fake.RecordingStateMachine

/** Real Ratis members in one JVM, on loopback ports, for tests. */
class RatisTestCluster(private val root: Path, size: Int) : AutoCloseable {
    val database = DatabaseRef("neo4j", UUID.randomUUID())

    inner class Node(val id: MemberId) {
        val directory: Path = Files.createDirectories(root.resolve(id.toString()))
        val machine = RecordingStateMachine()
        var port = 0
            private set

        var log: RatisConsensusLog? = null
            private set

        val consensus: RatisConsensusLog
            get() = checkNotNull(log) { "node $id is stopped" }

        val running: Boolean
            get() = log != null

        fun start() {
            check(log == null) { "node $id is running" }
            log =
                RatisConsensusLog(
                        RatisConsensusConfig(
                            memberId = id,
                            listenHost = "127.0.0.1",
                            listenPort = port,
                            advertisedAddress = null,
                            storageDirectory = directory,
                            electionTimeoutMin = ELECTION_TIMEOUT_MIN,
                            electionTimeoutMax = ELECTION_TIMEOUT_MAX,
                            metadata = { mapOf("neo4j.version" to "5.26.27") },
                        )
                    )
                    .also {
                        it.start()
                        port = it.self.raftAddress.port
                    }
        }

        fun stop() {
            log?.close()
            log = null
        }
    }

    val nodes: List<Node> = List(size) { Node(MemberId(UUID.randomUUID())) }

    /** Starts every node and creates the group with all of them as voting members. */
    fun form(stateMachine: (Node) -> ReplicatedStateMachine = { it.machine }) {
        nodes.forEach { it.start() }
        val members = nodes.map { it.consensus.self }
        nodes.forEach { it.consensus.startGroup(database, stateMachine(it), members) }
    }

    fun running(): List<Node> = nodes.filter { it.running }

    fun node(id: MemberId): Node = nodes.single { it.id == id }

    /**
     * Waits until every node taking part in the group sees the same leader, which considers itself
     * leader.
     */
    fun awaitLeader(): Node =
        await withPollInterval
            Duration.ofMillis(50) atMost
            AWAIT untilNotNull
            {
                val statuses =
                    running()
                        .map { it.consensus.status(database) }
                        .filter { it.role != Role.Inactive }
                val leader = statuses.map { it.leader }.distinct().singleOrNull()
                leader
                    ?.takeIf {
                        node(it).running && node(it).consensus.role(database) == Role.Leader
                    }
                    ?.let(::node)
            }

    override fun close() {
        nodes.forEach { it.stop() }
    }

    companion object {
        val ELECTION_TIMEOUT_MIN: Duration = Duration.ofMillis(300)
        val ELECTION_TIMEOUT_MAX: Duration = Duration.ofMillis(600)
        val AWAIT: Duration = Duration.ofSeconds(20)
    }
}
